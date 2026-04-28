// ============================================================
//  SMART ENERGY METER — MASTER NODE  (merged final)
//  ESP32 + PZEM004Tv30 + 24C32 EEPROM + DS3231 RTC + LCD 20x4
//  ESP-NOW multi-module + MQTT over WiFi
//
//  STORAGE SPLIT:
//    24C32 EEPROM  — energy/balance/tamper/theft/relay/fault/tariff
//    NVS Preferences — paired modules list, billing seq, relay lock
//
//  EEPROM MAP (2-byte addressing, 4 bytes each):
//    0x00  balance          0x04  cumulativeKwh
//    0x08  tamperCount      0x0C  theftFlag (sticky)
//    0x10  relayState       0x14  faultCount
//    0x18  tariff           0x1C  lastPzemEnergy
//
//  MQTT TOPICS (subscribe):
//    meter/<id>/command  — relay_on/off, lock_relay, unlock_relay,
//                          scan_modules, pair_module, unpair_module,
//                          module_relay_control, cleartheft, set_tariff
//  MQTT TOPICS (publish):
//    meter/<id>/telemetry    meter/<id>/heartbeat
//    meter/<id>/billing_sync meter/<id>/fault
//    meter/<id>/scan         meter/<id>/pair_ack
//    meter/<id>/ack          meter/<id>/stats
// ============================================================

#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <esp_now.h>
#include <esp_wifi.h>
#include <Preferences.h>
#include <PZEM004Tv30.h>
#include <Wire.h>
#include <LiquidCrystal_I2C.h>
#include <RTClib.h>

// forward declaration
void mqttCallback(char* topic, byte* payload, unsigned int length);

// ── Firmware identity ────────────────────────────────────────
const char* FIRMWARE_VERSION = "2.0.0";
const char* DEPLOY_DATE      = "2025-03-01";

// ── WiFi ─────────────────────────────────────────────────────
const char* ssid     = "boom";
const char* password = "1231231230";

// ── MQTT ─────────────────────────────────────────────────────
const char* mqttServer = "20.39.192.14";
const int   mqttPort   = 1883;

// ── Device credentials ───────────────────────────────────────
const int   deviceId  = 44188662;
const char* secretKey = "6vM3ihdppGGtwf54Yu";   // MQTT password

// ── MQTT topics ──────────────────────────────────────────────
// (built in setup() after deviceId is known)
String topicTelemetry;
String topicHeartbeat;
String topicCommand;
String topicBillingSync;
String topicFault;
String topicScan;
String topicPairAck;
String topicAck;
String topicStats;

// ── EEPROM (24C32, I2C addr 0x50, 2-byte page addressing) ────
#define EEPROM_ADDR      0x50
#define EE_BALANCE       0x00
#define EE_ENERGY        0x04
#define EE_TAMPER        0x08
#define EE_THEFT         0x0C
#define EE_RELAY         0x10
#define EE_FAULT         0x14
#define EE_TARIFF        0x18
#define EE_LAST_PZEM     0x1C

// ── NVS namespaces ───────────────────────────────────────────
Preferences nvsBilling;   // "billing"  — seq, periodStart, relayLock
Preferences nvsModules;   // "modules"  — paired module list

// ── PINS ─────────────────────────────────────────────────────
#define LED_ENERGY  18
#define LED_STATUS  19
#define LED_FAULT   23
#define RELAY_PIN   26
#define BUZZER_PIN  27
#define TAMPER_PIN  34
#define CT_SENSOR   36
#define BTN_MENU    32
#define BTN_SELECT  33
#define BTN_RELAY   35

// ── Peripherals ──────────────────────────────────────────────
PZEM004Tv30       pzem(Serial2, 16, 17);
LiquidCrystal_I2C lcd(0x27, 20, 4);
RTC_DS3231        rtc;
WiFiClient        espClient;
PubSubClient      mqttClient(espClient);

// ── Live sensor readings ─────────────────────────────────────
float voltage        = 0;
float current        = 0;
float power          = 0;
float energy         = 0;   // mirrors cumulativeKwh
float pf             = 0;
float frequency      = 0;
float leakageCurrent = 0;

// ── EEPROM-backed state ───────────────────────────────────────
float balance        = 0;
float cumulativeKwh  = 0;   // lifetime kWh, never resets
float lastPzemEnergy = 0;
float tamperCount    = 0;
float theftFlag      = 0;
float faultCount     = 0;
float tariff         = 6.0; // Rs/kWh

// ── Runtime state ─────────────────────────────────────────────
bool relayState         = true;
bool faultActive        = false;
bool theftDetected      = false;
bool relayLockedByServer= false;  // server billing lock

// ── Billing (NVS-backed) ─────────────────────────────────────
unsigned long billingSeqNo       = 0;
unsigned long periodStartSec     = 0;   // unix seconds
float         periodEnergyWh     = 0;   // Wh since last sync
unsigned long lastEnergyUpdateMs = 0;

// ── Timers ───────────────────────────────────────────────────
unsigned long lastBillingSync  = 0;
unsigned long lastTelemetry    = 0;
unsigned long lastHeartbeat    = 0;
unsigned long lastBillingMs    = 0;
unsigned long lastPublishMs    = 0;
unsigned long lastPageMs       = 0;

const unsigned long BILLING_LOCAL_INTERVAL = 10000UL;   // EEPROM write
const unsigned long BILLING_SYNC_INTERVAL  = 30000UL;   // server sync
const unsigned long TELEMETRY_INTERVAL     = 5000UL;
const unsigned long HEARTBEAT_INTERVAL     = 2000UL;

// ── LCD ──────────────────────────────────────────────────────
int page     = 0;
int lastPage = -1;

// ── Multi-module support ──────────────────────────────────────
#define MAX_MODULES 10

struct PairedModule {
    char    moduleId[16];
    uint8_t macAddr[6];
    int     capacity;
    bool    isPaired;
};

PairedModule pairedModules[MAX_MODULES];
int pairedCount = 0;

struct ModuleDataCache {
    char          moduleId[16];
    int           unitIndex;
    float         voltage;
    float         current;
    float         power;
    bool          relayState;
    char          health[8];
    unsigned long lastUpdate;
};

// MAX_MODULES * 4 units each = 40 cache slots
ModuleDataCache moduleCache[MAX_MODULES * 4];
int moduleCacheCount = 0;

// ── ESP-NOW structs ───────────────────────────────────────────
typedef struct {
    char type[16];
} ScanRequest;

typedef struct {
    char type[16];
    char moduleId[16];
    int  capacity;
} ScanResponse;

typedef struct {
    char type[16];
    char moduleId[16];
    char secret[32];
} PairRequest;

typedef struct {
    char type[16];
    bool success;
    char moduleId[16];
} PairAck;

typedef struct {
    char type[16];
    char moduleId[16];
} UnpairCommand;

typedef struct {
    char type[16];
    char moduleId[16];
    int  unitIndex;
    float voltage;
    float current;
    float power;
    bool  relayState;
    char  health[8];
} ModuleTelemetry;

typedef struct {
    char type[16];
    char moduleId[16];
    int  unitNumber;
    char faultType[32];
    char severity[16];
    char description[128];
    float measuredValue;
    float thresholdValue;
    char  unit[16];
} ModuleFault;

typedef struct {
    char type[16];    // "RELAY_CTRL"
    char moduleId[16];
    int  unitNumber;
    bool state;
} ModuleRelayControl;

typedef struct {
    char type[16];    // "RELAY_ACK"
    char moduleId[16];
    int  unitNumber;
    bool success;
    bool relayState;
} ModuleRelayAck;

// ============================================================
//  EEPROM HELPERS  (24C32 — 2-byte addressing)
// ============================================================
void writeFloat(int addr, float value) {
    byte* data = (byte*)(void*)&value;
    Wire.beginTransmission(EEPROM_ADDR);
    Wire.write((byte)(addr >> 8));
    Wire.write((byte)(addr & 0xFF));
    for (int i = 0; i < 4; i++) Wire.write(data[i]);
    Wire.endTransmission();
    delay(10);
}

float readFloat(int addr) {
    float value = 0.0f;
    byte* data  = (byte*)(void*)&value;
    Wire.beginTransmission(EEPROM_ADDR);
    Wire.write((byte)(addr >> 8));
    Wire.write((byte)(addr & 0xFF));
    Wire.endTransmission();
    Wire.requestFrom((uint8_t)EEPROM_ADDR, (uint8_t)4);
    for (int i = 0; i < 4; i++) {
        if (Wire.available()) data[i] = Wire.read();
    }
    return value;
}

bool eeValid(float v, float lo, float hi) {
    return (!isnan(v) && !isinf(v) && v >= lo && v <= hi);
}

void loadEEPROM() {
    float v;
    v = readFloat(EE_BALANCE);   balance       = eeValid(v, 0, 1e6f)  ? v : 0.0f;
    v = readFloat(EE_ENERGY);    cumulativeKwh = eeValid(v, 0, 1e7f)  ? v : 0.0f;
    v = readFloat(EE_TAMPER);    tamperCount   = eeValid(v, 0, 1e6f)  ? v : 0.0f;
    v = readFloat(EE_THEFT);     theftFlag     = eeValid(v, 0, 1.0f)  ? v : 0.0f;
    v = readFloat(EE_RELAY);     relayState    = eeValid(v, 0, 1.0f)  ? (v > 0.5f) : true;
    v = readFloat(EE_FAULT);     faultCount    = eeValid(v, 0, 1e6f)  ? v : 0.0f;
    v = readFloat(EE_TARIFF);    tariff        = eeValid(v, 0.01f, 1000.0f) ? v : 6.0f;
    v = readFloat(EE_LAST_PZEM); lastPzemEnergy= eeValid(v, 0, 1e7f) ? v : 0.0f;
    energy = cumulativeKwh;
    if (theftFlag > 0.5f) { theftDetected = true; digitalWrite(LED_FAULT, HIGH); }
    Serial.printf("[EEPROM] bal=%.2f kWh=%.4f tamper=%.0f relay=%d tariff=%.2f\n",
                  balance, cumulativeKwh, tamperCount, relayState, tariff);
}

// ── NVS billing state ─────────────────────────────────────────
void loadBillingNVS() {
    nvsBilling.begin("billing", false);
    billingSeqNo        = nvsBilling.getULong("seqNo",       0);
    periodStartSec      = nvsBilling.getULong("periodStart", 0);
    relayLockedByServer = nvsBilling.getBool ("relayLock",   false);
    nvsBilling.end();
    if (relayLockedByServer) {
        relayState = false;
        Serial.println("[NVS] Relay locked by server (insufficient balance)");
    }
    Serial.printf("[NVS] billing seqNo=%lu locked=%d\n", billingSeqNo, relayLockedByServer);
}

void saveBillingNVS() {
    nvsBilling.begin("billing", false);
    nvsBilling.putULong("seqNo",       billingSeqNo);
    nvsBilling.putULong("periodStart", periodStartSec);
    nvsBilling.putBool ("relayLock",   relayLockedByServer);
    nvsBilling.end();
}

// ── NVS module pairing ────────────────────────────────────────
void loadPairedModules() {
    nvsModules.begin("modules", false);
    pairedCount = nvsModules.getInt("count", 0);
    for (int i = 0; i < pairedCount && i < MAX_MODULES; i++) {
        String key = "mod" + String(i);
        if (nvsModules.getBytesLength(key.c_str()) == sizeof(PairedModule))
            nvsModules.getBytes(key.c_str(), &pairedModules[i], sizeof(PairedModule));
    }
    nvsModules.end();
    Serial.printf("[NVS] %d paired modules loaded\n", pairedCount);
}

void savePairedModules() {
    nvsModules.begin("modules", false);
    nvsModules.putInt("count", pairedCount);
    for (int i = 0; i < pairedCount; i++) {
        String key = "mod" + String(i);
        nvsModules.putBytes(key.c_str(), &pairedModules[i], sizeof(PairedModule));
    }
    nvsModules.end();
}

// ── Relay helper — always persists ───────────────────────────
void setRelay(bool on) {
    relayState = on;
    digitalWrite(RELAY_PIN, on ? HIGH : LOW);
    writeFloat(EE_RELAY, on ? 1.0f : 0.0f);
    Serial.printf("[RELAY] %s\n", on ? "ON" : "OFF");
}

// ============================================================
//  WiFi  — mode set BEFORE begin() for ESP-NOW compatibility
// ============================================================
void connectWiFi() {
    WiFi.mode(WIFI_STA);          // must be before begin()
    WiFi.begin(ssid, password);
    int tries = 0;
    while (WiFi.status() != WL_CONNECTED && tries < 40) { delay(500); tries++; }
    if (WiFi.status() == WL_CONNECTED) {
        digitalWrite(LED_STATUS, HIGH);
        Serial.printf("[WiFi] Connected  IP:%s  Ch:%d\n",
                      WiFi.localIP().toString().c_str(), WiFi.channel());
    } else {
        Serial.println("[WiFi] FAILED — running offline");
    }
}

// ============================================================
//  MQTT  — with authentication + 512-byte buffer
// ============================================================
void connectMQTT() {
    mqttClient.setServer(mqttServer, mqttPort);
    mqttClient.setCallback(mqttCallback);
    mqttClient.setBufferSize(512);
    int tries = 0;
    while (!mqttClient.connected() && tries < 5) {
        // connect(clientId, username, password)
        String cid = "meter_" + String(deviceId);
        if (mqttClient.connect(cid.c_str(),
                               String(deviceId).c_str(),
                               secretKey)) {
            mqttClient.subscribe(topicCommand.c_str());
            Serial.printf("[MQTT] Connected  subscribed:%s\n", topicCommand.c_str());
        } else {
            Serial.printf("[MQTT] Fail rc=%d\n", mqttClient.state());
            delay(1000);
        }
        tries++;
    }
}

// ============================================================
//  ENERGY READ  — real PZEM004T, cumulative accumulator
// ============================================================
void readEnergy() {
    float v   = pzem.voltage();
    float c   = pzem.current();
    float p   = pzem.power();
    float e   = pzem.energy();
    float f   = pzem.frequency();
    float pf_ = pzem.pf();

    if (!isnan(v))   voltage   = v;
    if (!isnan(c))   current   = c;
    if (!isnan(p))   power     = p;
    if (!isnan(f))   frequency = f;
    if (!isnan(pf_)) pf        = pf_;

    if (!isnan(e)) {
        if (e >= lastPzemEnergy) {
            float delta = e - lastPzemEnergy;
            if (delta > 0.0f && delta < 100.0f) {
                cumulativeKwh += delta;
                // Accumulate Wh for billing sync period
                periodEnergyWh += delta * 1000.0f;
            }
        }
        lastPzemEnergy = e;
        energy = cumulativeKwh;
    }
}

// ============================================================
//  LEAKAGE
// ============================================================
void readLeakage() {
    int   adc = analogRead(CT_SENSOR);
    float v   = adc * (3.3f / 4095.0f);
    leakageCurrent = v * 10.0f;
}

// ============================================================
//  FAULT CHECK
// ============================================================
void checkFaults() {
    bool prevFault = faultActive;
    faultActive = false;

    if (leakageCurrent > 0.025f) {
        faultActive = true;
        if (relayState) { setRelay(false); }
        digitalWrite(LED_FAULT, HIGH);
        tone(BUZZER_PIN, 1000);
    } else {
        noTone(BUZZER_PIN);
    }

    if (digitalRead(TAMPER_PIN) == LOW) {
        faultActive = true;
        if (relayState) { setRelay(false); }
        tamperCount++;
        writeFloat(EE_TAMPER, tamperCount);
        Serial.printf("[TAMPER] count=%.0f\n", tamperCount);
    }

    if (faultActive && !prevFault) {
        faultCount++;
        writeFloat(EE_FAULT, faultCount);
        // Publish fault event to MQTT
        char buf[256];
        snprintf(buf, sizeof(buf),
            "{\"source\":\"mainMeter\",\"faultType\":\"%s\","
            "\"severity\":\"critical\",\"measuredValue\":%.4f}",
            (leakageCurrent > 0.025f) ? "earthLeakage" : "tamper",
            leakageCurrent);
        mqttClient.publish(topicFault.c_str(), buf);
    }

    if (!faultActive && !theftDetected) digitalWrite(LED_FAULT, LOW);
}

// ============================================================
//  LOCAL BILLING  — runs every 10 s, writes EEPROM
// ============================================================
void updateBillingLocal() {
    if (!relayState || relayLockedByServer) return;

    static float savedKwh = -1.0f;
    if (savedKwh < 0.0f) savedKwh = readFloat(EE_ENERGY);

    float delta = cumulativeKwh - savedKwh;
    if (delta < 0.001f) return;

    float cost = delta * tariff;
    balance  -= cost;
    savedKwh  = cumulativeKwh;

    writeFloat(EE_BALANCE,   balance);
    writeFloat(EE_ENERGY,    cumulativeKwh);
    writeFloat(EE_LAST_PZEM, lastPzemEnergy);

    Serial.printf("[BILLING] delta=%.4fkWh cost=Rs%.4f balance=Rs%.2f\n",
                  delta, cost, balance);

    if (balance <= 0.0f) {
        balance = 0.0f;
        writeFloat(EE_BALANCE, 0.0f);
        setRelay(false);
        Serial.println("[BILLING] Balance zero — relay OFF");
    }
}

// ============================================================
//  BILLING SYNC  — sends Wh consumed to server every 30 s
//  Server uses this to manage balance and may send lock/unlock
//  Energy Wh is NOT zeroed until publish confirmed to avoid loss
// ============================================================
void publishBillingSync() {
    if (!mqttClient.connected()) return;

    DateTime now = rtc.now();
    if (periodStartSec == 0) periodStartSec = now.unixtime();

    char buf[256];
    snprintf(buf, sizeof(buf),
        "{\"deviceId\":%d,\"seqNo\":%lu,"
        "\"periodStart\":%lu,\"periodEnd\":%lu,"
        "\"energyWh\":%.1f,\"balance\":%.2f,"
        "\"meterState\":\"%s\",\"relayState\":%s,"
        "\"faultActive\":%s,\"firmware\":\"%s\"}",
        deviceId, billingSeqNo,
        periodStartSec, (unsigned long)now.unixtime(),
        periodEnergyWh, balance,
        relayLockedByServer ? "LOCKED" : "ACTIVE",
        relayState    ? "true" : "false",
        faultActive   ? "true" : "false",
        FIRMWARE_VERSION);

    // Only reset period counters after confirmed publish
    if (mqttClient.publish(topicBillingSync.c_str(), buf, true)) {
        periodEnergyWh  = 0.0f;
        periodStartSec  = now.unixtime();
        billingSeqNo++;
        saveBillingNVS();
        Serial.printf("[BILLING SYNC] seqNo=%lu sent\n", billingSeqNo);
    } else {
        Serial.println("[BILLING SYNC] publish failed — Wh kept for next attempt");
    }
}

// ============================================================
//  THEFT DETECTION
// ============================================================
void checkEnergyTheft() {
    if (pairedCount == 0) return;  // need at least one slave
    float slaveCurrent = 0;
    // Use first paired module's current from cache as reference
    for (int i = 0; i < moduleCacheCount; i++) {
        if (millis() - moduleCache[i].lastUpdate < 10000UL) {
            slaveCurrent = moduleCache[i].current;
            break;
        }
    }
    if (slaveCurrent <= 0.0f) return;

    bool prev = theftDetected;
    theftDetected = (fabsf(current - slaveCurrent) > (0.20f * current));

    if (theftDetected && !prev) {
        theftFlag = 1.0f;
        writeFloat(EE_THEFT, 1.0f);
        digitalWrite(LED_FAULT, HIGH);
        char buf[128];
        snprintf(buf, sizeof(buf),
            "{\"source\":\"mainMeter\",\"faultType\":\"energyTheft\","
            "\"severity\":\"critical\","
            "\"measuredValue\":%.3f,\"thresholdValue\":%.3f}",
            current, slaveCurrent);
        mqttClient.publish(topicFault.c_str(), buf);
        Serial.println("[THEFT] Detected — MQTT fault published");
    }
}

// ============================================================
//  PUBLISH COMMAND ACK
// ============================================================
void publishAck(const char* command, const char* status) {
    char buf[128];
    snprintf(buf, sizeof(buf),
        "{\"command\":\"%s\",\"status\":\"%s\","
        "\"relayState\":%s,\"relayLocked\":%s}",
        command, status,
        relayState          ? "true" : "false",
        relayLockedByServer ? "true" : "false");
    mqttClient.publish(topicAck.c_str(), buf);
}

// ============================================================
//  PUBLISH FAULT (for module faults forwarded from ESP-NOW)
// ============================================================
void publishModuleFault(ModuleFault& f) {
    char buf[512];
    snprintf(buf, sizeof(buf),
        "{\"source\":\"module\",\"moduleId\":\"%s\","
        "\"unitNumber\":%d,\"faultType\":\"%s\","
        "\"severity\":\"%s\",\"description\":\"%s\","
        "\"measuredValue\":%.3f,\"thresholdValue\":%.3f,\"unit\":\"%s\"}",
        f.moduleId, f.unitNumber, f.faultType,
        f.severity, f.description,
        f.measuredValue, f.thresholdValue, f.unit);
    mqttClient.publish(topicFault.c_str(), buf);
}

// ============================================================
//  MQTT CALLBACK
// ============================================================
void mqttCallback(char* topic, byte* payload, unsigned int length) {
    if (length == 0) return;
    payload[length] = '\0';
    StaticJsonDocument<256> doc;
    if (deserializeJson(doc, payload, length)) return;
    String cmd = doc["command"] | "";

    // ── Main meter relay ───────────────────────────────────
    if (cmd == "relay_on") {
        if (relayLockedByServer) { publishAck("relay_on", "LOCKED"); return; }
        if (!faultActive)        { setRelay(true);  publishAck("relay_on",  "SUCCESS"); }
    }
    else if (cmd == "relay_off") {
        setRelay(false); publishAck("relay_off", "SUCCESS");
    }

    // ── Server billing lock / unlock ───────────────────────
    else if (cmd == "lock_relay") {
        relayLockedByServer = true;
        setRelay(false);
        saveBillingNVS();
        publishAck("lock_relay", "SUCCESS");
        Serial.println("[BILLING] Relay locked by server");
    }
    else if (cmd == "unlock_relay") {
        relayLockedByServer = false;
        saveBillingNVS();
        if (!faultActive) setRelay(true);
        publishAck("unlock_relay", "SUCCESS");
        Serial.println("[BILLING] Relay unlocked by server");
    }

    // ── Tariff update ──────────────────────────────────────
    else if (cmd == "set_tariff") {
        float newRate = doc["tariff"] | 0.0f;
        if (newRate > 0.0f) {
            tariff = newRate;
            writeFloat(EE_TARIFF, tariff);
            publishAck("set_tariff", "SUCCESS");
            Serial.printf("[TARIFF] Updated to Rs%.2f/kWh\n", tariff);
        }
    }

    // ── Clear theft flag ───────────────────────────────────
    else if (cmd == "cleartheft") {
        theftDetected = false;
        theftFlag     = 0.0f;
        writeFloat(EE_THEFT, 0.0f);
        if (!faultActive) digitalWrite(LED_FAULT, LOW);
        publishAck("cleartheft", "SUCCESS");
        Serial.println("[THEFT] Cleared by operator");
    }

    // ── Module scan ────────────────────────────────────────
    else if (cmd == "scan_modules") {
        ScanRequest req;
        memset(&req, 0, sizeof(req));
        strcpy(req.type, "SCAN_REQ");
        uint8_t broadcast[6] = {0xFF,0xFF,0xFF,0xFF,0xFF,0xFF};
        esp_now_send(broadcast, (uint8_t*)&req, sizeof(req));
        Serial.println("[SCAN] Broadcast sent");
    }

    // ── Module pairing ─────────────────────────────────────
    else if (cmd == "pair_module") {
        String mId  = doc["moduleId"] | "";
        String sec  = doc["secret"]   | "";
        if (mId.length() == 0 || sec.length() == 0) return;
        PairRequest pr;
        memset(&pr, 0, sizeof(pr));
        strcpy(pr.type, "PAIR_REQ");
        strncpy(pr.moduleId, mId.c_str(), 15);
        strncpy(pr.secret,   sec.c_str(), 31);
        uint8_t broadcast[6] = {0xFF,0xFF,0xFF,0xFF,0xFF,0xFF};
        esp_now_send(broadcast, (uint8_t*)&pr, sizeof(pr));
        Serial.printf("[PAIR] Request sent for %s\n", mId.c_str());
    }

    // ── Module unpairing ───────────────────────────────────
    else if (cmd == "unpair_module") {
        String mId = doc["moduleId"] | "";
        if (mId.length() == 0) return;
        for (int i = 0; i < pairedCount; i++) {
            if (strcmp(pairedModules[i].moduleId, mId.c_str()) == 0) {
                UnpairCommand uc;
                memset(&uc, 0, sizeof(uc));
                strcpy(uc.type, "UNPAIR");
                strncpy(uc.moduleId, mId.c_str(), 15);
                esp_now_send(pairedModules[i].macAddr, (uint8_t*)&uc, sizeof(uc));
                esp_now_del_peer(pairedModules[i].macAddr);
                for (int j = i; j < pairedCount - 1; j++)
                    pairedModules[j] = pairedModules[j+1];
                pairedCount--;
                savePairedModules();
                publishAck("unpair_module", "SUCCESS");
                Serial.printf("[UNPAIR] %s removed\n", mId.c_str());
                return;
            }
        }
        publishAck("unpair_module", "NOT_FOUND");
    }

    // ── Per-unit module relay control ──────────────────────
    else if (cmd == "module_relay_control") {
        String mId    = doc["moduleId"]   | "";
        int    unit   = doc["unitNumber"] | -1;
        bool   state  = doc["state"]      | false;
        if (mId.length() == 0 || unit < 0) return;
        for (int i = 0; i < pairedCount; i++) {
            if (strcmp(pairedModules[i].moduleId, mId.c_str()) == 0) {
                ModuleRelayControl rc;
                memset(&rc, 0, sizeof(rc));
                strcpy(rc.type, "RELAY_CTRL");
                strncpy(rc.moduleId, mId.c_str(), 15);
                rc.unitNumber = unit;
                rc.state      = state;
                esp_now_send(pairedModules[i].macAddr, (uint8_t*)&rc, sizeof(rc));
                Serial.printf("[MODULE RELAY] %s unit%d -> %s\n",
                              mId.c_str(), unit, state ? "ON" : "OFF");
                return;
            }
        }
        Serial.printf("[MODULE RELAY] Module %s not found\n", mId.c_str());
    }
}

// ============================================================
//  ESP-NOW RECEIVE
// ============================================================
void onESPNowRecv(const esp_now_recv_info_t* info,
                  const uint8_t* data, int len) {
    char msgType[16];
    if (len < 16) return;
    memcpy(msgType, data, 16);

    // ── Scan response ──────────────────────────────────────
    if (strcmp(msgType, "SCAN_RESP") == 0) {
        ScanResponse resp;
        memcpy(&resp, data, sizeof(resp));
        char macStr[18];
        snprintf(macStr, sizeof(macStr), "%02X:%02X:%02X:%02X:%02X:%02X",
                 info->src_addr[0], info->src_addr[1], info->src_addr[2],
                 info->src_addr[3], info->src_addr[4], info->src_addr[5]);
        char buf[128];
        snprintf(buf, sizeof(buf),
                 "{\"moduleId\":\"%s\",\"capacity\":%d,\"mac\":\"%s\"}",
                 resp.moduleId, resp.capacity, macStr);
        mqttClient.publish(topicScan.c_str(), buf);
        Serial.printf("[SCAN] Found %s  MAC:%s\n", resp.moduleId, macStr);
    }

    // ── Pair acknowledgement ───────────────────────────────
    else if (strcmp(msgType, "PAIR_ACK") == 0) {
        PairAck ack;
        memcpy(&ack, data, sizeof(ack));
        if (ack.success && pairedCount < MAX_MODULES) {
            strncpy(pairedModules[pairedCount].moduleId, ack.moduleId, 15);
            memcpy(pairedModules[pairedCount].macAddr, info->src_addr, 6);
            pairedModules[pairedCount].capacity = 0;
            pairedModules[pairedCount].isPaired = true;

            esp_now_peer_info_t peer;
            memset(&peer, 0, sizeof(peer));
            memcpy(peer.peer_addr, info->src_addr, 6);
            peer.channel = WiFi.channel();
            peer.encrypt = false;
            peer.ifidx   = WIFI_IF_STA;
            if (esp_now_add_peer(&peer) == ESP_OK) {
                pairedCount++;
                savePairedModules();
                Serial.printf("[PAIR] %s paired OK\n", ack.moduleId);
            }
        }
        char buf[96];
        snprintf(buf, sizeof(buf),
                 "{\"moduleId\":\"%s\",\"success\":%s}",
                 ack.moduleId, ack.success ? "true" : "false");
        mqttClient.publish(topicPairAck.c_str(), buf);
    }

    // ── Module telemetry ───────────────────────────────────
    else if (strcmp(msgType, "TELEMETRY") == 0) {
        ModuleTelemetry t;
        memcpy(&t, data, sizeof(t));

        // Update or insert cache entry
        bool found = false;
        for (int i = 0; i < moduleCacheCount; i++) {
            if (strcmp(moduleCache[i].moduleId, t.moduleId) == 0 &&
                moduleCache[i].unitIndex == t.unitIndex) {
                moduleCache[i].voltage    = t.voltage;
                moduleCache[i].current    = t.current;
                moduleCache[i].power      = t.power;
                moduleCache[i].relayState = t.relayState;
                strncpy(moduleCache[i].health, t.health, 7);
                moduleCache[i].lastUpdate = millis();
                found = true;
                break;
            }
        }
        if (!found && moduleCacheCount < (MAX_MODULES * 4)) {
            strncpy(moduleCache[moduleCacheCount].moduleId, t.moduleId, 15);
            moduleCache[moduleCacheCount].unitIndex  = t.unitIndex;
            moduleCache[moduleCacheCount].voltage    = t.voltage;
            moduleCache[moduleCacheCount].current    = t.current;
            moduleCache[moduleCacheCount].power      = t.power;
            moduleCache[moduleCacheCount].relayState = t.relayState;
            strncpy(moduleCache[moduleCacheCount].health, t.health, 7);
            moduleCache[moduleCacheCount].lastUpdate = millis();
            moduleCacheCount++;
        }
    }

    // ── Module fault forwarded to server ───────────────────
    else if (strcmp(msgType, "FAULT") == 0) {
        ModuleFault f;
        memcpy(&f, data, sizeof(f));
        publishModuleFault(f);
        Serial.printf("[MODULE FAULT] %s unit%d %s\n",
                      f.moduleId, f.unitNumber, f.faultType);
    }

    // ── Module relay ACK ───────────────────────────────────
    else if (strcmp(msgType, "RELAY_ACK") == 0) {
        ModuleRelayAck ack;
        memcpy(&ack, data, sizeof(ack));
        char buf[192];
        snprintf(buf, sizeof(buf),
            "{\"command\":\"module_relay_ack\","
            "\"moduleId\":\"%s\",\"unitNumber\":%d,"
            "\"status\":\"%s\",\"relayState\":%s}",
            ack.moduleId, ack.unitNumber,
            ack.success ? "SUCCESS" : "FAILED",
            ack.relayState ? "true" : "false");
        mqttClient.publish(topicAck.c_str(), buf);
        Serial.printf("[MODULE RELAY ACK] %s unit%d %s\n",
                      ack.moduleId, ack.unitNumber,
                      ack.success ? "OK" : "FAIL");
    }
}

// ============================================================
//  PUBLISH HEARTBEAT
// ============================================================
void publishHeartbeat() {
    if (!mqttClient.connected()) return;
    char buf[96];
    snprintf(buf, sizeof(buf),
             "{\"deviceId\":%d,\"uptime\":%lu,\"firmware\":\"%s\"}",
             deviceId, millis()/1000, FIRMWARE_VERSION);
    mqttClient.publish(topicHeartbeat.c_str(), buf);
}

// ============================================================
//  PUBLISH FULL TELEMETRY
// ============================================================
void publishTelemetry() {
    if (!mqttClient.connected()) return;
    DateTime now = rtc.now();
    char ts[20];
    snprintf(ts, sizeof(ts), "%04d-%02d-%02dT%02d:%02d:%02d",
             now.year(), now.month(), now.day(),
             now.hour(), now.minute(), now.second());

    // Build main meter JSON
    char mainBuf[512];
    snprintf(mainBuf, sizeof(mainBuf),
        "{\"deviceId\":%d,\"ts\":\"%s\",\"unixtime\":%lu,"
        "\"firmware\":\"%s\","
        "\"voltage\":%.2f,\"current\":%.3f,\"power\":%.2f,"
        "\"energy\":%.4f,\"frequency\":%.2f,\"pf\":%.3f,"
        "\"balance\":%.2f,\"tariff\":%.2f,"
        "\"relay\":%s,\"relayLocked\":%s,"
        "\"fault\":%s,\"theft\":%s,"
        "\"tamperCount\":%d,\"faultCount\":%d,"
        "\"leakage\":%.4f,\"pairedModules\":%d}",
        deviceId, ts, (unsigned long)now.unixtime(),
        FIRMWARE_VERSION,
        voltage, current, power,
        cumulativeKwh, frequency, pf,
        balance, tariff,
        relayState          ? "true" : "false",
        relayLockedByServer ? "true" : "false",
        faultActive         ? "true" : "false",
        theftDetected       ? "true" : "false",
        (int)tamperCount, (int)faultCount,
        leakageCurrent, pairedCount);

    mqttClient.publish(topicTelemetry.c_str(), mainBuf, true);

    // Publish each module's cached data separately under its own topic
    for (int i = 0; i < moduleCacheCount; i++) {
        if (millis() - moduleCache[i].lastUpdate > 10000UL) continue;
        char mBuf[256];
        snprintf(mBuf, sizeof(mBuf),
            "{\"moduleId\":\"%s\",\"unitIndex\":%d,"
            "\"voltage\":%.2f,\"current\":%.3f,\"power\":%.2f,"
            "\"relayState\":%s,\"health\":\"%s\",\"ts\":%lu}",
            moduleCache[i].moduleId, moduleCache[i].unitIndex,
            moduleCache[i].voltage, moduleCache[i].current,
            moduleCache[i].power,
            moduleCache[i].relayState ? "true" : "false",
            moduleCache[i].health,
            (unsigned long)now.unixtime());
        String mTopic = "meter/" + String(deviceId) + "/module/"
                      + String(moduleCache[i].moduleId) + "/"
                      + String(moduleCache[i].unitIndex);
        mqttClient.publish(mTopic.c_str(), mBuf, true);
    }
}

// ── Publish lifetime stats ────────────────────────────────────
void publishStats() {
    if (!mqttClient.connected()) return;
    char buf[256];
    snprintf(buf, sizeof(buf),
        "{\"deviceId\":%d,\"totalKwh\":%.4f,"
        "\"tamperCount\":%d,\"faultCount\":%d,"
        "\"theftFlag\":%s,\"balance\":%.2f,"
        "\"tariff\":%.2f,\"firmware\":\"%s\","
        "\"deployDate\":\"%s\",\"pairedModules\":%d}",
        deviceId, cumulativeKwh,
        (int)tamperCount, (int)faultCount,
        (theftFlag > 0.5f) ? "true" : "false",
        balance, tariff,
        FIRMWARE_VERSION, DEPLOY_DATE, pairedCount);
    mqttClient.publish(topicStats.c_str(), buf, true);
}

// ============================================================
//  LCD  — 5-page rotation (added module health page)
// ============================================================
void showMain() {
    DateTime now = rtc.now();
    char line[21];
    if (page != lastPage) { lcd.clear(); lastPage = page; }

    switch (page) {
        case 0:  // Live power
            snprintf(line, 21, "V:%-6.1f  I:%-6.2f", voltage, current);
            lcd.setCursor(0,0); lcd.print(line);
            snprintf(line, 21, "P:%-6.1fW F:%-4.1fHz", power, frequency);
            lcd.setCursor(0,1); lcd.print(line);
            snprintf(line, 21, "PF:%-4.2f kWh:%-6.2f", pf, cumulativeKwh);
            lcd.setCursor(0,2); lcd.print(line);
            snprintf(line, 21, "Bal:Rs%-6.2f %s%s",
                     balance,
                     relayLockedByServer ? "LCK" : (relayState ? "ON " : "OFF"));
            lcd.setCursor(0,3); lcd.print(line);
            break;

        case 1:  // RTC + audit
            snprintf(line, 21, "%02d/%02d/%04d %02d:%02d:%02d",
                     now.day(), now.month(), now.year(),
                     now.hour(), now.minute(), now.second());
            lcd.setCursor(0,0); lcd.print(line);
            snprintf(line, 21, "Tamper:%-4d Fault:%-3d",
                     (int)tamperCount, (int)faultCount);
            lcd.setCursor(0,1); lcd.print(line);
            snprintf(line, 21, "Theft:%-14s",
                     theftDetected ? "DETECTED!" : "None");
            lcd.setCursor(0,2); lcd.print(line);
            snprintf(line, 21, "Tariff:Rs%.2f v%-6s", tariff, FIRMWARE_VERSION);
            lcd.setCursor(0,3); lcd.print(line);
            break;

        case 2:  // Module health overview
            lcd.setCursor(0,0); lcd.print("-- MODULES --       ");
            snprintf(line, 21, "Paired: %-12d", pairedCount);
            lcd.setCursor(0,1); lcd.print(line);
            if (moduleCacheCount > 0) {
                snprintf(line, 21, "%-8s u%d %-5s",
                         moduleCache[0].moduleId,
                         moduleCache[0].unitIndex,
                         moduleCache[0].health);
                lcd.setCursor(0,2); lcd.print(line);
                if (moduleCacheCount > 1) {
                    snprintf(line, 21, "%-8s u%d %-5s",
                             moduleCache[1].moduleId,
                             moduleCache[1].unitIndex,
                             moduleCache[1].health);
                    lcd.setCursor(0,3); lcd.print(line);
                }
            } else {
                lcd.setCursor(0,2); lcd.print("No module data      ");
            }
            break;

        case 3:  // Billing sync info
            lcd.setCursor(0,0); lcd.print("-- BILLING --       ");
            snprintf(line, 21, "Period Wh:%-8.1f", periodEnergyWh);
            lcd.setCursor(0,1); lcd.print(line);
            snprintf(line, 21, "SeqNo: %-13lu", billingSeqNo);
            lcd.setCursor(0,2); lcd.print(line);
            snprintf(line, 21, "Lock:%-15s",
                     relayLockedByServer ? "YES (server)" : "No");
            lcd.setCursor(0,3); lcd.print(line);
            break;

        case 4:  // Fault + connectivity
            lcd.setCursor(0,0); lcd.print("-- STATUS --        ");
            snprintf(line, 21, "Leakage:%-8.4f A ", leakageCurrent);
            lcd.setCursor(0,1); lcd.print(line);
            snprintf(line, 21, "Fault:%-14s",
                     faultActive ? "ACTIVE!" : "OK");
            lcd.setCursor(0,2); lcd.print(line);
            snprintf(line, 21, "WiFi:%-3s MQTT:%-3s    ",
                     WiFi.status() == WL_CONNECTED ? "OK" : "ERR",
                     mqttClient.connected()         ? "OK" : "ERR");
            lcd.setCursor(0,3); lcd.print(line);
            break;
    }
}

void handleButtons() {
    if (digitalRead(BTN_MENU) == LOW) {
        page = (page + 1) % 5;   // 5 pages now
        delay(300);
    }
    if (digitalRead(BTN_RELAY) == LOW) {
        if (!faultActive && !relayLockedByServer) setRelay(!relayState);
        delay(300);
    }
}

// ============================================================
//  SETUP
// ============================================================
void setup() {
    Serial.begin(115200);
    Serial.printf("\n[BOOT] Smart Meter v%s  deployed:%s\n",
                  FIRMWARE_VERSION, DEPLOY_DATE);

    // Build MQTT topic strings
    String base        = "meter/" + String(deviceId) + "/";
    topicTelemetry     = base + "telemetry";
    topicHeartbeat     = base + "heartbeat";
    topicCommand       = base + "command";
    topicBillingSync   = base + "billing_sync";
    topicFault         = base + "fault";
    topicScan          = base + "scan";
    topicPairAck       = base + "pair_ack";
    topicAck           = base + "ack";
    topicStats         = base + "stats";

    // Pins
    pinMode(RELAY_PIN,  OUTPUT);
    pinMode(LED_FAULT,  OUTPUT);
    pinMode(LED_STATUS, OUTPUT);
    pinMode(LED_ENERGY, OUTPUT);
    pinMode(BUZZER_PIN, OUTPUT);
    pinMode(BTN_MENU,   INPUT_PULLUP);
    pinMode(BTN_SELECT, INPUT_PULLUP);
    pinMode(BTN_RELAY,  INPUT_PULLUP);
    pinMode(TAMPER_PIN, INPUT_PULLUP);

    // Safe state before EEPROM load
    digitalWrite(RELAY_PIN, LOW);
    digitalWrite(LED_FAULT,  LOW);
    digitalWrite(LED_STATUS, LOW);
    digitalWrite(LED_ENERGY, LOW);

    Wire.begin(21, 22);

    lcd.init();
    lcd.backlight();
    lcd.setCursor(0,0); lcd.print("Smart Energy Meter  ");
    lcd.setCursor(0,1); lcd.print("Booting...          ");

    if (!rtc.begin())    Serial.println("[RTC] FAIL");
    if (rtc.lostPower()) {
        rtc.adjust(DateTime(F(__DATE__), F(__TIME__)));
        Serial.println("[RTC] Reset to compile time");
    }

    // Load all persistent state
    loadEEPROM();
    loadBillingNVS();
    loadPairedModules();

    // Restore relay AFTER all state is loaded
    // relayLockedByServer already applied in loadBillingNVS()
    if (!relayLockedByServer && !theftDetected)
        digitalWrite(RELAY_PIN, relayState ? HIGH : LOW);

    lcd.setCursor(0,2); lcd.print("Connecting WiFi...  ");
    connectWiFi();

    // ESP-NOW — after WiFi.mode(WIFI_STA) in connectWiFi()
    if (esp_now_init() != ESP_OK) {
        Serial.println("[ESP-NOW] Init FAIL");
    } else {
        esp_now_register_recv_cb(onESPNowRecv);

        // Fix channel for ESP-NOW
        uint8_t ch = WiFi.channel();
        esp_wifi_set_channel(ch, WIFI_SECOND_CHAN_NONE);

        // Broadcast peer for scan/pair
        esp_now_peer_info_t bcast;
        memset(&bcast, 0, sizeof(bcast));
        memset(bcast.peer_addr, 0xFF, 6);
        bcast.channel = ch;
        bcast.encrypt = false;
        bcast.ifidx   = WIFI_IF_STA;
        esp_now_add_peer(&bcast);

        // Re-register all saved paired modules
        for (int i = 0; i < pairedCount; i++) {
            if (!pairedModules[i].isPaired) continue;
            if (esp_now_is_peer_exist(pairedModules[i].macAddr)) continue;
            esp_now_peer_info_t p;
            memset(&p, 0, sizeof(p));
            memcpy(p.peer_addr, pairedModules[i].macAddr, 6);
            p.channel = ch;
            p.encrypt = false;
            p.ifidx   = WIFI_IF_STA;
            if (esp_now_add_peer(&p) == ESP_OK)
                Serial.printf("[ESP-NOW] Re-added peer: %s\n",
                              pairedModules[i].moduleId);
        }
        Serial.println("[ESP-NOW] Ready");
    }

    connectMQTT();

    // Set billing period start if first boot
    if (periodStartSec == 0) periodStartSec = rtc.now().unixtime();
    lastBillingSync = millis();

    lcd.clear();
    lcd.setCursor(0,0); lcd.print("System Ready        ");
    lcd.setCursor(0,1); lcd.printf("v%-19s", FIRMWARE_VERSION);
    lcd.setCursor(0,2); lcd.print(
        WiFi.status() == WL_CONNECTED ? "WiFi:  OK           "
                                      : "WiFi:  FAIL         ");
    lcd.setCursor(0,3); lcd.print(
        mqttClient.connected()        ? "MQTT:  OK           "
                                      : "MQTT:  FAIL         ");
    delay(2000);

    Serial.printf("[BOOT] relay=%s locked=%s modules=%d\n",
                  relayState ? "ON" : "OFF",
                  relayLockedByServer ? "YES" : "NO",
                  pairedCount);
}

// ============================================================
//  LOOP
// ============================================================
void loop() {
    unsigned long now = millis();

    if (!mqttClient.connected()) connectMQTT();
    mqttClient.loop();

    // Sensor reads every cycle (~2 s via delay)
    readEnergy();
    readLeakage();
    checkFaults();
    checkEnergyTheft();

    // Local billing + EEPROM write every 10 s
    if (now - lastBillingMs >= BILLING_LOCAL_INTERVAL) {
        updateBillingLocal();
        lastBillingMs = now;
    }

    // Server billing sync every 30 s
    if (now - lastBillingSync >= BILLING_SYNC_INTERVAL) {
        publishBillingSync();
        lastBillingSync = now;
    }

    // Telemetry + stats every 5 s
    if (now - lastPublishMs >= TELEMETRY_INTERVAL) {
        publishTelemetry();
        publishStats();
        lastPublishMs = now;
    }

    // Heartbeat every 2 s
    if (now - lastHeartbeat >= HEARTBEAT_INTERVAL) {
        publishHeartbeat();
        lastHeartbeat = now;
    }

    // LCD auto-rotate every 5 s
    if (now - lastPageMs >= 5000UL) {
        page = (page + 1) % 5;
        lastPageMs = now;
    }

    showMain();
    handleButtons();

    digitalWrite(LED_ENERGY, !digitalRead(LED_ENERGY));
    delay(2000);
}
