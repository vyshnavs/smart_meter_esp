// ============================================================
//  SMART ENERGY METER — MAIN METER (ESP32)
//  Merged & corrected from smart_meter_final2.ino +
//  Mainmetercode_with_tamper_detection.ino
//
//  Changes vs originals:
//  ✅ IP address removed from LCD
//  ✅ Tamper alert fires only on rising edge + 10-s throttle (no spam)
//  ✅ Relay ON/OFF state published as retained MQTT so website always knows
//  ✅ Fake fault-test loop REMOVED
//  ✅ Fake tamper-test loop REMOVED
//  ✅ Real PZEM readings used everywhere (no simulated values)
//  ✅ Forward declarations fixed — no "used before declared" errors
//  ✅ MQTT reconnect is non-blocking (max 3 attempts, retries in loop)
//  ✅ Buzzer & relay pin driven correctly on tamper / lock / unlock
// ============================================================

#include <Wire.h>
#include <LiquidCrystal_I2C.h>
#include <PZEM004Tv30.h>
#include <RTClib.h>
#include <WiFi.h>
#include <EEPROM.h>
#include <PubSubClient.h>
#include <esp_now.h>
#include <esp_wifi.h>
#include <Preferences.h>
#include <ArduinoJson.h>

// ============================================================
//  USER CONFIG — edit these before flashing
// ============================================================
const char* ssid            = "boom";
const char* password        = "1231231230";
const char* mqttServer      = "20.39.192.14";
const int   mqttPort        = 1883;
const int   deviceId        = 44188662;
const char* secretKey       = "6vM3ihdppGGtwf54Yu";
const char* firmwareVersion = "1.0.3";
const char* deploymentDate  = "2025-02-15";

// ============================================================
//  PIN DEFINITIONS
// ============================================================
#define RELAY_PIN    26     // LOW = load connected
#define BUZZER_PIN   27
#define LED_GREEN     5     // WiFi OK
#define LED_BLUE     19     // Current activity
#define LED_RED      18     // Fault / relay off
#define BTN_LEFT     32
#define BTN_RIGHT    33
#define BTN_RELAY    35     // Manual relay toggle
#define TAMPER_PIN   34     // HIGH = tamper detected

// ============================================================
//  HARDWARE OBJECTS
// ============================================================
LiquidCrystal_I2C lcd(0x27, 20, 4);
HardwareSerial    pzemSerial(2);
PZEM004Tv30       pzem(pzemSerial, 17, 16);
RTC_DS3231        rtc;

// ============================================================
//  SENSOR READINGS
// ============================================================
float voltage = 0, current_A = 0, power_W = 0, energy_kWh = 0, pf = 0, frequency = 0;
float storedEnergy = 0;

// ============================================================
//  RELAY / TAMPER / FAULT STATE
// ============================================================
bool relayState          = true;
bool tamperState         = false;   // current tamper pin level
bool lastTamperPin       = false;   // previous tamper pin level (edge detect)
bool relayLockedByServer = false;
bool faultActive         = false;

// ============================================================
//  LCD SCREEN INDEX  (0-4)
//    0 = Power readings
//    1 = Date / Time
//    2 = Stored energy
//    3 = Device info  (WiFi/MQTT status, device ID, version — NO IP)
//    4 = Status       (tamper, relay, modules, lock)
// ============================================================
int screen = 0;

// ============================================================
//  LED BLINK STATE
// ============================================================
unsigned long blueTimer = 0, redTimer = 0;
bool blueState = false, redState = false;

// ============================================================
//  GENERAL TIMERS
// ============================================================
unsigned long lcdTimer       = 0;
unsigned long eepromTimer    = 0;
unsigned long wifiRetryTimer = 0;

// ============================================================
//  TAMPER THROTTLE
//  Alert published only on rising edge OR once every 10 s while
//  tamper pin stays HIGH.  No infinite spam.
// ============================================================
#define TAMPER_ALERT_INTERVAL 10000UL
unsigned long lastTamperAlert = 0;

// ============================================================
//  BILLING
// ============================================================
Preferences preferences;

const unsigned long BILLING_SYNC_INTERVAL = 60000UL;;
unsigned long lastBillingSync      = 0;
unsigned long billingSequenceNo    = 0;
float         accumulatedEnergyWh  = 0.0;
unsigned long periodStartTimestamp = 0;
unsigned long lastEnergyUpdate     = 0;

// ============================================================
//  MQTT TOPICS
// ============================================================
String energyTopic      = "meter/" + String(deviceId) + "/energy";
String telemetryTopic   = "meter/" + String(deviceId) + "/telemetry";
String heartbeatTopic   = "meter/" + String(deviceId) + "/heartbeat";
String commandTopic     = "meter/" + String(deviceId) + "/command";
String scanTopic        = "meter/" + String(deviceId) + "/scan";
String pairAckTopic     = "meter/" + String(deviceId) + "/pair_ack";
String billingSyncTopic = "meter/" + String(deviceId) + "/billing_sync";
String ackTopic         = "meter/" + String(deviceId) + "/ack";
String faultTopic       = "meter/" + String(deviceId) + "/fault";
String tamperTopic      = "meter/" + String(deviceId) + "/tamper";
String relayStatusTopic = "meter/" + String(deviceId) + "/relay_status"; // retained

// ============================================================
//  TIMING CONSTANTS
// ============================================================
const unsigned long TELEMETRY_INTERVAL     = 3000UL;
const unsigned long HEARTBEAT_INTERVAL     = 2000UL;
const unsigned long ENERGY_UPDATE_INTERVAL = 1000UL;

unsigned long lastTelemetry = 0;
unsigned long lastHeartbeat = 0;

// ============================================================
//  ESP-NOW STRUCTS
// ============================================================
typedef struct { char type[16]; } ScanRequest;

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
  char  type[16];
  char  moduleId[16];
  float voltage;
  float current;
  float power;
  bool  relayState;
  char  health[8];
  int   unitIndex;
} ModuleTelemetry;

typedef struct {
  char  type[16];
  char  moduleId[16];
  int   unitNumber;
  char  faultType[32];
  char  severity[16];
  char  description[128];
  float measuredValue;
  float thresholdValue;
  char  unit[16];
} ModuleFault;

typedef struct {
  char type[16];
  char moduleId[16];
  int  unitNumber;
  bool state;
} ModuleRelayControl;

typedef struct {
  char type[16];
  char moduleId[16];
  int  unitNumber;
  bool success;
  bool relayState;
} ModuleRelayAck;

// ============================================================
//  PAIRED MODULES
// ============================================================
#define MAX_MODULES 10

struct PairedModule {
  char    moduleId[16];
  uint8_t macAddr[6];
  int     capacity;
  bool    isPaired;
};

PairedModule pairedModules[MAX_MODULES];
int pairedCount = 0;

struct ModuleData {
  char          moduleId[16];
  int           unitIndex;
  float         voltage;
  float         current;
  float         power;
  bool          relayState;
  char          health[8];
  unsigned long lastUpdate;
};

ModuleData moduleDataCache[MAX_MODULES * 10];
int moduleDataCount = 0;

// ============================================================
//  MQTT CLIENT
// ============================================================
WiFiClient   espClient;
PubSubClient client(espClient);

// ============================================================
//  FORWARD DECLARATIONS
//  (required so callback functions compile without order issues)
// ============================================================
void onMQTTMessage(char* topic, byte* payload, unsigned int length);
void onESPNowRecv(const esp_now_recv_info_t* info, const uint8_t* data, int len);
void publishCommandAck(const char* command, const char* status, bool currentRelayState);
void publishFault(const char* source, const char* moduleId, int unitNumber,
                  const char* faultType, const char* severity,
                  const char* description, float measuredValue,
                  float thresholdValue, const char* unit);
void publishRelayStatus();
void publishTamperAlert();

// ============================================================
//  NVS — RELAY STATE
// ============================================================
void loadRelayState() {
  preferences.begin("relay", false);
  relayState = preferences.getBool("state", true);
  preferences.end();
  Serial.print("Relay state loaded: ");
  Serial.println(relayState ? "ON" : "OFF");
}

void saveRelayState() {
  preferences.begin("relay", false);
  preferences.putBool("state", relayState);
  preferences.end();
}

// ============================================================
//  NVS — BILLING STATE
// ============================================================
void loadBillingState() {
  preferences.begin("billing", false);
  accumulatedEnergyWh    = preferences.getFloat("energyWh",    0.0);
  billingSequenceNo      = preferences.getULong("seqNo",       0);
  periodStartTimestamp   = preferences.getULong("periodStart", 0);
  relayLockedByServer    = preferences.getBool("relayLock",   false);
  preferences.end();

  Serial.print("Billing energy loaded: ");
  Serial.print(accumulatedEnergyWh);
  Serial.println(" Wh");

  if (relayLockedByServer) {
    relayState = false;
    Serial.println("RELAY LOCKED — Insufficient balance");
  }
}

void saveBillingState() {
  preferences.begin("billing", false);
  preferences.putFloat("energyWh",    accumulatedEnergyWh);
  preferences.putULong("seqNo",       billingSequenceNo);
  preferences.putULong("periodStart", periodStartTimestamp);
  preferences.putBool("relayLock",   relayLockedByServer);
  preferences.end();
}

void saveDeviceInfo() {
  preferences.begin("device", false);
  preferences.putInt("deviceId",    deviceId);
  preferences.putString("secret",  secretKey);
  preferences.putString("version", firmwareVersion);
  preferences.putString("deployed",deploymentDate);
  preferences.end();
}

// ============================================================
//  NVS — PAIRED MODULES
// ============================================================
void loadPairedModules() {
  preferences.begin("modules", false);
  pairedCount = preferences.getInt("count", 0);
  for (int i = 0; i < pairedCount && i < MAX_MODULES; i++) {
    String key = "mod" + String(i);
    if (preferences.getBytesLength(key.c_str()) == sizeof(PairedModule)) {
      preferences.getBytes(key.c_str(), &pairedModules[i], sizeof(PairedModule));
      Serial.print("Module loaded: ");
      Serial.println(pairedModules[i].moduleId);
    }
  }
  preferences.end();
  Serial.print("Total paired modules: ");
  Serial.println(pairedCount);
}

void savePairedModules() {
  preferences.begin("modules", false);
  preferences.putInt("count", pairedCount);
  for (int i = 0; i < pairedCount; i++) {
    String key = "mod" + String(i);
    preferences.putBytes(key.c_str(), &pairedModules[i], sizeof(PairedModule));
  }
  preferences.end();
  Serial.println("Paired modules saved");
}

// ============================================================
//  WIFI
// ============================================================
void connectWiFi() {
  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid, password);
  Serial.print("Connecting to WiFi");
  int attempts = 0;
  while (WiFi.status() != WL_CONNECTED && attempts < 20) {
    delay(500);
    Serial.print(".");
    attempts++;
  }
  if (WiFi.status() == WL_CONNECTED) {
    Serial.println("\nWiFi connected");
    // IP intentionally not printed / displayed for security
  } else {
    Serial.println("\nWiFi failed — will retry in loop");
  }
}

// ============================================================
//  MQTT
// ============================================================
void setupMQTT() {
  client.setServer(mqttServer, mqttPort);
  client.setCallback(onMQTTMessage);
  client.setBufferSize(2048);
}

// Non-blocking: max 3 attempts per call, loop retries automatically
void connectMQTT() {
  int attempts = 0;
  while (!client.connected() && attempts < 3) {
    Serial.println("Connecting to MQTT...");
    if (client.connect(String(deviceId).c_str(),
                       String(deviceId).c_str(),
                       secretKey)) {
      Serial.println("MQTT connected");
      client.subscribe(commandTopic.c_str());
      Serial.print("Subscribed: ");
      Serial.println(commandTopic);
      // Immediately re-publish retained relay state after reconnect
      publishRelayStatus();
    } else {
      Serial.print("MQTT failed rc=");
      Serial.println(client.state());
      delay(3000);
      attempts++;
    }
  }
}

// ============================================================
//  PUBLISH — RELAY STATUS  (retained so website always knows)
// ============================================================
void publishRelayStatus() {
  StaticJsonDocument<160> doc;
  doc["deviceId"]    = deviceId;
  doc["relayState"]  = relayState;
  doc["relayLocked"] = relayLockedByServer;
  doc["tamperActive"]= tamperState;
  doc["source"]      = "mainMeter";
  char buffer[160];
  serializeJson(doc, buffer);
  // retain = true  →  website gets state immediately on subscribe
  client.publish(relayStatusTopic.c_str(), buffer, true);
  Serial.print("Relay status: ");
  Serial.println(relayState ? "ON" : "OFF");
}

// ============================================================
//  PUBLISH — COMMAND ACK
// ============================================================
void publishCommandAck(const char* command, const char* status, bool currentRelayState) {
  StaticJsonDocument<128> doc;
  doc["command"]    = command;
  doc["status"]     = status;
  doc["relayState"] = currentRelayState;
  doc["relayLocked"]= relayLockedByServer;
  char buffer[128];
  serializeJson(doc, buffer);
  client.publish(ackTopic.c_str(), buffer);
  Serial.print("ACK: "); Serial.print(command);
  Serial.print(" | "); Serial.println(status);
}

// ============================================================
//  PUBLISH — FAULT
// ============================================================
void publishFault(const char* source, const char* moduleId, int unitNumber,
                  const char* faultType, const char* severity,
                  const char* description, float measuredValue,
                  float thresholdValue, const char* unit) {
  StaticJsonDocument<512> doc;
  doc["source"]    = source;
  if (moduleId != nullptr)                  doc["moduleId"]        = moduleId;
  if (unitNumber >= 0)                      doc["unitNumber"]      = unitNumber;
  doc["faultType"]   = faultType;
  doc["severity"]    = severity;
  doc["description"] = description;
  if (measuredValue  > 0)                   doc["measuredValue"]   = measuredValue;
  if (thresholdValue > 0)                   doc["thresholdValue"]  = thresholdValue;
  if (unit != nullptr && strlen(unit) > 0)  doc["unit"]            = unit;
  char buffer[512];
  serializeJson(doc, buffer);
  if (client.publish(faultTopic.c_str(), buffer)) {
    Serial.print("Fault published: "); Serial.println(faultType);
  } else {
    Serial.println("Fault publish failed");
  }
}

// ============================================================
//  PUBLISH — TAMPER ALERT  (real tamper only, no test loops)
// ============================================================
void publishTamperAlert() {
  StaticJsonDocument<256> doc;
  char tamperId[64];
  snprintf(tamperId, sizeof(tamperId), "TAMPER_%d_%lu", deviceId, millis());
  doc["tamperId"]    = tamperId;
  doc["tamperType"]  = "physical_breach";
  doc["severity"]    = "critical";
  doc["description"] = "Enclosure opened - unauthorized access detected";
  char buffer[256];
  serializeJson(doc, buffer);
  if (client.publish(tamperTopic.c_str(), buffer)) {
    Serial.println("TAMPER ALERT published");
  } else {
    Serial.println("Tamper alert publish failed");
  }
}

// ============================================================
//  ENERGY ACCUMULATION  (uses real PZEM power_W reading)
// ============================================================
void updateEnergyAccumulation() {
  unsigned long now = millis();
  if (lastEnergyUpdate == 0) {
    lastEnergyUpdate = now;
    return;
  }
  if (relayState && !relayLockedByServer && !isnan(power_W)) {
    unsigned long deltaMs = now - lastEnergyUpdate;
    float deltaHours      = deltaMs / 3600000.0f;
    accumulatedEnergyWh  += power_W * deltaHours;

    static unsigned long lastSave = 0;
    if (now - lastSave > 10000UL) {
      saveBillingState();
      lastSave = now;
    }
  }
  lastEnergyUpdate = now;
}

// ============================================================
//  PUBLISH — BILLING SYNC
// ============================================================
void publishBillingSync() {
  unsigned long nowSec = millis() / 1000;
  if (periodStartTimestamp == 0) periodStartTimestamp = nowSec;

  StaticJsonDocument<256> doc;
  doc["deviceId"]         = deviceId;
  doc["sequenceNo"]       = billingSequenceNo;
  doc["periodStart"]      = periodStartTimestamp;
  doc["periodEnd"]        = nowSec;
  doc["energyConsumedWh"] = round(accumulatedEnergyWh * 10) / 10.0;
  doc["meterState"]       = relayLockedByServer ? "LOCKED" : "ACTIVE";
  doc["relayState"]       = relayState;
  doc["faultActive"]      = faultActive;
  doc["firmwareVersion"]  = firmwareVersion;

  char buffer[256];
  serializeJson(doc, buffer);

  if (client.publish(billingSyncTopic.c_str(), buffer, true)) {
    Serial.print("Billing sync | Energy: ");
    Serial.print(accumulatedEnergyWh);
    Serial.println(" Wh");
    accumulatedEnergyWh  = 0.0;
    periodStartTimestamp = nowSec;
    billingSequenceNo++;
    saveBillingState();
  } else {
    Serial.println("Billing sync failed");
  }
}

// ============================================================
//  PUBLISH — TELEMETRY  (real PZEM readings)
// ============================================================
void publishTelemetry() {
  StaticJsonDocument<2048> doc;

  doc["deviceId"]    = deviceId;
  doc["timestamp"]   = millis() / 1000;
  doc["relayLocked"] = relayLockedByServer;

  JsonObject mainMeter = doc.createNestedObject("mainMeter");
  mainMeter["voltage"]             = (float)(isnan(voltage)    ? 0.0f : round(voltage    * 10)  / 10.0f);
  mainMeter["current"]             = (float)(isnan(current_A)  ? 0.0f : round(current_A  * 100) / 100.0f);
  mainMeter["power"]               = (float)(isnan(power_W)    ? 0.0f : round(power_W));
  mainMeter["powerFactor"]         = (float)(isnan(pf)         ? 0.0f : round(pf         * 100) / 100.0f);
  mainMeter["frequency"]           = (float)(isnan(frequency)  ? 0.0f : round(frequency  * 100) / 100.0f);
  mainMeter["energyKwh"]           = (float)(isnan(energy_kWh) ? 0.0f : round(energy_kWh * 100) / 100.0f);
  mainMeter["accumulatedEnergyWh"] = (float)(round(accumulatedEnergyWh * 10) / 10.0f);
  mainMeter["relayState"]          = relayState;
  mainMeter["relayLocked"]         = relayLockedByServer;

  JsonArray modules = doc.createNestedArray("modules");

  for (int i = 0; i < pairedCount; i++) {
    if (!pairedModules[i].isPaired) continue;
    bool       hasData = false;
    JsonArray  units;

    for (int j = 0; j < moduleDataCount; j++) {
      if (strcmp(moduleDataCache[j].moduleId, pairedModules[i].moduleId) == 0) {
        if (millis() - moduleDataCache[j].lastUpdate < 10000UL) {
          if (!hasData) {
            JsonObject module = modules.createNestedObject();
            module["moduleId"] = pairedModules[i].moduleId;
            units  = module.createNestedArray("units");
            hasData = true;
          }
          JsonObject unit = units.createNestedObject();
          unit["unitIndex"]  = moduleDataCache[j].unitIndex;
          unit["voltage"]    = (float)(round(moduleDataCache[j].voltage  * 10)  / 10.0f);
          unit["current"]    = (float)(round(moduleDataCache[j].current  * 100) / 100.0f);
          unit["power"]      = (float)(round(moduleDataCache[j].power));
          unit["relayState"] = moduleDataCache[j].relayState;
          unit["health"]     = moduleDataCache[j].health;
        }
      }
    }
  }

  char buffer[2048];
  serializeJson(doc, buffer);

  if (client.publish(telemetryTopic.c_str(), buffer)) {
    Serial.println("Telemetry published");
  } else {
    Serial.print("Telemetry failed, MQTT state: ");
    Serial.println(client.state());
  }
}

// ============================================================
//  PUBLISH — HEARTBEAT
// ============================================================
void publishHeartbeat() {
  client.publish(heartbeatTopic.c_str(), "alive");
}

// ============================================================
//  MQTT MESSAGE HANDLER
// ============================================================
void onMQTTMessage(char* topic, byte* payload, unsigned int length) {
  Serial.print("MQTT msg on: ");
  Serial.println(topic);

  StaticJsonDocument<256> doc;
  if (deserializeJson(doc, payload, length)) {
    Serial.println("JSON parse failed");
    return;
  }

  String cmd = doc["command"] | "";

  // ── MAIN RELAY CONTROL ─────────────────────────────────────
  if (cmd == "relay_on") {
    if (relayLockedByServer) {
      Serial.println("Cannot turn ON — relay locked by server");
      publishCommandAck("relay_on", "FAILED", relayState);
      return;
    }
    relayState = true;
    digitalWrite(RELAY_PIN, LOW);   // LOW = load connected
    saveRelayState();
    publishCommandAck("relay_on", "SUCCESS", relayState);
    publishRelayStatus();
  }
  else if (cmd == "relay_off") {
    relayState = false;
    digitalWrite(RELAY_PIN, HIGH);  // HIGH = load disconnected
    saveRelayState();
    publishCommandAck("relay_off", "SUCCESS", relayState);
    publishRelayStatus();
  }
  else if (cmd == "unlock_relay") {
    relayLockedByServer = false;
    relayState = true;
    digitalWrite(RELAY_PIN, LOW);
    saveBillingState();
    saveRelayState();
    publishCommandAck("unlock_relay", "SUCCESS", relayState);
    publishRelayStatus();
  }
  else if (cmd == "lock_relay") {
    relayLockedByServer = true;
    relayState = false;
    digitalWrite(RELAY_PIN, HIGH);
    saveBillingState();
    saveRelayState();
    publishCommandAck("lock_relay", "SUCCESS", relayState);
    publishRelayStatus();
  }

  // ── MODULE RELAY CONTROL ───────────────────────────────────
  else if (cmd == "module_relay_control") {
    String moduleId   = doc["moduleId"]   | "";
    int    unitNumber = doc["unitNumber"] | -1;
    bool   state      = doc["state"]      | false;
    if (moduleId.length() == 0 || unitNumber < 0) return;

    for (int i = 0; i < pairedCount; i++) {
      if (strcmp(pairedModules[i].moduleId, moduleId.c_str()) == 0) {
        ModuleRelayControl relayCmd;
        memset(&relayCmd, 0, sizeof(relayCmd));
        strcpy(relayCmd.type, "RELAY_CTRL");
        strncpy(relayCmd.moduleId, moduleId.c_str(), 15);
        relayCmd.unitNumber = unitNumber;
        relayCmd.state      = state;
        esp_now_send(pairedModules[i].macAddr, (uint8_t*)&relayCmd, sizeof(relayCmd));
        Serial.println("Relay control sent to module");
        break;
      }
    }
  }

  // ── SCAN MODULES ───────────────────────────────────────────
  else if (cmd == "scan_modules") {
    ScanRequest req;
    memset(&req, 0, sizeof(req));
    strcpy(req.type, "SCAN_REQ");
    uint8_t broadcastAddr[6] = {0xFF,0xFF,0xFF,0xFF,0xFF,0xFF};
    esp_now_send(broadcastAddr, (uint8_t*)&req, sizeof(req));
    Serial.println("Scan broadcast sent");
  }

  // ── PAIR MODULE ────────────────────────────────────────────
  else if (cmd == "pair_module") {
    String moduleId = doc["moduleId"] | "";
    String secret   = doc["secret"]   | "";
    if (moduleId.length() == 0 || secret.length() == 0) return;
    PairRequest pairReq;
    memset(&pairReq, 0, sizeof(pairReq));
    strcpy(pairReq.type, "PAIR_REQ");
    strncpy(pairReq.moduleId, moduleId.c_str(), 15);
    strncpy(pairReq.secret,   secret.c_str(),   31);
    uint8_t broadcastAddr[6] = {0xFF,0xFF,0xFF,0xFF,0xFF,0xFF};
    esp_now_send(broadcastAddr, (uint8_t*)&pairReq, sizeof(pairReq));
    Serial.print("Pair request sent: ");
    Serial.println(moduleId);
  }

  // ── UNPAIR MODULE ──────────────────────────────────────────
  else if (cmd == "unpair_module") {
    String moduleId = doc["moduleId"] | "";
    if (moduleId.length() == 0) return;
    for (int i = 0; i < pairedCount; i++) {
      if (strcmp(pairedModules[i].moduleId, moduleId.c_str()) == 0) {
        UnpairCommand unpairCmd;
        memset(&unpairCmd, 0, sizeof(unpairCmd));
        strcpy(unpairCmd.type, "UNPAIR");
        strncpy(unpairCmd.moduleId, moduleId.c_str(), 15);
        esp_now_send(pairedModules[i].macAddr, (uint8_t*)&unpairCmd, sizeof(unpairCmd));
        esp_now_del_peer(pairedModules[i].macAddr);
        for (int j = i; j < pairedCount - 1; j++) pairedModules[j] = pairedModules[j+1];
        pairedCount--;
        savePairedModules();
        publishCommandAck("unpair_module", "SUCCESS", relayState);
        return;
      }
    }
    publishCommandAck("unpair_module", "FAILED", relayState);
  }
}

// ============================================================
//  ESP-NOW RECEIVE CALLBACK
// ============================================================
void onESPNowRecv(const esp_now_recv_info_t* info, const uint8_t* data, int len) {
  char msgType[16];
  memcpy(msgType, data, 16);

  // ── SCAN RESPONSE ─────────────────────────────────────────
  if (strcmp(msgType, "SCAN_RESP") == 0) {
    ScanResponse resp;
    memcpy(&resp, data, sizeof(resp));
    char macStr[18];
    snprintf(macStr, sizeof(macStr), "%02X:%02X:%02X:%02X:%02X:%02X",
             info->src_addr[0], info->src_addr[1], info->src_addr[2],
             info->src_addr[3], info->src_addr[4], info->src_addr[5]);
    StaticJsonDocument<128> doc;
    doc["moduleId"] = resp.moduleId;
    doc["capacity"] = resp.capacity;
    doc["mac"]      = macStr;
    char buffer[128];
    serializeJson(doc, buffer);
    client.publish(scanTopic.c_str(), buffer);
    Serial.print("Scan response: "); Serial.println(resp.moduleId);
  }

  // ── PAIR ACK ──────────────────────────────────────────────
  else if (strcmp(msgType, "PAIR_ACK") == 0) {
    PairAck ack;
    memcpy(&ack, data, sizeof(ack));
    if (ack.success && pairedCount < MAX_MODULES) {
      strncpy(pairedModules[pairedCount].moduleId, ack.moduleId, 15);
      memcpy(pairedModules[pairedCount].macAddr, info->src_addr, 6);
      pairedModules[pairedCount].capacity = 0;
      pairedModules[pairedCount].isPaired = true;

      esp_now_peer_info_t peerInfo;
      memset(&peerInfo, 0, sizeof(peerInfo));
      memcpy(peerInfo.peer_addr, info->src_addr, 6);
      peerInfo.channel = WiFi.channel();
      peerInfo.encrypt = false;
      peerInfo.ifidx   = WIFI_IF_STA;

      if (esp_now_add_peer(&peerInfo) == ESP_OK) {
        pairedCount++;
        savePairedModules();
        Serial.print("Module paired: "); Serial.println(ack.moduleId);
      }
    }
    StaticJsonDocument<128> doc;
    doc["moduleId"] = ack.moduleId;
    doc["success"]  = ack.success;
    char buffer[128];
    serializeJson(doc, buffer);
    client.publish(pairAckTopic.c_str(), buffer);
  }

  // ── MODULE TELEMETRY ──────────────────────────────────────
  else if (strcmp(msgType, "TELEMETRY") == 0) {
    ModuleTelemetry telem;
    memcpy(&telem, data, sizeof(telem));

    bool found = false;
    for (int i = 0; i < moduleDataCount; i++) {
      if (strcmp(moduleDataCache[i].moduleId, telem.moduleId) == 0 &&
          moduleDataCache[i].unitIndex == telem.unitIndex) {
        moduleDataCache[i].voltage    = telem.voltage;
        moduleDataCache[i].current    = telem.current;
        moduleDataCache[i].power      = telem.power;
        moduleDataCache[i].relayState = telem.relayState;
        strcpy(moduleDataCache[i].health, telem.health);
        moduleDataCache[i].lastUpdate = millis();
        found = true;
        break;
      }
    }
    if (!found && moduleDataCount < (MAX_MODULES * 10)) {
      strcpy(moduleDataCache[moduleDataCount].moduleId,  telem.moduleId);
      moduleDataCache[moduleDataCount].unitIndex  = telem.unitIndex;
      moduleDataCache[moduleDataCount].voltage    = telem.voltage;
      moduleDataCache[moduleDataCount].current    = telem.current;
      moduleDataCache[moduleDataCount].power      = telem.power;
      moduleDataCache[moduleDataCount].relayState = telem.relayState;
      strcpy(moduleDataCache[moduleDataCount].health, telem.health);
      moduleDataCache[moduleDataCount].lastUpdate = millis();
      moduleDataCount++;
    }
  }

  // ── MODULE FAULT ──────────────────────────────────────────
  else if (strcmp(msgType, "FAULT") == 0) {
    ModuleFault fault;
    memcpy(&fault, data, sizeof(fault));
    publishFault("module", fault.moduleId, fault.unitNumber,
                 fault.faultType, fault.severity, fault.description,
                 fault.measuredValue, fault.thresholdValue, fault.unit);
    Serial.print("Module fault forwarded: "); Serial.println(fault.faultType);
  }

  // ── MODULE RELAY ACK ──────────────────────────────────────
  else if (strcmp(msgType, "RELAY_ACK") == 0) {
    ModuleRelayAck ack;
    memcpy(&ack, data, sizeof(ack));
    StaticJsonDocument<256> doc;
    doc["command"]    = "module_relay_ack";
    doc["moduleId"]   = ack.moduleId;
    doc["unitNumber"] = ack.unitNumber;
    doc["status"]     = ack.success ? "SUCCESS" : "FAILED";
    doc["relayState"] = ack.relayState;
    char buffer[256];
    serializeJson(doc, buffer);
    client.publish(ackTopic.c_str(), buffer);
    Serial.print("Module relay ack: ");
    Serial.println(ack.success ? "SUCCESS" : "FAILED");
  }
}

// ============================================================
//  SETUP
// ============================================================
void setup() {
  Serial.begin(115200);
  delay(500);
  Serial.println("\n=== Smart Energy Meter Starting ===");
  Serial.print("Firmware: "); Serial.println(firmwareVersion);

  // ── Pin modes ──────────────────────────────────────────────
  pinMode(RELAY_PIN,  OUTPUT);
  pinMode(BUZZER_PIN, OUTPUT);
  pinMode(LED_GREEN,  OUTPUT);
  pinMode(LED_BLUE,   OUTPUT);
  pinMode(LED_RED,    OUTPUT);
  pinMode(BTN_LEFT,   INPUT_PULLUP);
  pinMode(BTN_RIGHT,  INPUT_PULLUP);
  pinMode(BTN_RELAY,  INPUT_PULLUP);
  pinMode(TAMPER_PIN, INPUT);

  // ── PZEM + I2C ─────────────────────────────────────────────
  pzemSerial.begin(9600, SERIAL_8N1, 17, 16);
  Wire.begin(21, 22);

  // ── LCD ────────────────────────────────────────────────────
  lcd.init();
  lcd.backlight();
  lcd.setCursor(0, 0); lcd.print("SMART ENERGY METER");
  lcd.setCursor(0, 1); lcd.print("  Initializing...  ");
  delay(2000);
  lcd.clear();

  // ── RTC ────────────────────────────────────────────────────
  if (!rtc.begin()) {
    Serial.println("RTC not found — time unavailable");
  }

  // ── EEPROM (legacy energy backup) ──────────────────────────
  EEPROM.begin(32);
  EEPROM.get(0, storedEnergy);
  if (isnan(storedEnergy)) storedEnergy = 0;

  // ── NVS ────────────────────────────────────────────────────
  saveDeviceInfo();
  loadBillingState();
  loadRelayState();
  loadPairedModules();

  // ── Apply stored relay state to pin ────────────────────────
  digitalWrite(RELAY_PIN, relayState ? LOW : HIGH);

  // ── WiFi ───────────────────────────────────────────────────
  connectWiFi();

  // ── ESP-NOW ────────────────────────────────────────────────
  if (esp_now_init() != ESP_OK) {
    Serial.println("ESP-NOW init failed");
  } else {
    Serial.println("ESP-NOW initialized");
    esp_now_register_recv_cb(onESPNowRecv);

    uint8_t ch = WiFi.channel();
    esp_wifi_set_channel(ch, WIFI_SECOND_CHAN_NONE);

    // Broadcast peer
    esp_now_peer_info_t bcast;
    memset(&bcast, 0, sizeof(bcast));
    memset(bcast.peer_addr, 0xFF, 6);
    bcast.channel = ch;
    bcast.encrypt = false;
    bcast.ifidx   = WIFI_IF_STA;
    esp_now_add_peer(&bcast);

    // Re-add saved paired peers
    for (int i = 0; i < pairedCount; i++) {
      if (pairedModules[i].isPaired &&
          !esp_now_is_peer_exist(pairedModules[i].macAddr)) {
        esp_now_peer_info_t peer;
        memset(&peer, 0, sizeof(peer));
        memcpy(peer.peer_addr, pairedModules[i].macAddr, 6);
        peer.channel = ch;
        peer.encrypt = false;
        peer.ifidx   = WIFI_IF_STA;
        esp_now_add_peer(&peer);
        Serial.print("Peer re-added: ");
        Serial.println(pairedModules[i].moduleId);
      }
    }
  }

  // ── MQTT ───────────────────────────────────────────────────
  setupMQTT();
  connectMQTT();

  // Publish retained relay status right away
  publishRelayStatus();

  lastBillingSync      = millis();
  periodStartTimestamp = millis() / 1000;
  lastTamperPin        = digitalRead(TAMPER_PIN);

  Serial.println("=== Smart meter ready ===");
  Serial.print("Relay: "); Serial.println(relayState ? "ON" : "OFF (LOCKED)");
  Serial.print("Paired modules: "); Serial.println(pairedCount);
}

// ============================================================
//  LOOP
// ============================================================
void loop() {
  // ── MQTT ──────────────────────────────────────────────────
  if (!client.connected()) connectMQTT();
  client.loop();

  unsigned long now = millis();

  // ── WiFi status LED & retry ───────────────────────────────
  if (WiFi.status() == WL_CONNECTED) {
    digitalWrite(LED_GREEN, HIGH);
  } else {
    digitalWrite(LED_GREEN, LOW);
    if (now - wifiRetryTimer > 15000UL) {
      wifiRetryTimer = now;
      connectWiFi();
    }
  }

  // ── READ PZEM ─────────────────────────────────────────────
  voltage    = pzem.voltage();
  current_A  = pzem.current();
  power_W    = pzem.power();
  energy_kWh = pzem.energy();
  pf         = pzem.pf();
  frequency  = pzem.frequency();

  // ── EEPROM BACKUP ─────────────────────────────────────────
  if (now - eepromTimer > 30000UL) {
    eepromTimer = now;
    EEPROM.put(0, energy_kWh);
    EEPROM.commit();
    storedEnergy = isnan(energy_kWh) ? storedEnergy : energy_kWh;
  }

  // ── TAMPER DETECTION ──────────────────────────────────────
  //   Rising edge = tamper starts → publish immediately
  //   While HIGH  = repeat alert every TAMPER_ALERT_INTERVAL (10 s)
  //   Falling edge = tamper cleared → publish relay status update
  bool currentTamper = (digitalRead(TAMPER_PIN) == HIGH);

  if (currentTamper) {
    // Disconnect load and sound buzzer
    relayState = false;
    digitalWrite(RELAY_PIN,  HIGH);
    digitalWrite(BUZZER_PIN, HIGH);

    // Publish on rising edge OR if throttle interval has elapsed
    bool risingEdge = (!lastTamperPin && currentTamper);
    if (risingEdge || (now - lastTamperAlert >= TAMPER_ALERT_INTERVAL)) {
      lastTamperAlert = now;
      publishTamperAlert();
      publishFault("mainMeter", nullptr, -1,
                   "tamper", "critical",
                   "Physical tamper detected on main meter",
                   0, 0, "");
      publishRelayStatus();
    }
  } else {
    // Tamper not active
    digitalWrite(BUZZER_PIN, LOW);
    if (lastTamperPin) {
      // Just cleared — restore relay if not server-locked
      if (!relayLockedByServer) {
        relayState = true;
        digitalWrite(RELAY_PIN, LOW);
      }
      publishRelayStatus();
      Serial.println("Tamper cleared");
    }
  }

  lastTamperPin = currentTamper;
  tamperState   = currentTamper;

  // ── MANUAL RELAY BUTTON ───────────────────────────────────
  //   Only allowed when no tamper and not server-locked
  if (digitalRead(BTN_RELAY) == LOW && !tamperState && !relayLockedByServer) {
    relayState = !relayState;
    digitalWrite(RELAY_PIN, relayState ? LOW : HIGH);
    saveRelayState();
    publishRelayStatus();
    delay(300);  // simple debounce
  }

  // ── SCREEN NAVIGATION ─────────────────────────────────────
  if (digitalRead(BTN_RIGHT) == LOW) {
    screen = (screen >= 4) ? 0 : screen + 1;
    delay(300);
  }
  if (digitalRead(BTN_LEFT) == LOW) {
    screen = (screen <= 0) ? 4 : screen - 1;
    delay(300);
  }

  // ── BLUE LED (current activity) ───────────────────────────
  if (!isnan(current_A) && current_A > 0.02f) {
    unsigned long interval = 1000;
    if      (current_A > 5)   interval = 100;
    else if (current_A > 2)   interval = 250;
    else if (current_A > 0.5) interval = 500;
    if (now - blueTimer > interval) {
      blueTimer = now;
      blueState = !blueState;
      digitalWrite(LED_BLUE, blueState);
    }
  } else {
    digitalWrite(LED_BLUE, LOW);
  }

  // ── RED LED ───────────────────────────────────────────────
  if (tamperState) {
    // Fast blink during tamper
    if (now - redTimer > 200) {
      redTimer = now;
      redState = !redState;
      digitalWrite(LED_RED, redState);
    }
  } else if (!relayState) {
    digitalWrite(LED_RED, HIGH);  // steady on = relay off / locked
  } else {
    digitalWrite(LED_RED, LOW);
  }

  // ── ENERGY ACCUMULATION ───────────────────────────────────
  if (now - lastEnergyUpdate >= ENERGY_UPDATE_INTERVAL) {
    updateEnergyAccumulation();
  }

  // ── BILLING SYNC ──────────────────────────────────────────
  if (now - lastBillingSync >= BILLING_SYNC_INTERVAL) {
    publishBillingSync();
    lastBillingSync = now;
  }

  // ── HEARTBEAT ─────────────────────────────────────────────
  if (now - lastHeartbeat >= HEARTBEAT_INTERVAL) {
    publishHeartbeat();
    lastHeartbeat = now;
  }

  // ── TELEMETRY ─────────────────────────────────────────────
  if (now - lastTelemetry >= TELEMETRY_INTERVAL) {
    publishTelemetry();
    lastTelemetry = now;
  }

  // ── LCD UPDATE (every 1 s) ────────────────────────────────
  if (now - lcdTimer > 1000UL) {
    lcdTimer = now;
    lcd.clear();

    // ─── Screen 0: Power Readings ──────────────────────────
    if (screen == 0) {
      lcd.setCursor(0, 0);
      lcd.print("V:");
      lcd.print(isnan(voltage)   ? 0 : voltage,   1);
      lcd.print(" I:");
      lcd.print(isnan(current_A) ? 0 : current_A, 2);

      lcd.setCursor(0, 1);
      lcd.print("P:");
      lcd.print(isnan(power_W) ? 0 : power_W, 1);
      lcd.print("W PF:");
      lcd.print(isnan(pf) ? 0 : pf, 2);

      lcd.setCursor(0, 2);
      lcd.print("Energy:");
      lcd.print(isnan(energy_kWh) ? 0 : energy_kWh, 2);
      lcd.print("kWh");

      lcd.setCursor(0, 3);
      lcd.print("Load:");
      lcd.print(relayState ? "CONNECTED" : "DISCONNECTED");
    }

    // ─── Screen 1: Date / Time ────────────────────────────
    else if (screen == 1) {
      DateTime dt = rtc.now();
      lcd.setCursor(0, 0);
      lcd.print("Date:");
      lcd.print(dt.day());   lcd.print("/");
      lcd.print(dt.month()); lcd.print("/");
      lcd.print(dt.year());

      lcd.setCursor(0, 1);
      lcd.print("Time:");
      if (dt.hour()   < 10) lcd.print("0"); lcd.print(dt.hour());   lcd.print(":");
      if (dt.minute() < 10) lcd.print("0"); lcd.print(dt.minute()); lcd.print(":");
      if (dt.second() < 10) lcd.print("0"); lcd.print(dt.second());
    }

    // ─── Screen 2: Stored Energy ──────────────────────────
    else if (screen == 2) {
      lcd.setCursor(0, 0); lcd.print("Stored Energy:");
      lcd.setCursor(0, 1);
      lcd.print(storedEnergy, 2); lcd.print(" kWh");
      lcd.setCursor(0, 2); lcd.print("Period Energy:");
      lcd.setCursor(0, 3);
      lcd.print(accumulatedEnergyWh, 1); lcd.print(" Wh");
    }

    // ─── Screen 3: Device Info  (NO IP address shown) ────
    else if (screen == 3) {
      lcd.setCursor(0, 0);
      lcd.print("WiFi:");
      lcd.print(WiFi.status() == WL_CONNECTED ? "Connected" : "Offline");

      lcd.setCursor(0, 1);
      lcd.print("MQTT:");
      lcd.print(client.connected() ? "Connected" : "Offline");

      lcd.setCursor(0, 2);
      lcd.print("Dev:");
      lcd.print(deviceId);

      lcd.setCursor(0, 3);
      lcd.print("Ver:");
      lcd.print(firmwareVersion);
    }

    // ─── Screen 4: Status ────────────────────────────────
    else if (screen == 4) {
      lcd.setCursor(0, 0);
      lcd.print("Tamper:");
      lcd.print(tamperState ? "DETECTED!" : "OK");

      lcd.setCursor(0, 1);
      lcd.print("Relay:");
      lcd.print(relayState ? "CONNECTED" : "DISCONNECTED");

      lcd.setCursor(0, 2);
      lcd.print("Modules:");
      lcd.print(pairedCount);

      lcd.setCursor(0, 3);
      if (relayLockedByServer) lcd.print("SERVER LOCKED");
      else if (tamperState)    lcd.print("TAMPER LOCKOUT");
      else                     lcd.print("System Normal");
    }
  }
}
