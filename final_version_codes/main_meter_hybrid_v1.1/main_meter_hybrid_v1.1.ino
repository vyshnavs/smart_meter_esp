// ============================================================
//  SMART ENERGY METER — MAIN METER (ESP32) — HYBRID v1.1
//
//  Based on working v1.0.3 boot, adds v1.1.0 features:
//  ✅ Relay lock state persisted in NVS (like v1.0.3)
//  ✅ Locked relay cannot be controlled by button or web
//  ✅ Internet time sync (NTP) + RTC update (in loop, not boot)
//  ✅ Billing sync queue - retry on failure
//  ✅ All features preserved from v1.0.3, boot stays SIMPLE & FAST
//  ✅ Leakage current detection — trips relay, manual reset via button
//  ✅ v2.0: Module details persisted on pair, cleared on unpair
//  ✅ v2.0: New firmware flash auto-clears all module pairings
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
#include <time.h>
#include <sys/time.h>

// ============================================================
//  FIRMWARE VERSION — bump this string when flashing new code
//  to automatically clear all paired module state on boot
// ============================================================
const char* FIRMWARE_VERSION_KEY = "2.0.0";   // used for NVS reset check

// ============================================================
//  USER CONFIG — edit these before flashing
// ============================================================
const char* ssid            = "boom";
const char* password        = "123123123033";
const char* mqttServer      = "20.39.192.14";
const int   mqttPort        = 1883;
const int   deviceId        = 44188662;
const char* secretKey       = "6vM3ihdppGGtwf54Yu";
const char* firmwareVersion = "1.1.0";
const char* deploymentDate  = "2025-02-15";

// NTP Server for time sync
const char* ntpServer       = "pool.ntp.org";
const long  gmtOffset_sec   = 5.5 * 3600;           // IST (UTC+5:30)
const int   daylightOffset_sec = 0;

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
#define CT_PIN       36     // ESP32 ADC pin connected to Op-Amp output

// ============================================================
//  LEAKAGE CURRENT DETECTION — CIRCUIT PARAMETERS
// ============================================================
const float V_REF = 3.3;
const int ADC_RESOLUTION = 4095;
const float BURDEN_RESISTOR = 330.0;
const float OPAMP_GAIN = 41.0;
const float CT_RATIO = 1000.0;
float CALIBRATION_FACTOR = 1.48000;
const float NOISE_FILTER_MA = 1.0;

// ============================================================
//  LEAKAGE TRIP THRESHOLD & STATE
// ============================================================
const float LEAKAGE_THRESHOLD_MA = 20.0;
bool leakageTripped = false;
#define LEAKAGE_ALERT_INTERVAL 10000UL
unsigned long lastLeakageAlert = 0;

// ============================================================
//  FAULT DETECTION THRESHOLDS
// ============================================================
const float OVERVOLTAGE_THRESHOLD    = 253.0;
const float UNDERVOLTAGE_THRESHOLD   = 195.0;
const float OVERCURRENT_THRESHOLD    = 20.0;
const float LOW_PF_THRESHOLD         = 0.60;

#define FAULT_ALERT_INTERVAL 30000UL
unsigned long lastOvervoltageAlert   = 0;
unsigned long lastUndervoltageAlert  = 0;
unsigned long lastOvercurrentAlert   = 0;
unsigned long lastLowPFAlert         = 0;

// ============================================================
//  BILLING QUEUE STRUCTURES
// ============================================================
struct BillingQueueItem {
  unsigned long seqNo;
  unsigned long periodStart;
  unsigned long periodEnd;
  float energyWh;
  uint32_t timestamp;
};

#define MAX_BILLING_QUEUE 2
BillingQueueItem billingQueue[MAX_BILLING_QUEUE];
int billingQueueCount = 0;

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
bool tamperState         = false;
bool lastTamperPin       = false;
bool relayLockedByServer = false;
bool faultActive         = false;

// ============================================================
//  LCD SCREEN INDEX  (0-4)
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
unsigned long ntpSyncTimer   = 0;
unsigned long billingQueueRetry = 0;

// ============================================================
//  TAMPER THROTTLE
// ============================================================
#define TAMPER_ALERT_INTERVAL 10000UL
unsigned long lastTamperAlert = 0;

// ============================================================
//  BILLING
// ============================================================
Preferences preferences;

const unsigned long BILLING_SYNC_INTERVAL = 60000UL;
const unsigned long NTP_SYNC_INTERVAL     = 3600000UL;
const unsigned long BILLING_QUEUE_RETRY   = 30000UL;

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
String relayStatusTopic = "meter/" + String(deviceId) + "/relay_status";

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
typedef struct { char type[16]; char moduleId[16]; } UnpairCommand;
typedef struct {
  char type[16];
  char moduleId[16];
  float voltage;
  float current;
  float power;
  bool  relayState;
  char  health[8];
  int   unitIndex;
} ModuleTelemetry;
typedef struct {
  char type[16];
  char moduleId[16];
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

// Leakage current monitoring
double leakageCurrent_mA = 0.0;

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
void publishBillingSync();
void syncTimeWithNTP();
void publishLeakageAlert(float measuredMa);

// ============================================================
//  FIRMWARE VERSION RESET
//  Call once at boot. If FIRMWARE_VERSION_KEY differs from
//  what is stored in NVS, all module pairing data is wiped.
//  This ensures a clean slate every time new code is flashed.
// ============================================================
void checkFirmwareReset() {
  preferences.begin("meta", false);
  String stored = preferences.getString("fwkey", "");
  preferences.end();

  if (stored != FIRMWARE_VERSION_KEY) {
    Serial.println("[Boot] 🆕 New firmware — wiping module pairing state");
    Serial.print  ("[Boot]    Old key: "); Serial.println(stored);
    Serial.print  ("[Boot]    New key: "); Serial.println(FIRMWARE_VERSION_KEY);

    preferences.begin("modules", false); preferences.clear(); preferences.end();

    preferences.begin("meta", false);
    preferences.putString("fwkey", FIRMWARE_VERSION_KEY);
    preferences.end();

    pairedCount = 0;
    memset(pairedModules, 0, sizeof(pairedModules));
    Serial.println("[Boot] ✅ Module pairing state cleared");
  } else {
    Serial.println("[Boot] ✅ Firmware key matches — module state preserved");
  }
}

// ============================================================
//  NVS — RELAY STATE & BILLING (v1.0.3 pattern)
// ============================================================
void loadRelayState() {
  preferences.begin("relay", false);
  relayState = preferences.getBool("state", true);
  relayLockedByServer = preferences.getBool("locked", false);
  preferences.end();
  Serial.print("Relay state loaded: ");
  Serial.println(relayState ? "ON" : "OFF");
  Serial.print("Relay locked: ");
  Serial.println(relayLockedByServer ? "YES" : "NO");
  if (relayLockedByServer) {
    Serial.println("  ⚠️ Relay LOCKED by server");
  }
}

void clearRelayLock() {
  preferences.begin("relay", false);
  preferences.putBool("locked", false);
  preferences.end();
  relayLockedByServer = false;
  relayState = true;
  digitalWrite(RELAY_PIN, LOW);
  Serial.println("✅ [DEBUG] Relay lock cleared manually");
}

void testRelayLogic() {
  Serial.println("\n[RELAY TEST] ==== RELAY GPIO LOGIC TEST ====");
  Serial.println("[RELAY TEST] Setting GPIO 26 to HIGH...");
  digitalWrite(RELAY_PIN, HIGH);
  delay(100);
  Serial.print("[RELAY TEST] GPIO 26 state after HIGH: ");
  Serial.println(digitalRead(RELAY_PIN) ? "HIGH (1)" : "LOW (0)");
  delay(500);
  Serial.println("[RELAY TEST] Setting GPIO 26 to LOW...");
  digitalWrite(RELAY_PIN, LOW);
  delay(100);
  Serial.print("[RELAY TEST] GPIO 26 state after LOW: ");
  Serial.println(digitalRead(RELAY_PIN) ? "HIGH (1)" : "LOW (0)");
  Serial.println("[RELAY TEST] ==== END TEST ====\n");
  digitalWrite(RELAY_PIN, relayState ? LOW : HIGH);
}

void saveRelayState() {
  preferences.begin("relay", false);
  preferences.putBool("state",  relayState);
  preferences.putBool("locked", relayLockedByServer);
  preferences.end();

  preferences.begin("relay", false);
  bool vs = preferences.getBool("state",  true);
  bool vl = preferences.getBool("locked", false);
  preferences.end();

  Serial.print("[NVS] Relay state saved | state=");
  Serial.print(relayState ? "ON" : "OFF");
  Serial.print(" | locked=");
  Serial.println(relayLockedByServer ? "YES" : "NO");

  if (vs != relayState || vl != relayLockedByServer) {
    Serial.println("[NVS] ⚠️ WARNING: Verification failed!");
  } else {
    Serial.println("[NVS] ✅ Verification OK");
  }
}

// ============================================================
//  NVS — PAIRED MODULES
//  Full module details (moduleId, MAC, capacity) are stored
//  on pair and removed on unpair.
// ============================================================
void loadPairedModules() {
  preferences.begin("modules", false);
  pairedCount = preferences.getInt("count", 0);
  for (int i = 0; i < pairedCount && i < MAX_MODULES; i++) {
    String key = "mod" + String(i);
    if (preferences.getBytesLength(key.c_str()) == sizeof(PairedModule)) {
      preferences.getBytes(key.c_str(), &pairedModules[i], sizeof(PairedModule));
      Serial.print("[Modules] Loaded: ");
      Serial.print(pairedModules[i].moduleId);
      Serial.print(" MAC: ");
      for (int j = 0; j < 6; j++) {
        Serial.printf("%02X", pairedModules[i].macAddr[j]);
        if (j < 5) Serial.print(":");
      }
      Serial.print(" Cap:"); Serial.println(pairedModules[i].capacity);
    }
  }
  preferences.end();
  Serial.print("[Modules] Total paired: "); Serial.println(pairedCount);
}

void savePairedModules() {
  preferences.begin("modules", false);
  preferences.putInt("count", pairedCount);
  for (int i = 0; i < pairedCount; i++) {
    String key = "mod" + String(i);
    preferences.putBytes(key.c_str(), &pairedModules[i], sizeof(PairedModule));
  }
  preferences.end();
  Serial.print("[Modules] Saved "); Serial.print(pairedCount); Serial.println(" module(s) to NVS");
}

// Remove one module from NVS by shifting the array and rewriting
void removeModuleFromNVS(int index) {
  for (int j = index; j < pairedCount - 1; j++) {
    pairedModules[j] = pairedModules[j + 1];
  }
  pairedCount--;
  memset(&pairedModules[pairedCount], 0, sizeof(PairedModule)); // clear last slot
  savePairedModules();
  Serial.println("[Modules] Module removed from NVS");
}

// ============================================================
//  BILLING QUEUE
// ============================================================
void loadBillingQueue() {
  preferences.begin("billing", false);
  billingQueueCount = preferences.getInt("queueCount", 0);
  for (int i = 0; i < billingQueueCount && i < MAX_BILLING_QUEUE; i++) {
    billingQueue[i].seqNo       = preferences.getULong(("qSeq"   + String(i)).c_str(), 0);
    billingQueue[i].periodStart = preferences.getULong(("qStart" + String(i)).c_str(), 0);
    billingQueue[i].periodEnd   = preferences.getULong(("qEnd"   + String(i)).c_str(), 0);
    billingQueue[i].energyWh    = preferences.getFloat(("qEnergy"+ String(i)).c_str(), 0.0);
    billingQueue[i].timestamp   = millis() / 1000;
  }
  preferences.end();
  Serial.print("[Billing] Loaded queue | count="); Serial.println(billingQueueCount);
}

void saveBillingQueue() {
  preferences.begin("billing", false);
  preferences.putInt("queueCount", billingQueueCount);
  for (int i = 0; i < billingQueueCount && i < MAX_BILLING_QUEUE; i++) {
    preferences.putULong(("qSeq"   + String(i)).c_str(), billingQueue[i].seqNo);
    preferences.putULong(("qStart" + String(i)).c_str(), billingQueue[i].periodStart);
    preferences.putULong(("qEnd"   + String(i)).c_str(), billingQueue[i].periodEnd);
    preferences.putFloat(("qEnergy"+ String(i)).c_str(), billingQueue[i].energyWh);
  }
  preferences.end();
}

void addToBillingQueue(float energyWh, unsigned long periodStart,
                       unsigned long periodEnd, unsigned long seqNo) {
  if (billingQueueCount >= MAX_BILLING_QUEUE) {
    for (int i = 0; i < MAX_BILLING_QUEUE - 1; i++) billingQueue[i] = billingQueue[i + 1];
    billingQueueCount = MAX_BILLING_QUEUE - 1;
  }
  billingQueue[billingQueueCount] = { seqNo, periodStart, periodEnd, energyWh, (uint32_t)(millis()/1000) };
  billingQueueCount++;
  saveBillingQueue();
  Serial.print("[Billing] Added to queue | Total: "); Serial.println(billingQueueCount);
}

void retryBillingQueue() {
  if (billingQueueCount == 0 || !client.connected()) return;
  Serial.println("[Billing] Retrying queued syncs...");
  for (int i = 0; i < billingQueueCount; i++) {
    StaticJsonDocument<256> doc;
    doc["deviceId"]         = deviceId;
    doc["sequenceNo"]       = billingQueue[i].seqNo;
    doc["periodStart"]      = billingQueue[i].periodStart;
    doc["periodEnd"]        = billingQueue[i].periodEnd;
    doc["energyConsumedWh"] = round(billingQueue[i].energyWh * 10) / 10.0;
    doc["meterState"]       = relayLockedByServer ? "LOCKED" : "ACTIVE";
    doc["relayState"]       = relayState;
    doc["faultActive"]      = faultActive;
    doc["firmwareVersion"]  = firmwareVersion;
    char buffer[256]; serializeJson(doc, buffer);
    if (client.publish(billingSyncTopic.c_str(), buffer, true)) {
      Serial.print("[Billing] ✅ Sent queued sync #"); Serial.println(billingQueue[i].seqNo);
      for (int j = i; j < billingQueueCount - 1; j++) billingQueue[j] = billingQueue[j + 1];
      billingQueueCount--; i--;
    } else { break; }
  }
  saveBillingQueue();
}

// ============================================================
//  NTP TIME SYNC
// ============================================================
void syncTimeWithNTP() {
  if (WiFi.status() != WL_CONNECTED) { Serial.println("[NTP] ⚠️ WiFi not connected"); return; }
  Serial.println("[NTP] 🔄 Syncing time...");
  configTime(gmtOffset_sec, daylightOffset_sec, ntpServer);
  time_t now = time(nullptr);
  int timeout = 0;
  while (now < 24 * 3600 && timeout < 20) { delay(500); now = time(nullptr); timeout++; }
  if (now > 24 * 3600) {
    rtc.adjust(DateTime(now));
    Serial.print("[NTP] ✅ Time synced: "); Serial.println(ctime(&now));
  } else { Serial.println("[NTP] ⚠️ NTP timeout — will retry"); }
}

// ============================================================
//  NVS — BILLING STATE
// ============================================================
void loadBillingState() {
  preferences.begin("billing", false);
  accumulatedEnergyWh  = preferences.getFloat("energyWh",    0.0);
  billingSequenceNo    = preferences.getULong("seqNo",       0);
  periodStartTimestamp = preferences.getULong("periodStart", 0);
  preferences.end();
  Serial.print("Billing energy loaded: "); Serial.print(accumulatedEnergyWh); Serial.println(" Wh");
}

void saveBillingState() {
  preferences.begin("billing", false);
  preferences.putFloat("energyWh",    accumulatedEnergyWh);
  preferences.putULong("seqNo",       billingSequenceNo);
  preferences.putULong("periodStart", periodStartTimestamp);
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
//  WiFi
// ============================================================
void connectWiFi() {
  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid, password);
  Serial.print("Connecting to WiFi");
  int attempts = 0;
  while (WiFi.status() != WL_CONNECTED && attempts < 20) { delay(500); Serial.print("."); attempts++; }
  if (WiFi.status() == WL_CONNECTED) { Serial.println("\nWiFi connected"); }
  else { Serial.println("\nWiFi failed — will retry in loop"); }
}

// ============================================================
//  MQTT
// ============================================================
void setupMQTT() {
  client.setServer(mqttServer, mqttPort);
  client.setCallback(onMQTTMessage);
  client.setBufferSize(2048);
}

void connectMQTT() {
  int attempts = 0;
  while (!client.connected() && attempts < 3) {
    Serial.println("Connecting to MQTT...");
    if (client.connect(String(deviceId).c_str(), String(deviceId).c_str(), secretKey)) {
      Serial.println("MQTT connected");
      client.subscribe(commandTopic.c_str());
      publishRelayStatus();
      retryBillingQueue();
    } else {
      Serial.print("MQTT failed rc="); Serial.println(client.state());
      delay(3000); attempts++;
    }
  }
}

// ============================================================
//  PUBLISH — RELAY STATUS (retained)
// ============================================================
void publishRelayStatus() {
  StaticJsonDocument<160> doc;
  doc["deviceId"]    = deviceId;
  doc["relayState"]  = relayState;
  doc["relayLocked"] = relayLockedByServer;
  doc["tamperActive"]= tamperState;
  doc["source"]      = "mainMeter";
  char buffer[160]; serializeJson(doc, buffer);
  client.publish(relayStatusTopic.c_str(), buffer, true);
  Serial.print("Relay status: "); Serial.println(relayState ? "ON" : "OFF");
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
  char buffer[128]; serializeJson(doc, buffer);
  client.publish(ackTopic.c_str(), buffer);
  Serial.print("ACK: "); Serial.print(command); Serial.print(" | "); Serial.println(status);
}

// ============================================================
//  PUBLISH — FAULT
// ============================================================
void publishFault(const char* source, const char* moduleId, int unitNumber,
                  const char* faultType, const char* severity,
                  const char* description, float measuredValue,
                  float thresholdValue, const char* unit) {
  StaticJsonDocument<512> doc;
  doc["source"]      = source;
  if (moduleId != nullptr)                  doc["moduleId"]       = moduleId;
  if (unitNumber >= 0)                      doc["unitNumber"]     = unitNumber;
  doc["faultType"]   = faultType;
  doc["severity"]    = severity;
  doc["description"] = description;
  if (measuredValue  > 0)                   doc["measuredValue"]  = measuredValue;
  if (thresholdValue > 0)                   doc["thresholdValue"] = thresholdValue;
  if (unit != nullptr && strlen(unit) > 0)  doc["unit"]           = unit;
  char buffer[512]; serializeJson(doc, buffer);
  if (client.publish(faultTopic.c_str(), buffer)) {
    Serial.print("Fault published: "); Serial.println(faultType);
  } else { Serial.println("Fault publish failed"); }
}

// ============================================================
//  PUBLISH — TAMPER ALERT
// ============================================================
void publishTamperAlert() {
  StaticJsonDocument<256> doc;
  char tamperId[64];
  snprintf(tamperId, sizeof(tamperId), "TAMPER_%d_%lu", deviceId, millis());
  doc["tamperId"]    = tamperId;
  doc["tamperType"]  = "physical_breach";
  doc["severity"]    = "critical";
  doc["description"] = "Enclosure opened - unauthorized access detected";
  char buffer[256]; serializeJson(doc, buffer);
  if (client.publish(tamperTopic.c_str(), buffer)) { Serial.println("TAMPER ALERT published"); }
  else { Serial.println("Tamper alert publish failed"); }
}

// ============================================================
//  PUBLISH — LEAKAGE ALERT
// ============================================================
void publishLeakageAlert(float measuredMa) {
  publishFault("mainMeter", nullptr, -1,
               "leakage_current", "critical",
               "Earth leakage current exceeded threshold — relay tripped for safety",
               measuredMa, LEAKAGE_THRESHOLD_MA, "mA");
  Serial.print("[Leakage] ⚠️ ALERT | measured="); Serial.print(measuredMa, 2);
  Serial.print(" mA | threshold="); Serial.print(LEAKAGE_THRESHOLD_MA, 1); Serial.println(" mA");
}

// ============================================================
//  ENERGY ACCUMULATION
// ============================================================
void updateEnergyAccumulation() {
  unsigned long now = millis();
  if (lastEnergyUpdate == 0) { lastEnergyUpdate = now; return; }
  if (relayState && !relayLockedByServer && !isnan(power_W)) {
    unsigned long deltaMs = now - lastEnergyUpdate;
    accumulatedEnergyWh += power_W * (deltaMs / 3600000.0f);
    static unsigned long lastSave = 0;
    if (now - lastSave > 10000UL) { saveBillingState(); lastSave = now; }
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
  char buffer[256]; serializeJson(doc, buffer);
  if (client.publish(billingSyncTopic.c_str(), buffer, true)) {
    Serial.print("Billing sync | Energy: "); Serial.print(accumulatedEnergyWh); Serial.println(" Wh");
    accumulatedEnergyWh = 0.0; periodStartTimestamp = nowSec; billingSequenceNo++; saveBillingState();
  } else {
    Serial.println("[Billing] ⚠️ Publish failed — queuing");
    addToBillingQueue(round(accumulatedEnergyWh*10)/10.0, periodStartTimestamp, nowSec, billingSequenceNo);
    accumulatedEnergyWh = 0.0; periodStartTimestamp = nowSec; billingSequenceNo++; saveBillingState();
  }
}

// ============================================================
//  PUBLISH — TELEMETRY
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
  mainMeter["leakageCurrent_mA"]   = (float)(round(leakageCurrent_mA * 100) / 100.0);
  mainMeter["leakageTripped"]      = leakageTripped;
  JsonArray modules = doc.createNestedArray("modules");
  for (int i = 0; i < pairedCount; i++) {
    if (!pairedModules[i].isPaired) continue;
    bool hasData = false; JsonArray units;
    for (int j = 0; j < moduleDataCount; j++) {
      if (strcmp(moduleDataCache[j].moduleId, pairedModules[i].moduleId) == 0 &&
          millis() - moduleDataCache[j].lastUpdate < 10000UL) {
        if (!hasData) {
          JsonObject module = modules.createNestedObject();
          module["moduleId"] = pairedModules[i].moduleId;
          units = module.createNestedArray("units");
          hasData = true;
        }
        JsonObject unit = units.createNestedObject();
        unit["unitIndex"]  = moduleDataCache[j].unitIndex;
        unit["voltage"]    = (float)(round(moduleDataCache[j].voltage  * 10)  / 10.0f);
        unit["current"]    = (float)(round(moduleDataCache[j].current  * 100) / 100.0f);
        // Power = module V × module I × main meter PF (main meter PF is more accurate).
        // fabsf() ensures the result is always positive.
        unit["power"]      = (float)(round(fabsf(moduleDataCache[j].voltage * moduleDataCache[j].current * ((!isnan(pf) && pf > 0.0f) ? pf : 1.0f))));
        unit["relayState"] = moduleDataCache[j].relayState;
        unit["health"]     = moduleDataCache[j].health;
      }
    }
  }
  char buffer[2048]; serializeJson(doc, buffer);
  if (client.publish(telemetryTopic.c_str(), buffer)) { Serial.println("Telemetry published"); }
  else { Serial.print("Telemetry failed, MQTT state: "); Serial.println(client.state()); }
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
  Serial.print("MQTT msg on: "); Serial.println(topic);
  Serial.print("[MQTT DEBUG] Raw payload ("); Serial.print(length); Serial.print(" bytes): ");
  for (unsigned int i = 0; i < length; i++) Serial.print((char)payload[i]);
  Serial.println();

  StaticJsonDocument<256> doc;
  DeserializationError error = deserializeJson(doc, payload, length);
  if (error) {
    Serial.print("[MQTT ERROR] JSON parse failed: "); Serial.println(error.c_str()); return;
  }

  String cmd = doc["command"] | "";
  if (cmd.length() == 0) cmd = doc["cmd"]    | "";
  if (cmd.length() == 0) cmd = doc["action"] | "";
  if (cmd.length() == 0) cmd = doc["type"]   | "";
  cmd.toLowerCase();

  Serial.print("[MQTT DEBUG] Parsed command: '"); Serial.print(cmd); Serial.println("'");

  // ── MAIN RELAY CONTROL ──────────────────────────────────────
  if (cmd == "relay_on") {
    if (relayLockedByServer) { publishCommandAck("relay_on", "FAILED_LOCKED", relayState); return; }
    relayState = true; digitalWrite(RELAY_PIN, LOW);
    saveRelayState(); publishCommandAck("relay_on", "SUCCESS", relayState); publishRelayStatus();
  }
  else if (cmd == "relay_off") {
    if (relayLockedByServer) { publishCommandAck("relay_off", "FAILED_LOCKED", relayState); return; }
    relayState = false; digitalWrite(RELAY_PIN, HIGH);
    saveRelayState(); publishCommandAck("relay_off", "SUCCESS", relayState); publishRelayStatus();
  }
  else if (cmd == "unlock_relay") {
    relayLockedByServer = false; relayState = true;
    digitalWrite(RELAY_PIN, LOW); delay(50);
    saveRelayState();
    publishCommandAck("unlock_relay", "SUCCESS", relayState); publishRelayStatus();
    Serial.println("[Relay] 🔓 UNLOCKED by server!");
  }
  else if (cmd == "lock_relay") {
    relayLockedByServer = true; relayState = false;
    digitalWrite(RELAY_PIN, HIGH); delay(50);
    saveRelayState();
    publishCommandAck("lock_relay", "SUCCESS", relayState); publishRelayStatus();
    Serial.println("[Relay] 🔒 LOCKED by server!");
  }
  else if (cmd == "debug_clear_lock") {
    clearRelayLock();
    publishCommandAck("debug_clear_lock", "SUCCESS", relayState); publishRelayStatus();
  }
  else if (cmd == "test_relay") {
    testRelayLogic(); publishCommandAck("test_relay", "SUCCESS", relayState);
  }
  // ── MODULE RELAY CONTROL ─────────────────────────────────────
  else if (cmd == "module_relay_control") {
    if (relayLockedByServer) { Serial.println("Cannot control modules — main relay locked"); return; }
    String moduleId   = doc["moduleId"]  | "";
    int    unitNumber = doc["unitNumber"]| -1;
    bool   state      = doc["state"]     | false;
    if (moduleId.length() == 0 || unitNumber < 0) return;
    for (int i = 0; i < pairedCount; i++) {
      if (strcmp(pairedModules[i].moduleId, moduleId.c_str()) == 0) {
        ModuleRelayControl relayCmd;
        memset(&relayCmd, 0, sizeof(relayCmd));
        strcpy(relayCmd.type, "RELAY_CTRL");
        strncpy(relayCmd.moduleId, moduleId.c_str(), 15);
        relayCmd.unitNumber = unitNumber; relayCmd.state = state;
        esp_now_send(pairedModules[i].macAddr, (uint8_t*)&relayCmd, sizeof(relayCmd));
        Serial.println("Relay control sent to module"); break;
      }
    }
  }
  // ── SCAN MODULES ─────────────────────────────────────────────
  else if (cmd == "scan_modules") {
    ScanRequest req; memset(&req, 0, sizeof(req)); strcpy(req.type, "SCAN_REQ");
    uint8_t broadcastAddr[6] = {0xFF,0xFF,0xFF,0xFF,0xFF,0xFF};
    esp_now_send(broadcastAddr, (uint8_t*)&req, sizeof(req));
    Serial.println("Scan broadcast sent");
  }
  // ── PAIR MODULE ───────────────────────────────────────────────
  else if (cmd == "pair_module") {
    String moduleId = doc["moduleId"] | "";
    String secret   = doc["secret"]   | "";
    if (moduleId.length() == 0 || secret.length() == 0) return;
    PairRequest pairReq; memset(&pairReq, 0, sizeof(pairReq));
    strcpy(pairReq.type, "PAIR_REQ");
    strncpy(pairReq.moduleId, moduleId.c_str(), 15);
    strncpy(pairReq.secret,   secret.c_str(),   31);
    uint8_t broadcastAddr[6] = {0xFF,0xFF,0xFF,0xFF,0xFF,0xFF};
    esp_now_send(broadcastAddr, (uint8_t*)&pairReq, sizeof(pairReq));
    Serial.print("Pair request sent: "); Serial.println(moduleId);
  }
  // ── UNPAIR MODULE ─────────────────────────────────────────────
  else if (cmd == "unpair_module") {
    String moduleId = doc["moduleId"] | "";
    if (moduleId.length() == 0) return;
    for (int i = 0; i < pairedCount; i++) {
      if (strcmp(pairedModules[i].moduleId, moduleId.c_str()) == 0) {
        // Send unpair command to slave
        UnpairCommand unpairCmd; memset(&unpairCmd, 0, sizeof(unpairCmd));
        strcpy(unpairCmd.type, "UNPAIR");
        strncpy(unpairCmd.moduleId, moduleId.c_str(), 15);
        esp_now_send(pairedModules[i].macAddr, (uint8_t*)&unpairCmd, sizeof(unpairCmd));
        esp_now_del_peer(pairedModules[i].macAddr);
        // Remove from NVS — shifts array and rewrites
        removeModuleFromNVS(i);
        publishCommandAck("unpair_module", "SUCCESS", relayState);
        Serial.print("[Modules] Unpaired and removed from NVS: "); Serial.println(moduleId);
        return;
      }
    }
    publishCommandAck("unpair_module", "FAILED", relayState);
  }
  else {
    Serial.print("[MQTT WARNING] Unknown command: '"); Serial.print(cmd); Serial.println("'");
  }
}

// ============================================================
//  ESP-NOW RECEIVE CALLBACK
// ============================================================
void onESPNowRecv(const esp_now_recv_info_t* info, const uint8_t* data, int len) {
  char msgType[16]; memcpy(msgType, data, 16);

  if (strcmp(msgType, "SCAN_RESP") == 0) {
    ScanResponse resp; memcpy(&resp, data, sizeof(resp));
    char macStr[18];
    snprintf(macStr, sizeof(macStr), "%02X:%02X:%02X:%02X:%02X:%02X",
             info->src_addr[0], info->src_addr[1], info->src_addr[2],
             info->src_addr[3], info->src_addr[4], info->src_addr[5]);
    StaticJsonDocument<128> doc;
    doc["moduleId"] = resp.moduleId; doc["capacity"] = resp.capacity; doc["mac"] = macStr;
    char buffer[128]; serializeJson(doc, buffer);
    client.publish(scanTopic.c_str(), buffer);
    Serial.print("Scan response: "); Serial.println(resp.moduleId);
  }
  else if (strcmp(msgType, "PAIR_ACK") == 0) {
    PairAck ack; memcpy(&ack, data, sizeof(ack));
    if (ack.success && pairedCount < MAX_MODULES) {
      // Store full module details in pairedModules array
      strncpy(pairedModules[pairedCount].moduleId, ack.moduleId, 15);
      memcpy(pairedModules[pairedCount].macAddr, info->src_addr, 6);
      pairedModules[pairedCount].capacity = 0;  // updated when telemetry arrives
      pairedModules[pairedCount].isPaired = true;

      esp_now_peer_info_t peerInfo; memset(&peerInfo, 0, sizeof(peerInfo));
      memcpy(peerInfo.peer_addr, info->src_addr, 6);
      peerInfo.channel = WiFi.channel(); peerInfo.encrypt = false; peerInfo.ifidx = WIFI_IF_STA;

      if (esp_now_add_peer(&peerInfo) == ESP_OK) {
        pairedCount++;
        savePairedModules();   // ← persist to NVS immediately
        Serial.print("[Modules] ✅ Paired & saved: "); Serial.println(ack.moduleId);
        Serial.print("[Modules]    MAC: ");
        for (int i = 0; i < 6; i++) {
          Serial.printf("%02X", info->src_addr[i]);
          if (i < 5) Serial.print(":");
        }
        Serial.println();
      }
    }
    StaticJsonDocument<128> doc;
    doc["moduleId"] = ack.moduleId; doc["success"] = ack.success;
    char buffer[128]; serializeJson(doc, buffer);
    client.publish(pairAckTopic.c_str(), buffer);
  }
  else if (strcmp(msgType, "TELEMETRY") == 0) {
    ModuleTelemetry telem; memcpy(&telem, data, sizeof(telem));
    bool found = false;
    for (int i = 0; i < moduleDataCount; i++) {
      if (strcmp(moduleDataCache[i].moduleId, telem.moduleId) == 0 &&
          moduleDataCache[i].unitIndex == telem.unitIndex) {
        moduleDataCache[i].voltage = telem.voltage; moduleDataCache[i].current = telem.current;
        moduleDataCache[i].power = telem.power; moduleDataCache[i].relayState = telem.relayState;
        strcpy(moduleDataCache[i].health, telem.health);
        moduleDataCache[i].lastUpdate = millis(); found = true; break;
      }
    }
    if (!found && moduleDataCount < (MAX_MODULES * 10)) {
      strcpy(moduleDataCache[moduleDataCount].moduleId, telem.moduleId);
      moduleDataCache[moduleDataCount].unitIndex = telem.unitIndex;
      moduleDataCache[moduleDataCount].voltage = telem.voltage;
      moduleDataCache[moduleDataCount].current = telem.current;
      moduleDataCache[moduleDataCount].power = telem.power;
      moduleDataCache[moduleDataCount].relayState = telem.relayState;
      strcpy(moduleDataCache[moduleDataCount].health, telem.health);
      moduleDataCache[moduleDataCount].lastUpdate = millis();
      moduleDataCount++;
    }
  }
  else if (strcmp(msgType, "FAULT") == 0) {
    ModuleFault fault; memcpy(&fault, data, sizeof(fault));
    publishFault("module", fault.moduleId, fault.unitNumber,
                 fault.faultType, fault.severity, fault.description,
                 fault.measuredValue, fault.thresholdValue, fault.unit);
    Serial.print("Module fault forwarded: "); Serial.println(fault.faultType);
  }
  else if (strcmp(msgType, "RELAY_ACK") == 0) {
    ModuleRelayAck ack; memcpy(&ack, data, sizeof(ack));
    StaticJsonDocument<256> doc;
    doc["command"] = "module_relay_ack"; doc["moduleId"] = ack.moduleId;
    doc["unitNumber"] = ack.unitNumber;
    doc["status"] = ack.success ? "SUCCESS" : "FAILED"; doc["relayState"] = ack.relayState;
    char buffer[256]; serializeJson(doc, buffer);
    client.publish(ackTopic.c_str(), buffer);
    Serial.print("Module relay ack: "); Serial.println(ack.success ? "SUCCESS" : "FAILED");
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

  // ── Pin modes ───────────────────────────────────────────────
  pinMode(RELAY_PIN,  OUTPUT);
  pinMode(BUZZER_PIN, OUTPUT);
  pinMode(LED_GREEN,  OUTPUT);
  pinMode(LED_BLUE,   OUTPUT);
  pinMode(LED_RED,    OUTPUT);
  pinMode(BTN_LEFT,   INPUT_PULLUP);
  pinMode(BTN_RIGHT,  INPUT_PULLUP);
  pinMode(BTN_RELAY,  INPUT);
  pinMode(TAMPER_PIN, INPUT);
  pinMode(CT_PIN,     INPUT);
  analogSetPinAttenuation(CT_PIN, ADC_11db);
  Serial.println("Leakage current monitor initialized");

  pzemSerial.begin(9600, SERIAL_8N1, 17, 16);
  Wire.begin(21, 22);

  lcd.init(); lcd.backlight();
  lcd.setCursor(0, 0); lcd.print("SMART ENERGY METER");
  lcd.setCursor(0, 1); lcd.print("  Initializing...  ");
  delay(500); lcd.clear();

  if (!rtc.begin()) Serial.println("RTC not found");

  EEPROM.begin(32);
  EEPROM.get(0, storedEnergy);
  if (isnan(storedEnergy)) storedEnergy = 0;

  // ── Firmware version NVS reset check ───────────────────────
  checkFirmwareReset();   // ← wipes modules NVS if version changed

  saveDeviceInfo();
  loadBillingState();
  loadRelayState();
  loadBillingQueue();
  loadPairedModules();    // ← loads persisted module details

  digitalWrite(RELAY_PIN, relayState ? LOW : HIGH);

  connectWiFi();

  if (esp_now_init() != ESP_OK) {
    Serial.println("ESP-NOW init failed");
  } else {
    Serial.println("ESP-NOW initialized");
    esp_now_register_recv_cb(onESPNowRecv);
    uint8_t ch = WiFi.channel();
    esp_wifi_set_channel(ch, WIFI_SECOND_CHAN_NONE);

    // Broadcast peer
    esp_now_peer_info_t bcast; memset(&bcast, 0, sizeof(bcast));
    memset(bcast.peer_addr, 0xFF, 6);
    bcast.channel = ch; bcast.encrypt = false; bcast.ifidx = WIFI_IF_STA;
    esp_now_add_peer(&bcast);

    // Re-add all previously paired module peers
    for (int i = 0; i < pairedCount; i++) {
      if (pairedModules[i].isPaired && !esp_now_is_peer_exist(pairedModules[i].macAddr)) {
        esp_now_peer_info_t peer; memset(&peer, 0, sizeof(peer));
        memcpy(peer.peer_addr, pairedModules[i].macAddr, 6);
        peer.channel = ch; peer.encrypt = false; peer.ifidx = WIFI_IF_STA;
        esp_now_add_peer(&peer);
        Serial.print("[Boot] Peer re-added: "); Serial.println(pairedModules[i].moduleId);
      }
    }
  }

  setupMQTT();
  connectMQTT();
  publishRelayStatus();

  ntpSyncTimer = millis() - NTP_SYNC_INTERVAL + 5000;
  lastBillingSync = millis();
  periodStartTimestamp = millis() / 1000;
  lastTamperPin = digitalRead(TAMPER_PIN);

  Serial.println("=== Smart meter ready ===");
  Serial.print("Relay: ");       Serial.println(relayState ? "ON" : "OFF (LOCKED)");
  Serial.print("Paired modules: "); Serial.println(pairedCount);
  Serial.print("Queued billing: "); Serial.println(billingQueueCount);
}

// ============================================================
//  LOOP
// ============================================================
void loop() {
  if (!client.connected()) connectMQTT();
  client.loop();
  unsigned long now = millis();

  // ── LEAKAGE CURRENT DETECTION ──────────────────────────────
  static unsigned long lastLeakageRead = 0;
  if (now - lastLeakageRead >= 500) {
    lastLeakageRead = now;
    unsigned long startTime = millis(); int sampleCount = 0;
    double voltageSum = 0, sqVoltageSum = 0;
    while (millis() - startTime < 100) {
      int rawADC = analogRead(CT_PIN);
      double pv = (rawADC / (double)ADC_RESOLUTION) * V_REF;
      voltageSum += pv; sqVoltageSum += (pv * pv); sampleCount++;
    }
    double dcBias = voltageSum / sampleCount;
    double variance = (sqVoltageSum / sampleCount) - (dcBias * dcBias);
    if (variance < 0) variance = 0;
    double acRMS = sqrt(variance);
    double burden = acRMS / OPAMP_GAIN;
    double secA = burden / BURDEN_RESISTOR;
    leakageCurrent_mA = secA * CT_RATIO * 1000.0 * CALIBRATION_FACTOR;
    if (leakageCurrent_mA < NOISE_FILTER_MA) leakageCurrent_mA = 0.0;

    if (leakageCurrent_mA >= LEAKAGE_THRESHOLD_MA && relayState && !relayLockedByServer) {
      relayState = false; leakageTripped = true; faultActive = true;
      digitalWrite(RELAY_PIN, HIGH); digitalWrite(BUZZER_PIN, HIGH);
      Serial.print("[Leakage] ⚡ TRIP! Current=");
      Serial.print(leakageCurrent_mA, 2); Serial.println(" mA — relay OFF");
      publishRelayStatus();
    }
    if (leakageTripped && (now - lastLeakageAlert >= LEAKAGE_ALERT_INTERVAL)) {
      lastLeakageAlert = now;
      publishLeakageAlert((float)leakageCurrent_mA);
    }
    Serial.print("Leakage: "); Serial.print(leakageCurrent_mA, 2); Serial.println(" mA");
  }

  // ── WiFi status LED & retry ────────────────────────────────
  if (WiFi.status() == WL_CONNECTED) { digitalWrite(LED_GREEN, HIGH); }
  else {
    digitalWrite(LED_GREEN, LOW);
    if (now - wifiRetryTimer > 15000UL) { wifiRetryTimer = now; connectWiFi(); }
  }

  // ── NTP SYNC ───────────────────────────────────────────────
  if (WiFi.status() == WL_CONNECTED && now - ntpSyncTimer > NTP_SYNC_INTERVAL) {
    ntpSyncTimer = now; syncTimeWithNTP();
  }

  // ── RETRY BILLING QUEUE ────────────────────────────────────
  if (now - billingQueueRetry > BILLING_QUEUE_RETRY) {
    billingQueueRetry = now;
    if (billingQueueCount > 0 && client.connected()) retryBillingQueue();
  }

  // ── READ PZEM ──────────────────────────────────────────────
  voltage    = pzem.voltage();
  current_A  = pzem.current();
  power_W    = pzem.power();
  energy_kWh = pzem.energy();
  pf         = pzem.pf();
  frequency  = pzem.frequency();

  // ── PZEM FAULT DETECTION ───────────────────────────────────
  if (!isnan(voltage) && voltage > 0) {
    if (voltage > OVERVOLTAGE_THRESHOLD && now - lastOvervoltageAlert >= FAULT_ALERT_INTERVAL) {
      lastOvervoltageAlert = now; faultActive = true;
      Serial.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      Serial.println("[FAULT] ⚠️  OVERVOLTAGE DETECTED");
      Serial.print  ("[FAULT]    Measured : "); Serial.print(voltage, 1); Serial.println(" V");
      Serial.print  ("[FAULT]    Threshold: "); Serial.print(OVERVOLTAGE_THRESHOLD, 1); Serial.println(" V");
      Serial.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      publishFault("mainMeter", nullptr, -1, "overvoltage", "warning",
                   "Supply voltage exceeded safe upper limit", voltage, OVERVOLTAGE_THRESHOLD, "V");
    }
    if (voltage < UNDERVOLTAGE_THRESHOLD && now - lastUndervoltageAlert >= FAULT_ALERT_INTERVAL) {
      lastUndervoltageAlert = now; faultActive = true;
      Serial.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      Serial.println("[FAULT] ⚠️  UNDERVOLTAGE DETECTED");
      Serial.print  ("[FAULT]    Measured : "); Serial.print(voltage, 1); Serial.println(" V");
      Serial.print  ("[FAULT]    Threshold: "); Serial.print(UNDERVOLTAGE_THRESHOLD, 1); Serial.println(" V");
      Serial.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      publishFault("mainMeter", nullptr, -1, "undervoltage", "warning",
                   "Supply voltage dropped below safe lower limit", voltage, UNDERVOLTAGE_THRESHOLD, "V");
    }
  }
  if (!isnan(current_A) && current_A > 0) {
    if (current_A > OVERCURRENT_THRESHOLD && now - lastOvercurrentAlert >= FAULT_ALERT_INTERVAL) {
      lastOvercurrentAlert = now; faultActive = true;
      Serial.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      Serial.println("[FAULT] 🔴 OVERCURRENT DETECTED");
      Serial.print  ("[FAULT]    Measured : "); Serial.print(current_A, 2); Serial.println(" A");
      Serial.print  ("[FAULT]    Threshold: "); Serial.print(OVERCURRENT_THRESHOLD, 1); Serial.println(" A");
      Serial.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      publishFault("mainMeter", nullptr, -1, "overcurrent", "critical",
                   "Load current exceeded maximum rated threshold", current_A, OVERCURRENT_THRESHOLD, "A");
    }
  }
  if (!isnan(pf) && pf > 0) {
    if (pf < LOW_PF_THRESHOLD && now - lastLowPFAlert >= FAULT_ALERT_INTERVAL) {
      lastLowPFAlert = now; faultActive = true;
      Serial.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      Serial.println("[FAULT] ⚠️  LOW POWER FACTOR DETECTED");
      Serial.print  ("[FAULT]    Measured : "); Serial.println(pf, 2);
      Serial.print  ("[FAULT]    Threshold: "); Serial.println(LOW_PF_THRESHOLD, 2);
      Serial.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      publishFault("mainMeter", nullptr, -1, "low_power_factor", "warning",
                   "Power factor below acceptable minimum — possible reactive load", pf, LOW_PF_THRESHOLD, "pf");
    }
  }
  if (!leakageTripped &&
      (isnan(voltage)   || (voltage   >= UNDERVOLTAGE_THRESHOLD && voltage   <= OVERVOLTAGE_THRESHOLD)) &&
      (isnan(current_A) || current_A  <= OVERCURRENT_THRESHOLD) &&
      (isnan(pf)        || pf         >= LOW_PF_THRESHOLD)) {
    faultActive = false;
  }

  // ── EEPROM BACKUP ──────────────────────────────────────────
  if (now - eepromTimer > 30000UL) {
    eepromTimer = now;
    EEPROM.put(0, energy_kWh); EEPROM.commit();
    storedEnergy = isnan(energy_kWh) ? storedEnergy : energy_kWh;
  }

  // ── TAMPER DETECTION ───────────────────────────────────────
  bool currentTamper = (digitalRead(TAMPER_PIN) == HIGH);
  if (currentTamper) {
    relayState = false; digitalWrite(RELAY_PIN, HIGH); digitalWrite(BUZZER_PIN, HIGH);
    bool risingEdge = (!lastTamperPin && currentTamper);
    if (risingEdge || (now - lastTamperAlert >= TAMPER_ALERT_INTERVAL)) {
      lastTamperAlert = now; publishTamperAlert();
      publishFault("mainMeter", nullptr, -1, "tamper", "critical",
                   "Physical tamper detected on main meter", 0, 0, "");
      publishRelayStatus();
    }
  } else {
    digitalWrite(BUZZER_PIN, LOW);
    if (lastTamperPin) {
      if (!relayLockedByServer && !leakageTripped) { relayState = true; digitalWrite(RELAY_PIN, LOW); }
      publishRelayStatus(); Serial.println("Tamper cleared");
    }
  }
  lastTamperPin = currentTamper; tamperState = currentTamper;

  // ── MANUAL RELAY BUTTON ────────────────────────────────────
  if (digitalRead(BTN_RELAY) == LOW && !tamperState && !relayLockedByServer) {
    if (leakageTripped) {
      leakageTripped = false; faultActive = false; relayState = true;
      digitalWrite(RELAY_PIN, LOW);
      saveRelayState(); publishRelayStatus();
      Serial.println("[Leakage] 🔓 Trip cleared by user — relay ON");
    } else {
      relayState = !relayState;
      digitalWrite(RELAY_PIN, relayState ? LOW : HIGH);
      saveRelayState(); publishRelayStatus();
    }
    delay(300);
  }

  // ── SCREEN NAVIGATION ──────────────────────────────────────
  if (digitalRead(BTN_RIGHT) == LOW) { screen = (screen >= 4) ? 0 : screen + 1; delay(300); }
  if (digitalRead(BTN_LEFT)  == LOW) { screen = (screen <= 0) ? 4 : screen - 1; delay(300); }

  // ── BLUE LED ───────────────────────────────────────────────
  if (!isnan(current_A) && current_A > 0.02f) {
    unsigned long interval = 1000;
    if      (current_A > 5)   interval = 100;
    else if (current_A > 2)   interval = 250;
    else if (current_A > 0.5) interval = 500;
    if (now - blueTimer > interval) { blueTimer = now; blueState = !blueState; digitalWrite(LED_BLUE, blueState); }
  } else { digitalWrite(LED_BLUE, LOW); }

  // ── RED LED ────────────────────────────────────────────────
  if (tamperState || leakageTripped) {
    if (now - redTimer > 200) { redTimer = now; redState = !redState; digitalWrite(LED_RED, redState); }
  } else if (!relayState) { digitalWrite(LED_RED, HIGH); }
  else { digitalWrite(LED_RED, LOW); }

  // ── ENERGY ACCUMULATION ────────────────────────────────────
  if (now - lastEnergyUpdate >= ENERGY_UPDATE_INTERVAL) updateEnergyAccumulation();

  // ── BILLING SYNC ───────────────────────────────────────────
  if (now - lastBillingSync >= BILLING_SYNC_INTERVAL) { publishBillingSync(); lastBillingSync = now; }

  // ── HEARTBEAT ──────────────────────────────────────────────
  if (now - lastHeartbeat >= HEARTBEAT_INTERVAL) { publishHeartbeat(); lastHeartbeat = now; }

  // ── TELEMETRY ──────────────────────────────────────────────
  if (now - lastTelemetry >= TELEMETRY_INTERVAL) { publishTelemetry(); lastTelemetry = now; }

  // ── LCD UPDATE (every 1s) ──────────────────────────────────
  if (now - lcdTimer > 1000UL) {
    lcdTimer = now; lcd.clear();

    if (screen == 0) {
      lcd.setCursor(0, 0);
      lcd.print("V:"); lcd.print(isnan(voltage) ? 0 : voltage, 1);
      lcd.print("V I:"); lcd.print(isnan(current_A) ? 0 : current_A, 2); lcd.print("A");
      lcd.setCursor(0, 1);
      lcd.print("P:"); lcd.print(isnan(power_W) ? 0 : power_W, 0);
      lcd.print("W PF:"); lcd.print(isnan(pf) ? 0 : pf, 2);
      lcd.setCursor(0, 2);
      lcd.print("E:"); lcd.print(isnan(energy_kWh) ? 0 : energy_kWh, 2);
      lcd.print("kWh F:"); lcd.print(isnan(frequency) ? 0 : frequency, 1); lcd.print("Hz");
      lcd.setCursor(0, 3);
      if (relayLockedByServer)  lcd.print("** BALANCE OVER **  ");
      else if (leakageTripped)  lcd.print("!! LEAKAGE TRIP !!  ");
      else if (tamperState)     lcd.print("!! TAMPER ALERT !!  ");
      else if (faultActive)     lcd.print("! FAULT ACTIVE      ");
      else { lcd.print("Load:"); lcd.print(relayState ? "ON  OK " : "OFF     "); }
    }
    else if (screen == 1) {
      DateTime dt = rtc.now();
      lcd.setCursor(0, 0); lcd.print("Date:");
      lcd.print(dt.day()); lcd.print("/"); lcd.print(dt.month()); lcd.print("/"); lcd.print(dt.year());
      lcd.setCursor(0, 1); lcd.print("Time:");
      if (dt.hour()   < 10) lcd.print("0"); lcd.print(dt.hour());   lcd.print(":");
      if (dt.minute() < 10) lcd.print("0"); lcd.print(dt.minute()); lcd.print(":");
      if (dt.second() < 10) lcd.print("0"); lcd.print(dt.second());
    }
    else if (screen == 2) {
      lcd.setCursor(0, 0); lcd.print("Stored Energy:");
      lcd.setCursor(0, 1); lcd.print(storedEnergy, 2); lcd.print(" kWh");
      lcd.setCursor(0, 2); lcd.print("Period Energy:");
      lcd.setCursor(0, 3); lcd.print(accumulatedEnergyWh, 1); lcd.print(" Wh");
    }
    else if (screen == 3) {
      lcd.setCursor(0, 0); lcd.print("WiFi:"); lcd.print(WiFi.status() == WL_CONNECTED ? "OK  " : "ERR ");
      lcd.print("MQTT:"); lcd.print(client.connected() ? "OK" : "ERR");
      lcd.setCursor(0, 1); lcd.print("Dev:"); lcd.print(deviceId);
      lcd.setCursor(0, 2); lcd.print("Ver:"); lcd.print(firmwareVersion);
      lcd.setCursor(0, 3); lcd.print("Mod:"); lcd.print(pairedCount); lcd.print(" paired");
    }
    else if (screen == 4) {
      lcd.setCursor(0, 0);
      if (relayLockedByServer)  lcd.print("RELAY:OFF BAL.OVER  ");
      else if (leakageTripped)  lcd.print("RELAY:OFF LEAKAGE!! ");
      else if (tamperState)     lcd.print("RELAY:OFF TAMPER!!  ");
      else { lcd.print("RELAY:"); lcd.print(relayState ? "ON  " : "OFF "); lcd.print(faultActive ? "FAULT! " : "OK      "); }
      lcd.setCursor(0, 1);
      lcd.print("Leak:"); lcd.print(leakageCurrent_mA, 1); lcd.print("mA Tmp:"); lcd.print(tamperState ? "YES" : "NO ");
      lcd.setCursor(0, 2);
      lcd.print("WiFi:"); lcd.print(WiFi.status() == WL_CONNECTED ? "OK  " : "ERR ");
      lcd.print("MQTT:"); lcd.print(client.connected() ? "OK  " : "ERR ");
      lcd.setCursor(0, 3);
      lcd.print("Mod:"); lcd.print(pairedCount); lcd.print(" Acc:"); lcd.print(accumulatedEnergyWh, 0); lcd.print("Wh");
    }
  }
}
