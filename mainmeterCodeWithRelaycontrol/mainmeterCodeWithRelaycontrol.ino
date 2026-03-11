#include <WiFi.h>
#include <PubSubClient.h>
#include <esp_now.h>
#include <esp_wifi.h>
#include <Preferences.h>
#include <ArduinoJson.h>

// =====================================
// WIFI CONFIG
// =====================================
const char* ssid = "boom";
const char* password = "1231231230";

// =====================================
// MQTT CONFIG (Backend IP)
// =====================================
const char* mqttServer = "20.39.192.14";
const int mqttPort = 1883;

// =====================================
// DEVICE DETAILS (From Admin Panel)
// =====================================
const int deviceId = 44188662;
const char* secretKey = "6vM3ihdppGGtwf54Yu";

// =====================================
// NVS STORAGE FOR PAIRED MODULES & BILLING
// =====================================
Preferences preferences;

// =====================================
// BILLING CONFIGURATION
// =====================================
const unsigned long BILLING_SYNC_INTERVAL = 30000;
unsigned long lastBillingSync = 0;
unsigned long billingSequenceNo = 0;

float accumulatedEnergyWh = 0.0;
unsigned long periodStartTimestamp = 0;
unsigned long lastEnergyUpdate = 0;

bool relayLockedByServer = false;
bool relayState = true;
bool faultActive = false;

const char* firmwareVersion = "1.0.2";
const char* deploymentDate = "2025-02-15";

// =====================================
// FAULT TESTING CONFIGURATION
// =====================================
const unsigned long FAULT_TEST_INTERVAL = 600000;
unsigned long lastFaultTest = 0;
int faultTestCounter = 0;

// =====================================
// PAIRED MODULES (MAX 10)
// =====================================
#define MAX_MODULES 10

struct PairedModule {
  char moduleId[16];
  uint8_t macAddr[6];
  int capacity;
  bool isPaired;
};

PairedModule pairedModules[MAX_MODULES];
int pairedCount = 0;

// =====================================
// MQTT CLIENT
// =====================================
WiFiClient espClient;
PubSubClient client(espClient);

#define MQTT_MAX_PACKET_SIZE 2048

// =====================================
// MQTT TOPICS
// =====================================
String energyTopic     = "meter/" + String(deviceId) + "/energy";
String telemetryTopic  = "meter/" + String(deviceId) + "/telemetry";
String heartbeatTopic  = "meter/" + String(deviceId) + "/heartbeat";
String commandTopic    = "meter/" + String(deviceId) + "/command";
String scanTopic       = "meter/" + String(deviceId) + "/scan";
String pairAckTopic    = "meter/" + String(deviceId) + "/pair_ack";
String billingSyncTopic = "meter/" + String(deviceId) + "/billing_sync";
String ackTopic        = "meter/" + String(deviceId) + "/ack";
String faultTopic      = "meter/" + String(deviceId) + "/fault";

// =====================================
// ESP-NOW PAYLOAD STRUCTS
// =====================================
typedef struct {
  char type[16];
} ScanRequest;

typedef struct {
  char type[16];
  char moduleId[16];
  int capacity;
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
  float voltage;
  float current;
  float power;
  bool relayState;
  char health[8];
  int unitIndex;
} ModuleTelemetry;

typedef struct {
  char type[16];
  char moduleId[16];
  int unitNumber;
  char faultType[32];
  char severity[16];
  char description[128];
  float measuredValue;
  float thresholdValue;
  char unit[16];
} ModuleFault;

// NEW: Relay control structs
typedef struct {
  char type[16];        // "RELAY_CTRL"
  char moduleId[16];
  int unitNumber;
  bool state;
} ModuleRelayControl;

typedef struct {
  char type[16];        // "RELAY_ACK"
  char moduleId[16];
  int unitNumber;
  bool success;
  bool relayState;
} ModuleRelayAck;

struct ModuleData {
  char moduleId[16];
  int unitIndex;
  float voltage;
  float current;
  float power;
  bool relayState;
  char health[8];
  unsigned long lastUpdate;
};

ModuleData moduleDataCache[MAX_MODULES * 10];
int moduleDataCount = 0;

// =====================================
// SIMULATED MAIN METER READINGS
// =====================================
float mainVoltage = 230.0;
float mainCurrent = 8.0;
float mainPower = 1840.0;
float mainPowerFactor = 0.94;
float mainFrequency = 50.0;

// =====================================
// TIMING CONSTANTS
// =====================================
const unsigned long TELEMETRY_INTERVAL = 3000;
const unsigned long HEARTBEAT_INTERVAL = 2000;
const unsigned long ENERGY_UPDATE_INTERVAL = 1000;

unsigned long lastTelemetry = 0;
unsigned long lastHeartbeat = 0;

// =====================================
// FAULT PUBLISHING FUNCTION
// =====================================
void publishFault(const char* source, const char* moduleId, int unitNumber, 
                   const char* faultType, const char* severity, 
                   const char* description, float measuredValue, 
                   float thresholdValue, const char* unit) {
  StaticJsonDocument<512> doc;
  
  doc["source"] = source;
  
  if (moduleId != nullptr) {
    doc["moduleId"] = moduleId;
  }
  
  if (unitNumber >= 0) {
    doc["unitNumber"] = unitNumber;
  }
  
  doc["faultType"] = faultType;
  doc["severity"] = severity;
  doc["description"] = description;
  
  if (measuredValue > 0) {
    doc["measuredValue"] = measuredValue;
  }
  
  if (thresholdValue > 0) {
    doc["thresholdValue"] = thresholdValue;
  }
  
  if (unit != nullptr && strlen(unit) > 0) {
    doc["unit"] = unit;
  }
  
  char buffer[512];
  serializeJson(doc, buffer);
  
  if (client.publish(faultTopic.c_str(), buffer)) {
    Serial.println("🚨 Fault published:");
    Serial.print("  Source: ");
    Serial.println(source);
    Serial.print("  Type: ");
    Serial.println(faultType);
    Serial.print("  Severity: ");
    Serial.println(severity);
  } else {
    Serial.println("❌ Fault publish failed");
  }
}

// =====================================
// FAULT TESTING FUNCTION
// =====================================
void testFaultPublishing() {
  faultTestCounter++;
  
  int scenario = faultTestCounter % 4;
  
  switch (scenario) {
    case 0:
      publishFault(
        "mainMeter",
        nullptr,
        -1,
        "overvoltage",
        "critical",
        "Voltage exceeded safe operating limits",
        245.5,
        240.0,
        "V"
      );
      break;
      
    case 1:
      publishFault(
        "mainMeter",
        nullptr,
        -1,
        "overcurrent",
        "warning",
        "Current draw approaching maximum threshold",
        9.8,
        10.0,
        "A"
      );
      break;
      
    case 2:
      publishFault(
        "mainMeter",
        nullptr,
        -1,
        "low_power_factor",
        "info",
        "Power factor below optimal range",
        0.75,
        0.85,
        ""
      );
      break;
      
    case 3:
      publishFault(
        "mainMeter",
        nullptr,
        -1,
        "frequency_deviation",
        "warning",
        "Grid frequency outside normal range",
        50.8,
        50.5,
        "Hz"
      );
      break;
  }
}

// =====================================
// NVS FUNCTIONS - RELAY STATE
// =====================================
void loadRelayState() {
  preferences.begin("relay", false);
  relayState = preferences.getBool("state", true);
  preferences.end();
  
  Serial.print("📂 Main relay state loaded: ");
  Serial.println(relayState ? "ON" : "OFF");
}

void saveRelayState() {
  preferences.begin("relay", false);
  preferences.putBool("state", relayState);
  preferences.end();
  
  Serial.print("💾 Main relay state saved: ");
  Serial.println(relayState ? "ON" : "OFF");
}

// =====================================
// NVS FUNCTIONS - BILLING STATE
// =====================================
void loadBillingState() {
  preferences.begin("billing", false);
  
  accumulatedEnergyWh = preferences.getFloat("energyWh", 0.0);
  billingSequenceNo = preferences.getULong("seqNo", 0);
  periodStartTimestamp = preferences.getULong("periodStart", 0);
  relayLockedByServer = preferences.getBool("relayLock", false);
  
  Serial.println("📂 Billing state loaded:");
  Serial.print("  Energy: ");
  Serial.print(accumulatedEnergyWh);
  Serial.println(" Wh");
  Serial.print("  Sequence: ");
  Serial.println(billingSequenceNo);
  Serial.print("  Relay Locked: ");
  Serial.println(relayLockedByServer ? "YES" : "NO");
  
  preferences.end();
  
  if (relayLockedByServer) {
    relayState = false;
    Serial.println("🔒 RELAY LOCKED - Insufficient balance");
  }
}

void saveBillingState() {
  preferences.begin("billing", false);
  
  preferences.putFloat("energyWh", accumulatedEnergyWh);
  preferences.putULong("seqNo", billingSequenceNo);
  preferences.putULong("periodStart", periodStartTimestamp);
  preferences.putBool("relayLock", relayLockedByServer);
  
  preferences.end();
}

void saveDeviceInfo() {
  preferences.begin("device", false);
  
  preferences.putInt("deviceId", deviceId);
  preferences.putString("secret", secretKey);
  preferences.putString("version", firmwareVersion);
  preferences.putString("deployed", deploymentDate);
  
  preferences.end();
}

// =====================================
// ENERGY ACCUMULATION
// =====================================
void updateEnergyAccumulation() {
  unsigned long now = millis();
  
  if (lastEnergyUpdate == 0) {
    lastEnergyUpdate = now;
    return;
  }
  
  if (relayState && !relayLockedByServer) {
    unsigned long deltaMs = now - lastEnergyUpdate;
    float deltaHours = deltaMs / 3600000.0;
    
    float energyWh = mainPower * deltaHours;
    accumulatedEnergyWh += energyWh;
    
    static unsigned long lastSave = 0;
    if (now - lastSave > 10000) {
      saveBillingState();
      lastSave = now;
    }
  }
  
  lastEnergyUpdate = now;
}

// =====================================
// BILLING SYNC
// =====================================
void publishBillingSync() {
  unsigned long now = millis() / 1000;
  
  if (periodStartTimestamp == 0) {
    periodStartTimestamp = now;
  }
  
  StaticJsonDocument<256> doc;
  
  doc["deviceId"] = deviceId;
  doc["sequenceNo"] = billingSequenceNo;
  doc["periodStart"] = periodStartTimestamp;
  doc["periodEnd"] = now;
  doc["energyConsumedWh"] = round(accumulatedEnergyWh * 10) / 10.0;
  doc["meterState"] = relayLockedByServer ? "LOCKED" : "ACTIVE";
  doc["relayState"] = relayState;
  doc["faultActive"] = faultActive;
  doc["firmwareVersion"] = firmwareVersion;
  
  char buffer[256];
  serializeJson(doc, buffer);
  
  if (client.publish(billingSyncTopic.c_str(), buffer, true)) {
    Serial.println("💰 Billing sync published");
    Serial.print("  Energy: ");
    Serial.print(accumulatedEnergyWh);
    Serial.println(" Wh");
    
    accumulatedEnergyWh = 0.0;
    periodStartTimestamp = now;
    billingSequenceNo++;
    
    saveBillingState();
  } else {
    Serial.println("❌ Billing sync failed");
  }
}

// =====================================
// NVS FUNCTIONS - PAIRED MODULES
// =====================================
void loadPairedModules() {
  preferences.begin("modules", false);
  
  pairedCount = preferences.getInt("count", 0);
  
  for (int i = 0; i < pairedCount && i < MAX_MODULES; i++) {
    String key = "mod" + String(i);
    size_t len = preferences.getBytesLength(key.c_str());
    
    if (len == sizeof(PairedModule)) {
      preferences.getBytes(key.c_str(), &pairedModules[i], sizeof(PairedModule));
      Serial.print("📦 Loaded: ");
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
  Serial.println("💾 Paired modules saved");
}

// =====================================
// WIFI CONNECTION
// =====================================
void connectWiFi() {
  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid, password);
  
  Serial.print("Connecting to WiFi");
  while (WiFi.status() != WL_CONNECTED) {
    delay(300);
    Serial.print(".");
  }
  
  Serial.println("\n🟢 WiFi connected");
  Serial.print("📶 Channel: ");
  Serial.println(WiFi.channel());
  Serial.print("📡 IP: ");
  Serial.println(WiFi.localIP());
}

// =====================================
// MQTT SETUP
// =====================================
void setupMQTT() {
  client.setServer(mqttServer, mqttPort);
  client.setCallback(onMQTTMessage);
  client.setBufferSize(2048);
}

void connectMQTT() {
  while (!client.connected()) {
    Serial.println("Connecting to MQTT...");
    
    if (client.connect(
          String(deviceId).c_str(),
          String(deviceId).c_str(),
          secretKey
        )) {
      Serial.println("🟢 MQTT connected");
      
      client.subscribe(commandTopic.c_str());
      Serial.print("📬 Subscribed to: ");
      Serial.println(commandTopic);
      
    } else {
      Serial.print("❌ MQTT failed, rc=");
      Serial.println(client.state());
      delay(3000);
    }
  }
}

// =====================================
// PUBLISH COMMAND ACK
// =====================================
void publishCommandAck(const char* command, const char* status, bool currentRelayState) {
  StaticJsonDocument<128> doc;
  
  doc["command"] = command;
  doc["status"] = status;
  doc["relayState"] = currentRelayState;
  doc["relayLocked"] = relayLockedByServer;
  
  char buffer[128];
  serializeJson(doc, buffer);
  
  client.publish(ackTopic.c_str(), buffer);
  Serial.print("✅ ACK sent: ");
  Serial.print(command);
  Serial.print(" | Status: ");
  Serial.print(status);
  Serial.print(" | Relay: ");
  Serial.println(currentRelayState ? "ON" : "OFF");
}

// =====================================
// MQTT MESSAGE HANDLER
// =====================================
void onMQTTMessage(char* topic, byte* payload, unsigned int length) {
  Serial.print("📥 Message on: ");
  Serial.println(topic);
  
  StaticJsonDocument<256> doc;
  DeserializationError err = deserializeJson(doc, payload, length);
  
  if (err) {
    Serial.println("❌ JSON parse failed");
    return;
  }
  
  String cmd = doc["command"] | "";
  
  // ===============================
  // MAIN METER RELAY CONTROL
  // ===============================
  if (cmd == "relay_on") {
    if (relayLockedByServer) {
      Serial.println("⚠️ Cannot turn ON - Relay locked by server");
      publishCommandAck("relay_on", "FAILED", relayState);
      return;
    }
    
    relayState = true;
    saveRelayState();
    Serial.println("🔌 Main meter relay turned ON");
    publishCommandAck("relay_on", "SUCCESS", relayState);
  }
  else if (cmd == "relay_off") {
    relayState = false;
    saveRelayState();
    Serial.println("🔌 Main meter relay turned OFF");
    publishCommandAck("relay_off", "SUCCESS", relayState);
  }
  else if (cmd == "unlock_relay") {
    relayLockedByServer = false;
    relayState = true;
    saveBillingState();
    saveRelayState();
    Serial.println("🔓 Relay unlocked by server");
    publishCommandAck("unlock_relay", "SUCCESS", relayState);
  }
  else if (cmd == "lock_relay") {
    relayLockedByServer = true;
    relayState = false;
    saveBillingState();
    saveRelayState();
    Serial.println("🔒 Relay locked by server");
    publishCommandAck("lock_relay", "SUCCESS", relayState);
  }
  
  // ===============================
  // MODULE UNIT RELAY CONTROL
  // ===============================
  else if (cmd == "module_relay_control") {
    String moduleId = doc["moduleId"] | "";
    int unitNumber = doc["unitNumber"] | -1;
    bool state = doc["state"] | false;
    
    if (moduleId.length() == 0 || unitNumber < 0) {
      Serial.println("❌ Invalid module relay control request");
      return;
    }
    
    Serial.print("🔌 Module relay control: ");
    Serial.print(moduleId);
    Serial.print(" Unit ");
    Serial.print(unitNumber);
    Serial.print(" -> ");
    Serial.println(state ? "ON" : "OFF");
    
    bool found = false;
    for (int i = 0; i < pairedCount; i++) {
      if (strcmp(pairedModules[i].moduleId, moduleId.c_str()) == 0) {
        ModuleRelayControl relayCmd;
        memset(&relayCmd, 0, sizeof(relayCmd));
        strcpy(relayCmd.type, "RELAY_CTRL");
        strncpy(relayCmd.moduleId, moduleId.c_str(), 15);
        relayCmd.unitNumber = unitNumber;
        relayCmd.state = state;
        
        esp_now_send(pairedModules[i].macAddr, (uint8_t*)&relayCmd, sizeof(relayCmd));
        found = true;
        Serial.println("📤 Relay control command sent to module");
        break;
      }
    }
    
    if (!found) {
      Serial.println("⚠️ Module not found in paired list");
    }
  }
  
  // ===============================
  // MODULE SCAN
  // ===============================
  else if (cmd == "scan_modules") {
    Serial.println("📡 Scanning for modules...");
    
    ScanRequest req;
    memset(&req, 0, sizeof(req));
    strcpy(req.type, "SCAN_REQ");
    
    uint8_t broadcastAddr[6] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
    esp_now_send(broadcastAddr, (uint8_t*)&req, sizeof(req));
    
    Serial.println("📤 Scan request broadcast sent");
  }
  
  // ===============================
  // MODULE PAIRING
  // ===============================
  else if (cmd == "pair_module") {
    String moduleId = doc["moduleId"] | "";
    String secret = doc["secret"] | "";
    
    if (moduleId.length() == 0 || secret.length() == 0) {
      Serial.println("❌ Invalid pairing request");
      return;
    }
    
    Serial.print("🔗 Pairing request for: ");
    Serial.println(moduleId);
    
    PairRequest pairReq;
    memset(&pairReq, 0, sizeof(pairReq));
    strcpy(pairReq.type, "PAIR_REQ");
    strncpy(pairReq.moduleId, moduleId.c_str(), 15);
    strncpy(pairReq.secret, secret.c_str(), 31);
    
    uint8_t broadcastAddr[6] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
    esp_now_send(broadcastAddr, (uint8_t*)&pairReq, sizeof(pairReq));
    
    Serial.println("📤 Pairing request sent");
  }
  
  // ===============================
  // MODULE UNPAIRING
  // ===============================
  else if (cmd == "unpair_module") {
    String moduleId = doc["moduleId"] | "";
    
    if (moduleId.length() == 0) {
      Serial.println("❌ Invalid unpair request");
      return;
    }
    
    Serial.print("🔓 Unpairing: ");
    Serial.println(moduleId);
    
    for (int i = 0; i < pairedCount; i++) {
      if (strcmp(pairedModules[i].moduleId, moduleId.c_str()) == 0) {
        esp_now_del_peer(pairedModules[i].macAddr);
        
        UnpairCommand unpairCmd;
        memset(&unpairCmd, 0, sizeof(unpairCmd));
        strcpy(unpairCmd.type, "UNPAIR");
        strncpy(unpairCmd.moduleId, moduleId.c_str(), 15);
        
        esp_now_send(pairedModules[i].macAddr, (uint8_t*)&unpairCmd, sizeof(unpairCmd));
        
        for (int j = i; j < pairedCount - 1; j++) {
          pairedModules[j] = pairedModules[j + 1];
        }
        pairedCount--;
        
        savePairedModules();
        
        Serial.println("✅ Module unpaired");
        publishCommandAck("unpair_module", "SUCCESS", relayState);
        return;
      }
    }
    
    Serial.println("⚠️ Module not found");
    publishCommandAck("unpair_module", "FAILED", relayState);
  }
}

// =====================================
// ESP-NOW RECEIVE CALLBACK
// =====================================
void onESPNowRecv(const esp_now_recv_info_t* info, const uint8_t* data, int len) {
  char msgType[16];
  memcpy(msgType, data, 16);
  
  // --------------------------------------------------
  // SCAN RESPONSE
  // --------------------------------------------------
  if (strcmp(msgType, "SCAN_RESP") == 0) {
    ScanResponse resp;
    memcpy(&resp, data, sizeof(resp));
    
    Serial.println("📡 Scan response received:");
    Serial.print("  Module ID: ");
    Serial.println(resp.moduleId);
    Serial.print("  Capacity: ");
    Serial.println(resp.capacity);
    Serial.print("  MAC: ");
    for (int i = 0; i < 6; i++) {
      Serial.printf("%02X", info->src_addr[i]);
      if (i < 5) Serial.print(":");
    }
    Serial.println();
    
    StaticJsonDocument<128> doc;
    doc["moduleId"] = resp.moduleId;
    doc["capacity"] = resp.capacity;
    
    char macStr[18];
    snprintf(macStr, sizeof(macStr), "%02X:%02X:%02X:%02X:%02X:%02X",
             info->src_addr[0], info->src_addr[1], info->src_addr[2],
             info->src_addr[3], info->src_addr[4], info->src_addr[5]);
    doc["mac"] = macStr;
    
    char buffer[128];
    serializeJson(doc, buffer);
    
    client.publish(scanTopic.c_str(), buffer);
  }
  
  // --------------------------------------------------
  // PAIR ACKNOWLEDGEMENT
  // --------------------------------------------------
  else if (strcmp(msgType, "PAIR_ACK") == 0) {
    PairAck ack;
    memcpy(&ack, data, sizeof(ack));
    
    Serial.println("🔗 Pair ACK received:");
    Serial.print("  Module: ");
    Serial.println(ack.moduleId);
    Serial.print("  Success: ");
    Serial.println(ack.success ? "YES" : "NO");
    
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
      peerInfo.ifidx = WIFI_IF_STA;
      
      if (esp_now_add_peer(&peerInfo) == ESP_OK) {
        pairedCount++;
        savePairedModules();
        Serial.println("✅ Module paired and saved");
      }
    }
    
    StaticJsonDocument<128> doc;
    doc["moduleId"] = ack.moduleId;
    doc["success"] = ack.success;
    
    char buffer[128];
    serializeJson(doc, buffer);
    
    client.publish(pairAckTopic.c_str(), buffer);
  }
  
  // --------------------------------------------------
  // MODULE TELEMETRY
  // --------------------------------------------------
  else if (strcmp(msgType, "TELEMETRY") == 0) {
    ModuleTelemetry telem;
    memcpy(&telem, data, sizeof(telem));
    
    Serial.print("📊 TELEMETRY from ");
    Serial.print(telem.moduleId);
    Serial.print(" Unit ");
    Serial.print(telem.unitIndex);
    Serial.print(" | V: ");
    Serial.print(telem.voltage);
    Serial.print("V, I: ");
    Serial.print(telem.current);
    Serial.print("A, P: ");
    Serial.print(telem.power);
    Serial.print("W, Relay: ");
    Serial.println(telem.relayState ? "ON" : "OFF");
    
    bool found = false;
    for (int i = 0; i < moduleDataCount; i++) {
      if (strcmp(moduleDataCache[i].moduleId, telem.moduleId) == 0 && 
          moduleDataCache[i].unitIndex == telem.unitIndex) {
        moduleDataCache[i].voltage = telem.voltage;
        moduleDataCache[i].current = telem.current;
        moduleDataCache[i].power = telem.power;
        moduleDataCache[i].relayState = telem.relayState;
        strcpy(moduleDataCache[i].health, telem.health);
        moduleDataCache[i].lastUpdate = millis();
        found = true;
        Serial.println("  ✅ Updated existing cache entry");
        break;
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
      Serial.print("  ✅ Added new cache entry (total: ");
      Serial.print(moduleDataCount);
      Serial.println(")");
    }
  }
  
  // --------------------------------------------------
  // MODULE FAULT
  // --------------------------------------------------
  else if (strcmp(msgType, "FAULT") == 0) {
    ModuleFault fault;
    memcpy(&fault, data, sizeof(fault));
    
    Serial.println("🚨 FAULT received from module:");
    Serial.print("  Module: ");
    Serial.println(fault.moduleId);
    Serial.print("  Unit: ");
    Serial.println(fault.unitNumber);
    Serial.print("  Type: ");
    Serial.println(fault.faultType);
    Serial.print("  Severity: ");
    Serial.println(fault.severity);
    Serial.print("  Description: ");
    Serial.println(fault.description);
    
    publishFault(
      "module",
      fault.moduleId,
      fault.unitNumber,
      fault.faultType,
      fault.severity,
      fault.description,
      fault.measuredValue,
      fault.thresholdValue,
      fault.unit
    );
    
    Serial.println("  ✅ Fault forwarded to server");
  }
  
  // --------------------------------------------------
  // MODULE RELAY ACK
  // --------------------------------------------------
  else if (strcmp(msgType, "RELAY_ACK") == 0) {
    ModuleRelayAck ack;
    memcpy(&ack, data, sizeof(ack));
    
    Serial.println("🔌 Module relay ACK received:");
    Serial.print("  Module: ");
    Serial.println(ack.moduleId);
    Serial.print("  Unit: ");
    Serial.println(ack.unitNumber);
    Serial.print("  Success: ");
    Serial.println(ack.success ? "YES" : "NO");
    Serial.print("  State: ");
    Serial.println(ack.relayState ? "ON" : "OFF");
    
    StaticJsonDocument<256> doc;
    doc["command"] = "module_relay_ack";
    doc["moduleId"] = ack.moduleId;
    doc["unitNumber"] = ack.unitNumber;
    doc["status"] = ack.success ? "SUCCESS" : "FAILED";
    doc["relayState"] = ack.relayState;
    
    char buffer[256];
    serializeJson(doc, buffer);
    
    client.publish(ackTopic.c_str(), buffer);
  }
}

// =====================================
// PUBLISH HEARTBEAT
// =====================================
void publishHeartbeat() {
  if (client.publish(heartbeatTopic.c_str(), "alive")) {
    Serial.println("❤️ Heartbeat sent");
  }
}

// =====================================
// PUBLISH TELEMETRY DATA
// =====================================
void publishTelemetry() {
  if (!relayLockedByServer && relayState) {
    mainVoltage = 230.0 + random(-20, 20) / 10.0;
    mainCurrent = 8.0 + random(-10, 10) / 10.0;
    mainPower = mainVoltage * mainCurrent * mainPowerFactor;
    mainFrequency = 50.0 + random(-5, 5) / 100.0;
  } else {
    mainVoltage = 0.0;
    mainCurrent = 0.0;
    mainPower = 0.0;
  }
  
  StaticJsonDocument<2048> doc;
  
  doc["deviceId"] = deviceId;
  doc["timestamp"] = millis() / 1000;
  doc["relayLocked"] = relayLockedByServer;
  
  JsonObject mainMeter = doc.createNestedObject("mainMeter");
  mainMeter["voltage"] = round(mainVoltage * 10) / 10.0;
  mainMeter["current"] = round(mainCurrent * 10) / 10.0;
  mainMeter["power"] = round(mainPower);
  mainMeter["powerFactor"] = mainPowerFactor;
  mainMeter["frequency"] = round(mainFrequency * 100) / 100.0;
  mainMeter["relayState"] = relayState;
  
  JsonArray modules = doc.createNestedArray("modules");
  
  Serial.print("📊 Building telemetry | Module data cache entries: ");
  Serial.println(moduleDataCount);
  
  if (!relayLockedByServer) {
    for (int i = 0; i < pairedCount; i++) {
      if (!pairedModules[i].isPaired) continue;
      
      bool hasData = false;
      JsonArray units;
      
      Serial.print("  Checking module: ");
      Serial.println(pairedModules[i].moduleId);
      
      for (int j = 0; j < moduleDataCount; j++) {
        if (strcmp(moduleDataCache[j].moduleId, pairedModules[i].moduleId) == 0) {
          if (millis() - moduleDataCache[j].lastUpdate < 10000) {
            if (!hasData) {
              JsonObject module = modules.createNestedObject();
              module["moduleId"] = pairedModules[i].moduleId;
              units = module.createNestedArray("units");
              hasData = true;
            }
            
            JsonObject unit = units.createNestedObject();
            unit["unitIndex"] = moduleDataCache[j].unitIndex;
            unit["voltage"] = round(moduleDataCache[j].voltage * 10) / 10.0;
            unit["current"] = round(moduleDataCache[j].current * 10) / 10.0;
            unit["power"] = round(moduleDataCache[j].power);
            unit["relayState"] = moduleDataCache[j].relayState;
            unit["health"] = moduleDataCache[j].health;
            
            Serial.print("    ✅ Added Unit ");
            Serial.println(moduleDataCache[j].unitIndex);
          } else {
            Serial.print("    ⚠️ Unit ");
            Serial.print(moduleDataCache[j].unitIndex);
            Serial.println(" data too old");
          }
        }
      }
      
      if (!hasData) {
        Serial.println("    ⚠️ No recent data found");
      }
    }
  }
  
  char buffer[2048];
  size_t jsonSize = serializeJson(doc, buffer);
  
  Serial.print("📊 JSON size: ");
  Serial.print(jsonSize);
  Serial.println(" bytes");
  
  if (client.publish(telemetryTopic.c_str(), buffer)) {
    Serial.println("📊 Telemetry published");
    Serial.print("  Modules included: ");
    Serial.println(modules.size());
  } else {
    Serial.println("❌ Telemetry publish failed");
    Serial.print("  MQTT state: ");
    Serial.println(client.state());
    Serial.print("  Buffer size: ");
    Serial.println(client.getBufferSize());
  }
}

// =====================================
// SETUP
// =====================================
void setup() {
  Serial.begin(115200);
  delay(1000);
  
  Serial.println("\n🚀 Main Meter Starting...");
  Serial.print("Firmware: ");
  Serial.println(firmwareVersion);
  Serial.print("Deployed: ");
  Serial.println(deploymentDate);
  Serial.println("🧪 FAULT TESTING MODE ENABLED");
  
  saveDeviceInfo();
  loadBillingState();
  loadRelayState();
  loadPairedModules();
  
  connectWiFi();
  
  WiFi.mode(WIFI_STA);
  
  if (esp_now_init() != ESP_OK) {
    Serial.println("❌ ESP-NOW init failed");
    return;
  }
  
  Serial.println("✅ ESP-NOW initialized");
  
  esp_now_register_recv_cb(onESPNowRecv);
  
  uint8_t channel = WiFi.channel();
  esp_wifi_set_channel(channel, WIFI_SECOND_CHAN_NONE);
  
  Serial.print("📶 ESP-NOW Channel: ");
  Serial.println(channel);
  
  esp_now_peer_info_t peerInfo;
  memset(&peerInfo, 0, sizeof(peerInfo));
  memset(peerInfo.peer_addr, 0xFF, 6);
  peerInfo.channel = channel;
  peerInfo.encrypt = false;
  peerInfo.ifidx = WIFI_IF_STA;
  
  if (esp_now_add_peer(&peerInfo) == ESP_OK) {
    Serial.println("✅ Broadcast peer added");
  }
  
  for (int i = 0; i < pairedCount; i++) {
    if (pairedModules[i].isPaired) {
      if (!esp_now_is_peer_exist(pairedModules[i].macAddr)) {
        esp_now_peer_info_t modulePeer;
        memset(&modulePeer, 0, sizeof(modulePeer));
        memcpy(modulePeer.peer_addr, pairedModules[i].macAddr, 6);
        modulePeer.channel = channel;
        modulePeer.encrypt = false;
        modulePeer.ifidx = WIFI_IF_STA;
        
        if (esp_now_add_peer(&modulePeer) == ESP_OK) {
          Serial.print("✅ Re-added peer: ");
          Serial.println(pairedModules[i].moduleId);
        }
      }
    }
  }
  
  setupMQTT();
  connectMQTT();
  
  Serial.println("🟢 Main meter ready");
  Serial.print("Paired modules: ");
  Serial.println(pairedCount);
  Serial.print("Relay status: ");
  Serial.println(relayState ? "ON" : "OFF (LOCKED)");
  
  lastBillingSync = millis();
  periodStartTimestamp = millis() / 1000;
  lastFaultTest = millis();
}

// =====================================
// LOOP
// =====================================
void loop() {
  if (!client.connected()) {
    connectMQTT();
  }
  client.loop();
  
  unsigned long now = millis();
  
  if (now - lastEnergyUpdate >= ENERGY_UPDATE_INTERVAL) {
    updateEnergyAccumulation();
  }
  
  if (now - lastBillingSync >= BILLING_SYNC_INTERVAL) {
    publishBillingSync();
    lastBillingSync = now;
  }
  
  if (now - lastHeartbeat >= HEARTBEAT_INTERVAL) {
    publishHeartbeat();
    lastHeartbeat = now;
  }
  
  if (now - lastTelemetry >= TELEMETRY_INTERVAL) {
    publishTelemetry();
    lastTelemetry = now;
  }
  
  if (now - lastFaultTest >= FAULT_TEST_INTERVAL) {
    testFaultPublishing();
    lastFaultTest = now;
  }
  
  delay(100);
}