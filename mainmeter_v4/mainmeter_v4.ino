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
const char* password = "123123123456";

// =====================================
// MQTT CONFIG (Backend IP)
// =====================================
const char* mqttServer = "192.168.113.171";
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
const unsigned long BILLING_SYNC_INTERVAL = 30000; // 1 hour in milliseconds
unsigned long lastBillingSync = 0;
unsigned long billingSequenceNo = 0;

// Energy accumulation
float accumulatedEnergyWh = 0.0;
unsigned long periodStartTimestamp = 0;
unsigned long lastEnergyUpdate = 0;

// Relay & Wallet State
bool relayLockedByServer = false;  // Persistent lock from server
bool relayState = true;             // Current relay state
bool faultActive = false;

// Code deployment tracking
const char* firmwareVersion = "1.0.0";
const char* deploymentDate = "2025-01-24";

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

// Store latest telemetry from each module
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

ModuleData moduleDataCache[MAX_MODULES * 2];
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
const unsigned long ENERGY_UPDATE_INTERVAL = 1000; // Update energy every second

unsigned long lastTelemetry = 0;
unsigned long lastHeartbeat = 0;

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
  
  // If relay is locked, enforce it
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
  
  // Only accumulate if relay is ON
  if (relayState && !relayLockedByServer) {
    unsigned long deltaMs = now - lastEnergyUpdate;
    float deltaHours = deltaMs / 3600000.0;
    
    // Energy = Power × Time
    float energyWh = mainPower * deltaHours;
    accumulatedEnergyWh += energyWh;
    
    // Save periodically (every 10 seconds)
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
  unsigned long now = millis() / 1000; // Convert to seconds
  
  // Initialize period start if not set
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
    
    // Reset for next period
    accumulatedEnergyWh = 0.0;
    periodStartTimestamp = now;
    billingSequenceNo++;
    saveBillingState();
  } else {
    Serial.println("❌ Billing sync failed - will retry");
  }
}

// =====================================
// HELPER FUNCTIONS
// =====================================
bool isModulePaired(const char* moduleId) {
  for (int i = 0; i < pairedCount; i++) {
    if (strcmp(pairedModules[i].moduleId, moduleId) == 0 && pairedModules[i].isPaired) {
      return true;
    }
  }
  return false;
}

// =====================================
// NVS FUNCTIONS - PAIRED MODULES
// =====================================
void loadPairedModules() {
  preferences.begin("modules", false);
  pairedCount = preferences.getInt("count", 0);
  
  Serial.print("📂 Loading ");
  Serial.print(pairedCount);
  Serial.println(" paired modules from NVS");
  
  for (int i = 0; i < pairedCount && i < MAX_MODULES; i++) {
    String key = "mod_" + String(i);
    size_t len = preferences.getBytes(key.c_str(), &pairedModules[i], sizeof(PairedModule));
    
    if (len > 0 && pairedModules[i].isPaired) {
      Serial.print("  ✅ ");
      Serial.print(pairedModules[i].moduleId);
      Serial.print(" | MAC: ");
      for (int j = 0; j < 6; j++) {
        Serial.printf("%02X", pairedModules[i].macAddr[j]);
        if (j < 5) Serial.print(":");
      }
      Serial.println();
    }
  }
  
  preferences.end();
}

void savePairedModule(const char* moduleId, const uint8_t* macAddr, int capacity) {
  preferences.begin("modules", false);
  
  for (int i = 0; i < pairedCount; i++) {
    if (strcmp(pairedModules[i].moduleId, moduleId) == 0) {
      Serial.println("⚠️ Module already paired, updating...");
      memcpy(pairedModules[i].macAddr, macAddr, 6);
      pairedModules[i].capacity = capacity;
      pairedModules[i].isPaired = true;
      
      String key = "mod_" + String(i);
      preferences.putBytes(key.c_str(), &pairedModules[i], sizeof(PairedModule));
      preferences.end();
      return;
    }
  }
  
  if (pairedCount < MAX_MODULES) {
    strcpy(pairedModules[pairedCount].moduleId, moduleId);
    memcpy(pairedModules[pairedCount].macAddr, macAddr, 6);
    pairedModules[pairedCount].capacity = capacity;
    pairedModules[pairedCount].isPaired = true;
    
    String key = "mod_" + String(pairedCount);
    preferences.putBytes(key.c_str(), &pairedModules[pairedCount], sizeof(PairedModule));
    
    pairedCount++;
    preferences.putInt("count", pairedCount);
    
    Serial.print("💾 Saved module: ");
    Serial.println(moduleId);
  } else {
    Serial.println("❌ Max modules reached!");
  }
  
  preferences.end();
}

void unpairModule(const char* moduleId) {
  preferences.begin("modules", false);
  
  for (int i = 0; i < pairedCount; i++) {
    if (strcmp(pairedModules[i].moduleId, moduleId) == 0) {
      Serial.print("🗑️ Unpairing: ");
      Serial.println(moduleId);
      
      if (esp_now_is_peer_exist(pairedModules[i].macAddr)) {
        esp_now_del_peer(pairedModules[i].macAddr);
      }
      
      for (int j = i; j < pairedCount - 1; j++) {
        pairedModules[j] = pairedModules[j + 1];
        String key = "mod_" + String(j);
        preferences.putBytes(key.c_str(), &pairedModules[j], sizeof(PairedModule));
      }
      
      pairedCount--;
      preferences.putInt("count", pairedCount);
      
      String lastKey = "mod_" + String(pairedCount);
      preferences.remove(lastKey.c_str());
      
      Serial.println("✅ Module unpaired");
      preferences.end();
      return;
    }
  }
  
  Serial.println("⚠️ Module not found");
  preferences.end();
}

// =====================================
// WIFI CONNECT
// =====================================
void connectWiFi() {
  Serial.print("Connecting to WiFi");
  
  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid, password);
  
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  
  Serial.println("\n🟢 WiFi connected");
  Serial.print("ESP IP: ");
  Serial.println(WiFi.localIP());
  
  uint8_t channel = WiFi.channel();
  esp_wifi_set_channel(channel, WIFI_SECOND_CHAN_NONE);
  
  Serial.print("📶 WiFi / ESP-NOW Channel: ");
  Serial.println(channel);
}

// =====================================
// MQTT CONNECT
// =====================================
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
    } else {
      Serial.print("❌ MQTT failed, rc=");
      Serial.println(client.state());
      delay(3000);
    }
  }
}

// =====================================
// MQTT COMMAND HANDLER
// =====================================
void onMQTTMessage(char* topic, byte* payload, unsigned int length) {
  payload[length] = '\0';
  String msg = String((char*)payload);
  
  Serial.println("📨 MQTT command received: " + msg);
  
  // RELAY CONTROL COMMANDS
  if (msg.indexOf("RELAY_CUTOFF") >= 0) {
    Serial.println("🔒 RELAY_CUTOFF - Insufficient balance");
    
    relayLockedByServer = true;
    relayState = false;
    saveBillingState();
    
    // Send ACK
    StaticJsonDocument<128> ackDoc;
    ackDoc["command"] = "RELAY_CUTOFF";
    ackDoc["status"] = "SUCCESS";
    ackDoc["relayState"] = false;
    ackDoc["timestamp"] = millis() / 1000;
    
    char ackBuffer[128];
    serializeJson(ackDoc, ackBuffer);
    client.publish(ackTopic.c_str(), ackBuffer);
    
    return;
  }
  
  if (msg.indexOf("RELAY_RESTORE") >= 0) {
    Serial.println("🔓 RELAY_RESTORE - Balance recharged");
    
    relayLockedByServer = false;
    relayState = true;
    saveBillingState();
    
    // Send ACK
    StaticJsonDocument<128> ackDoc;
    ackDoc["command"] = "RELAY_RESTORE";
    ackDoc["status"] = "SUCCESS";
    ackDoc["relayState"] = true;
    ackDoc["timestamp"] = millis() / 1000;
    
    char ackBuffer[128];
    serializeJson(ackDoc, ackBuffer);
    client.publish(ackTopic.c_str(), ackBuffer);
    
    return;
  }
  
  // MODULE SCAN
  if (msg.indexOf("START_MODULE_SCAN") >= 0) {
    // Only allow scan if not locked
    if (relayLockedByServer) {
      Serial.println("⚠️ Module scan blocked - device locked");
      return;
    }
    
    Serial.println("🚀 START_MODULE_SCAN");
    
    ScanRequest scan;
    memset(&scan, 0, sizeof(scan));
    strcpy(scan.type, "SCAN_REQ");
    
    uint8_t broadcastAddr[6] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
    esp_err_t res = esp_now_send(broadcastAddr, (uint8_t*)&scan, sizeof(scan));
    
    Serial.println(res == ESP_OK ? "📡 Scan broadcast sent" : "❌ Scan failed");
  }
  
  // MODULE PAIRING
  if (msg.indexOf("MODULE_PAIRED") >= 0) {
    if (relayLockedByServer) {
      Serial.println("⚠️ Module pairing blocked - device locked");
      return;
    }
    
    Serial.println("🔐 MODULE_PAIRED");
    
    int moduleIdStart = msg.indexOf("\"moduleId\":\"") + 12;
    int moduleIdEnd = msg.indexOf("\"", moduleIdStart);
    String moduleId = msg.substring(moduleIdStart, moduleIdEnd);
    
    int secretStart = msg.indexOf("\"secret\":\"") + 10;
    int secretEnd = msg.indexOf("\"", secretStart);
    String secret = msg.substring(secretStart, secretEnd);
    
    PairRequest pairReq;
    memset(&pairReq, 0, sizeof(pairReq));
    strcpy(pairReq.type, "PAIR_REQ");
    moduleId.toCharArray(pairReq.moduleId, 16);
    secret.toCharArray(pairReq.secret, 32);
    
    uint8_t broadcastAddr[6] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
    esp_now_send(broadcastAddr, (uint8_t*)&pairReq, sizeof(pairReq));
    
    Serial.println("📡 PAIR_REQ sent");
  }
  
  // MODULE UNPAIRING
  if (msg.indexOf("UNPAIR_MODULE") >= 0) {
    Serial.println("🔓 UNPAIR_MODULE");
    
    int moduleIdStart = msg.indexOf("\"moduleId\":\"") + 12;
    int moduleIdEnd = msg.indexOf("\"", moduleIdStart);
    String moduleId = msg.substring(moduleIdStart, moduleIdEnd);
    
    UnpairCommand unpairCmd;
    memset(&unpairCmd, 0, sizeof(unpairCmd));
    strcpy(unpairCmd.type, "UNPAIR_CMD");
    moduleId.toCharArray(unpairCmd.moduleId, 16);
    
    for (int i = 0; i < pairedCount; i++) {
      if (strcmp(pairedModules[i].moduleId, moduleId.c_str()) == 0) {
        esp_now_send(pairedModules[i].macAddr, (uint8_t*)&unpairCmd, sizeof(unpairCmd));
        break;
      }
    }
    
    unpairModule(moduleId.c_str());
    
    char jsonBuffer[128];
    snprintf(jsonBuffer, sizeof(jsonBuffer),
      "{\"moduleId\":\"%s\",\"status\":\"unpaired\"}",
      moduleId.c_str()
    );
    client.publish(pairAckTopic.c_str(), jsonBuffer);
  }
}

void setupMQTT() {
  client.setServer(mqttServer, mqttPort);
  client.setCallback(onMQTTMessage);
}

// =====================================
// ESP-NOW RECEIVE CALLBACK
// =====================================
void onESPNowRecv(
  const esp_now_recv_info_t* info,
  const uint8_t* data,
  int len
) {
  if (len < 16) return;
  
  char type[16];
  memcpy(type, data, 16);
  
  if (strcmp(type, "SCAN_RESP") == 0) {
    ScanResponse resp;
    memcpy(&resp, data, sizeof(resp));
    
    if (isModulePaired(resp.moduleId)) {
      return;
    }
    
    Serial.print("📍 Found module: ");
    Serial.println(resp.moduleId);
    
    if (!esp_now_is_peer_exist(info->src_addr)) {
      esp_now_peer_info_t peerInfo;
      memset(&peerInfo, 0, sizeof(peerInfo));
      memcpy(peerInfo.peer_addr, info->src_addr, 6);
      peerInfo.channel = WiFi.channel();
      peerInfo.encrypt = false;
      peerInfo.ifidx = WIFI_IF_STA;
      esp_now_add_peer(&peerInfo);
    }
    
    char jsonBuffer[128];
    snprintf(jsonBuffer, sizeof(jsonBuffer),
      "{\"moduleId\":\"%s\",\"capacity\":%d}",
      resp.moduleId,
      resp.capacity
    );
    
    client.publish(scanTopic.c_str(), jsonBuffer);
  }
  
  if (strcmp(type, "PAIR_ACK") == 0) {
    PairAck ack;
    memcpy(&ack, data, sizeof(ack));
    
    Serial.print("🔐 PAIR_ACK: ");
    Serial.println(ack.success ? "✅ Success" : "❌ Failed");
    
    if (ack.success) {
      int capacity = 0;
      for (int i = 0; i < pairedCount; i++) {
        if (strcmp(pairedModules[i].moduleId, ack.moduleId) == 0) {
          capacity = pairedModules[i].capacity;
          break;
        }
      }
      
      savePairedModule(ack.moduleId, info->src_addr, capacity);
      
      char jsonBuffer[128];
      snprintf(jsonBuffer, sizeof(jsonBuffer),
        "{\"moduleId\":\"%s\",\"status\":\"paired\",\"success\":true}",
        ack.moduleId
      );
      client.publish(pairAckTopic.c_str(), jsonBuffer);
    }
  }
  
  if (strcmp(type, "TELEMETRY") == 0) {
    ModuleTelemetry telem;
    memcpy(&telem, data, sizeof(telem));
    
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
        break;
      }
    }
    
    if (!found && moduleDataCount < (MAX_MODULES * 2)) {
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
  // Simulate realistic variations (only if not locked)
  if (!relayLockedByServer && relayState) {
    mainVoltage = 230.0 + random(-20, 20) / 10.0;
    mainCurrent = 8.0 + random(-10, 10) / 10.0;
    mainPower = mainVoltage * mainCurrent * mainPowerFactor;
    mainFrequency = 50.0 + random(-5, 5) / 100.0;
  } else {
    // Device locked - minimal readings
    mainVoltage = 0.0;
    mainCurrent = 0.0;
    mainPower = 0.0;
  }
  
  StaticJsonDocument<1024> doc;
  
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
  
  // Only include module data if not locked
  if (!relayLockedByServer) {
    for (int i = 0; i < pairedCount; i++) {
      if (!pairedModules[i].isPaired) continue;
      
      bool hasData = false;
      JsonArray units;
      
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
          }
        }
      }
    }
  }
  
  char buffer[1024];
  serializeJson(doc, buffer);
  
  if (client.publish(telemetryTopic.c_str(), buffer)) {
    Serial.println("📊 Telemetry published");
  } else {
    Serial.println("❌ Telemetry publish failed");
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
  
  // Save device info
  saveDeviceInfo();
  
  // Load billing state
  loadBillingState();
  
  // Load paired modules
  loadPairedModules();
  
  WiFi.mode(WIFI_STA);
  
  if (esp_now_init() != ESP_OK) {
    Serial.println("❌ ESP-NOW init failed");
    return;
  }
  
  Serial.println("✅ ESP-NOW initialized");
  
  esp_now_register_recv_cb(onESPNowRecv);
  
  esp_now_peer_info_t peerInfo;
  memset(&peerInfo, 0, sizeof(peerInfo));
  memset(peerInfo.peer_addr, 0xFF, 6);
  peerInfo.channel = 0;
  peerInfo.encrypt = false;
  peerInfo.ifidx = WIFI_IF_STA;
  
  if (esp_now_add_peer(&peerInfo) != ESP_OK) {
    Serial.println("❌ Failed to add broadcast peer");
  }
  
  connectWiFi();
  
  uint8_t channel = WiFi.channel();
  esp_wifi_set_channel(channel, WIFI_SECOND_CHAN_NONE);
  
  esp_now_del_peer(peerInfo.peer_addr);
  peerInfo.channel = channel;
  esp_now_add_peer(&peerInfo);
  
  for (int i = 0; i < pairedCount; i++) {
    if (pairedModules[i].isPaired) {
      if (!esp_now_is_peer_exist(pairedModules[i].macAddr)) {
        esp_now_peer_info_t modulePeer;
        memset(&modulePeer, 0, sizeof(modulePeer));
        memcpy(modulePeer.peer_addr, pairedModules[i].macAddr, 6);
        modulePeer.channel = channel;
        modulePeer.encrypt = false;
        modulePeer.ifidx = WIFI_IF_STA;
        esp_now_add_peer(&modulePeer);
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
  
  // Initialize timing
  lastBillingSync = millis();
  periodStartTimestamp = millis() / 1000;
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
  
  // Update energy accumulation every second
  if (now - lastEnergyUpdate >= ENERGY_UPDATE_INTERVAL) {
    updateEnergyAccumulation();
  }
  
  // Publish billing sync every hour
  if (now - lastBillingSync >= BILLING_SYNC_INTERVAL) {
    publishBillingSync();
    lastBillingSync = now;
  }
  
  // Publish heartbeat every 2 seconds
  if (now - lastHeartbeat >= HEARTBEAT_INTERVAL) {
    publishHeartbeat();
    lastHeartbeat = now;
  }
  
  // Publish telemetry every 3 seconds
  if (now - lastTelemetry >= TELEMETRY_INTERVAL) {
    publishTelemetry();
    lastTelemetry = now;
  }
  
  delay(100);
}