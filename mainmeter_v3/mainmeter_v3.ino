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
const char* mqttServer = "192.168.67.171";
const int mqttPort = 1883;

// =====================================
// DEVICE DETAILS (From Admin Panel)
// =====================================
const int deviceId = 44188662;
const char* secretKey = "6vM3ihdppGGtwf54Yu";

// =====================================
// NVS STORAGE FOR PAIRED MODULES
// =====================================
Preferences preferences;

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
String heartbeatTopic  = "meter/" + String(deviceId) + "/heartbeat";  // HEARTBEAT
String commandTopic    = "meter/" + String(deviceId) + "/command";
String scanTopic       = "meter/" + String(deviceId) + "/scan";
String pairAckTopic    = "meter/" + String(deviceId) + "/pair_ack";

// =====================================
// ESP-NOW PAYLOAD STRUCTS
// =====================================
typedef struct {
  char type[16];   // "SCAN_REQ"
} ScanRequest;

typedef struct {
  char type[16];   // "SCAN_RESP"
  char moduleId[16];
  int capacity;
} ScanResponse;

typedef struct {
  char type[16];   // "PAIR_REQ"
  char moduleId[16];
  char secret[32];
} PairRequest;

typedef struct {
  char type[16];   // "PAIR_ACK"
  bool success;
  char moduleId[16];
} PairAck;

typedef struct {
  char type[16];   // "UNPAIR_CMD"
  char moduleId[16];
} UnpairCommand;

// =====================================
// TELEMETRY DATA FROM MODULES
// =====================================
typedef struct {
  char type[16];        // "TELEMETRY"
  char moduleId[16];
  float voltage;
  float current;
  float power;
  bool relayState;
  char health[8];       // "OK", "WARNING", "ERROR"
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

ModuleData moduleDataCache[MAX_MODULES * 2]; // Max 2 units per module
int moduleDataCount = 0;

// =====================================
// SIMULATED MAIN METER READINGS
// =====================================
float mainVoltage = 230.0;
float mainCurrent = 8.0;
float mainPower = 1840.0;
float mainPowerFactor = 0.94;
float mainFrequency = 50.0;
bool mainRelayState = true;

// =====================================
// TIMING CONSTANTS
// =====================================
const unsigned long TELEMETRY_INTERVAL = 3000;  // 3 seconds
const unsigned long HEARTBEAT_INTERVAL = 2000;  // 2 seconds (must be < 5s for presence worker)

unsigned long lastTelemetry = 0;
unsigned long lastHeartbeat = 0;

// =====================================
// HELPER FUNCTION TO CHECK IF MODULE IS PAIRED
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
// NVS FUNCTIONS
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

  Serial.println("📨 MQTT command received");

  if (msg.indexOf("START_MODULE_SCAN") >= 0) {
    Serial.println("🚀 START_MODULE_SCAN");

    ScanRequest scan;
    memset(&scan, 0, sizeof(scan));
    strcpy(scan.type, "SCAN_REQ");

    uint8_t broadcastAddr[6] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
    esp_err_t res = esp_now_send(broadcastAddr, (uint8_t*)&scan, sizeof(scan));

    Serial.println(res == ESP_OK ? "📡 Scan broadcast sent" : "❌ Scan failed");
  }

  if (msg.indexOf("MODULE_PAIRED") >= 0) {
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

  // SCAN RESPONSE
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

  // PAIR ACKNOWLEDGMENT
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

  // MODULE TELEMETRY DATA
  if (strcmp(type, "TELEMETRY") == 0) {
    ModuleTelemetry telem;
    memcpy(&telem, data, sizeof(telem));

    // Update cache
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
  // Simulate realistic variations
  mainVoltage = 230.0 + random(-20, 20) / 10.0;
  mainCurrent = 8.0 + random(-10, 10) / 10.0;
  mainPower = mainVoltage * mainCurrent * mainPowerFactor;
  mainFrequency = 50.0 + random(-5, 5) / 100.0;

  StaticJsonDocument<1024> doc;
  
  doc["deviceId"] = deviceId;
  doc["timestamp"] = millis() / 1000;

  JsonObject mainMeter = doc.createNestedObject("mainMeter");
  mainMeter["voltage"] = round(mainVoltage * 10) / 10.0;
  mainMeter["current"] = round(mainCurrent * 10) / 10.0;
  mainMeter["power"] = round(mainPower);
  mainMeter["powerFactor"] = mainPowerFactor;
  mainMeter["frequency"] = round(mainFrequency * 100) / 100.0;
  mainMeter["relayState"] = mainRelayState;

  JsonArray modules = doc.createNestedArray("modules");

  // Group module data by moduleId
  for (int i = 0; i < pairedCount; i++) {
    if (!pairedModules[i].isPaired) continue;

    bool hasData = false;
    JsonArray units;

    // Check if this module has any telemetry data
    for (int j = 0; j < moduleDataCount; j++) {
      if (strcmp(moduleDataCache[j].moduleId, pairedModules[i].moduleId) == 0) {
        // Only include recent data (within last 10 seconds)
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

  // Publish heartbeat every 2 seconds (CRITICAL for presence detection)
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