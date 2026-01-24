#include <WiFi.h>
#include <PubSubClient.h>
#include <esp_now.h>
#include <esp_wifi.h>
#include <Preferences.h>


// =====================================
// WIFI CONFIG
// =====================================
const char* ssid = "boom";
const char* password = "123123123456";


// =====================================
// MQTT CONFIG (Backend IP)
// =====================================
const char* mqttServer = "192.168.239.171";
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
String energyTopic  = "meter/" + String(deviceId) + "/energy";
String commandTopic = "meter/" + String(deviceId) + "/command";
String scanTopic    = "meter/" + String(deviceId) + "/scan";
String pairAckTopic = "meter/" + String(deviceId) + "/pair_ack";


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
 
  // Check if already exists
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
 
  // Add new module
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
     
      // Remove ESP-NOW peer
      if (esp_now_is_peer_exist(pairedModules[i].macAddr)) {
        esp_now_del_peer(pairedModules[i].macAddr);
      }
     
      // Shift array
      for (int j = i; j < pairedCount - 1; j++) {
        pairedModules[j] = pairedModules[j + 1];
        String key = "mod_" + String(j);
        preferences.putBytes(key.c_str(), &pairedModules[j], sizeof(PairedModule));
      }
     
      pairedCount--;
      preferences.putInt("count", pairedCount);
     
      // Clear last slot
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


void clearAllModules() {
  preferences.begin("modules", false);
  preferences.clear();
  preferences.end();
 
  pairedCount = 0;
  Serial.println("🗑️ All modules cleared");
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
  Serial.println(msg);


  // ==========================================
  // START MODULE SCAN
  // ==========================================
  if (msg.indexOf("START_MODULE_SCAN") >= 0) {
    Serial.println("🚀 START_MODULE_SCAN command detected");


    ScanRequest scan;
    memset(&scan, 0, sizeof(scan));
    strcpy(scan.type, "SCAN_REQ");


    uint8_t broadcastAddr[6] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
    esp_err_t res = esp_now_send(broadcastAddr, (uint8_t*)&scan, sizeof(scan));


    Serial.print("📡 ESP-NOW broadcast sent, result = ");
    Serial.println(res == ESP_OK ? "ESP_OK" : "FAILED");
  }


  // ==========================================
  // MODULE PAIRING REQUEST
  // Expected: {"action":"MODULE_PAIRED","moduleId":"MOD-45ED0F","secret":"ex5H6lUOcrKg"}
  // ==========================================
  if (msg.indexOf("MODULE_PAIRED") >= 0) {
    Serial.println("🔐 MODULE_PAIRED command detected");


    // Parse JSON manually (simple approach)
    int moduleIdStart = msg.indexOf("\"moduleId\":\"") + 12;
    int moduleIdEnd = msg.indexOf("\"", moduleIdStart);
    String moduleId = msg.substring(moduleIdStart, moduleIdEnd);


    int secretStart = msg.indexOf("\"secret\":\"") + 10;
    int secretEnd = msg.indexOf("\"", secretStart);
    String secret = msg.substring(secretStart, secretEnd);


    Serial.print("Module ID: ");
    Serial.println(moduleId);
    Serial.print("Secret: ");
    Serial.println(secret);


    // Send PAIR_REQ via broadcast (slave will validate)
    PairRequest pairReq;
    memset(&pairReq, 0, sizeof(pairReq));
    strcpy(pairReq.type, "PAIR_REQ");
    moduleId.toCharArray(pairReq.moduleId, 16);
    secret.toCharArray(pairReq.secret, 32);


    uint8_t broadcastAddr[6] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
    esp_err_t res = esp_now_send(broadcastAddr, (uint8_t*)&pairReq, sizeof(pairReq));


    Serial.print("📡 PAIR_REQ sent, result = ");
    Serial.println(res == ESP_OK ? "ESP_OK" : "FAILED");
   
    if (res != ESP_OK) {
      Serial.print("Error code: ");
      Serial.println(res);
    }
  }


  // ==========================================
  // MODULE UNPAIR REQUEST
  // Expected: {"action":"UNPAIR_MODULE","moduleId":"MOD-45ED0F"}
  // ==========================================
  if (msg.indexOf("UNPAIR_MODULE") >= 0) {
    Serial.println("🔓 UNPAIR_MODULE command detected");


    int moduleIdStart = msg.indexOf("\"moduleId\":\"") + 12;
    int moduleIdEnd = msg.indexOf("\"", moduleIdStart);
    String moduleId = msg.substring(moduleIdStart, moduleIdEnd);


    Serial.print("Unpairing: ");
    Serial.println(moduleId);


    // Send UNPAIR command to slave
    UnpairCommand unpairCmd;
    memset(&unpairCmd, 0, sizeof(unpairCmd));
    strcpy(unpairCmd.type, "UNPAIR_CMD");
    moduleId.toCharArray(unpairCmd.moduleId, 16);


    // Find module MAC and send directly
    for (int i = 0; i < pairedCount; i++) {
      if (strcmp(pairedModules[i].moduleId, moduleId.c_str()) == 0) {
        esp_now_send(pairedModules[i].macAddr, (uint8_t*)&unpairCmd, sizeof(unpairCmd));
        break;
      }
    }


    // Remove from local storage
    unpairModule(moduleId.c_str());


    // Confirm to backend
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


  // ==========================================
  // SCAN RESPONSE
  // Only process if module is NOT already paired
  // ==========================================
  if (strcmp(type, "SCAN_RESP") == 0) {
    ScanResponse resp;
    memcpy(&resp, data, sizeof(resp));

    // Check if module is already paired
    if (isModulePaired(resp.moduleId)) {
      Serial.print("⚠️ Module ");
      Serial.print(resp.moduleId);
      Serial.println(" is already paired - ignoring scan response");
      return;
    }

    Serial.print("📍 Found unpaired module: ");
    Serial.print(resp.moduleId);
    Serial.print(" | Capacity: ");
    Serial.println(resp.capacity);


    // Add as temporary peer for pairing
    if (!esp_now_is_peer_exist(info->src_addr)) {
      esp_now_peer_info_t peerInfo;
      memset(&peerInfo, 0, sizeof(peerInfo));
      memcpy(peerInfo.peer_addr, info->src_addr, 6);
      peerInfo.channel = WiFi.channel();
      peerInfo.encrypt = false;
      peerInfo.ifidx = WIFI_IF_STA;
      esp_now_add_peer(&peerInfo);
    }


    // Publish to backend
    char jsonBuffer[128];
    snprintf(jsonBuffer, sizeof(jsonBuffer),
      "{\"moduleId\":\"%s\",\"capacity\":%d}",
      resp.moduleId,
      resp.capacity
    );


    Serial.print("📤 Publishing to: ");
    Serial.println(scanTopic);
    Serial.print("📦 Payload: ");
    Serial.println(jsonBuffer);
   
    bool published = client.publish(scanTopic.c_str(), jsonBuffer);
    Serial.println(published ? "✅ Published successfully" : "❌ Publish failed");
  }


  // ==========================================
  // PAIR ACKNOWLEDGMENT
  // ==========================================
  if (strcmp(type, "PAIR_ACK") == 0) {
    PairAck ack;
    memcpy(&ack, data, sizeof(ack));


    Serial.print("🔐 PAIR_ACK received from module: ");
    Serial.println(ack.moduleId);
    Serial.println(ack.success ? "✅ Pairing successful" : "❌ Pairing rejected");


    if (ack.success) {
      // Find module capacity from previous scan
      int capacity = 0;
      for (int i = 0; i < pairedCount; i++) {
        if (strcmp(pairedModules[i].moduleId, ack.moduleId) == 0) {
          capacity = pairedModules[i].capacity;
          break;
        }
      }


      // Save to NVS
      savePairedModule(ack.moduleId, info->src_addr, capacity);


      // Notify backend
      char jsonBuffer[128];
      snprintf(jsonBuffer, sizeof(jsonBuffer),
        "{\"moduleId\":\"%s\",\"status\":\"paired\",\"success\":true}",
        ack.moduleId
      );
      client.publish(pairAckTopic.c_str(), jsonBuffer);
     
      Serial.println("💾 Module saved to NVS");
    } else {
      // Notify backend of failure
      char jsonBuffer[128];
      snprintf(jsonBuffer, sizeof(jsonBuffer),
        "{\"moduleId\":\"%s\",\"status\":\"failed\",\"success\":false}",
        ack.moduleId
      );
      client.publish(pairAckTopic.c_str(), jsonBuffer);
    }
  }
}


// =====================================
// SETUP
// =====================================
void setup() {
  Serial.begin(115200);
  delay(1000);


  Serial.println("\n🚀 Main Meter Starting...");


  // Load paired modules from NVS
  loadPairedModules();


  // Initialize ESP-NOW FIRST
  WiFi.mode(WIFI_STA);
 
  if (esp_now_init() != ESP_OK) {
    Serial.println("❌ ESP-NOW init failed");
    return;
  }
 
  Serial.println("✅ ESP-NOW initialized");


  // Register receive callback
  esp_now_register_recv_cb(onESPNowRecv);


  // Add broadcast peer
  esp_now_peer_info_t peerInfo;
  memset(&peerInfo, 0, sizeof(peerInfo));
  memset(peerInfo.peer_addr, 0xFF, 6);
  peerInfo.channel = 0;
  peerInfo.encrypt = false;
  peerInfo.ifidx = WIFI_IF_STA;


  if (esp_now_add_peer(&peerInfo) != ESP_OK) {
    Serial.println("❌ Failed to add broadcast peer");
  } else {
    Serial.println("✅ Broadcast peer added");
  }


  // Connect to WiFi
  connectWiFi();
 
  // Update ESP-NOW channel
  uint8_t channel = WiFi.channel();
  esp_wifi_set_channel(channel, WIFI_SECOND_CHAN_NONE);
 
  // Update broadcast peer channel
  esp_now_del_peer(peerInfo.peer_addr);
  peerInfo.channel = channel;
  esp_now_add_peer(&peerInfo);
 
  Serial.print("✅ ESP-NOW locked to WiFi channel: ");
  Serial.println(channel);


  // Re-add all paired modules as peers
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
       
        Serial.print("✅ Re-added peer: ");
        Serial.println(pairedModules[i].moduleId);
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


  float energyValue = random(100, 900);
  char payload[16];
  dtostrf(energyValue, 1, 2, payload);


  client.publish(energyTopic.c_str(), payload);
  Serial.print("⚡ Energy published: ");
  Serial.println(payload);


  delay(3000);
}