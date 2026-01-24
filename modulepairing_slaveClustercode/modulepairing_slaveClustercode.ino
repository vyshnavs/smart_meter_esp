#include <WiFi.h>
#include <esp_now.h>
#include <esp_wifi.h>
#include <Preferences.h>

// ======================================================
// WIFI CONFIG  (MUST MATCH MAIN METER)
// ======================================================
const char* ssid     = "12345678";
const char* password = "1123344409";

// ======================================================
// SLAVE MODULE IDENTITY (FROM ADMIN TXT FILE)
// ======================================================
const char* moduleId = "MOD-A30B02";
const int   slaveCapacity = 2;
const char* pairingSecret = "xushYqwW67pp";   // stored securely

// ======================================================
// NVS STORAGE FOR PAIRING STATE
// ======================================================
Preferences preferences;

struct PairingState {
  bool isPaired;
  uint8_t masterMac[6];
  char masterId[32];
  unsigned long pairedAt;
};

PairingState pairingState;

// ======================================================
// ESP-NOW PAYLOAD STRUCTS
// ======================================================
typedef struct {
  char type[16];   // "SCAN_REQ"
} ScanRequest;

typedef struct {
  char type[16];   // "SCAN_RESP"
  char moduleId[16];
  int  capacity;
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

// ======================================================
// NVS FUNCTIONS
// ======================================================
void loadPairingState() {
  preferences.begin("pairing", false);
  
  pairingState.isPaired = preferences.getBool("isPaired", false);
  preferences.getBytes("masterMac", pairingState.masterMac, 6);
  preferences.getString("masterId", pairingState.masterId, 32);
  pairingState.pairedAt = preferences.getULong("pairedAt", 0);
  
  preferences.end();

  if (pairingState.isPaired) {
    Serial.println("🔗 Already paired with master:");
    Serial.print("  Master ID: ");
    Serial.println(pairingState.masterId);
    Serial.print("  Master MAC: ");
    for (int i = 0; i < 6; i++) {
      Serial.printf("%02X", pairingState.masterMac[i]);
      if (i < 5) Serial.print(":");
    }
    Serial.println();
  } else {
    Serial.println("🔓 Not paired - ready for pairing");
  }
}

void savePairingState(const uint8_t* masterMac, const char* masterId) {
  preferences.begin("pairing", false);
  
  preferences.putBool("isPaired", true);
  preferences.putBytes("masterMac", masterMac, 6);
  preferences.putString("masterId", masterId);
  preferences.putULong("pairedAt", millis());
  
  preferences.end();

  pairingState.isPaired = true;
  memcpy(pairingState.masterMac, masterMac, 6);
  strcpy(pairingState.masterId, masterId);
  pairingState.pairedAt = millis();

  Serial.println("💾 Pairing state saved to NVS");
}

void clearPairingState() {
  preferences.begin("pairing", false);
  preferences.clear();
  preferences.end();

  pairingState.isPaired = false;
  memset(pairingState.masterMac, 0, 6);
  memset(pairingState.masterId, 0, 32);
  pairingState.pairedAt = 0;

  Serial.println("🗑️ Pairing state cleared");
}

// ======================================================
// ESP-NOW RECEIVE CALLBACK
// ======================================================
void onESPNowRecv(
  const esp_now_recv_info_t* info,
  const uint8_t* data,
  int len
) {
  if (len < 16) return;

  char msgType[16];
  memcpy(msgType, data, 16);

  // --------------------------------------------------
  // SCAN REQUEST
  // Only respond if NOT paired
  // --------------------------------------------------
  if (strcmp(msgType, "SCAN_REQ") == 0) {
    if (pairingState.isPaired) {
      Serial.println("⚠️ Scan request received but already paired - ignoring");
      return;
    }

    Serial.println("📡 Scan request received (unpaired mode)");

    // Add sender as peer if not exists
    if (!esp_now_is_peer_exist(info->src_addr)) {
      esp_now_peer_info_t peerInfo;
      memset(&peerInfo, 0, sizeof(peerInfo));
      memcpy(peerInfo.peer_addr, info->src_addr, 6);
      peerInfo.channel = WiFi.channel();
      peerInfo.encrypt = false;
      peerInfo.ifidx = WIFI_IF_STA;
      
      if (esp_now_add_peer(&peerInfo) == ESP_OK) {
        Serial.println("✅ Added sender as peer");
      }
    }

    // Send scan response
    ScanResponse resp;
    memset(&resp, 0, sizeof(resp));
    strcpy(resp.type, "SCAN_RESP");
    strcpy(resp.moduleId, moduleId);
    resp.capacity = slaveCapacity;

    esp_err_t r = esp_now_send(
      info->src_addr,
      (uint8_t*)&resp,
      sizeof(resp)
    );

    Serial.print("📤 Scan response sent: ");
    Serial.println(r == ESP_OK ? "OK" : "FAILED");
  }

  // --------------------------------------------------
  // PAIR REQUEST (WITH SECRET VALIDATION)
  // --------------------------------------------------
  if (strcmp(msgType, "PAIR_REQ") == 0) {
    PairRequest req;
    memcpy(&req, data, sizeof(req));

    Serial.println("🔐 Pair request received");
    Serial.print("  Requested module: ");
    Serial.println(req.moduleId);
    Serial.print("  Provided secret: ");
    Serial.println(req.secret);

    // Add sender as peer
    if (!esp_now_is_peer_exist(info->src_addr)) {
      esp_now_peer_info_t peerInfo;
      memset(&peerInfo, 0, sizeof(peerInfo));
      memcpy(peerInfo.peer_addr, info->src_addr, 6);
      peerInfo.channel = WiFi.channel();
      peerInfo.encrypt = false;
      peerInfo.ifidx = WIFI_IF_STA;
      esp_now_add_peer(&peerInfo);
    }

    // Prepare acknowledgment
    PairAck ack;
    memset(&ack, 0, sizeof(ack));
    strcpy(ack.type, "PAIR_ACK");
    strcpy(ack.moduleId, moduleId);

    // VALIDATE: Module ID + Secret
    if (strcmp(req.moduleId, moduleId) == 0 &&
        strcmp(req.secret, pairingSecret) == 0) {
      
      // Check if already paired with different master
      if (pairingState.isPaired && 
          memcmp(info->src_addr, pairingState.masterMac, 6) != 0) {
        Serial.println("⚠️ Already paired with different master");
        ack.success = false;
      } else {
        // Success - save pairing
        ack.success = true;
        savePairingState(info->src_addr, "MASTER");
        
        Serial.println("✅ Pairing successful!");
        Serial.print("  Master MAC: ");
        for (int i = 0; i < 6; i++) {
          Serial.printf("%02X", info->src_addr[i]);
          if (i < 5) Serial.print(":");
        }
        Serial.println();
      }
    } else {
      // Failed validation
      ack.success = false;
      Serial.println("❌ Pairing validation failed!");
      
      if (strcmp(req.moduleId, moduleId) != 0) {
        Serial.println("  Reason: Module ID mismatch");
      }
      if (strcmp(req.secret, pairingSecret) != 0) {
        Serial.println("  Reason: Secret mismatch");
      }
    }

    // Send acknowledgment
    esp_err_t r = esp_now_send(
      info->src_addr,
      (uint8_t*)&ack,
      sizeof(ack)
    );
    
    Serial.print("📤 PAIR_ACK sent: ");
    Serial.println(r == ESP_OK ? "OK" : "FAILED");
  }

  // --------------------------------------------------
  // UNPAIR COMMAND
  // --------------------------------------------------
  if (strcmp(msgType, "UNPAIR_CMD") == 0) {
    UnpairCommand cmd;
    memcpy(&cmd, data, sizeof(cmd));

    Serial.println("🔓 Unpair command received");
    Serial.print("  Module: ");
    Serial.println(cmd.moduleId);

    // Validate: Only from paired master
    if (!pairingState.isPaired) {
      Serial.println("⚠️ Not paired - ignoring unpair command");
      return;
    }

    if (memcmp(info->src_addr, pairingState.masterMac, 6) != 0) {
      Serial.println("⚠️ Unpair from unknown device - ignoring");
      return;
    }

    // Validate module ID
    if (strcmp(cmd.moduleId, moduleId) == 0) {
      Serial.println("✅ Unpair validated - clearing state");
      
      // Remove master as peer
      esp_now_del_peer(pairingState.masterMac);
      
      // Clear pairing state
      clearPairingState();
      
      Serial.println("🔓 Module unpaired successfully");
    } else {
      Serial.println("⚠️ Module ID mismatch - ignoring");
    }
  }
}

// ======================================================
// SETUP
// ======================================================
void setup() {
  Serial.begin(115200);
  delay(1000);

  Serial.println("\n🚀 Slave Cluster Starting...");
  Serial.print("Module ID: ");
  Serial.println(moduleId);
  Serial.print("Capacity: ");
  Serial.println(slaveCapacity);

  // Load pairing state from NVS
  loadPairingState();

  // ---------------- WIFI ----------------
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

  // Lock ESP-NOW to WiFi channel
  uint8_t channel = WiFi.channel();
  esp_wifi_set_channel(channel, WIFI_SECOND_CHAN_NONE);

  // ---------------- ESP-NOW ----------------
  if (esp_now_init() != ESP_OK) {
    Serial.println("❌ ESP-NOW init failed");
    return;
  }

  Serial.println("✅ ESP-NOW initialized");

  // Register receive callback
  esp_now_register_recv_cb(onESPNowRecv);

  // If already paired, add master as peer
  if (pairingState.isPaired) {
    esp_now_peer_info_t masterPeer;
    memset(&masterPeer, 0, sizeof(masterPeer));
    memcpy(masterPeer.peer_addr, pairingState.masterMac, 6);
    masterPeer.channel = channel;
    masterPeer.encrypt = false;
    masterPeer.ifidx = WIFI_IF_STA;
    
    if (esp_now_add_peer(&masterPeer) == ESP_OK) {
      Serial.println("✅ Re-added paired master as peer");
    }
  }

  Serial.println("🟢 Slave cluster ready");
  Serial.println(pairingState.isPaired ? "  Mode: PAIRED" : "  Mode: PAIRING");
}

// ======================================================
// LOOP
// ======================================================
void loop() {
  // Heartbeat
  static unsigned long lastBlink = 0;
  if (millis() - lastBlink > 5000) {
    lastBlink = millis();
    Serial.print("💓 Heartbeat | Paired: ");
    Serial.println(pairingState.isPaired ? "YES" : "NO");
  }
  
  delay(1000);
}