#include <WiFi.h>
#include <esp_now.h>
#include <esp_wifi.h>
#include <Preferences.h>

// ======================================================
// WIFI CONFIG  (MUST MATCH MAIN METER)
// ======================================================
const char* ssid     = "boom";
const char* password = "123123123456";

// ======================================================
// SLAVE MODULE IDENTITY (FROM ADMIN TXT FILE)
// ======================================================
const char* moduleId = "MOD-7FCEAB";
const int   slaveCapacity = 4;
const char* pairingSecret = "CpmZ3p5dus84";   // stored securely

// ======================================================
// NVS STORAGE FOR PAIRING STATE
// ======================================================
Preferences preferences;

struct PairingState {
  bool isPaired;
  uint8_t masterMac[6];
  char masterId[32];
  unsigned long pairedAt;
  char storedModuleId[32];     // NEW: Store module ID that was paired
  char storedSecret[32];        // NEW: Store secret that was paired
};

PairingState pairingState;

// ======================================================
// TELEMETRY CONFIGURATION
// ======================================================
const unsigned long TELEMETRY_INTERVAL = 2000; // Send telemetry every 2 seconds
unsigned long lastTelemetrySent = 0;

// Relay states for each unit - FIXED: Array size matches slaveCapacity
bool relayStates[4] = {true, true, true, true};

// ======================================================
// FAULT TESTING CONFIGURATION
// ======================================================
const unsigned long FAULT_TEST_INTERVAL = 15000; // Send fault every 15 seconds
unsigned long lastFaultTest = 0;
int faultTestCounter = 0;

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

typedef struct {
  char type[16];        // "TELEMETRY"
  char moduleId[16];
  float voltage;
  float current;
  float power;
  bool relayState;
  char health[8];
  int unitIndex;
} ModuleTelemetry;

// NEW: Fault message structure
typedef struct {
  char type[16];           // "FAULT"
  char moduleId[16];
  int unitNumber;
  char faultType[32];
  char severity[16];
  char description[128];
  float measuredValue;
  float thresholdValue;
  char unit[16];
} ModuleFault;

// ======================================================
// NVS FUNCTIONS
// ======================================================
void loadPairingState() {
  preferences.begin("pairing", false);
  
  pairingState.isPaired = preferences.getBool("isPaired", false);
  preferences.getBytes("masterMac", pairingState.masterMac, 6);
  preferences.getString("masterId", pairingState.masterId, 32);
  pairingState.pairedAt = preferences.getULong("pairedAt", 0);
  preferences.getString("storedModuleId", pairingState.storedModuleId, 32);
  preferences.getString("storedSecret", pairingState.storedSecret, 32);
  
  preferences.end();

  // Check if module ID or secret has changed
  if (pairingState.isPaired) {
    if (strcmp(pairingState.storedModuleId, moduleId) != 0 ||
        strcmp(pairingState.storedSecret, pairingSecret) != 0) {
      Serial.println("⚠️ Hardware configuration changed detected!");
      Serial.println("  Module ID or Secret has changed");
      Serial.println("  Automatically unpairing...");
      clearPairingState();
      return;
    }
  }

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
  preferences.putString("storedModuleId", moduleId);      // NEW: Save current module ID
  preferences.putString("storedSecret", pairingSecret);   // NEW: Save current secret
  
  preferences.end();

  pairingState.isPaired = true;
  memcpy(pairingState.masterMac, masterMac, 6);
  strcpy(pairingState.masterId, masterId);
  pairingState.pairedAt = millis();
  strcpy(pairingState.storedModuleId, moduleId);
  strcpy(pairingState.storedSecret, pairingSecret);

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
  memset(pairingState.storedModuleId, 0, 32);
  memset(pairingState.storedSecret, 0, 32);

  Serial.println("🗑️ Pairing state cleared");
}

// ======================================================
// FAULT TESTING FUNCTION
// ======================================================
void sendTestFault() {
  if (!pairingState.isPaired) {
    return; // Don't send faults if not paired
  }
  
  faultTestCounter++;
  
  // Cycle through different fault scenarios for different units
  int scenario = faultTestCounter % 5;
  int unitNumber = faultTestCounter % slaveCapacity;
  
  ModuleFault fault;
  memset(&fault, 0, sizeof(fault));
  
  strcpy(fault.type, "FAULT");
  strcpy(fault.moduleId, moduleId);
  fault.unitNumber = unitNumber;
  
  switch (scenario) {
    case 0:
      // Overvoltage on unit
      strcpy(fault.faultType, "overvoltage");
      strcpy(fault.severity, "critical");
      sprintf(fault.description, "Unit %d voltage exceeded safe limits", unitNumber);
      fault.measuredValue = 242.5;
      fault.thresholdValue = 240.0;
      strcpy(fault.unit, "V");
      break;
      
    case 1:
      // Undervoltage on unit
      strcpy(fault.faultType, "undervoltage");
      strcpy(fault.severity, "warning");
      sprintf(fault.description, "Unit %d voltage below minimum threshold", unitNumber);
      fault.measuredValue = 195.0;
      fault.thresholdValue = 200.0;
      strcpy(fault.unit, "V");
      break;
      
    case 2:
      // Overcurrent on unit
      strcpy(fault.faultType, "overcurrent");
      strcpy(fault.severity, "critical");
      sprintf(fault.description, "Unit %d current draw exceeds capacity", unitNumber);
      fault.measuredValue = 10.8;
      fault.thresholdValue = 10.0;
      strcpy(fault.unit, "A");
      break;
      
    case 3:
      // Low power factor
      strcpy(fault.faultType, "low_power_factor");
      strcpy(fault.severity, "info");
      sprintf(fault.description, "Unit %d power factor below optimal", unitNumber);
      fault.measuredValue = 0.72;
      fault.thresholdValue = 0.85;
      strcpy(fault.unit, "");
      break;
      
    case 4:
      // Hardware issue
      strcpy(fault.faultType, "hardware_failure");
      strcpy(fault.severity, "warning");
      sprintf(fault.description, "Unit %d sensor reading anomaly detected", unitNumber);
      fault.measuredValue = 0;
      fault.thresholdValue = 0;
      strcpy(fault.unit, "");
      break;
  }
  
  // Send fault to master meter
  esp_err_t result = esp_now_send(
    pairingState.masterMac,
    (uint8_t*)&fault,
    sizeof(fault)
  );
  
  if (result == ESP_OK) {
    Serial.println("🚨 TEST FAULT sent:");
    Serial.print("  Unit: ");
    Serial.println(fault.unitNumber);
    Serial.print("  Type: ");
    Serial.println(fault.faultType);
    Serial.print("  Severity: ");
    Serial.println(fault.severity);
    Serial.print("  Description: ");
    Serial.println(fault.description);
  } else {
    Serial.println("❌ Failed to send test fault");
  }
}

// ======================================================
// TELEMETRY GENERATION & TRANSMISSION
// ======================================================
void sendTelemetryToMaster() {
  if (!pairingState.isPaired) {
    return; // Don't send telemetry if not paired
  }

  // Send telemetry for each unit in the cluster
  for (int i = 0; i < slaveCapacity; i++) {
    ModuleTelemetry telemetry;
    memset(&telemetry, 0, sizeof(telemetry));
    
    // Set message type
    strcpy(telemetry.type, "TELEMETRY");
    
    // Set module ID
    strcpy(telemetry.moduleId, moduleId);
    
    // Set unit index
    telemetry.unitIndex = i;
    
    // Generate realistic dummy data with slight variations
    telemetry.voltage = 230.0 + random(-20, 20) / 10.0;  // 228-232V
    telemetry.current = 4.0 + random(-10, 10) / 10.0;    // 3-5A
    telemetry.power = telemetry.voltage * telemetry.current * 0.95; // ~920W with PF
    
    // Set relay state
    telemetry.relayState = relayStates[i];
    
    // Set health status (random between OK and WARNING)
    if (random(0, 100) > 10) {
      strcpy(telemetry.health, "OK");
    } else {
      strcpy(telemetry.health, "WARN");
    }
    
    // Send to master meter
    esp_err_t result = esp_now_send(
      pairingState.masterMac,
      (uint8_t*)&telemetry,
      sizeof(telemetry)
    );
    
    if (result == ESP_OK) {
      Serial.print("📊 Telemetry sent for Unit ");
      Serial.print(i);
      Serial.print(" | V: ");
      Serial.print(telemetry.voltage);
      Serial.print("V, I: ");
      Serial.print(telemetry.current);
      Serial.print("A, P: ");
      Serial.print(telemetry.power);
      Serial.print("W, Relay: ");
      Serial.print(telemetry.relayState ? "ON" : "OFF");
      Serial.print(", Health: ");
      Serial.println(telemetry.health);
    } else {
      Serial.print("❌ Failed to send telemetry for Unit ");
      Serial.println(i);
    }
    
    // Small delay between units
    delay(50);
  }
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
        Serial.println("📊 Telemetry transmission will begin");
        Serial.println("🧪 Fault testing enabled");
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
      Serial.println("📊 Telemetry transmission stopped");
      Serial.println("🧪 Fault testing disabled");
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
  Serial.println("🧪 FAULT TESTING MODE ENABLED");

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
  Serial.println(pairingState.isPaired ? "  Mode: PAIRED (Sending Telemetry & Test Faults)" : "  Mode: PAIRING");
  
  lastFaultTest = millis();
}

// ======================================================
// LOOP
// ======================================================
void loop() {
  unsigned long now = millis();
  
  // Send telemetry at regular intervals if paired
  if (pairingState.isPaired && (now - lastTelemetrySent >= TELEMETRY_INTERVAL)) {
    sendTelemetryToMaster();
    lastTelemetrySent = now;
  }
  
  // 🧪 FAULT TESTING - Send test fault every 15 seconds if paired
  if (pairingState.isPaired && (now - lastFaultTest >= FAULT_TEST_INTERVAL)) {
    sendTestFault();
    lastFaultTest = now;
  }
  
  // Heartbeat
  static unsigned long lastBlink = 0;
  if (millis() - lastBlink > 5000) {
    lastBlink = millis();
    Serial.print("💓 Heartbeat | Paired: ");
    Serial.print(pairingState.isPaired ? "YES" : "NO");
    if (pairingState.isPaired) {
      Serial.print(" | Telemetry: ");
      Serial.print(TELEMETRY_INTERVAL / 1000);
      Serial.print("s | Test Faults: ");
      Serial.print(FAULT_TEST_INTERVAL / 1000);
      Serial.print("s");
    }
    Serial.println();
  }
  
  delay(100);
}
