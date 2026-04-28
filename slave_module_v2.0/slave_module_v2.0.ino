// ============================================================
//  SMART ENERGY METER — SLAVE MODULE (ESP32) — v2.1
//  SINGLE UNIT VERSION
//
//  Changes from v2.0:
//  ✅ Single unit only (capacity = 1)
//  ✅ Real relay state sent in telemetry
//  ✅ No dummy data for unused units
//  ✅ Simplified pin configuration
//  ✅ Functional relay control that works like main meter
// ============================================================

#include <WiFi.h>
#include <esp_now.h>
#include <esp_wifi.h>
#include <Preferences.h>
#include "EmonLib.h"

// ============================================================
//  FIRMWARE VERSION
// ============================================================
const char* FIRMWARE_VERSION = "2.1.0";

// ============================================================
//  WiFi CONFIG
// ============================================================
const char* ssid     = "boom";
const char* password = "123123123033";

// ============================================================
//  SLAVE MODULE IDENTITY
// ============================================================
const char* moduleId      = "MOD-7FCEAB";
const int   slaveCapacity = 1;              // SINGLE UNIT
const char* pairingSecret = "CpmZ3p5dus84";

// ============================================================
//  HARDWARE PIN MAPPING - SINGLE UNIT ONLY
// ============================================================
struct UnitPins {
  int voltagePin;
  int currentPin;
  int relayPin;
  int buttonPin;
  int ledPin;
};

const UnitPins UNIT_PINS = { 34, 35, 32, 33, 25 };

// ============================================================
//  EMON CALIBRATION
// ============================================================
const float VOLTAGE_CALIBRATION = 234.26;
const float CURRENT_CALIBRATION = 2.28;
const float PHASE_SHIFT         = 1.7;

// ============================================================
//  EMON OBJECT - SINGLE UNIT
// ============================================================
EnergyMonitor emon;

// ============================================================
//  NVS STORAGE
// ============================================================
Preferences preferences;

// ============================================================
//  PAIRING STATE
// ============================================================
struct PairingState {
  bool     isPaired;
  uint8_t  masterMac[6];
  char     masterId[32];
  unsigned long pairedAt;
  char     storedModuleId[32];
  char     storedSecret[32];
};

PairingState pairingState;

// ============================================================
//  RELAY & BUTTON STATE - SINGLE UNIT
// ============================================================
bool relayState             = false;  // false = load OFF
volatile bool buttonPressed = false;
unsigned long lastInterruptTime = 0;
unsigned long lastRelayCommandTime = 0;  // ✅ Track when relay was commanded
const unsigned long RELAY_COMMAND_LOCKOUT = 1000;  // 1 second lockout after command

// ============================================================
//  SENSOR READINGS - SINGLE UNIT
// ============================================================
float unitVoltage = 0;
float unitCurrent = 0;
float unitPower   = 0;

// ============================================================
//  TIMING
// ============================================================
const unsigned long TELEMETRY_INTERVAL = 2000UL;
unsigned long lastTelemetrySent = 0;

// ============================================================
//  ESP-NOW STRUCTS
// ============================================================
typedef struct { char type[16]; }                          ScanRequest;
typedef struct { char type[16]; char moduleId[16]; int capacity; } ScanResponse;
typedef struct { char type[16]; char moduleId[16]; char secret[32]; } PairRequest;
typedef struct { char type[16]; bool success; char moduleId[16]; }   PairAck;
typedef struct { char type[16]; char moduleId[16]; }                 UnpairCommand;

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
//  BUTTON INTERRUPT - WITH RELAY COMMAND LOCKOUT
// ============================================================
void IRAM_ATTR buttonISR() {
  unsigned long t = millis();
  
  // ✅ IGNORE button presses within 1 second after relay command from main meter
  if ((t - lastRelayCommandTime) < RELAY_COMMAND_LOCKOUT) {
    Serial.println("[Button] 🔒 IGNORED (relay command lockout active)");
    return;
  }

  if (t - lastInterruptTime > 250) {
    buttonPressed = true;
    lastInterruptTime = t;
    Serial.println("[Button] 🔴 Button press detected!");
  }
}

// ============================================================
//  RELAY HELPERS
//  NC relay: HIGH = load OFF, LOW = load ON
// ============================================================
void applyRelay(bool loadOn) {
  digitalWrite(UNIT_PINS.relayPin, loadOn ? LOW : HIGH);
  digitalWrite(UNIT_PINS.ledPin,   loadOn ? HIGH : LOW);
  relayState = loadOn;
}

// ============================================================
//  NVS — FIRMWARE VERSION CHECK
// ============================================================
void checkFirmwareReset() {
  preferences.begin("meta", false);
  String storedVersion = preferences.getString("fw", "");
  preferences.end();

  if (storedVersion != FIRMWARE_VERSION) {
    Serial.println("[Boot] 🆕 New firmware detected — clearing pairing & relay state");
    Serial.print("[Boot]    Old: "); Serial.println(storedVersion);
    Serial.print("[Boot]    New: "); Serial.println(FIRMWARE_VERSION);

    // Wipe all namespaces
    preferences.begin("pairing", false); preferences.clear(); preferences.end();
    preferences.begin("relay",   false); preferences.clear(); preferences.end();

    // Save new version
    preferences.begin("meta", false);
    preferences.putString("fw", FIRMWARE_VERSION);
    preferences.end();

    Serial.println("[Boot] ✅ NVS cleared — module starts fresh");
  } else {
    Serial.println("[Boot] ✅ Firmware version matches — keeping stored state");
  }
}

// ============================================================
//  NVS — PAIRING STATE
// ============================================================
void loadPairingState() {
  preferences.begin("pairing", false);
  pairingState.isPaired = preferences.getBool("isPaired", false);
  preferences.getBytes("masterMac", pairingState.masterMac, 6);
  preferences.getString("masterId",      pairingState.masterId,      32);
  preferences.getString("storedModId",   pairingState.storedModuleId, 32);
  preferences.getString("storedSecret",  pairingState.storedSecret,   32);
  pairingState.pairedAt = preferences.getULong("pairedAt", 0);
  preferences.end();

  if (pairingState.isPaired) {
    if (strcmp(pairingState.storedModuleId, moduleId) != 0 ||
        strcmp(pairingState.storedSecret,   pairingSecret) != 0) {
      Serial.println("[Pairing] ⚠️ Module identity changed — auto-unpairing");
      clearPairingState();
      return;
    }
    Serial.println("[Pairing] ✅ Paired with main meter:");
    Serial.print  ("  Master deviceId: "); Serial.println(pairingState.masterId);
    Serial.print  ("  Master MAC: ");
    for (int i = 0; i < 6; i++) {
      Serial.printf("%02X", pairingState.masterMac[i]);
      if (i < 5) Serial.print(":");
    }
    Serial.println();
  } else {
    Serial.println("[Pairing] 🔓 Not paired — waiting for scan/pair from main meter");
  }
}

void savePairingState(const uint8_t* masterMac, const char* masterId) {
  preferences.begin("pairing", false);
  preferences.putBool("isPaired",     true);
  preferences.putBytes("masterMac",   masterMac, 6);
  preferences.putString("masterId",   masterId);
  preferences.putString("storedModId",  moduleId);
  preferences.putString("storedSecret", pairingSecret);
  preferences.putULong("pairedAt",    millis());
  preferences.end();

  pairingState.isPaired = true;
  memcpy(pairingState.masterMac, masterMac, 6);
  strncpy(pairingState.masterId,      masterId,      31);
  strncpy(pairingState.storedModuleId, moduleId,     31);
  strncpy(pairingState.storedSecret,   pairingSecret, 31);
  pairingState.pairedAt = millis();

  Serial.println("[Pairing] 💾 Pairing state saved to NVS");
}

void clearPairingState() {
  preferences.begin("pairing", false);
  preferences.clear();
  preferences.end();

  pairingState.isPaired = false;
  memset(pairingState.masterMac,       0, 6);
  memset(pairingState.masterId,        0, 32);
  memset(pairingState.storedModuleId,  0, 32);
  memset(pairingState.storedSecret,    0, 32);
  pairingState.pairedAt = 0;

  Serial.println("[Pairing] 🗑️ Pairing state fully cleared from NVS");
}

// ============================================================
//  NVS — RELAY STATE
// ============================================================
void loadRelayState() {
  preferences.begin("relay", false);
  relayState = preferences.getBool("state", false);  // default OFF
  preferences.end();
  Serial.print("[Relay] Loaded relay state: ");
  Serial.println(relayState ? "ON" : "OFF");
}

void saveRelayStateNVS() {
  preferences.begin("relay", false);
  preferences.putBool("state", relayState);
  preferences.end();
}

// ============================================================
//  READ REAL SENSOR DATA VIA EMON
// ============================================================
void readSensors() {
  emon.calcVI(20, 2000);  // 20 half-cycles, 2s timeout
  unitVoltage = emon.Vrms;

  if (relayState) {
    unitCurrent = emon.Irms;
    unitPower   = emon.realPower;
  } else {
    // Load off — suppress noise
    unitCurrent = 0.0;
    unitPower   = 0.0;
  }
}

// ============================================================
//  SEND TELEMETRY TO MAIN METER (SINGLE UNIT)
// ============================================================
void sendTelemetryToMaster() {
  if (!pairingState.isPaired) return;

  readSensors();

  ModuleTelemetry telem;
  memset(&telem, 0, sizeof(telem));
  strcpy(telem.type,     "TELEMETRY");
  strcpy(telem.moduleId, moduleId);
  telem.unitIndex  = 0;  // Single unit = unit 0
  telem.voltage    = unitVoltage;
  telem.current    = unitCurrent;
  telem.power      = unitPower;
  telem.relayState = relayState;  // ✅ SEND ACTUAL RELAY STATE

  // Health check
  if (unitVoltage < 180.0 || unitVoltage > 260.0) {
    strcpy(telem.health, "WARN");
  } else {
    strcpy(telem.health, "OK");
  }

  esp_err_t result = esp_now_send(
    pairingState.masterMac,
    (uint8_t*)&telem,
    sizeof(telem)
  );

  Serial.print("[Telem] Unit 0");
  Serial.print(" | V:"); Serial.print(telem.voltage, 1);
  Serial.print(" I:"); Serial.print(telem.current, 3);
  Serial.print(" P:"); Serial.print(telem.power, 1);
  Serial.print(" Relay:"); Serial.print(telem.relayState ? "ON" : "OFF");
  Serial.print(" Health:"); Serial.print(telem.health);
  Serial.println(result == ESP_OK ? " ✅" : " ❌");
}

// ============================================================
//  ESP-NOW RECEIVE CALLBACK
// ============================================================
void onESPNowRecv(const esp_now_recv_info_t* info, const uint8_t* data, int len) {
  if (len < 16) return;

  char msgType[16];
  memcpy(msgType, data, 16);

  // ── SCAN REQUEST ────────────────────────────────────────────
  if (strcmp(msgType, "SCAN_REQ") == 0) {
    if (pairingState.isPaired) {
      Serial.println("[Scan] Already paired — ignoring scan");
      return;
    }
    Serial.println("[Scan] 📡 Scan request received");

    if (!esp_now_is_peer_exist(info->src_addr)) {
      esp_now_peer_info_t peer;
      memset(&peer, 0, sizeof(peer));
      memcpy(peer.peer_addr, info->src_addr, 6);
      peer.channel = WiFi.channel();
      peer.encrypt = false;
      peer.ifidx   = WIFI_IF_STA;
      esp_now_add_peer(&peer);
    }

    ScanResponse resp;
    memset(&resp, 0, sizeof(resp));
    strcpy(resp.type,     "SCAN_RESP");
    strcpy(resp.moduleId, moduleId);
    resp.capacity = slaveCapacity;

    esp_err_t r = esp_now_send(info->src_addr, (uint8_t*)&resp, sizeof(resp));
    Serial.print("[Scan] Response sent: "); Serial.println(r == ESP_OK ? "✅" : "❌");
  }

  // ── PAIR REQUEST ────────────────────────────────────────────
  else if (strcmp(msgType, "PAIR_REQ") == 0) {
    PairRequest req;
    memcpy(&req, data, sizeof(req));
    Serial.print("[Pair] 🔐 Pair request for module: "); Serial.println(req.moduleId);

    if (!esp_now_is_peer_exist(info->src_addr)) {
      esp_now_peer_info_t peer;
      memset(&peer, 0, sizeof(peer));
      memcpy(peer.peer_addr, info->src_addr, 6);
      peer.channel = WiFi.channel();
      peer.encrypt = false;
      peer.ifidx   = WIFI_IF_STA;
      esp_now_add_peer(&peer);
    }

    PairAck ack;
    memset(&ack, 0, sizeof(ack));
    strcpy(ack.type,     "PAIR_ACK");
    strcpy(ack.moduleId, moduleId);

    bool moduleMatch = (strcmp(req.moduleId, moduleId) == 0);
    bool secretMatch = (strcmp(req.secret,   pairingSecret) == 0);

    if (moduleMatch && secretMatch) {
      if (pairingState.isPaired &&
          memcmp(info->src_addr, pairingState.masterMac, 6) != 0) {
        Serial.println("[Pair] ⚠️ Already paired with different master — rejected");
        ack.success = false;
      } else {
        ack.success = true;
        savePairingState(info->src_addr, "mainMeter");
        Serial.println("[Pair] ✅ Pairing successful");
      }
    } else {
      ack.success = false;
      if (!moduleMatch) Serial.println("[Pair] ❌ Module ID mismatch");
      if (!secretMatch) Serial.println("[Pair] ❌ Secret mismatch");
    }

    esp_now_send(info->src_addr, (uint8_t*)&ack, sizeof(ack));
    Serial.print("[Pair] ACK sent: "); Serial.println(ack.success ? "SUCCESS" : "FAILED");
  }

  // ── UNPAIR COMMAND ──────────────────────────────────────────
  else if (strcmp(msgType, "UNPAIR") == 0) {
    UnpairCommand cmd;
    memcpy(&cmd, data, sizeof(cmd));
    Serial.print("[Unpair] 🔓 Unpair command for: "); Serial.println(cmd.moduleId);

    if (!pairingState.isPaired) {
      Serial.println("[Unpair] Not paired — ignoring");
      return;
    }
    if (memcmp(info->src_addr, pairingState.masterMac, 6) != 0) {
      Serial.println("[Unpair] ⚠️ From unknown device — ignoring");
      return;
    }
    if (strcmp(cmd.moduleId, moduleId) == 0) {
      esp_now_del_peer(pairingState.masterMac);
      clearPairingState();
      Serial.println("[Unpair] ✅ Unpaired — telemetry stopped");
    }
  }

  // ── RELAY CONTROL ───────────────────────────────────────────
  else if (strcmp(msgType, "RELAY_CTRL") == 0) {
    ModuleRelayControl cmd;
    memcpy(&cmd, data, sizeof(cmd));
    Serial.print("[Relay] Control: Unit "); Serial.print(cmd.unitNumber);
    Serial.print(" → "); Serial.println(cmd.state ? "ON" : "OFF");

    ModuleRelayAck ack;
    memset(&ack, 0, sizeof(ack));
    strcpy(ack.type,     "RELAY_ACK");
    strcpy(ack.moduleId, moduleId);
    ack.unitNumber = cmd.unitNumber;

    // Verify pairing
    if (!pairingState.isPaired || memcmp(info->src_addr, pairingState.masterMac, 6) != 0) {
      Serial.println("[Relay] ⚠️ Not paired or unknown source — rejected");
      ack.success    = false;
      ack.relayState = relayState;
      esp_now_send(info->src_addr, (uint8_t*)&ack, sizeof(ack));
      return;
    }

    // Verify command is for this module and unit 0 (single unit)
    if (strcmp(cmd.moduleId, moduleId) == 0 && cmd.unitNumber == 0) {
      applyRelay(cmd.state);
      lastRelayCommandTime = millis();  // ✅ SET LOCKOUT - button will be ignored for 1 second
      saveRelayStateNVS();
      ack.success    = true;
      ack.relayState = cmd.state;
      Serial.print("[Relay] ✅ Unit "); Serial.print(cmd.unitNumber);
      Serial.println(cmd.state ? " → ON" : " → OFF");
      Serial.println("[Relay] 🔒 Button lockout activated (1 second)");
    } else {
      ack.success    = false;
      ack.relayState = relayState;
      Serial.println("[Relay] ❌ Invalid module ID or unit number");
    }

    esp_now_send(pairingState.masterMac, (uint8_t*)&ack, sizeof(ack));
  }
}

// ============================================================
//  SETUP
// ============================================================
void setup() {
  Serial.begin(115200);
  delay(500);
  Serial.println("\n=== Slave Module (Single Unit) Starting ===");
  Serial.print("Module ID  : "); Serial.println(moduleId);
  Serial.print("Capacity   : "); Serial.println(slaveCapacity);
  Serial.print("Firmware   : "); Serial.println(FIRMWARE_VERSION);

  checkFirmwareReset();
  loadPairingState();
  loadRelayState();

  // ── Init pins + EmonLib for single unit ─────────────────────
  pinMode(UNIT_PINS.relayPin,  OUTPUT);
  pinMode(UNIT_PINS.ledPin,    OUTPUT);
  pinMode(UNIT_PINS.buttonPin, INPUT_PULLUP);

  // NC relay: start safe with stored state
  applyRelay(relayState);

  // EmonLib sensor init
  emon.voltage(UNIT_PINS.voltagePin, VOLTAGE_CALIBRATION, PHASE_SHIFT);
  emon.current(UNIT_PINS.currentPin, CURRENT_CALIBRATION);

  analogReadResolution(12);

  // ── Button interrupt ──────────────────────────────────────
  attachInterrupt(digitalPinToInterrupt(UNIT_PINS.buttonPin), buttonISR, FALLING);

  // ── WiFi ───────────────────────────────────────────────────
  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid, password);
  Serial.print("Connecting to WiFi");
  while (WiFi.status() != WL_CONNECTED) {
    delay(300);
    Serial.print(".");
  }
  Serial.println("\nWiFi connected");

  uint8_t ch = WiFi.channel();
  esp_wifi_set_channel(ch, WIFI_SECOND_CHAN_NONE);
  Serial.print("Channel: "); Serial.println(ch);

  // ── ESP-NOW ────────────────────────────────────────────────
  if (esp_now_init() != ESP_OK) {
    Serial.println("ESP-NOW init failed");
    return;
  }
  Serial.println("ESP-NOW initialized");
  esp_now_register_recv_cb(onESPNowRecv);

  // Re-add paired master as peer
  if (pairingState.isPaired) {
    esp_now_peer_info_t peer;
    memset(&peer, 0, sizeof(peer));
    memcpy(peer.peer_addr, pairingState.masterMac, 6);
    peer.channel = ch;
    peer.encrypt = false;
    peer.ifidx   = WIFI_IF_STA;
    if (esp_now_add_peer(&peer) == ESP_OK) {
      Serial.println("[Boot] ✅ Main meter re-added as ESP-NOW peer");
    }
  }

  Serial.println("=== Slave module ready ===");
  Serial.println(pairingState.isPaired
    ? "  Mode: PAIRED — sending telemetry"
    : "  Mode: WAITING FOR PAIR");
}

// ============================================================
//  LOOP
// ============================================================
void loop() {
  unsigned long now = millis();

  // ── Handle manual button press ─────────────────────────────
  if (buttonPressed) {
    buttonPressed = false;
    bool newState = !relayState;
    applyRelay(newState);
    lastRelayCommandTime = millis();  // ✅ SET LOCKOUT - debounce subsequent presses for 1 second
    saveRelayStateNVS();
    Serial.print("[Button] Unit 0");
    Serial.println(newState ? " → ON (manual)" : " → OFF (manual)");
    Serial.println("[Button] 🔒 Button lockout activated (1 second)");
  }

  // ── Send telemetry to main meter ───────────────────────────
  if (pairingState.isPaired && (now - lastTelemetrySent >= TELEMETRY_INTERVAL)) {
    lastTelemetrySent = now;
    sendTelemetryToMaster();
  }

  // ── Heartbeat ──────────────────────────────────────────────
  static unsigned long lastHB = 0;
  if (now - lastHB >= 5000UL) {
    lastHB = now;
    Serial.print("[HB] Paired:"); Serial.print(pairingState.isPaired ? "YES" : "NO");
    if (pairingState.isPaired) {
      Serial.print(" | Unit 0:"); Serial.print(relayState ? "ON" : "OFF");
    }
    Serial.println();
  }
}
