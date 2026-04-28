#include <Wire.h>
#include <LiquidCrystal_I2C.h>
#include <math.h>

// -------- LCD ----------
LiquidCrystal_I2C lcd(0x27, 20, 4);

// -------- Pins ----------
#define CT_PIN 36
#define RELAY_PIN 26
#define LED_RED 18
#define BUZZER_PIN 27

// -------- ADC ----------
const float V_REF = 3.3;
const int ADC_RESOLUTION = 4095;

// -------- Circuit Parameters ----------
const float BURDEN_RESISTOR = 330.0;
const float OPAMP_GAIN = 41.0;
const float CT_RATIO = 1000.0;

// -------- Calibration ----------
float CALIBRATION_FACTOR = 1.000;
const float NOISE_FILTER_MA = 1.0;

// -------- Leakage Trip ----------
const float LEAKAGE_LIMIT = 20.0;   // 40 mA

void setup()
{
  Serial.begin(115200);

  pinMode(RELAY_PIN, OUTPUT);
  pinMode(LED_RED, OUTPUT);
  pinMode(BUZZER_PIN, OUTPUT);

  digitalWrite(RELAY_PIN, LOW);  // Load connected
  digitalWrite(LED_RED, LOW);
  digitalWrite(BUZZER_PIN, LOW);

  analogSetPinAttenuation(CT_PIN, ADC_11db);

  Wire.begin(21,22);
  lcd.init();
  lcd.backlight();

  lcd.setCursor(0,0);
  lcd.print("Leakage Protection");
  delay(2000);
  lcd.clear();
}

void loop()
{
  unsigned long startTime = millis();
  int sampleCount = 0;

  double voltageSum = 0;
  double sqVoltageSum = 0;

  // Sample waveform for 100ms
  while (millis() - startTime < 100)
  {
    int rawADC = analogRead(CT_PIN);

    double pinVoltage = (rawADC / (double)ADC_RESOLUTION) * V_REF;

    voltageSum += pinVoltage;
    sqVoltageSum += (pinVoltage * pinVoltage);
    sampleCount++;
  }

  double dcBiasVoltage = voltageSum / sampleCount;

  double meanSqVoltage = sqVoltageSum / sampleCount;
  double variance = meanSqVoltage - (dcBiasVoltage * dcBiasVoltage);

  if (variance < 0) variance = 0;

  double acVoltageRMS = sqrt(variance);

  double burdenVoltageRMS = acVoltageRMS / OPAMP_GAIN;
  double secondaryCurrent_A = burdenVoltageRMS / BURDEN_RESISTOR;
  double primaryCurrent_A = secondaryCurrent_A * CT_RATIO;
  double primaryCurrent_mA = primaryCurrent_A * 1000.0;

  primaryCurrent_mA *= CALIBRATION_FACTOR;

  if (primaryCurrent_mA < NOISE_FILTER_MA)
      primaryCurrent_mA = 0.0;

  Serial.print("Leakage Current: ");
  Serial.print(primaryCurrent_mA);
  Serial.println(" mA");

  lcd.clear();

  if(primaryCurrent_mA >= LEAKAGE_LIMIT)
  {
    // TRIP
    digitalWrite(RELAY_PIN, HIGH);   // Disconnect load
    digitalWrite(LED_RED, HIGH);
    digitalWrite(BUZZER_PIN, HIGH);

    lcd.setCursor(0,0);
    lcd.print("LEAKAGE DETECTED");

    lcd.setCursor(0,2);
    lcd.print("Leakage Current:");

    lcd.setCursor(0,3);
    lcd.print(primaryCurrent_mA,1);
    lcd.print(" mA");
  }
  else
  {
    // NORMAL
    digitalWrite(RELAY_PIN, LOW);
    digitalWrite(LED_RED, LOW);
    digitalWrite(BUZZER_PIN, LOW);

    lcd.setCursor(0,0);
    lcd.print("System Normal");

    lcd.setCursor(0,2);
    lcd.print("Leakage Current:");

    lcd.setCursor(0,3);
    lcd.print(primaryCurrent_mA,1);
    lcd.print(" mA");
  }

  delay(500);
}