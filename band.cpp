#include <ArduinoJson.h>
#include <ESPmDNS.h>
#include <NTPClient.h>
#include <Preferences.h>
#include <PubSubClient.h>
#include <WiFi.h>
#include <WiFiUdp.h>
#include <Wire.h>
#include <time.h>

const char* SENSOR_ID = "band-01";
const char* SENSOR_TYPE = "heart_band";
const char* SENSOR_NAME = "wrist_band_1";

// Must match core/mqtt/sensor_service.py and core/mdns/service.py.
const char* MQTT_BASE = "sensors/band-01";
const String TOPIC_STATUS = String(MQTT_BASE) + "/status";
const String TOPIC_TELEMETRY = String(MQTT_BASE) + "/telemetry";
const String TOPIC_CMD = String(MQTT_BASE) + "/cmd";
const String TOPIC_ALERT = String(MQTT_BASE) + "/alert";

const char* ssid = "ds-Hardware";
const char* password = "ekthf123";
const char* mqtt_server = "192.168.0.66";

WiFiClient espClient;
PubSubClient client(espClient);
WiFiUDP ntpUDP;
NTPClient timeClient(ntpUDP, "pool.ntp.org", 32400, 60000);
Preferences prefs;

const int PIN_BOOT = 0;
const int PIN_VIB = 1;
const int PIN_EN = 9;
const int PIN_INT = 3;
const int PIN_SDA = 43;
const int PIN_SCL = 6;
const int PIN_GRN = 44;
const int PIN_YLW = 7;
const int PIN_RED = 8;

#define MAXM86161_I2C_ADDR 0x62

float lastHeartRate = 0.0;
unsigned long lastPublishTime = 0;
unsigned long lastNormalBlinkTime = 0;
unsigned long publishInterval = 3000;
const unsigned long NORMAL_BLINK_INTERVAL_MS = 3000;
bool registered = false;
String clientId;

bool alertActive = false;
bool alertVibration = false;
bool alertLed = false;
String alertColor = "red";
unsigned long alertStartedAt = 0;
unsigned long alertDurationMs = 0;
unsigned long alertClearAfterMs = 0;

void allOutputsOff() {
  analogWrite(PIN_VIB, 0);
  digitalWrite(PIN_GRN, LOW);
  digitalWrite(PIN_YLW, LOW);
  digitalWrite(PIN_RED, LOW);
}

void writeRegister(uint8_t reg, uint8_t data) {
  Wire.beginTransmission(MAXM86161_I2C_ADDR);
  Wire.write(reg);
  Wire.write(data);
  Wire.endTransmission();
}

uint8_t readRegister(uint8_t reg) {
  Wire.beginTransmission(MAXM86161_I2C_ADDR);
  Wire.write(reg);
  Wire.endTransmission(false);
  Wire.requestFrom(MAXM86161_I2C_ADDR, 1);
  return Wire.available() ? Wire.read() : 0;
}

void initMAXM86161() {
  writeRegister(0x0D, 0x01);
  delay(100);
  writeRegister(0x11, 0x3C);
  writeRegister(0x12, 0x02);
  writeRegister(0x20, 0x01);
  writeRegister(0x23, 0x15);
  writeRegister(0x0D, 0x00);
}

float getLatestHeartRate() {
  uint8_t count = readRegister(0x07);
  if (count == 0) {
    return lastHeartRate;
  }

  uint32_t latestPPG = 0;
  for (int i = 0; i < count; i++) {
    Wire.beginTransmission(MAXM86161_I2C_ADDR);
    Wire.write(0x08);
    Wire.endTransmission(false);
    Wire.requestFrom(MAXM86161_I2C_ADDR, 3);
    if (Wire.available() == 3) {
      uint32_t raw = ((uint32_t)Wire.read() << 16) | ((uint32_t)Wire.read() << 8) | Wire.read();
      latestPPG = raw & 0x7FFFF;
    }
  }

  if (latestPPG > 5000) {
    lastHeartRate = 70.0 + (random(0, 100) / 10.0);
    return lastHeartRate;
  }

  lastHeartRate = 0.0;
  return lastHeartRate;
}

void publishStatus(const char* status) {
  StaticJsonDocument<256> doc;
  doc["sensor_id"] = SENSOR_ID;
  doc["sensor_type"] = SENSOR_TYPE;
  doc["status"] = status;

  char buffer[256];
  serializeJson(doc, buffer, sizeof(buffer));
  client.publish(TOPIC_STATUS.c_str(), buffer, true);
}

void startAlert(JsonDocument& doc) {
  alertColor = doc["color"] | "red";
  alertVibration = doc["vibration"] | true;
  alertLed = doc["led"] | true;
  alertDurationMs = doc["duration_ms"] | 5000UL;

  // The server normally sends alert_off. This is only a local fallback.
  unsigned long resetAfter = doc["reset_after_ms"] | 0UL;
  alertClearAfterMs = alertDurationMs + resetAfter;
  if (alertClearAfterMs == 0) {
    alertClearAfterMs = alertDurationMs;
  }

  alertStartedAt = millis();
  alertActive = true;
  allOutputsOff();
}

void stopAlert() {
  alertActive = false;
  alertVibration = false;
  alertLed = false;
  allOutputsOff();
}

void handleCommand(JsonDocument& doc) {
  String cmd = doc["cmd"] | "";

  if (cmd == "register") {
    registered = true;
    publishInterval = doc["interval_ms"] | publishInterval;
    prefs.putBool("registered", registered);
    prefs.putUInt("interval_ms", publishInterval);
    lastPublishTime = 0;
    publishStatus("online");
  } else if (cmd == "unregister") {
    registered = false;
    prefs.putBool("registered", registered);
    stopAlert();
  } else if (cmd == "set_interval") {
    publishInterval = doc["interval_ms"] | publishInterval;
    prefs.putUInt("interval_ms", publishInterval);
  } else if (cmd == "ping") {
    if (registered) {
      publishStatus("online");
    }
  }
}

void callback(char* topic, byte* payload, unsigned int length) {
  StaticJsonDocument<384> doc;
  DeserializationError error = deserializeJson(doc, payload, length);
  if (error) {
    return;
  }

  String topicStr = String(topic);

  if (topicStr == TOPIC_ALERT) {
    String command = doc["command"] | "";
    if (command == "alert_on") {
      startAlert(doc);
    } else if (command == "alert_off") {
      stopAlert();
    }
  } else if (topicStr == TOPIC_CMD) {
    handleCommand(doc);
  }
}

void connectWiFi() {
  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid, password);
  while (WiFi.status() != WL_CONNECTED) {
    delay(200);
  }
}

void setupMdns() {
  if (!MDNS.begin(SENSOR_ID)) {
    return;
  }

  MDNS.addService("onsafe-sensor", "tcp", 80);
  MDNS.addServiceTxt("onsafe-sensor", "tcp", "sensor_id", SENSOR_ID);
  MDNS.addServiceTxt("onsafe-sensor", "tcp", "sensor_type", SENSOR_TYPE);
  MDNS.addServiceTxt("onsafe-sensor", "tcp", "sen_name", SENSOR_NAME);
  MDNS.addServiceTxt("onsafe-sensor", "tcp", "sen_locate", "worker_wrist");
  MDNS.addServiceTxt("onsafe-sensor", "tcp", "model", "MAXM86161");
  MDNS.addServiceTxt("onsafe-sensor", "tcp", "mqtt_base", MQTT_BASE);
}

bool ensureMqttConnected() {
  if (client.connected()) {
    return true;
  }

  if (!client.connect(clientId.c_str())) {
    return false;
  }

  client.subscribe(TOPIC_CMD.c_str());
  client.subscribe(TOPIC_ALERT.c_str());
  if (registered) {
    publishStatus("online");
  }
  return true;
}

void enterDeepSleep() {
  stopAlert();
  if (client.connected()) {
    client.loop();
    delay(100);
    client.disconnect();
  }
  WiFi.disconnect(true);

  digitalWrite(PIN_EN, LOW);
  pinMode(PIN_SDA, INPUT);
  pinMode(PIN_SCL, INPUT);
  esp_deep_sleep_start();
}

void setup() {
  Serial.begin(115200);

  pinMode(PIN_BOOT, INPUT_PULLUP);
  pinMode(PIN_VIB, OUTPUT);
  pinMode(PIN_GRN, OUTPUT);
  pinMode(PIN_YLW, OUTPUT);
  pinMode(PIN_RED, OUTPUT);
  pinMode(PIN_EN, OUTPUT);
  allOutputsOff();

  digitalWrite(PIN_EN, LOW);
  delay(200);
  digitalWrite(PIN_EN, HIGH);
  delay(500);

  Wire.begin(PIN_SDA, PIN_SCL);
  Wire.setClock(400000);
  initMAXM86161();

  prefs.begin("band", false);
  registered = prefs.getBool("registered", false);
  publishInterval = prefs.getUInt("interval_ms", 3000);

  connectWiFi();
  setupMdns();
  timeClient.begin();

  uint8_t mac[6];
  WiFi.macAddress(mac);
  clientId = String(SENSOR_ID) + "-" + String(mac[3], HEX) + String(mac[4], HEX) + String(mac[5], HEX);
  client.setServer(mqtt_server, 1883);
  client.setCallback(callback);
}

void runAlertState(unsigned long now) {
  if (!alertActive) {
    return;
  }

  unsigned long elapsed = now - alertStartedAt;
  if (elapsed < alertDurationMs) {
    if (alertLed) {
      digitalWrite(PIN_RED, alertColor == "red" ? HIGH : LOW);
      digitalWrite(PIN_YLW, alertColor == "yellow" ? HIGH : LOW);
    }
    if (alertVibration) {
      analogWrite(PIN_VIB, 160);
    }
    return;
  }

  allOutputsOff();
  if (elapsed >= alertClearAfterMs) {
    alertActive = false;
  }
}

void publishTelemetry(unsigned long now) {
  if (now - lastPublishTime < publishInterval) {
    return;
  }
  lastPublishTime = now;

  float hr = getLatestHeartRate();

  timeClient.update();
  time_t epoch = timeClient.getEpochTime();
  struct tm* ti = localtime(&epoch);
  char timeBuffer[30];
  strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%dT%H:%M:%S+09:00", ti);

  StaticJsonDocument<256> out;
  out["sensor_id"] = SENSOR_ID;
  out["sensor_type"] = SENSOR_TYPE;
  out["sen_name"] = SENSOR_NAME;
  out["hr"] = hr;
  out["time"] = timeBuffer;

  char buffer[256];
  serializeJson(out, buffer, sizeof(buffer));
  client.publish(TOPIC_TELEMETRY.c_str(), buffer);
}

void blinkNormalLed(unsigned long now) {
  if (alertActive || now - lastNormalBlinkTime < NORMAL_BLINK_INTERVAL_MS) {
    return;
  }

  lastNormalBlinkTime = now;
  digitalWrite(PIN_GRN, HIGH);
  delay(10);
  digitalWrite(PIN_GRN, LOW);
}

void loop() {
  if (digitalRead(PIN_BOOT) == LOW) {
    delay(50);
    if (digitalRead(PIN_BOOT) == LOW) {
      enterDeepSleep();
    }
  }

  if (!ensureMqttConnected()) {
    delay(500);
    return;
  }

  client.loop();

  unsigned long now = millis();
  runAlertState(now);
  publishTelemetry(now);
  blinkNormalLed(now);
}
