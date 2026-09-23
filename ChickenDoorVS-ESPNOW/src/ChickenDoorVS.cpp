#include <Arduino.h>
#include <esp_now.h>
#include <esp_wifi.h>
#include <esp_camera.h>
#include <esp_sleep.h>
#include <WiFi.h>

#include <queue>

#include "TimeHandler.h"

// L298N motor driver pins
const int IN1 = 25; //26;  // GPIO 26
const int IN2 = 26; //27;  // GPIO 27
const int ENA = 33; //25;  // PWM pin, GPIO 25

// Relay pin
const int RELAY1_PIN = 4;

// Door limit switchs
const int DOOR_OPENED_PIN = 14;
const int DOOR_CLOSED_PIN = 27; //12 OLD;

// toggle switch position
const int MANUAL_DOOR_SWITCH_PIN  = 18;

// battery
const int BATTERY_PIN = 34; //36;
unsigned long lastCheckTime = 0; // Tracks last voltage check time
#define ADC_REFERENCE 3.3 // ESP32 ADC reference voltage (3.3V)
#define ADC_RESOLUTION 4096.0 // 12-bit ADC (0-4095)
#define R1             30000.0 // resistor values in voltage sensor (in ohms) 
#define R2             7500.0  // resistor values in voltage sensor (in ohms) 
#define VOLT_OFFSET     .4   // Correct offset

// Motor speed (0-255 for PWM)
const int motorSpeed = 85; // set for 12V input

const int openTime = 3500;
const int closeTime = 3400;
volatile bool closeDoor = false;
volatile bool openDoor = false;

// Server MAC — boot the server once and paste its printed MAC here
static uint8_t SERVER_MAC[] = {0xE0, 0x8C, 0xFE, 0x41, 0x29, 0xE0};
static uint8_t SERVER_CHANNEL =  7;            // MUST match the server's channel

RTC_DATA_ATTR int lastServerChannel  = -1;
RTC_DATA_ATTR uint8_t lastServerMac[6];
unsigned long lastCmdCheckTime = 0; 
unsigned long lastPingTime = 0;

unsigned long lastTimeUpdateSecs = 0; 
uint64_t lastTimeOffsetSecs = 0;  // Diff between device timer and unix

volatile int8_t g_rssi = 0;

// ---------------- Message types (keep in sync with server) ----------------
enum MsgType : uint8_t {
  MSG_REGISTER = 1
  ,MSG_IMG_START = 2
  ,MSG_IMG_CHUNK = 3
  ,MSG_IMG_END = 4
  ,MSG_DHT = 5
  ,MSG_CMD_POLL = 6
  ,MSG_CMD = 7
  ,MSG_IMG_ACK = 8
  ,MSG_IMG_NACK = 9
  ,MSG_FLASHENABLED = 10
  ,MSG_SETFLASHON = 11
  ,MSG_WIFIDB = 12
  ,MSG_SLEEPDELAY = 13
  ,MSG_REGREQ =14
  ,MSG_ACK = 15
  ,MSG_MOTION = 16
  ,MSG_ERROR      = 17 
  ,MSG_IGNOREMOTION = 18
  ,MSG_FRAMESIZE = 19
  ,MSG_QUALITY = 20
  ,MSG_OPENSTATE = 21
  ,MSG_CLOSESTATE = 22
  ,MSG_RELAY1 = 23
  ,MSG_VOLTAGE = 24
  ,MSG_DEVICETIME = 25
  ,MSG_GETDEVICETIME = 26
  ,MSG_DOORSTATE = 27
};

enum CmdType : uint8_t {
  CMD_NONE = 0,
  CMD_SNAPSHOT  = 1,  // take picture
  CMD_FLASHENABLED   = 2,   // {uint8_t (on/off)}
  CMD_SETFLASHON   = 3,   // {uint8_t (on/off)}
  CMD_SETSLEEPDELAY = 4,  // {uint16_t (on/off)}
  CMD_GETWIFIDB = 5  // get WiFi dB value
  ,CMD_IGNOREMOTION = 6  // Ignore motion detection
  ,CMD_FRAMESIZE = 7  // Ignore motion detection
  ,CMD_QUALITY = 8  // Ignore motion detection
  ,CMD_RELAY1 = 9
  ,CMD_OPENCLOSEDOOR = 10
  ,CMD_SETDEVICETIME = 11
};

#define CMDINFO_VERSION     1

struct __attribute__((packed)) CmdInfo {
    uint8_t  msgID;  // Always MSG_CMD
    CmdType  cmdID;
    uint8_t  version;
    uint8_t  bufferLength;  // Length of buffer following this
};
static_assert(sizeof(CmdInfo) <= 250, "ESP-NOW payload limit is 250 bytes");


static const uint8_t WIFI_BROADCAST_ADDR[6] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};

#define DISCOVERY_MAGIC      0xC0FFFF01UL   // identifies our protocol
#define PROTOCOL_VERSION     1

struct __attribute__((packed)) DiscoveryMsg {
    uint8_t  messageID;
    uint32_t magic;         // DISCOVERY_MAGIC
    uint8_t  version;
    uint8_t  channel;       // server's WiFi channel (valid in RESP, 0 in REQ)
    uint8_t  bridge_mac[6]; // server's STA MAC (RESP only)
};
static_assert(sizeof(DiscoveryMsg) <= 250, "ESP-NOW payload limit is 250 bytes");

// Message Q
#define MAX_MESSAGES  20
struct MsgInfo {
  MsgType msgType;
  uint8_t bufferLen;
  uint8_t* buffer;
};
static_assert(sizeof(MsgInfo) <= 250, "ESP-NOW payload limit is 250 bytes");

// Command Q
#define MAX_COMMANDS  10
struct CmdQInfo {
  CmdType cmdType;
  uint8_t bufferLen;
  uint8_t* buffer;
};

std::deque<MsgInfo> sendMsgQ;
std::deque<CmdQInfo> processCmdQ;


const uint8_t discoverChannels[] = {7,6,11,1,2,3,4,5,8,9,10};  // standard non-overlapping set;
                                        // extend to 1..13 for a full sweep
const uint8_t TRIES_PER_CHANNEL = 3;
const uint32_t WAIT_PER_TRY_MS  = 3000;

// Receiver must decode packets into this:
struct DiscoveredServer {
    uint8_t  mac[6];
    uint8_t  channel;
};

#define MAX_SEND_FAIL_CNT   20
#define MAX_SERVER_TIMEOUT  (120UL * 60UL * 1000UL)    // Reconnect to bridge if thought lost make sure this is bigger than any polling delay
#define UPDATE_DEVICETIME (15UL * 60UL)  //seconds between updating devicetime from server (also used to make server ping me)
#define UPDATE_STATUS_TIME (10UL * 60UL * 1000UL) // in milliseconds
#define MISSING_BRIDGE_RETEST (10UL * 60UL * 1000UL)

#define SLEEP_NO_BRIDGE (5UL * 60UL * 1000UL)  //ms seconds to go to sleep before next bridge try
#define uMS_TO_microS_FACTOR 1000ULL // Conversion factor ms to microseconds

DiscoveredServer foundServer;
volatile bool     haveServer = false;
uint32_t lastServerContactMs = 0;
uint32_t lastServerConnectAttempt = 0;
int  sendFailCnt = 0;
volatile bool sendOk      = false;


unsigned long lastReconnectAttempt = 0;

//unsigned long previousMillis = 0;
//unsigned long CHECK_WIFI_TIME = 10000;
int wifiConnection = -1;

// WiFi and MQTT clients
WiFiClient espClient;
TimerHandle_t wifiReconnectTimer;

bool doorOpen = false;
int doorOpenedLimit = -1;
int doorClosedLimit = -1;

int doorToggleSwitch = -1;

// Latch flags — set once, cleared only when the door actually arrives
bool closeDoorTriggered = false;
bool openDoorTriggered = false;


const char *macToTopicId(const uint8_t *mac) {
  static char s[13];
  for (int i = 0; i < 6; i++) sprintf(s + i * 2, "%02X", mac[i]);
  s[12] = 0;
  return s;
}

void addPeer(const uint8_t* mac, int channel) {
    Serial.printf("Adding peer. %s (%d)\n",macToTopicId(mac),channel);

    esp_now_peer_info_t peer = {};
    memcpy(peer.peer_addr, mac, 6);
    peer.channel = channel;          // 0 = use whatever channel the radio is on now
    //peer.ifidx   = WIFI_IF_STA;
    peer.encrypt = false;      // broadcast + encryption is never allowed
    if (esp_now_add_peer(&peer) != ESP_OK) {
        Serial.println("Failed to add peer");
    }
}

/// @brief Chickendoor only sends states so ignoring new updates that haven't been sent is fine
/// @param msgType 
/// @param msgs 
/// @return 
bool isNewMsg(MsgType msgType,const uint8_t* buf,int bufLen)
{
  bool notFound = true;

//  Serial.printf("isNewMsg: %d\n",msgType);
  for (MsgInfo c : sendMsgQ)  
  {
    if (c.msgType == msgType)
    {
      // Compare entire message
      if (c.bufferLen == bufLen)
      {
        if (0 != bufLen)
        {
          if (0 == memcmp(c.buffer,buf,bufLen))
          {    
            notFound = false;
          }
        }
        else
        {
          notFound = false;
        }
      }
    }
  }
  if (!notFound)
  {
    Serial.printf("Duplicate message: %d\n",msgType);
  }

  return notFound;
}


void queueSendMessage(const uint8_t* buf,int bufLen) {
  if (sendMsgQ.size() < MAX_MESSAGES)
  {
//    Serial.printf("queueSendMessage: buflen=%d\n",buflen);

    // Nothing there
    if (bufLen > 0)
    {
      MsgType msgType = (MsgType)buf[0];
      //Serial.printf("queueSendMessage: %d\n",msgType);

      if (isNewMsg(msgType,buf,bufLen))
      {
        //Serial.println("queueSendMessage: add");
        MsgInfo info;
        info.msgType = msgType;

        uint8_t* clientBuf = (uint8_t*)malloc(bufLen);
        if (nullptr == clientBuf)
        {
          Serial.printf("queueMessage: Malloc failed (%d)",bufLen);         
          return;
        }
        //Serial.println("queueSendMessage: copy");

        memcpy(clientBuf,buf,bufLen);
        info.buffer = clientBuf;
        info.bufferLen = bufLen;

        //Serial.printf("queueSendMessage: Push msg qsize=%d\n",sendMsgQ.size());
        sendMsgQ.push_back(info);
      }
    }
    else
    {
      Serial.println("queueSendMessage: buffer empty");
    }
  }
  else
  {
    Serial.println("queueSendMessage: Q filled");
  }

}

bool GetPayloadBool(const uint8_t* payload,int len)
{
  bool b = false;

  if (len > 0)
  {
    b = (0 != *(payload + sizeof(CmdInfo)));
  }

  return b;
}

uint32_t GetPayloadInt32(const uint8_t* payload,int len)
{
  int32_t val = 0;

  if (len >= sizeof(int32_t))
  {
    memcpy(&val,payload + sizeof(CmdInfo),sizeof(int32_t));
  }

  return val;
}

uint64_t GetPayloadUint64(const uint8_t* payload,int len)
{
  uint64_t val = 0;

  if (len >= (sizeof(CmdInfo) + sizeof(uint64_t)))
  {
    memcpy(&val,payload + sizeof(CmdInfo),sizeof(uint64_t));
  }
  //Serial.printf("GetPayloadUint64: %u\n",val);

  return val;
}

// Queue msgs

void queueSendMsgBool(MsgType msgType,bool on)
{
    uint8_t pkt[5];
    pkt[0] = msgType; pkt[1] = 0; pkt[2] = 0; pkt[3] = 0;
    memcpy(pkt + 4, &on, 1);
    queueSendMessage(pkt,sizeof(pkt));   
}

void queueSendMsgInt32(MsgType msgType,int32_t val)
{
    uint8_t pkt[8];
    pkt[0] = msgType; pkt[1] = 0; pkt[2] = 0; pkt[3] = 0;
    memcpy(pkt + 4, &val, 4);

    queueSendMessage(pkt,sizeof(pkt));   
}

void queueSendMsgUInt64(MsgType msgType,uint64_t val)
{
    uint8_t pkt[12];
    pkt[0] = msgType; pkt[1] = 0; pkt[2] = 0; pkt[3] = 0;
    memcpy(pkt + 4, &val, 8);

    queueSendMessage(pkt,sizeof(pkt));   
}

void queueSendMsgString(MsgType msgType,const char* str)
{
    int len = strlen(str);
    if (len > 255)
    {
      return;
    }

    uint8_t* pkt = (uint8_t*)malloc(len + 3);

    pkt[0] = msgType; pkt[1] = len;
    memcpy(pkt + 2, str, len+1);

    //Serial.printf("queueSendMsgString: %s %d\n",(char*)(pkt + 2),len);
    queueSendMessage(pkt,len + 2);   
}

bool sendRequest(const uint8_t* mac) {
    uint8_t regInfo[4] = {MSG_REGREQ,PROTOCOL_VERSION,0,0};
    esp_err_t err = esp_now_send(mac,
              (uint8_t*)&regInfo, sizeof(regInfo));
    if (err != ESP_OK) {
        Serial.printf("esp_now_send failed: %s (0x%X)\n",
                      esp_err_to_name(err), err);
        return false;
    }    

    return true;
}

void logRadioState(const char* tag) {
    uint8_t prim;
    wifi_second_chan_t sec;
    esp_wifi_get_channel(&prim, &sec);
    Serial.printf("[%s] STA MAC=%s  ch=%u%s  WiFi-mode=%d\n",
                  tag,
                  WiFi.macAddress().c_str(),
                  prim, sec != WIFI_SECOND_CHAN_NONE ? "(+) HT40" : "");
}

void lockChannel(uint8_t ch) {
    esp_wifi_set_promiscuous(true);
    esp_wifi_set_channel(ch, WIFI_SECOND_CHAN_NONE);
    esp_wifi_set_promiscuous(true);
    //esp_wifi_set_promiscuous(false);
}

void testBridge(const uint8_t* mac,int channel)
{
    lockChannel(channel);
    //logRadioState("sweep");

    haveServer = false;
    memcpy(foundServer.mac,mac,sizeof(foundServer.mac));
    foundServer.channel = channel;

    addPeer(mac,channel);

    for (int t = 0; !haveServer && (t < TRIES_PER_CHANNEL); t++) {
        //Serial.printf("Broadcast to Channel %d\n",lastServerChannel);

        if (sendRequest(mac)) {
          uint32_t until = millis() + WAIT_PER_TRY_MS;
          while ((int32_t)(millis() - until) < 0) {
              delay(250);   // recv callbacks fire in WiFi task context
              //if ((int32_t)(millis() - deadline) >= 0) break;
          }
        }
        delay(250);
    }

    if (!haveServer)
    {
        esp_now_del_peer(mac);   // stay quiet again afterwards
    }
}

// Returns true if at least one bridge was found.
bool discoverBridges(uint32_t timeout_ms) {

    // attempt lastServer first
    if (-1 != lastServerChannel)
    {
      Serial.println("Try last server");
      testBridge(lastServerMac,lastServerChannel);

      if (!haveServer)
      {
          Serial.println("Failed to add last server");
          lastServerChannel = -1;
      }
    }

    // try hardcoded
    if (!haveServer)
    {
      Serial.println("Try hard coded server");
      testBridge(SERVER_MAC,SERVER_CHANNEL);

      if (!haveServer)
      {
          Serial.println("Failed to add hard coded server");
      }
    }

    if (!haveServer)
    {
      for (uint8_t i = 0; i < sizeof(discoverChannels); i++) {

        if (haveServer)
          break;

        int ch = discoverChannels[i];

        testBridge(WIFI_BROADCAST_ADDR,ch);         
      }
    }

    return haveServer;
}

void promiscuousRxCB(void *buf, wifi_promiscuous_pkt_type_t type) {
  if (type != WIFI_PKT_DATA) return;
  const wifi_promiscuous_pkt_t *pkt = (const wifi_promiscuous_pkt_t *)buf;
  g_rssi = pkt->rx_ctrl.rssi;
//  memcpy((void *)g_rssiMac, pkt->payload + 10, 6);  // addr2 = transmitter
}

void setDoorPosition(bool open)
{
  Serial.print("setDoorPosition: ");
  Serial.println(open ? "OPEN" : "CLOSE");

    if (open) {
      openDoor = true;      
    } else {
      closeDoor = true;
    }
}

void setInsideCamRelay(bool enable)
{
  Serial.print("setRelay1: ");
  Serial.println(enable ? "ON" : "OFF");
  
  digitalWrite(RELAY1_PIN,enable ? HIGH : LOW);

  queueSendMsgBool(MSG_RELAY1,enable);   
  //mqttClient.publish(mqtt_relay1_state_topic,0,true, enable ? "ON" : "OFF");
}

// ---------------- Command handling ----------------
static void processCommand(const uint8_t* buf, int len) {
  CmdInfo* cmdInfo = (CmdInfo*)buf;

  Serial.print("Process Cmd: ");
  Serial.println(cmdInfo->cmdID);

  switch (cmdInfo->cmdID)
  {
    case CMD_NONE:
      break;

    case CMD_GETWIFIDB:
      Serial.println("CMD_GETWIFIDB");        
      queueSendMsgInt32(MSG_WIFIDB,g_rssi);   
      break;

    case CMD_RELAY1:
      if (cmdInfo->bufferLength > 0)
      {
        // Current flashon status
        Serial.print("CMD_RELAY1: ");        
        bool b = GetPayloadBool(buf,len);
        Serial.println(b);

        setInsideCamRelay(b);
      }
      break;

    case CMD_OPENCLOSEDOOR:
      if (cmdInfo->bufferLength > 0)
      {
        // Current flashon status
        Serial.print("CMD_OPENCLOSEDOOR: ");        
        bool b = GetPayloadBool(buf,len);
        Serial.println(b);

        setDoorPosition(b);
      }
      break;

    case CMD_SETDEVICETIME:
      if (cmdInfo->bufferLength > 0)
      {
        // Current flashon status
        Serial.print("CMD_SETDEVICETIME: ");        
        uint64_t realTimeSecs = GetPayloadUint64(buf,len);
        Serial.println(realTimeSecs);

        unsigned long devTimeSecs = millis() / 1000UL;
        lastTimeOffsetSecs = realTimeSecs - devTimeSecs;
        Serial.printf("DevTime: %lu Offset %llu\n",devTimeSecs,lastTimeOffsetSecs);
        lastTimeUpdateSecs = devTimeSecs;
      }
      break;

    default: 
      {
      String s = "Unknown CMD ";
      s += cmdInfo->cmdID;
      queueSendMsgString(MSG_ERROR,s.c_str());
      }
      break;

  }
}

bool isNewCmd(CmdType cmdType,const uint8_t* buf,int bufLen)
{
  bool notFound = true;

  //Serial.printf("isNewCmd: %d\n",cmdType);
  for (CmdQInfo c : processCmdQ)  
  {
    if (c.cmdType == cmdType)
    {
      // Compare entire message
      if (c.bufferLen == bufLen)
      {
        if (0 != bufLen)
        {
          if (0 == memcmp(c.buffer,buf,bufLen))
          {    
            notFound = false;
          }
        }
        else
        {
          notFound = false;
        }
      }
    }
  }
  if (!notFound)
  {
    Serial.printf("Duplicate command: %d\n",cmdType);
  }

  return notFound;
}

void queueProcessCmd(const uint8_t* buf,int bufLen) {
  if (processCmdQ.size() < MAX_COMMANDS)
  {
//    Serial.printf("queueProcessCmd: buflen=%d\n",buflen);

    // Nothing there
    if (bufLen >= sizeof(CmdInfo))
    {
      CmdInfo* cmdInfo = (CmdInfo*)buf;
      //Serial.printf("queueProcessCmd: %d\n",cmdInfo->cmdID);

      if (isNewCmd(cmdInfo->cmdID,buf,bufLen))
      {
        //Serial.println("queueProcessCmd: add");
        CmdQInfo info;
        info.cmdType = cmdInfo->cmdID;

        uint8_t* clientBuf = (uint8_t*)malloc(bufLen);
        if (nullptr == clientBuf)
        {
          Serial.printf("queueProcessCmd: Malloc failed (%d)",bufLen);         
          return;
        }
        //Serial.println("queueSendMessage: copy");

        memcpy(clientBuf,buf,bufLen);
        info.buffer = clientBuf;
        info.bufferLen = bufLen;

        //Serial.printf("queueSendMessage: Push msg qsize=%d\n",sendMsgQ.size());
        processCmdQ.push_back(info);
      }
    }
    else
    {
      Serial.println("queueProcessCmd: buffer empty");
    }
  }
  else
  {
    Serial.println("queueProcessCmd: Q filled");
  }
}

void processQCommands(int max)
{
  int procCnt = 0;

  while ((processCmdQ.size() > 0) && (procCnt < max))
  {      
    CmdQInfo info = processCmdQ.front();

    //Serial.printf("processQCommands: qsize=%d\n",processCmdQ.size());

    processCommand(info.buffer,info.bufferLen);
    // free buff
    if (info.bufferLen > 0)
    {
      free(info.buffer);
      info.buffer = nullptr;
      info.bufferLen = 0;
    }

    // remove command
    processCmdQ.pop_front();
    procCnt++;
  }
}

// ---------------- ESP-NOW callbacks (core-version aware) ----------------
// Shared packet parser (called from either callback signature)
//static void onDataRecv(const uint8_t *mac,const uint8_t *data, int len) {
static void onDataRecv(const uint8_t* mac,const uint8_t *data, int len) {

  Serial.print("Message recieved: "); 
  lastServerContactMs = millis();

  if (len < 1) return;

  Serial.println((MsgType)data[0]);

  switch (data[0]) {
    case MSG_REGISTER:
      Serial.println("MSG_REGISTER");

      if (sizeof(DiscoveryMsg) <= len)
      {        
        DiscoveryMsg msg;
        memcpy(&msg,data,sizeof(msg));

        Serial.printf("Reg Macs: %s / %s\n",macToTopicId(mac),macToTopicId(msg.bridge_mac));

        if (foundServer.channel > 0)
        {
          if ((msg.channel != foundServer.channel) || (0 != memcmp(foundServer.mac,mac,sizeof(foundServer.mac))))
          {
            Serial.printf("Remove old server: %s\n",macToTopicId(foundServer.mac));
            esp_now_del_peer(foundServer.mac); 
            haveServer = false;            
          }
          else
          {
            Serial.printf("Server Already Registered: %s (%d)\n",macToTopicId(foundServer.mac),foundServer.channel);
            lastServerChannel = foundServer.channel;
            memcpy(lastServerMac,mac,sizeof(lastServerMac));
            haveServer = true;
          }
        }

        if (!haveServer)
        {          
          memcpy(foundServer.mac,mac,sizeof(foundServer.mac));
          foundServer.channel = msg.channel;

          // change current channel
          lockChannel(foundServer.channel);

          addPeer(foundServer.mac,foundServer.channel);        

          haveServer = true;

          lastServerChannel = foundServer.channel;
          memcpy(lastServerMac,foundServer.mac,sizeof(foundServer.mac));

          Serial.printf("Server Registered: %s (%d)\n",macToTopicId(foundServer.mac),foundServer.channel);
        }
      }
      break;
    case MSG_CMD:
      Serial.println("MSG_CMD");
      if (len >= sizeof(CmdInfo)) {
        queueProcessCmd(data,len);
      }
      break;
  }
}

void onSendCb(const uint8_t *mac, esp_now_send_status_t status) {

    if (status != ESP_NOW_SEND_SUCCESS)
    {
      //lastSentStatus = status;
      //esp_err_t lastErr = esp_now_err();  // IDF 5.x: gives the esp_err_t of the queued send
      Serial.printf("TX to %02X:%02X:%02X:%02X:%02X:%02X -> %s (err=%d)\n",
                    mac[0], mac[1], mac[2], mac[3], mac[4], mac[5],
                    status == ESP_NOW_SEND_SUCCESS ? "OK" : "FAIL",
                    status);
      sendOk = false;
    }
    else
    {
      sendOk = true;
    }
}

void setupWiFi() {
  WiFi.mode(WIFI_STA);
  delay(20);

  Serial.print("Client MAC: ");
  Serial.println(WiFi.macAddress());

  if (esp_now_init() != ESP_OK) {
      Serial.println("ESP-NOW init failed");
      while (true) delay(1000);
  }
  esp_now_register_recv_cb(onDataRecv);  
  esp_now_register_send_cb(onSendCb);

  esp_wifi_set_promiscuous_rx_cb(promiscuousRxCB);
  esp_wifi_set_promiscuous(true);  

  // clear out errors
  queueSendMsgString(MSG_ERROR,"Started");

  Serial.println("Scanning for LAN bridges...");
  if (discoverBridges(15000))
  {
    Serial.println("Bridge found.");  
    lastServerContactMs = millis();
    lastPingTime = lastServerContactMs;
  }
  else
  {
    Serial.println("No bridge found. Going to sleep.");  
    esp_sleep_enable_timer_wakeup(SLEEP_NO_BRIDGE * uMS_TO_microS_FACTOR);
    esp_deep_sleep_start();
  }
}

void setRelayTopic(const char *topic, const char *payload, int retain, int qos, bool dup)
{
    Serial.printf("%s Payload: %s\r\n",topic, payload);
    if (strcmp(payload,"ON") == 0) {
      setInsideCamRelay(true);
    } else if (strcmp(payload,"OFF") == 0) {
      setInsideCamRelay(false);
    }
}

void printDoorState()
{
  const char* opened = "OPEN";
  const char* closed = "CLOSED";
  const char* unknown = "UNKNOWN";

  const char* curState;

  int reading = digitalRead(DOOR_OPENED_PIN);
  Serial.print("Door open state: ");
  if (LOW == reading)
  {
    curState = opened;
    doorOpen = true;
  }
  else
  {
    curState = unknown;
  }
  Serial.println(curState);

  queueSendMsgString(MSG_OPENSTATE,curState);   

  reading = digitalRead(DOOR_CLOSED_PIN);
  Serial.print("Door close state: ");
  if (LOW == reading)
  {
    curState = closed;
    doorOpen = false;
  }
  else
  {
    curState = unknown;
  }
  Serial.println(curState);
  queueSendMsgString(MSG_CLOSESTATE,curState);   

  // update the door switch just in case unknown
  queueSendMsgBool(MSG_DOORSTATE,doorOpen);   

}

// Function to stop the motor
void stopMotor() {
  Serial.println("Stopping motor...");
  digitalWrite(IN1, LOW);
  digitalWrite(IN2, LOW);
  analogWrite(ENA, 0);
}

// Function to run motor forward
void runMotorOpen() {
  Serial.println("Running motor forward...");

  //printDoorState();

  Serial.print("Opening");

  digitalWrite(IN1, HIGH);
  digitalWrite(IN2, LOW);
  analogWrite(ENA, motorSpeed);

  // loop watching limit
  unsigned long startTime = millis();
  while ((millis() - startTime) < openTime)
  {
    delay(10);
    int reading = digitalRead(DOOR_OPENED_PIN);
    if (LOW == reading)
    {
      stopMotor();
      Serial.println("OPENED");
      break;
    }
    //Serial.print(".");
  }

  stopMotor();

  doorOpen = true;

  // Send message that opened the door
  queueSendMsgBool(MSG_DOORSTATE,true);   

  printDoorState();
}

// Function to run motor reverse 
void runMotorClose() {
  Serial.println("Running motor reverse...");

  //printDoorState();

  Serial.print("Closing");

  digitalWrite(IN1, LOW);
  digitalWrite(IN2, HIGH);
  analogWrite(ENA, motorSpeed);

  // loop watching limit
  unsigned long startTime = millis();
  while ((millis() - startTime) < closeTime)
  {
    delay(10);
    int reading = digitalRead(DOOR_CLOSED_PIN);
    if (LOW == reading)
    {
      stopMotor();
      Serial.println("CLOSED");
      break;
    }
    //Serial.print(".");
  }

  stopMotor();

  doorOpen = false;

  // Send message that closed the door
  queueSendMsgBool(MSG_DOORSTATE,false);   

  printDoorState();
  
}

void setup() {
  //Serial.println("setup");

  // Initialize serial communication
  Serial.begin(115200,SERIAL_8N1);
  Serial.setDebugOutput(false);

  btStop();
  
  if (psramInit())
  {
    Serial.println("PSRAM Init OK");
     if (psramFound()) {
        Serial.println("PSRAM Found");
     }
  }
  else
  {
    Serial.println("PSRAM Init Failed");
  }

  // Set manual door switch
  pinMode(MANUAL_DOOR_SWITCH_PIN, INPUT_PULLUP);
  doorToggleSwitch = digitalRead(MANUAL_DOOR_SWITCH_PIN);

  // Set Door closed limit pin
  pinMode(DOOR_OPENED_PIN, INPUT_PULLUP);
  pinMode(DOOR_CLOSED_PIN, INPUT_PULLUP);

  // set relay  output
  pinMode(RELAY1_PIN, OUTPUT);
  digitalWrite(RELAY1_PIN,LOW);

  // Set motor control pins as outputs
  pinMode(IN1, OUTPUT);
  pinMode(IN2, OUTPUT);
  pinMode(ENA, OUTPUT);

  // Stop motor initially
  digitalWrite(IN1, LOW);
  digitalWrite(IN2, LOW);
  analogWrite(ENA, 0);

  // Battery
  analogReadResolution(12); // Set ADC to 12-bit resolution

  setupWiFi();

  lastCheckTime = 0;
  delay(1000);

  // Get real time
  queueSendMsgBool(MSG_GETDEVICETIME,true);   
}

// Check the current state update sensor states to MQTT
void updateStatus()
{
  unsigned long currentTime = millis();

  //Serial.printf("%ld / %ld\n\r",lastCheckTime,currentTime);

  if ((0 == lastCheckTime) || ((currentTime - lastCheckTime) >= UPDATE_STATUS_TIME))
  {
    Serial.println("Update State");

    // Read ADC value
    int adcValue = analogRead(BATTERY_PIN);
    //Serial.println(adcValue, 2);
    
    // determine voltage at adc input
    float voltage_adc = ((float)adcValue * ADC_REFERENCE) / ADC_RESOLUTION;
    //Serial.println(voltage_adc, 2);

    // calculate voltage at the sensor input
    float voltage_in = voltage_adc * (R1 + R2) / R2 + VOLT_OFFSET;

    // Print battery voltage
    String voltageStr = String(voltage_in,2);
    queueSendMsgString(MSG_VOLTAGE,voltageStr.c_str());   
    Serial.print("Battery Voltage: ");
    Serial.print(voltage_in, 2);
    Serial.println(" V");

    // update wifi status
    Serial.print("update WiFi dB: ");        
    Serial.println(g_rssi);
    queueSendMsgInt32(MSG_WIFIDB,g_rssi);   

    // update time (convert to secs for server)
    if (0 != lastTimeOffsetSecs)
    {
      Serial.print("Current Device Time: ");        
      uint64_t devTimeSecs = millis() / 1000UL;
      Serial.print(devTimeSecs);

      // request updated time
      if ((devTimeSecs - lastTimeUpdateSecs) >= UPDATE_DEVICETIME)
      {
        queueSendMsgBool(MSG_GETDEVICETIME,true);   
      }

      devTimeSecs += lastTimeOffsetSecs;
      Serial.printf(" adjusted: %u \n",devTimeSecs);
      queueSendMsgUInt64(MSG_DEVICETIME,devTimeSecs);   
    }
    else
    {
      // ask for time sync again
        queueSendMsgBool(MSG_GETDEVICETIME,true);   
    }

    lastCheckTime = currentTime;
  }
}

void checkManualDoorSwitch()
{
  int reading = digitalRead(MANUAL_DOOR_SWITCH_PIN);
  if (reading != doorToggleSwitch)
  {
    Serial.println("Door switch toggled");

    // verify
    delay(50);
    reading = digitalRead(MANUAL_DOOR_SWITCH_PIN);
    if (reading != doorToggleSwitch)
    {
      doorToggleSwitch = reading;
      if (doorOpen)
      {
        // Don't try to close it if already closed
        reading = digitalRead(DOOR_CLOSED_PIN);
        if (LOW != reading)
        {
          Serial.println("Manually close door");
          closeDoor = true;
        }
        else 
        {
          Serial.println("Door already closed");
        }
      }
      else
      {
        // Don't try to open it if already opened
        reading = digitalRead(DOOR_OPENED_PIN);
        if (LOW != reading)
        {
          Serial.println("Manually open door");
          openDoor = true;
        }
        else 
        {
          Serial.println("Door already open");
        }
      }
    }
  }
}

void checkDoorStateChange()
{

// !!!!!!!!!!!!!! Test
    // if (0 != lastTimeOffsetSecs)
    // {
    //   uint64_t timeSecs = millis() / 1000UL;
    //   timeSecs += lastTimeOffsetSecs;
    //   if (epoch_hour_in_range(timeSecs,18,19)) {
    //     Serial.println("It is between 2pm and 3pm ET");
    //   }      
    // }
// !!!!!!!!!!!!!! Test


  int reading = digitalRead(DOOR_OPENED_PIN);
  if (reading != doorOpenedLimit)
  {
    doorOpenedLimit = reading;
    printDoorState();
  }

  // Check if door is opened too long (maybe wifi loss)
  if (LOW == reading)
  {
     // Clear the open-latch once the door is back open (condition reset for next cycle)
    openDoorTriggered = false;

    // Check if too late for door to be open (unix time, -4 ET)
    // only if we have been set with realtime
    if (0 != lastTimeOffsetSecs)
    {
      uint64_t timeSecs = millis() / 1000UL;
      timeSecs += lastTimeOffsetSecs;
      if (!closeDoorTriggered && epoch_hour_in_range(timeSecs,1,2)) {
        Serial.println("Time close door");
        closeDoor = true;
        closeDoorTriggered = true;   // fire once; re-arm only when door reaches CLOSED
      }
    }      
  }

  reading = digitalRead(DOOR_CLOSED_PIN);
  if (reading != doorClosedLimit)
  {
    doorClosedLimit = reading;
    printDoorState();
  }

  // Check if door is closed too long (maybe wifi loss)
  if (LOW == reading)
  {
  // Clear the close-latch once the door is back closed
    closeDoorTriggered = false;

    // Check if too late for door to be closed (unix time, -4 ET)
    if (0 != lastTimeOffsetSecs)
    {
      uint64_t timeSecs = millis() / 1000UL;
      timeSecs += lastTimeOffsetSecs;
      if (!openDoorTriggered && epoch_hour_in_range(timeSecs,13,14)) {
        Serial.println("Time open door");
        openDoor = true;
        openDoorTriggered = true;    // fire once; re-arm only when door reaches OPEN
      }      
    }
  }
}


// ---------------- Send with retries ----------------
static bool sendAndWait(const uint8_t *buf, size_t len, uint32_t timeoutMs = 800) {
  sendOk = false;

  for (uint8_t attempt = 0; attempt < 3; attempt++) {
    esp_err_t err = esp_now_send(foundServer.mac, buf, len);
    if (err != ESP_OK) {
        Serial.printf("esp_now_send failed: %s (0x%X)\n",
                      esp_err_to_name(err), err);
        delay(500);
    }    
    else
    {
      uint32_t stop = millis() + timeoutMs;
      while (!sendOk && (millis() < stop)) 
        delay(100);
    }

    // keep track of failures
    if (sendOk)
    {
      sendFailCnt = 0;
      break;
    }
    else
    {
      sendFailCnt++;
    }
  }

  return sendOk;
}

/// @brief Send all the waiting ESP-NOW messages 
/// @param max 
void sendQMessages(int max)
{
  int sendCnt = 0;

  if (haveServer)
  {
    while ((sendMsgQ.size() > 0) && (sendCnt < max))
    {      
      MsgInfo info = sendMsgQ.front();

      //Serial.printf("sendQMessages: to %s qsize=%d\n",macToTopicId(foundServer.mac),sendMsgQ.size());

      if (sendAndWait(info.buffer,info.bufferLen))
      {
        // free buff
        if (info.bufferLen > 0)
        {
          free(info.buffer);
          info.buffer = nullptr;
          info.bufferLen = 0;
        }

        // remove command
        sendMsgQ.pop_front();
        sendCnt++;
      }
    }
  }
}

void checkForServerReconnect()
{
  if ((sendFailCnt > MAX_SEND_FAIL_CNT) || ((millis() - lastServerContactMs) > MAX_SERVER_TIMEOUT))
  {    
    if ((millis() - lastReconnectAttempt) > MISSING_BRIDGE_RETEST)
    {
      Serial.println("Bridge not responding. Trying to reconnect");  
      if (discoverBridges(15000))
      {
        Serial.println("Bridge found.");  
        lastServerContactMs = millis();
        lastPingTime = lastServerContactMs;
        lastReconnectAttempt = 0;
        sendFailCnt = 0;
      }
      else
      {
        lastReconnectAttempt = millis();
      }
    }

    // Serial.println("Bridge not responding. Going to sleep.");  
    // esp_sleep_enable_timer_wakeup(SLEEP_NO_BRIDGE * uMS_TO_microS_FACTOR);
    // esp_deep_sleep_start();
    
  }
}

void loop() {

  checkForServerReconnect();

  //Serial.print(".");
  checkDoorStateChange();

  checkManualDoorSwitch();

  sendQMessages(10);

  if (closeDoor)
  {
    closeDoor = false;
    runMotorClose();
  }
  else if (openDoor)
  {
    openDoor = false;
    runMotorOpen();
  }

  updateStatus();

  processQCommands(4);

  sendQMessages(4);
  
  delay(20);
}

