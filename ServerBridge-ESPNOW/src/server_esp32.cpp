/**
 * ESP32 SERVER (rev 3.1) — ESP-NOW <-> MQTT bridge, PsychicMqttClient
 *
 * New in this rev:
 * - LWT: broker publishes retained "offline" to esp32cam/bridge/availability
 *   if the bridge dies unexpectedly; "online" published on every connect.
 * - Per-client availability on esp32cam/<MACID>/availability (retained):
 *   "online" on first contact, "offline" only after 1 h without a ping.
 *   Expired clients are deactivated and their RAM released.
 * - Any ESP-NOW message (DHT, image, register, cmd poll) refreshes lastSeen.
 */

#include <Arduino.h>
#include <esp_now.h>
#include <esp_wifi.h>
#include <WiFi.h>
#include <PsychicMqttClient.h>

#include <base64.h>
#include <queue>

#include "KeepTime.h"

// -------------- Config --------------
const char *WIFI_SSID = "ADD_HERE";
const char *WIFI_PASS = "ADD_HERE";
// Configure MQTT Broker connection
const char* mqtt_url = "mqtt://ADD_HERE:1883";
const char* mqtt_user = "ADD_HERE";
const char* mqtt_pwd = "ADD_HERE";
char mqtt_client_id[20];


const char *mqtt_bridge_clients = "ESPNbridge/attachedclients";
const char* mqtt_bridge_last_time = "ESPNbridge/currenttime";
const char* mqtt_bridge_wifi = "ESPNbridge/wifisettings";
const char* mqtt_bridge_error = "ESPNbridge/error";

const char *mqtt_cam_available   = "mobilecam/%s/availability";
const char* mqtt_lastping = "mobilecam/%s/lastping";
const char* mqtt_event_error = "mobilecam/%s/error";

const char* mqtt_getimage_topic = "mobilecam/GetImage";
const char* mqtt_image_state_topic = "mobilecam/%s/Image";
const char* mqtt_image_update_time = "mobilecam/%s/ImageTimeStamp";

const char* mqtt_getwifidb_topic = "mobilecam/getwifidb";
const char* mqtt_wifi_db_topic = "mobilecam/%s/wifidb";

const char* mqtt_setflashenabled = "mobilecam/SetFlashEnabled";
const char* mqtt_flashenabled = "mobilecam/%s/FlashEnabled";

const char* mqtt_setflashon_topic = "mobilecam/SetFlashOn";
const char* mqtt_flashstatus_topic = "mobilecam/%s/FlashStatus";

const char* mqtt_ignore_motion = "mobilecam/IgnoreMotion";
const char* mqtt_ignore_motion_status = "mobilecam/%s/IgnoreMotionStatus";

const char* mqtt_setsleepdelay_topic = "mobilecam/setsleepdelay";
const char* mqtt_sleepdelay_topic = "mobilecam/%s/sleepdelay";

const char* mqtt_setframesize = "mobilecam/setframesize";
const char* mqtt_framesize = "mobilecam/%s/framesize";

const char* mqtt_setquality = "mobilecam/setquality";
const char* mqtt_quality = "mobilecam/%s/quality";


const char* mqtt_motion_state_topic = "mobilecam/%s/motion";
const char* mqtt_temp_state = "mobilecam/%s/temperature";
const char* mqtt_humidity_state = "mobilecam/%s/humidity";

// ---------------  chicken door ---------------
const char* mqtt_setmotor_openclose = "ENBridgeClient/setopenclose";
const char* mqtt_setmotor_openclose_state = "ENBridgeClient/%s/openclose/state";
const char* mqtt_setrelay1 = "ENBridgeClient/setrelay1";
const char* mqtt_relay1_state = "ENBridgeClient/%s/relay1/state";

const char* mqtt_voltage_state = "ENBridgeClient/%s/voltage";
const char* mqtt_open_state = "ENBridgeClient/%s/open/state";
const char* mqtt_close_state = "ENBridgeClient/%s/close/state";
const char* mqtt_devicetime_state = "ENBridgeClient/%s/devicetime";



// Client staleness: deactivate after 30 m of silence
#define STALE_TIMEOUT_MS  (1800ULL * 1000ULL)
#define STALE_CHECK_MS    (2ULL * 60ULL * 1000ULL)      // check for stales
#define PING_UPDATE_DELAY (1ULL * 60ULL * 1000ULL)      // space out updating MQTT

int  sendFailCnt = 0;

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
  CMD_NONE = 0
  ,CMD_SNAPSHOT  = 1  // take picture
  ,CMD_FLASHENABLED   = 2   // {uint8_t (on/off)}
  ,CMD_SETFLASHON   = 3   // {uint8_t (on/off)}
  ,CMD_SETSLEEPDELAY = 4  // {uint16_t (on/off)}
  ,CMD_GETWIFIDB = 5  // get WiFi dB value
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
    uint8_t  cmdID;
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
  uint8_t  mac[6]; 
  uint8_t bufferLen;
  uint8_t* buffer;
};
static_assert(sizeof(MsgInfo) <= 250, "ESP-NOW payload limit is 250 bytes");
std::queue<MsgInfo> sendMsgQ;

struct ImgInfo {
  uint8_t  mac[6]; 
};
static_assert(sizeof(ImgInfo) <= 250, "ESP-NOW payload limit is 250 bytes");
std::queue<ImgInfo> sendImgQ;

// check if message made it out
volatile bool sendOk      = false;

uint32_t crc32_calc(const uint8_t *data, size_t len) {
  uint32_t crc = 0xFFFFFFFFul;
  for (size_t i = 0; i < len; i++) {
    crc ^= data[i];
    for (int b = 0; b < 8; b++)
      crc = (crc >> 1) ^ (0xEDB88320UL & (0UL - (crc & 1UL)));
  }
  return crc ^ 0xFFFFFFFFUL;
}

// -------------- Per-client state --------------
#define MAX_CLIENTS   4
#define MAX_CHUNKS    255
#define CHUNK_SIZE     220 //200
#define MAX_COMMANDS  10

struct ClientCmdInfo {
  uint8_t  cmdId;
  uint8_t* buffer;
  uint8_t  bufLen;
};

struct ClientState {
  uint8_t  mac[6];
  bool     active;
  bool     online;                  // mirrored to availability topic
  uint32_t lastSeenMs;              // millis() of last ESP-NOW message
  uint32_t lastPingUpdate;
  uint8_t *imgBuf;
  size_t   imgLen;
  size_t   imgReceived;
  uint8_t  expectedChunks;
  bool     chunkSeen[MAX_CHUNKS];
  bool     imgInQ;
  std::deque<ClientCmdInfo>     cmds;
};

ClientState clients[MAX_CLIENTS];
PsychicMqttClient mqtt;

int curChannel = -1;

static uint32_t lastStaleCheck = 0;

bool topicIdToMac(const char *topic, uint8_t *outMac) {
  const char *p = strchr(topic, '/');
  if (!p) 
  {
    Serial.println("topicIdToMac: No /");
    return false;
  }
  ++p;
  for (int i = 0; i < 6; i++) {
    char hex[3] = {p[i * 2], p[i * 2 + 1], 0};
    char *end;
    long v = strtol(hex, &end, 16);
    if (end != hex + 2)
    {
      Serial.printf("topicIdToMac: missing numbers pos=%d id=%s\n",i,p);
      return false;
    }
    outMac[i] = (uint8_t)v;
  }
  return true;
}

const char *macToTopicId(const uint8_t *mac) {
  static char s[13];
  for (int i = 0; i < 6; i++) sprintf(s + i * 2, "%02X", mac[i]);
  s[12] = 0;
  return s;
}

// forward
void addPeer(const uint8_t *mac);

ClientState *findClient(const uint8_t *mac) {
  //Serial.print("findClient: ");

  for (auto &c : clients)
  {
    if (c.active && (0 == memcmp(c.mac, mac, 6))) 
    {
      //Serial.println("Found");
      return &c;
    }
  }

  //Serial.println("Not Found");
  return nullptr;
}

String getClients()
{
  String clientInfo;

  int cnt = 0;
  for (auto &c : clients)
  {
    if (c.active)
    {
      if (0 != cnt)
      {
        clientInfo += " ";
      }
      clientInfo += macToTopicId(c.mac);
      cnt++;
    }
  }

  clientInfo = String(cnt) + " " + clientInfo;

  // Serial.print("getClients: ");
  // Serial.println(clientInfo);

  return clientInfo;
}


bool isNewCmd(uint8_t cmdId,std::deque<ClientCmdInfo> cmds)
{
  for (ClientCmdInfo c : cmds)  
  {
    if (c.cmdId == cmdId)
    {
      Serial.println("Duplicate command");
      return false;
    }
  }
  return true;
}

// -------------- Command queue --------------
void queueCommand(ClientState *c, uint8_t cmd,const uint8_t* buf,int buflen) {

  //Serial.printf("queueCommand: %d\n",cmd);

  if (c->cmds.size() < MAX_COMMANDS)
  {
     if (isNewCmd(cmd,c->cmds))
     {
      ClientCmdInfo info;
      info.cmdId = cmd;
      if (buflen > 0)
      {
        uint8_t* clientBuf = (uint8_t*)malloc(buflen);
        if (nullptr == clientBuf)
        {
          Serial.println("queueCommand: Malloc failed");         
          return;
        }
        memcpy(clientBuf,buf,buflen);
        info.buffer = clientBuf;
        info.bufLen = buflen;
      }
      else
      {
        info.bufLen = 0;
        info.buffer = nullptr;
      }
      //Serial.printf("queueCommand: Push cmd (%d) to %s bufsize=%d qsize=%d\n",cmd,macToTopicId(c->mac),info.bufLen,c->cmds.size());
      c->cmds.push_back(info);
      //Serial.printf("queueCommand: qsize=%d DONE\n",c->cmds.size());
    }
  }
}

void addCmdMessage(const char *topic,const CmdType cmdId, const uint8_t *payload,int payloadLen) {

  //Serial.printf("addCmdMessage (%d): ",cmdId);

  uint8_t mac[6];
  if (!topicIdToMac(topic, mac)) return;

  //Serial.println(macToTopicId(mac));

  ClientState *c = findClient(mac);
  if (c)
  {
    queueCommand(c,cmdId,payload,payloadLen);
  }
  //Serial.printf("addCmdMessage (%d): DONE\n",cmdId);
}

String addTimeStamp(const char* msg)
{
  // 2026-01-01 13:14:01
  struct tm timeinfo;
  time_t now = getUnixTimestamp();

  // offset to ET zone
  now -= (4UL * 60UL * 60UL);   // offset time to ET (-4hr)

  localtime_r(&now, &timeinfo);

  char timeStr[21];
  snprintf(timeStr,sizeof(timeStr),"%4d-%02d-%02d %02d:%02d:%02d "
    ,timeinfo.tm_year+1900,timeinfo.tm_mon+1,timeinfo.tm_mday,timeinfo.tm_hour,timeinfo.tm_min,timeinfo.tm_sec);

  String s = timeStr;
  s += msg;  

  return s;
}


void publishBridgeError(const char *msg) {
  String s = addTimeStamp(msg);

  Serial.println(s);

  int packetID = mqtt.publish(mqtt_bridge_error,0,true, s.c_str());
}

void queueSendMessage(const uint8_t* mac,const uint8_t* buf,int buflen) {
  //Serial.println("queueSendMessage");

  if (sendMsgQ.size() < MAX_MESSAGES)
  {
      MsgInfo info;
      memcpy(info.mac, mac, 6);

      if (buflen > 0)
      {
        uint8_t* clientBuf = (uint8_t*)malloc(buflen);
        if (nullptr == clientBuf)
        {
          Serial.printf("queueMessage: Malloc failed (%d)",buflen);         
          return;
        }
        memcpy(clientBuf,buf,buflen);
        info.buffer = clientBuf;
        info.bufferLen = buflen;
      }
      else
      {
        info.bufferLen = 0;
      }
      //Serial.printf("queueSendMessage: Push msg to %s size=%d\n",macToTopicId(mac),sendMsgQ.size());
      sendMsgQ.push(info);
      //Serial.printf("queueSendMessage: Done msg Qsize=%d\n",sendMsgQ.size());
  }
}

void addRegRequest(const uint8_t *mac)
{
    Serial.printf("Send registration response to: %s\n",macToTopicId(mac));

    DiscoveryMsg resp;
    resp.messageID = MSG_REGISTER;
    resp.magic   = DISCOVERY_MAGIC;
    resp.version = PROTOCOL_VERSION;
    resp.channel = WiFi.channel();

    // WiFi.macAddress() returns a String; parse it properly:
    sscanf(WiFi.macAddress().c_str(), "%hhx:%hhx:%hhx:%hhx:%hhx:%hhx",
           &resp.bridge_mac[0], &resp.bridge_mac[1], &resp.bridge_mac[2],
           &resp.bridge_mac[3], &resp.bridge_mac[4], &resp.bridge_mac[5]);    

    queueSendMessage(mac,(uint8_t*)&resp,sizeof(DiscoveryMsg));           
}

// -------------- MQTT events --------------
void onMqttConnect(bool sessionPresent) {
  Serial.printf("[MQTT] %s connected\n\r",mqtt_client_id);

  mqtt.publish(mqtt_bridge_last_time,0,true, String(getUnixTimestamp()).c_str());
  mqtt.publish(mqtt_bridge_clients, 0, true, getClients().c_str());

  String wifiSettings;
  wifiSettings = WiFi.macAddress() + " " + WiFi.channel();

  mqtt.publish(mqtt_bridge_wifi, 0, true, wifiSettings.c_str());
  publishBridgeError("Started");

  Serial.println("[MQTT] working");

  // Do a test send
  addRegRequest(WIFI_BROADCAST_ADDR);
}

void onMqttDisconnect(bool sessionPresent) {
  Serial.println("[MQTT] disconnected — auto-reconnecting");
}

 // QoS 1/2 only: broker has ACKed this id
 void onMqttPublished(int msgId) {
  Serial.println("[MQTT] onMqttPublished");
}


void onGetWiFidBTopic(const char *topic, const char *payload, int retain, int qos, bool dup)
{
  Serial.printf("%s Payload: %s\r\n",topic, payload);
  addCmdMessage(payload,CMD_GETWIFIDB,NULL,0);
}

void onSetFlashEnabled(const char *topic, const char *payload, int retain, int qos, bool dup)
{
    uint8_t on = (('O' == payload[0]) ? 1 : 0);  
    Serial.printf("onSetFlashEnabled: %s Payload: %s (%d)\r\n",topic, payload,on);
    
    addCmdMessage(payload,CMD_FLASHENABLED,(uint8_t*)&on,sizeof(on));
}

void onSetFlashOnTopic(const char *topic, const char *payload, int retain, int qos, bool dup)
{
    uint8_t on = (('O' == payload[0]) ? 1 : 0);  
    Serial.printf("onSetFlashOnTopic: %s Payload: %s (%d)\r\n",topic, payload,on);

    addCmdMessage(payload,CMD_SETFLASHON,(uint8_t*)&on,sizeof(on));
}


void onSetSleepDelayTopic(const char *topic, const char *payload, int retain, int qos, bool dup)
{
    Serial.printf("%s Payload: %s\r\n",topic, payload);
    int32_t val = atoi(payload);

    addCmdMessage(payload,CMD_SETSLEEPDELAY,(uint8_t*)&val,sizeof(val));
}

void onSetFrameSizeTopic(const char *topic, const char *payload, int retain, int qos, bool dup)
{
    Serial.printf("%s Payload: %s\r\n",topic, payload);
    int32_t val = atoi(payload);

    addCmdMessage(payload,CMD_FRAMESIZE,(uint8_t*)&val,sizeof(val));
}

void onSetQualityTopic(const char *topic, const char *payload, int retain, int qos, bool dup)
{
    Serial.printf("%s Payload: %s\r\n",topic, payload);
    int32_t val = atoi(payload);

    addCmdMessage(payload,CMD_QUALITY,(uint8_t*)&val,sizeof(val));
}

void onGetImageTopic(const char *topic, const char *payload, int retain, int qos, bool dup)
{
    Serial.printf("%s Payload: %s\r\n",topic, payload);
    addCmdMessage(payload,CMD_SNAPSHOT,NULL,0);
}

void onIgnoreMotionTopic(const char *topic, const char *payload, int retain, int qos, bool dup)
{
    uint8_t on = (('O' == payload[0]) ? 1 : 0);  
    Serial.printf("onIgnoreMotionTopic: %s Payload: %s (%d)\r\n",topic, payload,on);

    addCmdMessage(payload,CMD_IGNOREMOTION,(uint8_t*)&on,sizeof(on));
}

void onMotorTopic(const char *topic, const char *payload, int retain, int qos, bool dup)
{
  uint8_t on = (('O' == payload[0]) ? 1 : 0);  
  Serial.printf("onMotorTopic: %s Payload: %s (%d)\r\n",topic, payload,on);

  addCmdMessage(payload,CMD_OPENCLOSEDOOR,(uint8_t*)&on,sizeof(on));
}

void onRelayTopic(const char *topic, const char *payload, int retain, int qos, bool dup)
{
  uint8_t on = (('O' == payload[0]) ? 1 : 0);  
  Serial.printf("onRelayTopic: %s Payload: %s (%d)\r\n",topic, payload,on);

   addCmdMessage(payload,CMD_RELAY1,(uint8_t*)&on,sizeof(on));
}


// -------------- Helpers --------------

void addPeer(const uint8_t *mac) {
    Serial.printf("Adding peer. (%s) %d\n",macToTopicId(mac),WiFi.channel());

    esp_now_peer_info_t peer = {};
    memcpy(peer.peer_addr, mac, 6);
    peer.channel = WiFi.channel();          // 0 = use whatever channel the radio is on now
    peer.ifidx   = WIFI_IF_STA;
    peer.encrypt = false;      // broadcast + encryption is never allowed
    if (esp_now_add_peer(&peer) != ESP_OK) {
        Serial.println("Failed to add client peer");
    }
}

void removePeer(const uint8_t *mac)
{
    esp_now_del_peer(mac);  
}



// ---------------- Send with retries ----------------
static bool sendAndWait(const uint8_t *mac,const uint8_t *buf, size_t len, uint32_t timeoutMs = 500) {
  sendOk = false;

  for (uint8_t attempt = 0; attempt < 3; attempt++) {
    esp_err_t err = esp_now_send(mac, buf, len);
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

    while ((sendMsgQ.size() > 0) && (sendCnt < max))
    {      
      MsgInfo info = sendMsgQ.front();

      //Serial.printf("sendQMessages: to %s qsize=%d\n",macToTopicId(info.mac),sendMsgQ.size());

      sendAndWait(info.mac,info.buffer,info.bufferLen);

      // free buff
      if (info.bufferLen > 0)
      {
        free(info.buffer);
        info.buffer = nullptr;
        info.bufferLen = 0;
      }

      // remove command
      sendMsgQ.pop();
      sendCnt++;
    }
}



void deliverNextCommand(ClientState *c) {
   
//  Serial.printf("deliverNextCommand to %s\n", macToTopicId(c->mac));

  // !!!!! check if clients are all cleared
  // for(int i=0; i < MAX_CLIENTS; i++)
  // {
  //   Serial.printf("deliverNextCommand: Client %d  %s active:%d cmds:%d\n",i,macToTopicId(clients[i].mac),clients[i].active,clients[i].cmds.size());
  // }

  if (c)
  {
    //Serial.print("deliverNextCommand: cmds = ");
    //Serial.println(c->cmds.size());
    
    if (c->cmds.size() > 0)
    {
      CmdInfo info;

      ClientCmdInfo clientInfo = c->cmds.front();

      info.msgID = MSG_CMD;
      info.cmdID = clientInfo.cmdId;
      info.version = CMDINFO_VERSION;
      info.bufferLength = clientInfo.bufLen;

      //Serial.printf("deliverNextCommand: cmd = %d buflen=%d\n",info.cmdID,info.bufferLength);

      uint8_t sendBufLen = sizeof(info) + clientInfo.bufLen;
      uint8_t* sendBuf = (uint8_t*)malloc(sendBufLen);
      if (sendBuf)
      {
        memcpy(sendBuf,&info,sizeof(info));
        if (clientInfo.bufLen > 0)
        {
          memcpy(sendBuf + sizeof(info),clientInfo.buffer,clientInfo.bufLen);
        }

        //Serial.println("deliverNextCommand: queueSendMessage");
        queueSendMessage(c->mac,sendBuf,sendBufLen);
        free(sendBuf);
        sendBuf = nullptr;
      }
      else
      {
        Serial.println("deliverNextCommand: Malloc failed");
      }

      // free cmd buff
      if (clientInfo.bufLen > 0)
      {
        free(clientInfo.buffer);
        clientInfo.buffer = nullptr;
        clientInfo.bufLen = 0;
      }

      // remove command
      c->cmds.pop_front();
      //Serial.printf("deliverNextCommand: Done qsize=%d\n",c->cmds.size());
    }
  }
}


// -------------- Publish helpers --------------

void TopicPublishOnOff(const uint8_t *mac,const char* topicBase,bool on,bool macFlag = true)
{
  const char* topicID = macToTopicId(mac);

  char topic[64];
  snprintf(topic, sizeof(topic), topicBase, topicID);
  //Serial.printf("getTopicPublishOnOff: %s\n",topic);

  char payload[16];
  if (macFlag)
  {
    snprintf(payload, sizeof(payload),on ? "O/%s" : "F/%s", topicID);
  }
  else
  {
    snprintf(payload, sizeof(payload),on ? "ON" : "OFF");
  }
  //Serial.printf("getTopicPublishOnOff: %s %s\n",topic,payload);

  
  mqtt.publish(topic,0,true, payload);
}

void TopicPublishInt(const uint8_t *mac,const char* topicBase,int val,bool macFlag = true)
{
  const char* topicID = macToTopicId(mac);

  char topic[64];
  snprintf(topic, sizeof(topic), topicBase, topicID);
  //Serial.printf("TopicPublishInt: %s\n",topic);

  char payload[32];
  if (macFlag)
  {
    snprintf(payload, sizeof(payload),"%d/%s", val,topicID);
  }
  else
  {
    snprintf(payload, sizeof(payload),"%d", val);
  }
  
  mqtt.publish(topic,0,true, payload);
}

void TopicPublishFloat(const uint8_t *mac,const char* topicBase,float val,bool macFlag = true)
{
  const char* topicID = macToTopicId(mac);

  char topic[64];
  snprintf(topic, sizeof(topic), topicBase, topicID);
  //Serial.printf("TopicPublishFloat: %s\n",topic);

  char payload[32];
  if (macFlag)
  {
    snprintf(payload, sizeof(payload),"%.2f/%s", val,topicID);
  }
  else
  {
    snprintf(payload, sizeof(payload),"%.2f", val);
  }
  
  mqtt.publish(topic,0,true, payload);
}

void TopicPublishString(const uint8_t *mac,const char* topicBase,const char* str,bool macFlag = true)
{
  const char* topicID = macToTopicId(mac);

  char topic[64];
  snprintf(topic, sizeof(topic), topicBase, topicID);

  String payload = str;
  if (macFlag)
  {
    payload += "/";    
    payload += topicID;
  }

  //Serial.printf("TopicPublishString: %s (%s)\n",topic,payload.c_str());
  
  int packetID = mqtt.publish(topic,0,true, payload.c_str());
  if (-1 != packetID) {
//      Serial.printf("[MQTT] Published %u bytes to %s\n", payload.length(), topic);
  } else {
      Serial.println("[MQTT] Publish failed. check buffer size!");
  }

}

void publishClientPingTime(const uint8_t *mac)
{
    time_t curTime = getUnixTimestamp();
    String curTimeStr = String(curTime);

    TopicPublishString(mac,mqtt_lastping,curTimeStr.c_str(),false);
}

void publishAvailability(ClientState *c, bool online) {

  c->online = online;
  Serial.printf("[AVAIL] %s is %s\n", macToTopicId(c->mac),online ? "ONLINE" : "OFFLINE");

  TopicPublishString(c->mac,mqtt_cam_available,online ? "ONLINE" : "OFFLINE",false);

  if (c->online)
  {
    publishClientPingTime(c->mac);
    c->lastPingUpdate = millis();    
  }      

  mqtt.publish(mqtt_bridge_clients, 0, true, getClients().c_str());
}

void publishWiFidB(const uint8_t *mac, float dB) {
  Serial.print("WiFi dB: ");
  Serial.print(dB);
  Serial.println(" dB");

  TopicPublishFloat(mac,mqtt_wifi_db_topic,dB,false);
}

void publishFlashEnabled(const uint8_t *mac, bool on) {
  Serial.print("publishFlashEnabled: ");
  Serial.println(on);
  
  TopicPublishOnOff(mac,mqtt_flashenabled,on);
}

void publishSetFlashOn(const uint8_t *mac, bool on) {
  Serial.print("Set Flash On: ");
  Serial.println(on);

  TopicPublishOnOff(mac,mqtt_flashstatus_topic,on);
}

void publishMotion(const uint8_t *mac, bool on) {
  Serial.print("Motion Triggered: ");
  Serial.println(on);

  TopicPublishOnOff(mac,mqtt_motion_state_topic,on,false);
}

void publishIgnoreMotion(const uint8_t *mac, bool on) {
  Serial.print("publishIgnoreMotion: ");
  Serial.println(on);
  
  TopicPublishOnOff(mac,mqtt_ignore_motion_status,on);
}


void publishSleepDelay(const uint8_t *mac, int32_t delay) {
  Serial.print("Sleep Delay: ");
  Serial.println(delay);

  TopicPublishInt(mac,mqtt_sleepdelay_topic,delay);
}

void publishFrameSize(const uint8_t *mac, int32_t frameSize) {
  Serial.print("frame size: ");
  Serial.println(frameSize);

  TopicPublishInt(mac,mqtt_framesize,frameSize);
}

void publishQuality(const uint8_t *mac, int32_t val) {
  Serial.print("quality: ");
  Serial.println(val);

  TopicPublishInt(mac,mqtt_quality,val);
}

void publishDht(const uint8_t *mac, float temperatureF, float humidity) {

  Serial.print("Humidity: ");
  Serial.print(humidity);
  Serial.print(" %\t");
  Serial.print("Temperature: ");
  Serial.print(temperatureF);
  Serial.println(" °F");

  TopicPublishFloat(mac,mqtt_temp_state,temperatureF,false);
  TopicPublishFloat(mac,mqtt_humidity_state,humidity,false);

}

void publishClientError(const uint8_t *mac, const char *msg) {
  Serial.print("publishClientError: ");
  Serial.println(msg);

  String s = addTimeStamp(msg);

  TopicPublishString(mac,mqtt_event_error,s.c_str(),false);
}

void publishClientDoorState(const uint8_t *mac, bool open) {
  Serial.print("publishClientDoorState: ");
  Serial.println(open);

  TopicPublishOnOff(mac,mqtt_setmotor_openclose_state,open);
}

void publishClientOpenState(const uint8_t *mac, const char *msg) {
  Serial.print("publishClientOpenState: ");
  Serial.println(msg);

  TopicPublishString(mac,mqtt_open_state,msg,false);
}

void publishClientCloseState(const uint8_t *mac, const char *msg) {
  Serial.print("publishClientCloseState: ");
  Serial.println(msg);

  TopicPublishString(mac,mqtt_close_state,msg,false);
}

void publishRelayState(const uint8_t *mac, bool on) {
  Serial.print("publishRelayState: ");
  Serial.println(on);

  TopicPublishOnOff(mac,mqtt_relay1_state,on,false);
}

void publishClientVoltage(const uint8_t *mac, const char *msg) {
  Serial.print("publishClientVoltage: ");
  Serial.println(msg);

  TopicPublishString(mac,mqtt_voltage_state,msg,false);
}

void publishDeviceTime(const uint8_t *mac, const char *msg) {
  Serial.print("publishDeviceTime: ");
  Serial.println(msg);

  TopicPublishString(mac,mqtt_devicetime_state,msg,false);
}

void publishImage(ClientState *c) {
  Serial.println("publishImage");

  if (c->imgBuf)
  {
    char topic[64];
    snprintf(topic, sizeof(topic), mqtt_image_state_topic, macToTopicId(c->mac));

    int packetID = mqtt.publish(topic,1,true,(const char*)(c->imgBuf), c->imgReceived,true);  

    free(c->imgBuf);
    c->imgBuf = nullptr;
    c->imgLen = 0;
    c->imgInQ = false;

    if (-1 != packetID) {
      delay(10);

      time_t curTime = getUnixTimestamp();
      String curTimeStr = String(curTime);

      TopicPublishString(c->mac,mqtt_image_update_time,curTimeStr.c_str(),false);
//      Serial.printf("[MQTT] Published %u bytes to %s\n", c->imgReceived, topic);
    } else {
        publishBridgeError("Publish Image to MQTT failed.");
        Serial.println("[MQTT] Publish failed. check buffer size!");
    }
  }

}

void sendNack(ClientState *c) {
  uint8_t missing[64], count = 0;
  for (int i = 0; i < c->expectedChunks && count < 64; i++)
    if (!c->chunkSeen[i]) missing[count++] = i;
  uint8_t buf[70];
  buf[0] = MSG_IMG_NACK; buf[1] = 0; buf[2] = count;
  memcpy(buf + 3, missing, count);
  //esp_now_send(c->mac, buf, 3 + count);
  //sendAndWait(c->mac,buf,3 + count);
  queueSendMessage(c->mac,buf,3 + count);           

  Serial.printf("[NACK] %u missing chunks for %s\n", count, macToTopicId(c->mac));
}

void releaseClient(ClientState* c)
{
    // Release resources so a future client can reuse the slot
    if (c->imgBuf) { 
      free(c->imgBuf); 
      c->imgBuf = nullptr; 
      c->imgLen = 0; 
      c->imgReceived = 0;
      c->imgInQ = false;
    }

    // free up the cmd q
    while (c->cmds.size() > 0)
    {
      ClientCmdInfo clientInfo = c->cmds.front();
      if (clientInfo.bufLen > 0)
      {
        free(clientInfo.buffer);
      }
      c->cmds.pop_front();
    }

    c->active = false;
    c->online = false;
    removePeer(c->mac); 
}

// -------------- Stale client expiry (1 h) --------------
void expireStaleClients() {
  uint32_t now = millis();
  for (auto &c : clients) {
    if (!c.active || !c.online) continue;

    if (now - c.lastSeenMs < STALE_TIMEOUT_MS) continue;

    Serial.printf("Remove Stale Client: %s\n",macToTopicId(c.mac));

    publishAvailability(&c, false);

    releaseClient(&c);
  }
}


// Mark activity on ANY message from a client (acts as a ping)
void touchClient(ClientState *c) {
  unsigned long curTime = millis();
  if (!c->online) 
  {
    publishAvailability(c, true);   // came back after outage
  }
  else
  {
    // make sure ping updates are not to frequent!
    if ((curTime - c->lastPingUpdate) > PING_UPDATE_DELAY)
    {
      c->lastPingUpdate = curTime;
      publishClientPingTime(c->mac);
    }
  }
  c->lastSeenMs = curTime;
}

ClientState *findOrCreateClient(const uint8_t *mac) {
  //Serial.println("findOrCreateClient");

  for (auto &c : clients)
    if (c.active && (0 == memcmp(c.mac, mac, 6))) 
    {
      //Serial.println("Client Found");
      return &c;
    }

    for (auto &c : clients)
    if (!c.active) {
      Serial.printf("Add Client: %s\n",macToTopicId(mac));

      // free anything left in client
      releaseClient(&c);

      // clear client (do not use memset because it trashed Q)
      c.lastSeenMs = millis();              // millis() of last ESP-NOW message
      c.lastPingUpdate = 0;
      c.imgBuf = nullptr;
      c.imgLen = 0;
      c.imgReceived = 0;
      c.expectedChunks = 0;
      for(int i=0; i < MAX_CHUNKS; i++)
      {
        c.chunkSeen[i] = false;
      }

      // set new client settings
      memcpy(c.mac, mac, 6);
      c.active = true;      
      c.online = true;                  

      // Add as peer
      addPeer(mac);

      publishAvailability(&c,true);

      return &c;
    }
  return nullptr;
}

// -------------- ESP-NOW receive --------------
  void onDataRecv(const uint8_t *mac, const uint8_t *data, int len) {
  
  //Serial.print("[ESP-NOW] Message received: ");
  //Serial.println((MsgType)data[0]);

  //if (len < 4) return;


  uint8_t type = data[0];
  ClientState *c = nullptr;

  if (0 != memcmp(WIFI_BROADCAST_ADDR, mac, 6))
  {
    c = findOrCreateClient(mac);
    if (!c) { publishBridgeError("client table full"); return; }

    touchClient(c);                                  // any message = ping
  }

  switch (type) {
    case MSG_REGREQ:
      addRegRequest(mac);
      break;

    case MSG_WIFIDB: {
      Serial.print("MSG_WIFIDB: ");
      if (len < 5) return;
      int8_t db;
      memcpy(&db, data + 4, 1);
      Serial.println(db);
      publishWiFidB(mac,db);
      }
      break;

    case MSG_FLASHENABLED: {
      Serial.print("MSG_FLASHENABLED: ");
      if (len < 5) return;

      byte b;
      memcpy(&b, data + 4, 1);
      Serial.println(b);
      publishFlashEnabled(mac,(0 != b) ? true : false);
      }
      break;

    case MSG_SETFLASHON: {
      Serial.print("MSG_SETFLASHON: ");
      if (len < 5) return;

      byte b;
      memcpy(&b, data + 4, 1);
      Serial.println(b);
      publishSetFlashOn(mac,(0 != b) ? true : false);
      }
      break;

    case MSG_MOTION: {
      Serial.print("MSG_MOTION: ");
      if (len < 5) return;

      byte b;
      memcpy(&b, data + 4, 1);
      Serial.println(b);
      publishMotion(mac,(0 != b) ? true : false);
      }
      break;

    case MSG_IGNOREMOTION: {
      Serial.print("MSG_IGNOREMOTION: ");
      if (len < 5) return;

      byte b;
      memcpy(&b, data + 4, 1);
      Serial.println(b);
      publishIgnoreMotion(mac,(0 != b) ? true : false);
      }
      break;

    case MSG_SLEEPDELAY: {
      Serial.print("MSG_SLEEPDELAY: ");
      if (len < 8) return;

      int32_t delay;
      memcpy(&delay, data + 4, 4);
      Serial.println(delay);
      publishSleepDelay(mac,delay);
      }
      break;

    case MSG_FRAMESIZE: {
      Serial.print("MSG_FRAMESIZE: ");
      if (len < 8) return;

      int32_t frameSize;
      memcpy(&frameSize, data + 4, 4);
      Serial.println(frameSize);
      publishFrameSize(mac,frameSize);
      }
      break;

    case MSG_QUALITY: {
      Serial.print("MSG_QUALITY: ");
      if (len < 8) return;

      int32_t quality;
      memcpy(&quality, data + 4, 4);
      Serial.println(quality);
      publishQuality(mac,quality);
      }
      break;

    case MSG_DHT: {
      Serial.println("DHT");
      if (len < 12) return;

      uint32_t crc; float t, h;
      memcpy(&t, data + 4, 4);
      memcpy(&h, data + 8, 4);
      publishDht(mac, t, h);
      }
      break;

    case MSG_IMG_START: {
      Serial.println("MSG_IMG_START");

      if (c->imgBuf) { free(c->imgBuf); c->imgBuf = nullptr; c->imgLen = 0;}

      uint32_t imgLen; uint16_t chunks;
      memcpy(&imgLen, data + 4, 4);
      memcpy(&chunks, data + 8, 2);
      if (chunks == 0 || chunks > MAX_CHUNKS)
      { 
        publishBridgeError("Bad chunk count");
        Serial.printf("MSG_IMG_START: Too many chunks (%d)\n",chunks);
        return;
      }
      Serial.printf("MSG_IMG_START: chunks=%d imgLen=%d\n",chunks,imgLen);

      c->imgLen = imgLen;
      c->expectedChunks = chunks;
      c->imgReceived = 0;
      memset(c->chunkSeen, 0, sizeof(c->chunkSeen));

      if (psramFound()) {
        //Serial.println("PSRAM found");
        c->imgBuf = (uint8_t *)ps_malloc(imgLen);
      } else {
        //Serial.println("PSRAM not found");
        c->imgBuf = (uint8_t *)malloc(imgLen);
      }

      //c->imgBuf = (uint8_t *)malloc(imgLen);
      if (!c->imgBuf) 
      {
        Serial.println("MSG_IMG_START: No Buffer");
        publishBridgeError("img alloc failed");
      }

      }
      break;

    case MSG_IMG_CHUNK: {
        //Serial.println("MSG_IMG_CHUNK");
        if (!c->imgBuf) {
          Serial.println("MSG_IMG_CHUNK: No Buffer");
          return;
        }
        uint8_t seq = data[1];
        uint8_t plen = data[2];

        Serial.printf("MSG_IMG_CHUNK: seq=%d plen=%d\n",seq,plen);

        if (seq >= c->expectedChunks || len < (int)(8 + plen)) 
        {
          Serial.printf("MSG_IMG_CHUNK: Out of order: %d  len=%d\n",seq,len);
          publishBridgeError("MSG_IMG_CHUNK: Out of order");
          return;
        }
        uint32_t crc;
        memcpy(&crc, data + 4, 4);
        if (crc32_calc(data + 8, plen) != crc) {
          Serial.println("MSG_IMG_CHUNK: chunk crc failed");
          publishBridgeError("chunk crc fail");
          return;
        }
        if (!c->chunkSeen[seq]) {
          size_t off = (size_t)seq * CHUNK_SIZE;
          if (off + plen > c->imgLen)
          {
            Serial.printf("MSG_IMG_CHUNK: too big %d\n",c->imgLen);
            publishBridgeError("MSG_IMG_CHUNK: too big");
            return;
          }
          memcpy(c->imgBuf + off, data + 8, plen);
          c->imgReceived += plen;
          c->chunkSeen[seq] = true;
        }
      }
      break;

    case MSG_IMG_END: {
        Serial.println("MSG_IMG_END");
        if (!c->imgBuf) {
          Serial.println("MSG_IMG_END: No Buffer");
          return;
        }

        // check if already have end
        if (c->imgInQ)
        {
          Serial.println("MSG_IMG_END: Already got end");
          uint8_t ack = MSG_IMG_ACK;
          //sendAndWait(c->mac,&ack,1);
          queueSendMessage(c->mac,(uint8_t*)&ack,sizeof(ack));    
          sendNack(c);          
          return;
        }

        uint8_t missing = 0;
        for (int i = 0; i < c->expectedChunks; i++)
          if (!c->chunkSeen[i]) missing++;

        if (missing == 0) {
          c->imgInQ = true;
          ImgInfo info;
          memcpy(&info.mac,c->mac,6);
          sendImgQ.push(info);

          uint8_t ack = MSG_IMG_ACK;
          //sendAndWait(c->mac,&ack,1);
          queueSendMessage(c->mac,(uint8_t*)&ack,sizeof(ack));           

          sendNack(c);

          Serial.printf("MSG_IMG_END:  complete %u bytes from %s\n",c->imgReceived, macToTopicId(c->mac));
        }
        else
        {
          sendNack(c);          
        }
      }
      break;

    case MSG_CMD_POLL:      
      Serial.println("MSG_CMD_POLL");
      if (0 == c->cmds.size())
      {
        // deliver no more commands
        CmdInfo info;

        info.msgID = MSG_CMD;
        info.cmdID = CMD_NONE;
        info.version = CMDINFO_VERSION;
        info.bufferLength = 0;

        queueSendMessage(c->mac,(const uint8_t*)&info,sizeof(info));
      }
      break;

    case MSG_ERROR: {
      Serial.print("MSG_ERROR: ");
      if (len < 3) return;

      String str;
      uint8_t lenUsed = data[1];
      if (lenUsed <= (len - 2))
      {
        for(int i=0; i < lenUsed; i++)
        {
          str += (char)data[2+i];
        }
        Serial.println(str);

        publishClientError(mac,str.c_str());
      }
      else
      {
        Serial.printf("buf to small (%d) %d\n",lenUsed,len);
      }
      }      
      break;
    case MSG_DOORSTATE: {
      Serial.print("MSG_DOORSTATE: ");      
      if (len < 5) return;

      byte b;
      memcpy(&b, data + 4, 1);
      Serial.println(b);
      publishClientDoorState(mac,b);

      }      
      break;
    case MSG_OPENSTATE: {
      Serial.print("MSG_OPENSTATE: ");
      if (len < 3) return;

      String str;
      uint8_t lenUsed = data[1];
      if (lenUsed <= (len - 2))
      {
        for(int i=0; i < lenUsed; i++)
        {
          str += (char)data[2+i];
        }
        Serial.println(str);

        publishClientOpenState(mac,str.c_str());
      }
      else
      {
        Serial.printf("buf to small (%d) %d\n",lenUsed,len);
      }
      }      
      break;
    case MSG_CLOSESTATE: {
      Serial.print("MSG_CLOSESTATE: ");
      if (len < 3) return;

      String str;
      uint8_t lenUsed = data[1];
      if (lenUsed <= (len - 2))
      {
        for(int i=0; i < lenUsed; i++)
        {
          str += (char)data[2+i];
        }
        Serial.println(str);

        publishClientCloseState(mac,str.c_str());
      }
      else
      {
        Serial.printf("buf to small (%d) %d\n",lenUsed,len);
      }
      }      
      break;
    case MSG_RELAY1: {
      Serial.print("MSG_RELAY1: ");
      if (len < 5) return;

      byte b;
      memcpy(&b, data + 4, 1);
      Serial.println(b);
      publishRelayState(mac,(0 != b) ? true : false);
      }
      break;
    case MSG_VOLTAGE: {
      Serial.print("MSG_VOLTAGE: ");
      if (len < 3) return;

      String str;
      uint8_t lenUsed = data[1];
      if (lenUsed <= (len - 2))
      {
        for(int i=0; i < lenUsed; i++)
        {
          str += (char)data[2+i];
        }
        Serial.println(str);

        publishClientVoltage(mac,str.c_str());
      }
      else
      {
        Serial.printf("buf to small (%d) %d\n",lenUsed,len);
      }
      }      
      break;
    case MSG_DEVICETIME: {
      Serial.print("MSG_DEVICETIME: ");
      if (len < 12) return;

      uint64_t deviceTime;
      memcpy(&deviceTime, data + 4, 8);
      Serial.println(deviceTime);
      publishDeviceTime(mac,String(deviceTime).c_str());
      }
      break;
    case MSG_GETDEVICETIME:      {
      Serial.println("MSG_GETDEVICETIME");

      // Don't send crappy times
      uint64_t unixTimeSecs = getUnixTimeInSeconds();
      if (unixTimeSecs > 1789856833)
      {
        // Get the current time and do CMD_SETDEVICETIME
        uint8_t* pkt = (uint8_t*)malloc(sizeof(CmdInfo) + sizeof(uint64_t));
        CmdInfo* info = (CmdInfo*)pkt;

        info->msgID = MSG_CMD;
        info->cmdID = CMD_SETDEVICETIME;
        info->version = CMDINFO_VERSION;
        info->bufferLength = sizeof(uint64_t);

        memcpy(pkt + sizeof(CmdInfo),&unixTimeSecs,sizeof(uint64_t));
        Serial.printf("Seconds: %u",unixTimeSecs);

        queueSendMessage(c->mac,(const uint8_t*)pkt,sizeof(info) + sizeof(uint64_t));

        free(pkt);
      }
      }
      break;
    default:
      Serial.print("[ESP-NOW] Unknown message received: ");
      Serial.println(data[0]);
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


void lockChannel(uint8_t ch) {
    esp_wifi_set_promiscuous(true);
    esp_wifi_set_channel(ch, WIFI_SECOND_CHAN_NONE);
    esp_wifi_set_promiscuous(false);
}

void updateWiFiChannel()
{
  // remove broadcast peer
  esp_now_del_peer(WIFI_BROADCAST_ADDR);
  
  // Mark all clients as disconnected
  for (auto &c : clients)
  {
    releaseClient(&c);
  }

  lockChannel(WiFi.channel());

  // reconnect broadcast
  addPeer(WIFI_BROADCAST_ADDR);
}

// -------------- Setup / Loop --------------
void setup() {
  Serial.begin(115200,SERIAL_8N1);
  Serial.setDebugOutput(false);

  btStop();

  if (psramFound()) {
    Serial.println("PSRAM found");
    Serial.println(ESP.getPsramSize());
  } else {
    Serial.println("PSRAM not found");
  }


  WiFi.mode(WIFI_STA);

  WiFi.begin(WIFI_SSID, WIFI_PASS);
  while (WiFi.status() != WL_CONNECTED) delay(250);

  Serial.print("Server MAC (put into client SERVER_MAC): ");
  Serial.println(WiFi.macAddress());
  Serial.printf("Channel (define WIFI_CHANNEL on client): %d\n", WiFi.channel());
  
  // --- Last Will & Testament ---
  // If the bridge crashes / loses power / drops off Wi-Fi, the broker
  // publishes retained "offline" so subscribers see the whole bridge die.
  mqtt.setWill(mqtt_bridge_clients, 0, true, "offline");
  mqtt.setKeepAlive(30);

  mqtt.onConnect(onMqttConnect);
  mqtt.onDisconnect(onMqttDisconnect);
  mqtt.onPublish(onMqttPublished);

  // Create a unique MQTT client;
  // "DVES_" + max 3 digits + null terminator
  snprintf(mqtt_client_id,12,"ENB_%u", random(10,200));
  //Serial.println(mqtt_client_id);

  mqtt.setClientId(mqtt_client_id);
  mqtt.setCredentials(mqtt_user,mqtt_pwd);
  
  mqtt.setServer(mqtt_url);
  //mqtt.setBufferSize(46000);

  // subscriptions
  mqtt.onTopic(mqtt_setflashenabled, 0, onSetFlashEnabled);
  mqtt.onTopic(mqtt_setflashon_topic, 0, onSetFlashOnTopic);
  mqtt.onTopic(mqtt_getwifidb_topic, 0, onGetWiFidBTopic);
  mqtt.onTopic(mqtt_setsleepdelay_topic, 0, onSetSleepDelayTopic);
  mqtt.onTopic(mqtt_setframesize, 0, onSetFrameSizeTopic);
  mqtt.onTopic(mqtt_setquality, 0, onSetQualityTopic);
  mqtt.onTopic(mqtt_getimage_topic, 0, onGetImageTopic);
  mqtt.onTopic(mqtt_ignore_motion, 0, onIgnoreMotionTopic);

  mqtt.onTopic(mqtt_setmotor_openclose,0,onMotorTopic);
  mqtt.onTopic(mqtt_setrelay1,0,onRelayTopic);

  
  //Serial.printf("Heap (%ld)...\n",ESP.getFreeHeap());
  mqtt.connect();
  //Serial.printf("Done connecting to MQTT (%ld)...\n",ESP.getFreeHeap());

  delay(100);

  if (esp_now_init() != ESP_OK)
  {
    Serial.println("ESP NOW Init Failed");
    esp_restart();
  }

  esp_now_register_recv_cb(onDataRecv);
  esp_now_register_send_cb(onSendCb);

  curChannel = WiFi.channel();
  lockChannel(curChannel);
  addPeer(WIFI_BROADCAST_ADDR);
}

void deliverClientCmds(int maxPerClient)
{
  //Serial.println("deliverClientCmds");
  for (auto &c : clients)
  {
    if (c.active)
    {
      int sendCnt = 0;
      while ((c.cmds.size() > 0) && (sendCnt < maxPerClient))
      {
        // Serial.printf("deliverClientCmds: %s %d\n",macToTopicId(c.mac),c.cmds.size());

        // for (auto &info : c.cmds)
        // {
        //   Serial.printf("deliverClientCmds: cmd %d\n",info.cmdId);
        // }

        deliverNextCommand(&c);
        sendCnt++;

        //Serial.printf("deliverClientCmds: %s %d DONE\n",macToTopicId(c.mac), c.cmds.size());
      }
    }
  }
  //Serial.println("deliverClientCmds: DONE");
}

void checkStaleClient()
{
  if ((millis() - lastStaleCheck) >= STALE_CHECK_MS) {
    lastStaleCheck = millis();
    if (mqtt.connected()) {        // avoid publishing offline during a drop
      expireStaleClients();  

      mqtt.publish(mqtt_bridge_last_time,0,true, String(getUnixTimestamp()).c_str());
      mqtt.publish(mqtt_bridge_clients, 0, true, getClients().c_str());
    }
  }
}

void sendImages()
{
  while (sendImgQ.size() > 0)
  {
    ImgInfo info = sendImgQ.front();
    sendImgQ.pop();

    for (auto &c : clients)
    {
      if (c.active && (0 == memcmp(c.mac, info.mac, 6))) 
      {
        Serial.printf("sendImages: found %s  %d left\n",macToTopicId(c.mac),sendImgQ.size());
        publishImage(&c);
        break;
      }        
    }
    delay(100);
  }
}

void loop() {

  if (WiFi.status() != WL_CONNECTED) WiFi.reconnect();

  if (curChannel != WiFi.channel())
  {
    curChannel = WiFi.channel();
    updateWiFiChannel();
  }

  deliverClientCmds(4); 

  sendQMessages(4);

  checkStaleClient();

  sendImages();

  delay(10);
}
