# Uber Chicken Coop
Projects files to allow communication and control of parts to a chicken coop

**ChickenDoorVS-ESPNOW**
This project controls the opening and closing of the door.  It uses a ESP32 controller to control and communicates to a bridge ESP32 using the ESP-NOW protocol.

**ServerBridge-ESPNOW**
This project serves as a bridge to handle incoming ESP-NOW messages from remote devices.  It translates the messages and rebroadcasts them to an MQTT broker. It also takes MQTT commands and sends them to the correct clients.

