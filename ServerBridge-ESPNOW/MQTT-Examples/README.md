# Examples of MQTT message handling

**OpenHab DSL**

Thing mqtt:topic:MQTTNetworkStuff:6ddf558f2d "ESP-NOW Chicken Door" (mqtt:broker:MQTTNetworkStuff) @ "backyard" {
	Channels:
		Type string : chickedoorEN_availability "chickedoorEN_availability" [stateTopic="mobilecam/E08CFE41C3B4/availability"]
		Type string : chickedoorEN_Error "chickedoorEN_Error" [stateTopic="mobilecam/E08CFE41C3B4/error"]
		Type datetime : chickedoorEN_LastPing "chickedoorEN_LastPing" [stateTopic="mobilecam/E08CFE41C3B4/lastping"]
		Type switch : chickedoorEN_GetWiFidB "chickedoorEN_GetWiFidB" [stateTopic="mobilecam/E08CFE41C3B4/getwifidb", commandTopic="mobilecam/getwifidb", on="O/E08CFE41C3B4", off="F/E08CFE41C3B4"]
		Type number : chickedoorEN_WiFidB "chickedoorEN_WiFidB" [stateTopic="mobilecam/E08CFE41C3B4/wifidb"]
		Type switch : chickedoorEN_OpenClose "chickedoorEN_OpenClose" [stateTopic="ENBridgeClient/E08CFE41C3B4/openclose/state", commandTopic="ENBridgeClient/setopenclose", on="O/E08CFE41C3B4", off="F/E08CFE41C3B4"]
		Type switch : chickedoorEN_SetRelay1 "chickedoorEN_SetRelay1" [stateTopic="ENBridgeClient/E08CFE41C3B4/relay1/state", commandTopic="ENBridgeClient/setrelay1", on="O/E08CFE41C3B4", off="F/E08CFE41C3B4"]
		Type number : chickedoorEN_Voltage "chickedoorEN_Voltage" [stateTopic="ENBridgeClient/E08CFE41C3B4/voltage"]
		Type string : chickedoorEN_OpenState "chickedoorEN_OpenState" [stateTopic="ENBridgeClient/E08CFE41C3B4/open/state"]
		Type string : chickedoorEN_CloseState "chickedoorEN_CloseState" [stateTopic="ENBridgeClient/E08CFE41C3B4/close/state"]
		Type datetime : chickedoorEN_DeviceTime "chickedoorEN_DeviceTime" [stateTopic="ENBridgeClient/E08CFE41C3B4/devicetime"]
}
