#include <WiFi.h>
#include <time.h>
#include <esp_sntp.h>

// Configuration
const char* ntpServer1 = "192.168.244.2";
const char* ntpServer2 = "time.nist.gov";
const long  gmtOffset_sec = 0;          // Adjust for your timezone
const int   daylightOffset_sec = 0;
const unsigned long NTP_RESYNC_INTERVAL = 4UL * 60 * 60 * 1000; // 4 hours

unsigned long lastNtpSync = 0;
bool timeEverSet = false;               // Tracks if we ever got a valid time

/**
 * Returns the current Unix timestamp.
 * - Resyncs with NTP every 12 hours (only when Wi-Fi is connected)
 * - Continues returning the last valid time even if Wi-Fi/NTP is unavailable
 * - Returns 0 only if the clock has never been set
 */
time_t getUnixTimestamp() {
  // Attempt NTP resync every 12 hours, but only if Wi-Fi is connected
  if ((millis() - lastNtpSync >= NTP_RESYNC_INTERVAL || lastNtpSync == 0) 
      && WiFi.status() == WL_CONNECTED) {
    
    configTime(gmtOffset_sec, daylightOffset_sec, ntpServer1, ntpServer2);

    // On first boot, wait a little for a valid time
    if (!timeEverSet) {
      int retry = 0;
      while (time(nullptr) < 100000 && retry < 15) {
        delay(400);
        retry++;
      }
    }

    lastNtpSync = millis();
    Serial.println("NTP resync attempted");
  }

  time_t now = time(nullptr);

  // Mark time as valid once we get a reasonable timestamp
  if (now > 100000) {
    timeEverSet = true;
  }

  // Return the current time if it has ever been set,
  // otherwise return 0
  return timeEverSet ? now : 0;
}

uint64_t getUnixTimeInSeconds()
{
  return time(nullptr);  
}

/**
 * Returns true if the current time is between start and end (inclusive), every day.
 * Works even when the period crosses midnight.
 *
 * Example:
 *   isBetweenTime(8, 0, 17, 30)   → true between 08:00 and 17:30
 *   isBetweenTime(22, 0, 6, 0)    → true between 22:00 and 06:00
 */
bool isBetweenTime(int startHour, int startMinute, int endHour, int endMinute) {
  time_t now = getUnixTimestamp();
  
  // Time has never been set
  if (now == 0) {
    return false;
  }

  struct tm timeinfo;
  localtime_r(&now, &timeinfo);

  int currentMinutes = timeinfo.tm_hour * 60 + timeinfo.tm_min;
  int startMinutes   = startHour * 60 + startMinute;
  int endMinutes     = endHour   * 60 + endMinute;

  if (startMinutes <= endMinutes) {
    // Normal case (does not cross midnight)
    return (currentMinutes >= startMinutes && currentMinutes <= endMinutes);
  } else {
    // Crosses midnight (e.g. 22:00 → 06:00)
    return (currentMinutes >= startMinutes || currentMinutes <= endMinutes);
  }
}