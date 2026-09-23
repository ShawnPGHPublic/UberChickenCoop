#include <time.h>
#include <stdbool.h>

/**
 * Return true if the hour-of-day of `epoch` is inside [start_hour, stop_hour).
 *
 * Hours are 0–23.
 * If start_hour == stop_hour the window is treated as empty (always false).
 * If start_hour > stop_hour the window wraps midnight
 *   (e.g. 22–6 means 22,23,0,1,2,3,4,5).
 *
 * Uses UTC. Swap gmtime_r for localtime_r if you want local time
 * (and have set the TZ / called setenv("TZ", ...) + tzset()).
 */
bool epoch_hour_in_range(time_t epoch, int start_hour, int stop_hour)
{
    if (start_hour < 0 || start_hour > 23 ||
        stop_hour  < 0 || stop_hour  > 23) {
        return false;
    }

    struct tm tm_buf;
    if (gmtime_r(&epoch, &tm_buf) == NULL) {
        return false;
    }

    const int hour = tm_buf.tm_hour;

    if (start_hour == stop_hour) {
        return false;                 /* empty window */
    }

    if (start_hour < stop_hour) {
        return hour >= start_hour && hour < stop_hour;
    }

    /* wraps midnight */
    return hour >= start_hour || hour < stop_hour;
}