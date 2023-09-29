//
// Created by rosroble on 25.10.22.
//

#include "util.h"
#define NANOS_IN_SECOND 1000000000

struct timespec getCurrentTime()
{
    struct timespec ts;
    timespec_get(&ts, TIME_UTC);
    return ts;
}

long long timediff_microseconds(struct timespec start, struct timespec end) {
    long long sec1 = start.tv_sec;
    long long sec2 = end.tv_sec;
    long long ns1 = start.tv_nsec;
    long long ns2 = end.tv_nsec;

    if (ns2 > ns1) {
        return (sec2 - sec1) * 1000000 + (ns2 - ns1) / 1000;
    } else {
        return (sec2 - sec1 - 1) * 1000000 + (NANOS_IN_SECOND + ns2 - ns1) / 1000;
    }
}
