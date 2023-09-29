//
// Created by rosroble on 25.10.22.
//

#ifndef LAB1_RDB_UTIL_H
#define LAB1_RDB_UTIL_H
// #include <sys/time.h>
#include <time.h>

struct timespec getCurrentTime();
long long timediff_microseconds(struct timespec start, struct timespec end);
#endif //LAB1_RDB_UTIL_H
