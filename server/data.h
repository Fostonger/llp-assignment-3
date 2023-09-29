//
// Created by rosroble on 10.10.22.
//

#ifndef LAB1_RDB_DATA_H
#define LAB1_RDB_DATA_H

typedef struct data data;
typedef struct dataHeader dataHeader;

#include "table.h"

struct __attribute__((packed)) dataHeader {
    bool valid;
    int32_t nextInvalidRowPage;
    int32_t nextInvalidRowOrdinal;
};

struct data {
    dataHeader* dataHeader;
    table* table;
    void** bytes;
    uint16_t ptr;
};

data* init_data(table* tb);
void data_init_integer(data* dt, int32_t val, char* columnName);
void data_init_string(data* dt, char* val, char* columnName);
void data_init_boolean(data* dt, bool val, char* columnName);
void data_init_float(data* dt, double val, char* columnName);
void data_init_some(data* dt, columnType type, void* val, char* columnName);
bool insert_data(data* data, database* db);

// int32_t getIntegerData(data*, uint8_t columnOrdinal)

#endif //LAB1_RDB_DATA_H
