//
// Created by rosroble on 10.10.22.
//

#ifndef LAB1_RDB_TABLE_H
#define LAB1_RDB_TABLE_H

typedef enum columnType columnType;
typedef struct columnHeader columnHeader;
typedef struct column column;
typedef struct tableHeader tableHeader;
typedef struct table table;

#include <inttypes.h>
#include "database.h"
#include "data.h"
#include <string.h>
#include <stdlib.h>

#define MAX_NAME_LENGTH 31
#define DEFAULT_STRING_LENGTH 255

enum columnType {
    INTEGER = 1, STRING = 2, BOOLEAN = 3, FLOAT = 4
};

struct columnHeader {
    columnType type;
    uint16_t size;
    char columnName[MAX_NAME_LENGTH];
};

struct column {
    columnHeader* header;
    char* columnName;
};

struct tableHeader {
    uint32_t thisSize;
    uint16_t oneRowSize;
    uint16_t columnAmount;
    uint16_t columnInitAmount;
    uint16_t tableNameLength;
    char tableName[MAX_NAME_LENGTH];
    columnHeader columns[];
};

struct table{
    database* db;
    tableHeader* header;
    page* firstPage;
    page* firstAvailableForWritePage;
};

table* new_table(const char* name, uint8_t columnAmount);
table* open_table(database* db, const char* name);
void close_table(table* tb);
uint8_t typeToColumnSize(columnType type);
void init_fixedsize_column(table* tb, uint8_t ordinal, const char* name, columnType type);
void init_varsize_column(table* tb, uint8_t ordinal, const char* name, columnType type, uint16_t size);
void table_apply(database* db, table* table);
columnHeader* get_col_header_by_name(table* tb, const char* name);





#endif //LAB1_RDB_TABLE_H
