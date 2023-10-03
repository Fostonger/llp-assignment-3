#pragma once

#include <inttypes.h>

typedef enum column_type column_type;
typedef struct column_header column_header;
typedef struct table_header table_header;
typedef struct table table;

#include "util.h"

#define MAX_NAME_LENGTH 31
#define MAX_COLUMN_AMOUNT 50

enum column_type {
    INT_32,
    FLOAT,
    STRING,
    BOOL,
    NONE
};

struct column_header {
    column_type type;
    char name[MAX_NAME_LENGTH];
};

struct table_header {
    uint8_t column_amount;
    uint16_t row_size;
    uint64_t first_data_page_num;
    uint64_t first_string_page_num;
    char name[MAX_NAME_LENGTH];
    column_header columns[MAX_COLUMN_AMOUNT];
};

#include "database_manager.h"

struct table {
    page *first_page;
    page *first_string_page;
    page *first_page_to_write;
    page *first_string_page_to_write;
    table_header* header;
    database *db;
};

OPTIONAL(table)

uint8_t type_to_size(column_type type);
maybe_table read_table(const char *tablename, database *db);
maybe_table create_table(const char *tablename, database *db);
size_t offset_to_column(table_header *tb, const char *column_name, column_type type);
void release_table(table *tb);
result add_column(table *tb, const char *column_name, column_type type);
result save_table(database *db, table *tb);
void print_columns(table_header *tbheader);
result join_columns(table *dst, table *tb1, table *tb2, const char *column_name, column_type type);
void copy_columns(table *dst, table *src);
column_type type_from_name(table_header *tbheader, const char *name);
column_header *header_from_name(table_header *tbheader, const char *name);
