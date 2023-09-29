//
// Created by rosroble on 10.10.22.
//

#ifndef LAB1_RDB_DATABASE_H
#define LAB1_RDB_DATABASE_H

#define DEFAULT_PAGE_SIZE 4096

typedef struct db_header db_header;
typedef struct database database;
typedef struct pageHeader pageHeader;
typedef struct page page;

#include <stdio.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdlib.h>
#include "table.h"



struct pageHeader {
    bool free;
    uint32_t page_ordinal;
    uint16_t rowsAmount;
    uint32_t freeRowOffset;
    uint32_t next_page_ordinal;
};

struct page {
    pageHeader* header;
    tableHeader* tableHeader;
    page* nextPage;
    void* bytes;
};

struct db_header {
    uint64_t page_size;
    uint32_t pages_amount;
    uint16_t first_free_page_ordinal;
};

struct database {
    FILE* file;
    db_header* header;
};

bool write_db_header(FILE* file, db_header* header);
page* allocate_page(database* db);
page* read_page(database* db, uint32_t pageOrdinal);
page* getFirstAvailableForWritePage(database* db, table* table);
bool write_page_to_file(page* pg, FILE* file);
database* init_db(FILE* file, bool overwrite);
void close_db(database* db);
void close_page(page* pg);
bool pageHasSpaceForWrite(page* pg);
void table_page_link(page* pg, table* tb);


#endif //LAB1_RDB_DATABASE_H
