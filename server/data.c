//
// Created by rosroble on 10.10.22.
//

#include "data.h"

data* init_data(table* tb) {
    data* dt = malloc(sizeof(data));
    dataHeader* dHeader = malloc(sizeof (dataHeader));
    dHeader->valid = true;
    dHeader->nextInvalidRowPage = -1; // indicates there is no next invalid row
    dHeader->nextInvalidRowOrdinal = -1;
    dt->table = tb;
    dt->bytes = malloc(tb->header->oneRowSize);
    memcpy(dt->bytes, dHeader, sizeof(dataHeader));
    dt->ptr = sizeof(dataHeader);
    dt->dataHeader = dHeader;
    return dt;
}

void data_init_integer(data* dt, int32_t val, char* columnName) {
    columnHeader* header = get_col_header_by_name(dt->table, columnName);
    char* ptr = (char*) dt->bytes + dt->ptr;
    *((int32_t*) (ptr)) = val;
    dt->ptr += header->size;
}

void data_init_string(data* dt, char* val, char* columnName) {
    columnHeader* header = get_col_header_by_name(dt->table, columnName);
    char* ptr = (char*) dt->bytes + dt->ptr;
    strcpy((char *) (ptr), val);
    dt->ptr += header->size;
}

void data_init_boolean(data* dt, bool val, char* columnName) {
    columnHeader* header = get_col_header_by_name(dt->table, columnName);
    char* ptr = (char*) dt->bytes + dt->ptr;
    *((bool*) (ptr)) = val;
    dt->ptr += header->size;
}

void data_init_float(data* dt, double val, char* columnName) {
    columnHeader* header = get_col_header_by_name(dt->table, columnName);
    char* ptr = (char*) dt->bytes + dt->ptr;
    *((double*) (ptr)) = val;
    dt->ptr += header->size;
}

void data_init_some(data* dt, columnType type, void* val, char* columnName) {
    switch (type) {
        case INTEGER:
            data_init_integer(dt, *((int32_t*) val), columnName);
            break;
        case STRING:
            data_init_string(dt, *((char**) val), columnName);
            break;
        case BOOLEAN:
            data_init_boolean(dt, *((bool*) val), columnName);
            break;
        case FLOAT:
            data_init_float(dt, *((double*) val), columnName);
            break;
    }
}

void close_data(data* dt) {
    free(dt->dataHeader);
    free(dt->bytes);
    free(dt);
}

bool insert_data(data* dt, database* db) {
    bool success = false;
    page* pg = dt->table->firstAvailableForWritePage;
    uint32_t offset = dt->table->header->oneRowSize * pg->header->rowsAmount;
    memcpy(pg->bytes + offset, dt->bytes, dt->ptr);
    pg->header->rowsAmount++;
    pg->header->freeRowOffset += pg->tableHeader->oneRowSize;
    if (!pageHasSpaceForWrite(pg)) {
        pg->header->free = false;
        dt->table->firstAvailableForWritePage = allocate_page(db);
        pg->header->next_page_ordinal = dt->table->firstAvailableForWritePage->header->page_ordinal;
        table_page_link(dt->table->firstAvailableForWritePage, dt->table);
        if (!pg->nextPage) {
            pg->nextPage = dt->table->firstAvailableForWritePage;
        }
        if (pg != dt->table->firstPage) {
            if (db) {
                success = write_page_to_file(pg, db->file);
                write_page_to_file(dt->table->firstAvailableForWritePage, db->file);
                close_page(pg);
            }
            close_data(dt);
            return success;
        }
    }
    success = write_page_to_file(pg, db->file);
    close_data(dt);
    return success;
}

void close_db(database* db) {
    write_db_header(db->file, db->header);
    free(db->header);
}
