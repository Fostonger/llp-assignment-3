//
// Created by rosroble on 10.10.22.
//

#include "table.h"

uint8_t typeToColumnSize(columnType type) {
    switch (type) {
        case INTEGER:
            return sizeof(int32_t);
        case BOOLEAN:
            return sizeof(bool);
        case STRING:
            return sizeof(char) * DEFAULT_STRING_LENGTH;
        case FLOAT:
            return sizeof (double);
        default:
            return 0;
    }
}

columnHeader* get_col_header_by_name(table* tb, const char* name) {
    for (int i = 0; i < tb->header->columnAmount; i++) {
        columnHeader* currentColumn = &tb->header->columns[i];
        if (strcmp(name, currentColumn->columnName) == 0) {
            return currentColumn;
        }
    }
    return NULL;
}

table* new_table(const char* name, uint8_t columnAmount) {
    table* tb = malloc(sizeof (table));

    tableHeader* header = malloc(sizeof (tableHeader) + columnAmount * sizeof (columnHeader));
    header->columnAmount = columnAmount;
    header->columnInitAmount = 0;
    header->tableNameLength = strlen(name);
    header->oneRowSize = sizeof(dataHeader);
    header->thisSize = sizeof(tableHeader) + columnAmount * sizeof(columnHeader);

    if (header->tableNameLength > MAX_NAME_LENGTH) {
        printf("Name length exceeded");
        return NULL;
    }

    for (int i = 0; i < header->tableNameLength; i++) {
        header->tableName[i] = name[i]; // copy name
    }
    header->tableName[header->tableNameLength] = '\0';

    tb->header = header;

    return tb;
}

void table_apply(database* db, table* table) {
    page* pg = allocate_page(db);
    table_page_link(pg, table);
    table->firstPage = pg;
    table->firstAvailableForWritePage = pg;
    table->db = db;
    if (db) {
        write_page_to_file(pg, db->file);
    }
}


void init_fixedsize_column(table* tb, uint8_t ordinal, const char* name, columnType type) {
    columnHeader header = {type, typeToColumnSize(type)};
    tb->header->columns[ordinal] = header;
    tb->header->oneRowSize += header.size;
    int len = -1;
    while (name[++len] != '\0') tb->header->columns[ordinal].columnName[len] = name[len];
    tb->header->columns[ordinal].columnName[len] = '\0';
}

void init_varsize_column(table* tb, uint8_t ordinal, const char* name, columnType type, uint16_t size) {
    columnHeader header = {type, size};
    tb->header->columns[ordinal] = header;
    tb->header->oneRowSize += header.size;
    int len = -1;
    while (name[++len] != '\0') tb->header->columns[ordinal].columnName[len] = name[len];
    tb->header->columns[ordinal].columnName[len] = '\0';
}

table* open_table(database* db, const char* name) {
    page* currentPage = read_page(db, 0);
    int pageCount = 0;
    while (pageCount < db->header->pages_amount && strcmp(currentPage->tableHeader->tableName, name) != 0) {
        close_page(currentPage);
        currentPage = read_page(db, ++pageCount);
    }
    if (pageCount == db->header->pages_amount) return NULL;
    table* tb = malloc(sizeof(table));
    tb->header = currentPage->tableHeader;
    tb->firstPage = currentPage;
    tb->db = db;
    tb->firstAvailableForWritePage = getFirstAvailableForWritePage(db, tb);
    return tb;
}

void close_table(table* tb) {
    close_page(tb->firstAvailableForWritePage);
    if (tb->firstAvailableForWritePage != tb->firstPage) {
        close_page(tb->firstPage);
    }
    free(tb->header);
    free(tb);
}


