//
// Created by rosroble on 10.10.22.
//

#include "database.h"


page* allocate_page(database* db) {
    page* pg = malloc(sizeof (page));

    pageHeader* header = malloc(sizeof(pageHeader));
    header->free = true;
    header->page_ordinal = db ? db->header->pages_amount++ : 0;
    header->rowsAmount = 0;
    header->freeRowOffset = 0;
    header->next_page_ordinal = 0;

    pg->header = header;
    pg->tableHeader = NULL;
    pg->nextPage = NULL;
    pg->bytes = NULL; // bytes are allocated when assigned to a table

    if (db) {
        write_db_header(db->file, db->header);
    }

    return pg;
}

bool write_page_to_file(page* pg, FILE* file) {
    fseek(file, sizeof(db_header) + pg->header->page_ordinal * DEFAULT_PAGE_SIZE, SEEK_SET);
    size_t pageHeaderSize = sizeof (pageHeader);
    size_t tableHeaderSize = pg->tableHeader != NULL ? pg->tableHeader->thisSize : 0;
    bool success = true;
    success = success & fwrite(pg->header, pageHeaderSize, 1, file);
    if (pg->tableHeader) {
        success &= fwrite(pg->tableHeader, tableHeaderSize, 1, file);
    }
    if (pg->bytes) {
        success &= fwrite(pg->bytes, DEFAULT_PAGE_SIZE - tableHeaderSize - pageHeaderSize, 1, file);
    }
    return success;
}

bool read_db_header(FILE* file, db_header* header) {
    fseek(file, 0, SEEK_SET);
    size_t read = fread(header, sizeof(db_header), 1, file);
    if (read != 1) {
        return false;
    }
    return true;
}

bool write_db_header(FILE* file, db_header* header) {
    fseek(file, 0, SEEK_SET);
    size_t written = fwrite(header, sizeof(db_header), 1, file);
    if (written != 1) {
        return false;
    }
    return true;
}


database* read_db(FILE* file) {
    database* db = malloc(sizeof(database));
    db_header* header = malloc(sizeof(db_header));
    if (!read_db_header(file, header)) {
        printf("Error reading db header");
        exit(EXIT_FAILURE);
    }
    db->header = header;
    db->file = file;
    // db -> available page ?
    return db;
}

database* overwrite_db(FILE* file) {
    database* db = malloc(sizeof(database));
    db_header* header = malloc(sizeof(db_header));

    header->page_size = DEFAULT_PAGE_SIZE;
    header->pages_amount = 0;
    header->first_free_page_ordinal = 0;

    db->header = header;
    db->file = file;

    write_db_header(file, header);

    return db;
}


database* init_db(FILE* file, bool overwrite) {
    if (overwrite) {
        return overwrite_db(file);
    }
    return read_db(file);
}

bool pageHasSpaceForWrite(page* pg) {
    tableHeader* tbHeader = pg->tableHeader;
    return tbHeader->thisSize + sizeof(pageHeader) + pg->header->rowsAmount * tbHeader->oneRowSize
    <= DEFAULT_PAGE_SIZE - tbHeader->oneRowSize;
}

void table_page_link(page* pg, table* tb) {
    pg->tableHeader = tb->header;
    pg->bytes = malloc(DEFAULT_PAGE_SIZE - sizeof(pageHeader) - pg->tableHeader->thisSize);
}

page* getFirstAvailableForWritePage(database* db, table* table) {
    page* pg = table->firstPage;
    while (!pg->header->free) {
        uint32_t nextPageOrdinal = pg->header->next_page_ordinal;
        if (pg->header->next_page_ordinal == 0) {
            page* newPage = allocate_page(db);
            pg->header->next_page_ordinal = newPage->header->page_ordinal;
            table_page_link(newPage, table);
            write_page_to_file(pg, db->file);
            write_page_to_file(newPage, db->file);
            return newPage;
        }
        if (pg != table->firstPage) {
            close_page(pg);
        }
        pg = read_page(db, nextPageOrdinal);
    }
    return pg;
}


void close_page(page* pg) {
    free(pg->header);
    free(pg->bytes);
    free(pg);
}

page* read_page(database* db, uint32_t pageOrdinal) {
    if (pageOrdinal >= db->header->pages_amount) {
        printf("Bad page ordinal");
        return NULL;
    }
    page* pg = malloc(sizeof (page));
    FILE* file = db->file;
    fseek(file, sizeof(db_header) + pageOrdinal * DEFAULT_PAGE_SIZE, SEEK_SET);

    pageHeader* pgHeader = malloc(sizeof(pageHeader));
    size_t readPgHeader = fread(pgHeader, sizeof(pageHeader), 1, file);
    if (readPgHeader < 1) {
        printf("Error reading page header.");
    }
    uint32_t tableHeaderSize;
    size_t read = fread(&tableHeaderSize, sizeof(uint32_t), 1, file);
    tableHeader* tbHeader = malloc(tableHeaderSize);
    fseek(file, -sizeof(uint32_t), SEEK_CUR);
    size_t readTbHeader = fread(tbHeader, tableHeaderSize, 1, file);
    if (readTbHeader < 1) {
        printf("Error reading table header.");
    }
    pg->bytes = malloc(DEFAULT_PAGE_SIZE - tableHeaderSize - sizeof(pageHeader));
    size_t readBytes = fread(pg->bytes, DEFAULT_PAGE_SIZE - tableHeaderSize - sizeof(pageHeader), 1, file);
    if (readBytes < 1) {
        printf("Error reading bytes");
    }
    pg->header = pgHeader;
    pg->tableHeader = tbHeader;
    pg->nextPage = NULL;

    return pg;
}



