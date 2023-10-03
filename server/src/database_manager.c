#include <stdlib.h>
#include <string.h>

#include "database_manager.h"

result write_db_header(database *db) {
    fseek(db->file, 0, SEEK_SET);
    size_t written = fwrite(db->header, sizeof(database_header), 1, db->file);
    if (written != 1) return WRITE_ERROR;
    return OK;
}

result read_db_header(database *db, database_header* header) {
    fseek(db->file, 0, SEEK_SET);
    size_t read = fread(header, sizeof(database_header), 1, db->file);
    if (read != 1) return READ_ERROR;
    return OK;
}

maybe_database allocate_db(FILE *file) {
    if (file == NULL) return (maybe_database){ .error=DONT_EXIST, .value=NULL };

    database *db_val = (database *)malloc(sizeof(database));
    if (db_val == NULL) return (maybe_database){ .error=MALLOC_ERROR, .value=NULL };

    maybe_database db = (maybe_database) {.error=OK, .value=db_val};
    db_val->file = file;

    database_header *header = (database_header *)malloc(sizeof(database_header));
    if (header == NULL) {
        release_db(db_val);
        return (maybe_database){ .error=MALLOC_ERROR, .value=NULL };
    }

    header->page_size = PAGE_SIZE;
    header->first_free_page = 1;
    header->next_page_to_save_number = 1;
    db_val->header = header;

    db_val->all_loaded_pages = (page **)malloc(sizeof(page *) * 100);
    db_val->loaded_pages_capacity = 100;
    return db;
}

maybe_database create_new_db(char *filename) {
    FILE *file = fopen(filename, "w+");
    if (file == NULL) return (maybe_database) { .error=OPEN_ERROR , .value=NULL };

    maybe_database db = allocate_db(file);
    if (db.error) return db;

    result write_error = write_db_header(db.value);
    if (write_error) {
        release_db(db.value);
        return (maybe_database){ .error=write_error, .value=NULL };
    }
    return db;
}

maybe_database read_db(char *filename) {
    FILE *file = fopen(filename, "r+");
    if (file == NULL) return (maybe_database) { .error=OPEN_ERROR , .value=NULL };

    maybe_database db = allocate_db(file);
    if (db.error) return db;
    
    result read_error = read_db_header(db.value, db.value->header);
    if (read_error) {
        release_db(db.value);
        return (maybe_database){ .error=read_error, .value=NULL };
    }
    db.value->header->first_free_page = db.value->header->next_page_to_save_number;
    db.value->loaded_pages_capacity = 100;
    return db;
}

maybe_database initdb(char *filename, bool overwrite) {
    if (overwrite) return create_new_db(filename);
    return read_db(filename);
}

void expand_loaded_pages_memory(database *db) {
    page **new_pages_storage = (page **)malloc(sizeof(page *) * (db->loaded_pages_capacity + 100));
    memcpy(new_pages_storage, db->all_loaded_pages, sizeof(page *) * db->loaded_pages_capacity);
    db->loaded_pages_capacity += 100;
    free(db->all_loaded_pages);
    db->all_loaded_pages = new_pages_storage;
}

uint16_t get_page_number(database *db, page *pg) {
    uint16_t page_num = db->header->first_free_page++;
    while (page_num >= db->loaded_pages_capacity) // первый указатель в all_loaded_pages не используется
        expand_loaded_pages_memory(db);
    db->all_loaded_pages[page_num] = pg;
    return page_num;
}

size_t count_offset_to_page_header(database *db, size_t ordinal_number) {
    return sizeof(database_header) + (sizeof(page_header) + db->header->page_size) * (ordinal_number - 1);
}

size_t count_offset_to_page_data(database *db, size_t ordinal_number) {
    return count_offset_to_page_header(db, ordinal_number) + sizeof(page_header);
}

void mark_page_saved_without_saving(page *pg) {
    if (pg->tb->db->header->next_page_to_save_number == pg->pgheader->page_number)
        pg->tb->db->header->next_page_to_save_number++;
}

result write_page_header(page *pg) {
    pg->pgheader->table_header = *(pg->tb->header);
    mark_page_saved_without_saving(pg);

    size_t page_offset = count_offset_to_page_header(pg->tb->db, pg->pgheader->page_number);
    fseek(pg->tb->db->file, page_offset, SEEK_SET);
    size_t written = fwrite(pg->pgheader, sizeof(page_header), 1, pg->tb->db->file);
    if (written != 1) return WRITE_ERROR;
    return OK;
}

result write_page_data(page *pg) {
    size_t page_offset = count_offset_to_page_data(pg->tb->db, pg->pgheader->page_number);
    fseek(pg->tb->db->file, page_offset, SEEK_SET);
    size_t written = fwrite(pg->data, pg->tb->db->header->page_size, 1, pg->tb->db->file);
    if (written != 1) return WRITE_ERROR;

    write_db_header(pg->tb->db);
    
    return OK;
}

result read_page_data(database *db, page *pg_to_read) {
    if (pg_to_read == NULL || pg_to_read->pgheader == NULL) return DONT_EXIST;
    if (pg_to_read->data != NULL) return OK;
    size_t page_offset = count_offset_to_page_data(db, pg_to_read->pgheader->page_number);
    fseek(db->file, page_offset, SEEK_SET);
    pg_to_read->data = malloc(db->header->page_size);
    size_t read = fread(pg_to_read->data, sizeof(char), db->header->page_size, db->file);
    if (read != db->header->page_size) return READ_ERROR;
    return OK;
}

maybe_page create_empty_page_with_header(page_header *header, table *tb) {
    page *pg = malloc(sizeof(page));
    if (pg == NULL) return (maybe_page) { .error=MALLOC_ERROR, .value = NULL };
    pg->pgheader = header;
    pg->tb = tb;
    return (maybe_page) { .error=OK, .value=pg };
}

maybe_page read_page_header(database *db, table *tb, uint16_t page_ordinal) {
    if (page_ordinal == 0 || page_ordinal >= db->header->next_page_to_save_number) 
        return (maybe_page){.error=DONT_EXIST, .value=NULL };

    size_t page_offset = count_offset_to_page_header(db, page_ordinal);
    fseek(db->file, page_offset, SEEK_SET);
    page_header *header = (page_header *) malloc(sizeof(page_header));
    size_t read = fread(header, sizeof(page_header), 1, db->file);
    if (read != 1) return (maybe_page){.error=READ_ERROR, .value=NULL };
    return create_empty_page_with_header(header, tb);
}

void update_table_header(page *pg, uint16_t new_pg_number) {
    if (pg->tb->header->first_data_page_num == pg->pgheader->page_number)
        pg->tb->header->first_data_page_num = new_pg_number;

    if (pg->tb->header->first_string_page_num == pg->pgheader->page_number)
        pg->tb->header->first_string_page_num = new_pg_number;
}

page *rearrange_page_order(page *pg) {
    database *db = pg->tb->db;
    page *pg_to_move = db->all_loaded_pages[db->header->next_page_to_save_number];

    bool move_second_page = pg_to_move != NULL && pg_to_move->pgheader != NULL;

    size_t moved_page_number = pg->pgheader->page_number;
    size_t target_page_number = db->header->next_page_to_save_number;

    if (move_second_page) {
        update_table_header(pg_to_move, moved_page_number);
        pg_to_move->pgheader->page_number = moved_page_number;
    }
    update_table_header(pg, target_page_number);
    pg->pgheader->page_number = target_page_number;

    for (size_t pages_index = 0; pages_index < moved_page_number; pages_index++) {
        maybe_page pg_header_on_disk = get_page_header(pg->tb->db, pg->tb, pages_index);
        if(pg_header_on_disk.error) continue;
        if(pg_header_on_disk.value->pgheader->next_page_number == moved_page_number) {
            pg_header_on_disk.value->pgheader->next_page_number = target_page_number;
            if (pages_index < target_page_number) write_page_header(pg_header_on_disk.value);
        } else if (pg_header_on_disk.value->pgheader->next_page_number == target_page_number) {
            pg_header_on_disk.value->pgheader->next_page_number = moved_page_number;
            if (pages_index < target_page_number) write_page_header(pg_header_on_disk.value);
        }
    }
    db->all_loaded_pages[moved_page_number] = pg_to_move;
    db->all_loaded_pages[target_page_number] = pg;

    return move_second_page ? pg_to_move : NULL;
}

maybe_page get_page_header(database *db, table *tb, size_t page_number) {
    if (db->all_loaded_pages[page_number] != NULL && db->all_loaded_pages[page_number]->pgheader != NULL) {
        if (tb != NULL) db->all_loaded_pages[page_number]->tb = tb;
        return (maybe_page) { .error=OK, .value=db->all_loaded_pages[page_number] };
    } else {
        maybe_page read_page = read_page_header(db, tb, page_number);
        if (read_page.error) return read_page;
        while (read_page.value->pgheader->page_number > db->loaded_pages_capacity)
            expand_loaded_pages_memory(db);
        db->all_loaded_pages[page_number] = read_page.value;
        return read_page;
    }
}

maybe_page find_page(database *db, database_closure predicate) {
    for (size_t pg_index = 1; pg_index < db->loaded_pages_capacity; pg_index++) {
        maybe_page pg_header = get_page_header(db, NULL, pg_index);
        if (pg_header.error) return pg_header;
        if (predicate.func(predicate.value1, pg_header.value->pgheader)) {
            result page_data_read_error = read_page_data(db, pg_header.value);
            if (page_data_read_error) 
                return (maybe_page) { .error=page_data_read_error, .value=NULL };
            return pg_header;
        }
    }
    return (maybe_page) { .error=DONT_EXIST, .value=NULL };
}

result write_page(page *pg) {
    if (pg->tb->db->header->next_page_to_save_number < pg->pgheader->page_number) {
        return INVALID_PAGE_NUMBER;
    }
    result write_error = write_page_header(pg);
    if (write_error) return write_error;
    write_error = write_page_data(pg);
    if (write_error) return write_error;
    return OK;
}

maybe_page create_page(database *db, table *tb, page_type type) {
    page_header *header = malloc(sizeof(page_header));
    if (header == NULL) return (maybe_page) { .error=MALLOC_ERROR, .value = NULL };

    header->data_offset = 0;
    header->table_header = *tb->header;
    header->next_page_number = 0;
    header->type = type;

    page *pg = malloc(sizeof(page));
    if (pg == NULL) return (maybe_page) { .error=MALLOC_ERROR, .value = NULL };

    header->page_number = get_page_number(db, pg);

    pg->pgheader = header;
    pg->tb = tb;
    pg->data = malloc (PAGE_SIZE);
    if (pg->data == NULL) return (maybe_page) { .error=MALLOC_ERROR, .value = NULL };
    pg->next_page = NULL;

    return (maybe_page) { .error=OK, .value=pg };
}

page *get_first_writable_page(page *pg) {
    page *cur_pg = pg;
    while (cur_pg->pgheader->next_page_number != 0 && PAGE_SIZE - cur_pg->pgheader->data_offset < MIN_VALUABLE_SIZE) {
        maybe_page next_pg = get_page_header(pg->tb->db, pg->tb, cur_pg->pgheader->next_page_number);
        if (next_pg.error) break;
        cur_pg = next_pg.value;
    }
    return cur_pg;
}

// Перебираем все страницы с доступным местом пока не найдем ту, в которую можем пихнуть строку
maybe_page get_suitable_page(table *tb, size_t data_size, page_type type) {
    maybe_page pg = (maybe_page) {};
    if (tb->first_string_page_to_write == NULL && ( pg = create_page(tb->db, tb, type) ).error )
        return (maybe_page) { .error=pg.error, .value=NULL };
    if (pg.value != NULL) {
        tb->first_string_page_to_write = pg.value;
        tb->first_string_page = pg.value;
        tb->header->first_string_page_num = pg.value->pgheader->page_number;
        pg.value->pgheader->table_header = *(tb->header);
        pg.value->pgheader->data_offset = 0;
    }

    page *writable_page = tb->first_string_page_to_write;

    while (writable_page->pgheader->data_offset + data_size > PAGE_SIZE && writable_page->next_page != NULL)
        writable_page = writable_page->next_page;

    // не нашли страницу подходящего размера, но это не значит, что можно забраковать все страницы!
    // проверку на MIN_VALUABLE_SIZE все равно сделать нужно
    if (writable_page->pgheader->data_offset + data_size > PAGE_SIZE) {
        maybe_page new_pg = read_page_header(tb->db, tb, writable_page->pgheader->next_page_number);
        while (!new_pg.error && new_pg.value->pgheader->data_offset + data_size > PAGE_SIZE && new_pg.value->pgheader->next_page_number != 0) {
            new_pg = read_page_header(tb->db, tb, new_pg.value->pgheader->next_page_number);
        }
        if (!new_pg.error && new_pg.value->pgheader->data_offset + data_size <= PAGE_SIZE) {
            read_page_data(tb->db, new_pg.value);
            tb->first_string_page_to_write->next_page = new_pg.value;
            tb->first_string_page_to_write = new_pg.value;
            writable_page = new_pg.value;
        } else {
            if ( ( new_pg = create_page(tb->db, tb, type) ).error )
                return (maybe_page) { .error=new_pg.error, .value=NULL };
            writable_page = new_pg.value;

            page *last_page = tb->first_string_page;
            while (last_page->pgheader->next_page_number != 0) {
                maybe_page next_pg = get_page_header(tb->db, tb, last_page->pgheader->next_page_number);
                if (next_pg.error) return next_pg;
                last_page->next_page = next_pg.value;
                last_page = next_pg.value;
            }
            last_page->next_page = new_pg.value;
            last_page->pgheader->next_page_number = new_pg.value->pgheader->page_number;
        }
    }

    result read_data_error = read_page_data(tb->db, writable_page);

    // проверка на MIN_VALUABLE_SIZE
    page *first_writable_page = get_first_writable_page(tb->first_string_page_to_write);
    if (first_writable_page != NULL) tb->first_string_page_to_write = first_writable_page;

    return (maybe_page) { .error=read_data_error, .value=writable_page };
}

// TODO: load from disk if page is in storage
maybe_page get_next_page_or_load(page *pg) {
    if (pg->next_page != NULL) return (maybe_page) { .error=OK, .value=pg->next_page };
    return (maybe_page) { .error=DONT_EXIST, .value=NULL };
}

maybe_page get_page_by_number(database *db, table *tb, uint64_t page_ordinal) {
    if (page_ordinal == 0) 
        return (maybe_page) { .error=DONT_EXIST, .value=NULL };
    while (page_ordinal > db->loaded_pages_capacity) {
        expand_loaded_pages_memory(db);
    }
    if ( db->all_loaded_pages[page_ordinal] == NULL ) {
        maybe_page read_page = read_page_header(db, tb, page_ordinal);
        if (read_page.error) return read_page;

        result read_page_data_error = read_page_data(db, read_page.value);
        if (read_page_data_error) return (maybe_page) { .error=read_page_data_error, .value=NULL };

        db->all_loaded_pages[page_ordinal] = read_page.value;
        return read_page;
    }
    page *loaded_page = db->all_loaded_pages[page_ordinal];
    maybe_page pg = (maybe_page) { .error=OK, .value=loaded_page };
    return pg;
}

result ensure_enough_space_table(table *tb, size_t data_size, page_type type) {
    maybe_page pg = (maybe_page) {};
    if (tb->first_page_to_write == NULL && ( pg = create_page(tb->db, tb, type) ).error )
        return pg.error;
    if (pg.value != NULL) {
        tb->first_page_to_write = pg.value;
        tb->first_page = pg.value;
        tb->header->first_data_page_num = pg.value->pgheader->page_number;
        pg.value->pgheader->data_offset = 0;
    }

    page_header *writable_page_header = tb->first_page_to_write->pgheader;

    if (writable_page_header->data_offset + data_size > PAGE_SIZE) {
        maybe_page new_pg = read_page_header(tb->db, tb, writable_page_header->next_page_number);
        while (!new_pg.error && new_pg.value->pgheader->data_offset + data_size > PAGE_SIZE) {
            new_pg = read_page_header(tb->db, tb, writable_page_header->next_page_number);
        }
        if (!new_pg.error && new_pg.value->pgheader->data_offset + data_size <= PAGE_SIZE) {
            read_page_data(tb->db, new_pg.value);
            tb->first_page_to_write->next_page = new_pg.value;
            tb->first_page_to_write = new_pg.value;
        } else {
            if ( ( new_pg = create_page(tb->db, tb, type) ).error )
                return new_pg.error;
            if (new_pg.value != NULL) {
                tb->first_page_to_write->next_page = new_pg.value;
                tb->first_page_to_write->pgheader->next_page_number = new_pg.value->pgheader->page_number;
                tb->first_page_to_write = new_pg.value;
                new_pg.value->pgheader->data_offset = 0;
            }
        }
    }

    return OK;
}

void release_page(page *pg) {
    page *cur_page = pg;
    page *prev_page = cur_page;
    while (cur_page != NULL) {
        free(cur_page->data);
        cur_page->tb->db->all_loaded_pages[cur_page->pgheader->page_number] = NULL;
        free(cur_page->pgheader);
        prev_page = cur_page;
        cur_page = cur_page->next_page;
        free(prev_page);
    }
}

void release_db(database *db) {
    free(db->header);
    fclose(db->file);
    free(db);
}
