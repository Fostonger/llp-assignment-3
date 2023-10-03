#include <string.h>
#include <stdlib.h>

#include "table.h"
#include "data.h"

uint8_t type_to_size(column_type type) {
    switch (type) {
    case INT_32:
        return sizeof(int32_t);
    case BOOL:
        return sizeof(int8_t);
    case FLOAT:
        return sizeof(float);
    case STRING:
        return sizeof(uint64_t) * 2; // 1 - номер страницы; 2 - отступ от начала страницы до строки
    default:
        return 0;
    }
}

maybe_table create_table(const char *tablename, database *db) {
    table_header *header = malloc(sizeof(table_header));
    if (header == NULL) return (maybe_table) { .error=MALLOC_ERROR };

    header->column_amount = 0;
    header->row_size = 0;
    header->first_data_page_num = 0;
    header->first_string_page_num = 0;
    strcpy(header->name, tablename);

    table *tb = malloc(sizeof(table));
    if (tb == NULL) {
        free(header);
        return (maybe_table) { .error=MALLOC_ERROR };
    }

    tb->header = header;
    tb->db = db;
        
    return (maybe_table) { .error=OK, .value=tb };
}

result save_table(database *db, table *tb) {
    page *page_to_save = tb->first_page;
    while (page_to_save != NULL) {
        if (page_to_save->pgheader->page_number > db->header->next_page_to_save_number) {
            page *moved_page = rearrange_page_order(page_to_save);
            if (moved_page != NULL)
                prepare_data_for_saving(moved_page);
            prepare_data_for_saving(page_to_save);
        }
        mark_page_saved_without_saving(page_to_save);
        page_to_save = page_to_save->next_page;
    }
    page_to_save = tb->first_string_page;
    while (page_to_save != NULL) {
        if (page_to_save->pgheader->page_number > db->header->next_page_to_save_number) {
            page *moved_page = rearrange_page_order(page_to_save);
            if (moved_page != NULL)
                prepare_data_for_saving(moved_page);
            prepare_data_for_saving(page_to_save);
        }
        mark_page_saved_without_saving(page_to_save);
        page_to_save = page_to_save->next_page;
    }
    
    page_to_save = tb->first_page;
    while (page_to_save != NULL) {
        result write_error = write_page(page_to_save);
        if (write_error) return write_error;
        page_to_save = page_to_save->next_page;
    }

    page_to_save = tb->first_string_page;
    while (page_to_save != NULL) {
        result write_error = write_page(page_to_save);
        if (write_error) return write_error;
        page_to_save = page_to_save->next_page;
    }
    return OK;
}

bool get_table_by_name(any_value val1, page_header *header) {
    return !strcmp(val1.string_value, header->table_header.name);
}

maybe_table read_table(const char *tablename, database *db) {
    database_closure predicate = (database_closure) { .func=get_table_by_name, .value1=(any_value) { .string_value=(char *)tablename } };
    maybe_page pg_with_table = find_page(db, predicate);
    if (pg_with_table.error) return (maybe_table) { .error= pg_with_table.error, .value=NULL };

    maybe_table tb = create_table(tablename, db);
    if (tb.error) return tb;

    *(tb.value->header) = pg_with_table.value->pgheader->table_header;
    tb.value->db = db;
    tb.value->first_page = pg_with_table.value;
    tb.value->first_page_to_write = pg_with_table.value;
    if (tb.value->header->first_string_page_num != 0) {
        maybe_page first_string_page = get_page_by_number(tb.value->db, tb.value, tb.value->header->first_string_page_num);
        if (first_string_page.error) {
            release_table(tb.value);
            return (maybe_table) { .error=first_string_page.error, .value=NULL };
        }
        tb.value->first_string_page = first_string_page.value;
        tb.value->first_string_page_to_write = first_string_page.value;
        first_string_page.value->tb = tb.value;
    }

    pg_with_table.value->tb = tb.value;

    return tb;
}

void release_table(table *tb) {
    if (tb == NULL) return;
    release_page(tb->first_page);
    release_page(tb->first_string_page);
    free(tb->header);
    free(tb);
}

result add_column(table* tb, const char *column_name, column_type type) {
    if (tb->first_page != NULL && tb->first_page->pgheader->data_offset != 0) return MUST_BE_EMPTY;

    tb->header->column_amount += 1;
    tb->header->row_size += type_to_size(type);

    column_header *column = tb->header->columns + tb->header->column_amount - 1;
    strcpy(column->name, column_name);
    column->type = type;

    return OK;
}

size_t offset_to_column(table_header *tb_header, const char *column_name, column_type type) {
    size_t offset = 0;
    for (size_t column_index = 0; column_index < tb_header->column_amount; column_index++ ) {
        column_header column = tb_header->columns[column_index];
        if (column.type == type && !strcmp(column.name, column_name)) return offset;
        offset += type_to_size(column.type);
    }
    return -1;
}

void copy_columns(table *dst, table *src) {
    for (size_t column_index = 0; column_index < src->header->column_amount; column_index++) {
        column_header header = src->header->columns[column_index];
        add_column(dst, header.name, header.type);
    }
}

result join_columns(table *dst, table *tb1, table *tb2, const char *column_name, column_type type) {
    bool found_in_common = false;
    for (size_t column_index = 0; column_index < tb1->header->column_amount; column_index++) {
        column_header header = tb1->header->columns[column_index];
        add_column(dst, header.name, header.type);
        if (header.type == type && !strcmp(header.name, column_name)) found_in_common = true;
    }
    if (!found_in_common) {
        release_table(dst);
        return DONT_EXIST;
    }
    found_in_common = false;
    for (size_t column_index = 0; column_index < tb2->header->column_amount; column_index++) {
        column_header header = tb2->header->columns[column_index];
        if (header.type == type && !strcmp(header.name, column_name)) {
            found_in_common = true;
            continue;
        }
        add_column(dst, header.name, header.type);
    }
    if (!found_in_common) {
        release_table(dst);
        return DONT_EXIST;
    }
    return OK;
}


void print_columns(table_header *tbheader) {
    size_t line_lenght = 0;
    for (size_t column_index = 0; column_index < tbheader->column_amount; column_index++) {
        column_header header = tbheader->columns[column_index];
        size_t str_len = strlen(header.name);
        line_lenght += 7 + str_len + 8 - str_len%8 + 1;
        printf("\t%s\t|", header.name);
    }
    printf("\n");
    for (size_t i = 0; i < line_lenght; i++) {
        printf("-");
    }
    printf("\n");
}

column_type type_from_name(table_header *tbheader, const char *name) {
    for (size_t i = 0; i < tbheader->column_amount; i++) {
        if (!strcmp(tbheader->columns[i].name, name))
            return tbheader->columns[i].type;
    }
    return NONE;
}

column_header *header_from_name(table_header *tbheader, const char *name) {
    for (size_t i = 0; i < tbheader->column_amount; i++) {
        if (!strcmp(tbheader->columns[i].name, name))
            return &(tbheader->columns[i]);
    }
    return NULL;
}
