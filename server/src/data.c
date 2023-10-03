#include <string.h>
#include <stdlib.h>

#include "data.h"
#include "database_manager.h"
#include "functional.h"

#define MIN(X, Y) (((X) < (Y)) ? (X) : (Y))

typedef struct {
    uint64_t string_page_number;
    uint64_t offset;
} string_data_ref;

typedef struct {
    string_data_ref     link_to_current;
    uint64_t            str_len;
    string_data_ref     next_part;
    bool                enabled;
    char                string[];
} string_in_storage;

maybe_data init_empty_data(table *tb) {
    data *dt = (data *)malloc(sizeof(data));
    if (dt == NULL) return (maybe_data) { .error=MALLOC_ERROR, .value=NULL };
    dt->table = tb;
    dt->size = 0;
    return (maybe_data) { .error=OK, .value=dt };
}

maybe_data init_data(table *tb) {
    maybe_data dt = init_empty_data(tb);
    if (dt.error) return dt;
    dt.value->bytes = malloc(tb->header->row_size);
    if (dt.value->bytes == NULL) return (maybe_data) { .error=MALLOC_ERROR, .value=NULL };
    return dt;
}

void release_data(data *dt) {
    if (dt == NULL) return;
    free(dt->bytes);
    free(dt);
}

void clear_data(data *dt) {
    dt->size = 0;
}

result data_init_integer(data *dt, int32_t val) {
    if (dt->size + type_to_size(INT_32) > dt->table->header->row_size) return NOT_ENOUGH_SPACE;
    char *ptr = (char *)dt->bytes + dt->size;
    *( (int32_t*) ptr ) = val;
    dt->size += type_to_size(INT_32);
    return OK;
}

// NOTE: Вызывать эту функцию с длиной строки ВКЛЮЧАЯ СИМВОЛ \0
// Возвращает сслыку на место в строке, где указывается следующая часть строки. Нужно для последующей связи частей строк
string_data_ref *data_init_one_page_string(data *dt, const char *val, string_data_ref *link_to_current, 
                                            string_data_ref **link_to_prev, size_t string_len) {
    size_t string_data_len = sizeof(string_in_storage) + string_len;

    maybe_page suitable_page = get_suitable_page(dt->table, string_data_len, STRING_PAGE);
    if (suitable_page.error) return NULL;

    page *writable_page = suitable_page.value;

    // Данные о строке заносим в таблицу: ссылка на страницу, в которой хранится строка, и отступ от начала страницы
    if (link_to_current != NULL) {
        link_to_current->offset = writable_page->pgheader->data_offset;
        link_to_current->string_page_number = writable_page->pgheader->page_number;
    }

    string_in_storage *string_data_ptr = (string_in_storage *)((char *)writable_page->data + writable_page->pgheader->data_offset);
    string_data_ptr->str_len = string_len;
    string_data_ptr->enabled = true;

    char *data = string_data_ptr->string;

    memcpy(data, val, string_len);

    writable_page->pgheader->data_offset += string_data_len;
    string_data_ptr->next_part = (string_data_ref){}; string_data_ptr->link_to_current = (string_data_ref){};

    *link_to_prev = &(string_data_ptr->link_to_current);

    return &(string_data_ptr->next_part);
}

result data_init_string(data *dt, const char* val) {
    if (dt->size + type_to_size(STRING) > dt->table->header->row_size) return NOT_ENOUGH_SPACE;
    char *ptr = dt->bytes + dt->size;
    dt->size += type_to_size(STRING);

    size_t string_len = strlen(val) + 1;
    string_data_ref *cur_string_ref = (string_data_ref *)ptr;
    string_data_ref *prev_string_ref = NULL;
    string_data_ref written_string_ref = (string_data_ref){};
    size_t string_offset = 0;

    while (string_len > 0) {
        size_t cur_string_len = MIN(string_len, PAGE_SIZE - sizeof(string_in_storage));

        string_data_ref previously_written_ref = written_string_ref;
        string_data_ref *future_string_ref = data_init_one_page_string(dt, val + string_offset, cur_string_ref, &prev_string_ref, cur_string_len);
        written_string_ref = *cur_string_ref;
        cur_string_ref = future_string_ref;
        if (prev_string_ref != NULL && previously_written_ref.string_page_number != 0) {
            previously_written_ref.offset += sizeof(string_data_ref) + sizeof(uint64_t);
            *prev_string_ref = previously_written_ref;
        }

        string_offset += cur_string_len;
        string_len -= cur_string_len;
    }
    
    return OK;
}

result data_init_boolean(data *dt, bool val) {
    if (dt->size + type_to_size(BOOL) > dt->table->header->row_size) return NOT_ENOUGH_SPACE;
    char *ptr = (char *)dt->bytes + dt->size;
    *( (bool*) ptr ) = val;
    dt->size += type_to_size(BOOL);
    return OK;
}

result data_init_float(data *dt, float val) {
    if (dt->size + type_to_size(FLOAT) > dt->table->header->row_size) return NOT_ENOUGH_SPACE;
    char *ptr = (char *)dt->bytes + dt->size;
    *( (float*) ptr ) = val;
    dt->size += type_to_size(FLOAT);
    return OK;
}

result data_init_any(data *dt, const any_value val, column_type type) {
    switch (type) {
    case INT_32:
        return data_init_integer(dt, val.int_value);
    case FLOAT:
        return data_init_float(dt, val.float_value);
    case BOOL:
        return data_init_boolean(dt, val.bool_value);
    case STRING:
        return data_init_string(dt, val.string_value);
    default:
        return DONT_EXIST;
    }
}

string_in_storage *get_next_string_data_on_page(string_in_storage *prev_string, page *pg_with_string_data) {
    if (!prev_string || !pg_with_string_data) return NULL;

    string_in_storage *next_string = (string_in_storage *)((char *)prev_string + sizeof(string_in_storage) + prev_string->str_len);
    // Проверка, не является ли prev_string последней строкой на странице
    if ( (char *)next_string >= (char *)pg_with_string_data->data + pg_with_string_data->pgheader->data_offset ) return NULL;
    return (string_in_storage *)next_string;
}

void set_page_info_in_strings(data *dt, size_t string_pg_num, uint64_t offset_to_row) {
    page *pg_with_string_data = dt->table->first_string_page;
    for (size_t column_index = 0; column_index < dt->table->header->column_amount; column_index++) {
        column_header header = dt->table->header->columns[column_index];
        if (header.type != STRING) continue;

        size_t string_offset = offset_to_column(dt->table->header, header.name, STRING);
        string_data_ref *string_ref = (string_data_ref *)((char *)dt->bytes + string_offset);

        if (pg_with_string_data->pgheader->page_number != string_ref->string_page_number) {
            maybe_page page_with_string = get_page_by_number(dt->table->db, dt->table, string_ref->string_page_number);
            if (page_with_string.error) return;
            pg_with_string_data = page_with_string.value;
        }

        string_in_storage *string_in_storage_ptr = (string_in_storage *)((char *)pg_with_string_data->data + string_ref->offset);
        string_in_storage_ptr->link_to_current.string_page_number = string_pg_num;
        string_in_storage_ptr->link_to_current.offset = offset_to_row + string_offset;
    }
}

result set_data(data *dt) {
    result is_enough_space = ensure_enough_space_table(dt->table, dt->size, TABLE_DATA);
    if (is_enough_space) return is_enough_space;

    char *data_ptr = dt->bytes;

    page *pg_to_write = dt->table->first_page_to_write;
    uint64_t offset_to_row = pg_to_write->pgheader->data_offset;

    set_page_info_in_strings(dt, pg_to_write->pgheader->page_number, offset_to_row);

    void **table_ptr = pg_to_write->data + offset_to_row;
    memcpy(table_ptr, data_ptr, dt->size);

    dt->table->first_page_to_write->pgheader->data_offset += dt->size;

    return OK;
}

result copy_data(data *dst, data *src) {
    for (size_t column_index = 0; column_index < src->table->header->column_amount; column_index++) {
        column_header header = src->table->header->columns[column_index];
        size_t offset = offset_to_column(src->table->header, header.name, header.type);
        any_value val;
        get_any_from_data(src, &val, offset, header.type);
        result copy_error = data_init_any(dst, val, header.type);
        if (header.type == STRING) free(val.string_value);
        if (copy_error) return copy_error;
    }
    return OK;
}

result join_data(data *dst, data *dt1, data *dt2, const char *column_name, column_type type) {
    if (dst == NULL || dt1 == NULL || dt2 == NULL || column_name == NULL) return DONT_EXIST;
    if (dst->bytes == dt1->bytes || dst->bytes == dt2->bytes || dt1->bytes == dt2->bytes) return CROSS_ON_JOIN;
    if (dst->table->header->column_amount != dt1->table->header->column_amount + dt2->table->header->column_amount - 1)
        return NOT_ENOUGH_SPACE;
    
    if (offset_to_column(dt1->table->header, column_name, type) == -1 || 
        offset_to_column(dt2->table->header, column_name, type) == -1) {    
        return DONT_EXIST;
    }

    result copy_error = copy_data(dst, dt1);
    if (copy_error) return copy_error;

    for (size_t column_index = 0; column_index < dt2->table->header->column_amount; column_index++) {
        column_header header = dt2->table->header->columns[column_index];
        if (header.type == type && !strcmp(column_name, header.name)) continue;
        size_t offset = offset_to_column(dt2->table->header, header.name, header.type);
        any_value val;
        get_any_from_data(dt2, &val, offset, header.type);
        copy_error = data_init_any(dst, val, header.type);
        if (header.type == STRING) free(val.string_value);
        if (copy_error) return copy_error;
    }

    return OK;
}

size_t delete_saved_string(page *page_string, uint16_t offset) {
    string_in_storage *string_to_delete = (string_in_storage *)((char *)page_string->data + offset);
    size_t delete_string_len = string_to_delete->str_len + sizeof(string_in_storage);
    string_in_storage *next_strings = (string_in_storage *)((char *)string_to_delete + delete_string_len);
    size_t bytes_to_end = PAGE_SIZE - offset;

    memmove(string_to_delete, next_strings, bytes_to_end);

    return (char *)next_strings - (char *)string_to_delete;
}

void make_string_disabled(page *page_string, uint16_t offset) {
    string_in_storage *string_to_delete = (string_in_storage *)((char *)page_string->data + offset);
    string_to_delete->enabled = false;

    while (string_to_delete->next_part.string_page_number != 0) {
        maybe_page page_with_string = get_page_by_number(page_string->tb->db, page_string->tb, string_to_delete->next_part.string_page_number);
        if (page_with_string.error) return;
        string_to_delete = (string_in_storage *)((char *)page_with_string.value->data + string_to_delete->next_part.offset);
        string_to_delete->enabled = false;
    }
}

bool has_next_data_on_page(page *cur_page, char *cur_data) {
    return cur_data - (char *)cur_page->data < cur_page->pgheader->data_offset 
            && ( cur_data - (char *)cur_page->data ) / cur_page->tb->header->row_size < 
                cur_page->pgheader->data_offset / cur_page->tb->header->row_size;
}

result delete_strings_from_row(data *dt) {
    for (size_t column_index = 0; column_index < dt->table->header->column_amount; column_index++) {
        column_header header = dt->table->header->columns[column_index];
        if (header.type != STRING) continue;
        
        size_t string_offset = offset_to_column(dt->table->header, header.name, STRING);
        string_data_ref *string_data = (string_data_ref *)((char *)dt->bytes + string_offset);

        maybe_page page_with_string = get_page_by_number(dt->table->db, dt->table, string_data->string_page_number);
        if (page_with_string.error) return page_with_string.error;
        make_string_disabled(page_with_string.value, string_data->offset);
    }
    return OK;
}

void sync_storage_by_table(data *dt, size_t new_offset, size_t new_pg_num) {
    for (size_t column_index = 0; column_index < dt->table->header->column_amount; column_index++) {
        column_header header = dt->table->header->columns[column_index];
        if (header.type != STRING) continue;
        
        size_t string_offset = offset_to_column(dt->table->header, header.name, STRING);
        string_data_ref *string_ref = (string_data_ref *)((char *)dt->bytes + string_offset);

        maybe_page page_with_string = get_page_by_number(dt->table->db, dt->table, string_ref->string_page_number);
        string_in_storage *cur_string_data = (string_in_storage *)(page_with_string.value->data + string_ref->offset);

        cur_string_data->link_to_current.offset = new_offset;
        cur_string_data->link_to_current.string_page_number = new_pg_num;
    }
}

result delete_saved_row(data *dt, page *pg) {
    result delete_string_result = delete_strings_from_row(dt);
    if (delete_string_result) return delete_string_result;
    if (dt->table->first_page_to_write->pgheader->data_offset == 0) {
        if (dt->table->first_page->pgheader->next_page_number == 0 ) return DONT_EXIST;
        size_t last_suitable_pg_num = dt->table->first_page->pgheader->data_offset == 0 ? 0 : dt->table->first_page->pgheader->page_number;
        maybe_page new_page_to_write = get_page_by_number( dt->table->db, dt->table, dt->table->first_page->pgheader->page_number);
        while ( !new_page_to_write.error && new_page_to_write.value->pgheader->next_page_number != 0) {
            new_page_to_write = get_page_by_number( dt->table->db, dt->table, new_page_to_write.value->pgheader->next_page_number);
            if (new_page_to_write.error) return new_page_to_write.error;
            if (new_page_to_write.value->pgheader->data_offset > dt->size) {
                last_suitable_pg_num = new_page_to_write.value->pgheader->page_number;
            }
        }
        new_page_to_write = get_page_by_number( dt->table->db, dt->table, last_suitable_pg_num);
        if (new_page_to_write.error) return new_page_to_write.error;
        dt->table->first_page_to_write = new_page_to_write.value;
    }

    dt->table->first_page_to_write->pgheader->data_offset -= dt->size;
    void *data_ptr = dt->bytes;
    void *table_ptr = dt->table->first_page_to_write->data
                            + dt->table->first_page_to_write->pgheader->data_offset;

    if (data_ptr != table_ptr) {
        memcpy(data_ptr, table_ptr, dt->size);
        sync_storage_by_table(dt, data_ptr - pg->data, pg->pgheader->page_number);
    }

    return OK;
}

void update_string_data_for_row(data *dst, page *dst_pg, data *src) {
    delete_saved_row(dst, dst_pg);
    set_data(src);
}

void prepare_string_data_for_saving(page *pg) {
    string_in_storage *cur_string_data = (string_in_storage *)(pg->data);
    maybe_page pg_with_string_ref = get_page_by_number(pg->tb->db, pg->tb, cur_string_data->link_to_current.string_page_number);
    if (pg_with_string_ref.error) return;

    size_t bytes_moved = 0;
    while(cur_string_data != NULL) {
        size_t bytes_moved_now = 0;
        if (!cur_string_data->enabled) {
            bytes_moved_now = delete_saved_string(pg, (char *)cur_string_data - (char *)pg->data);
            bytes_moved += bytes_moved_now;
            pg->pgheader->data_offset -= bytes_moved_now;
            continue;
        }
        if (cur_string_data->link_to_current.string_page_number != pg_with_string_ref.value->pgheader->page_number) {
            pg_with_string_ref = get_page_by_number(pg->tb->db, pg->tb, cur_string_data->link_to_current.string_page_number);
            if (pg_with_string_ref.error) return;
        }

        string_data_ref *cur_string_ref = (string_data_ref *) ((char *)pg_with_string_ref.value->data + cur_string_data->link_to_current.offset);

        cur_string_ref->string_page_number = pg->pgheader->page_number;
        cur_string_ref->offset -= bytes_moved;
        
        if (cur_string_data->next_part.string_page_number != 0) {
            if (cur_string_data->next_part.string_page_number != pg_with_string_ref.value->pgheader->page_number) {
                pg_with_string_ref = get_page_by_number(pg->tb->db, pg->tb, cur_string_data->next_part.string_page_number);
                if (pg_with_string_ref.error) return;
            }

            string_data_ref *next_string_ref = (string_data_ref *) ((char *)pg_with_string_ref.value->data + cur_string_data->next_part.offset);

            next_string_ref->string_page_number = pg->pgheader->page_number;
            next_string_ref->offset -= bytes_moved;
        }

        cur_string_data = get_next_string_data_on_page(cur_string_data, pg);
    }
}

void prepare_table_data_for_saving(page *pg) {
    maybe_data dt = init_empty_data(pg->tb);
    if (dt.error) return;
    dt.value->bytes = pg->data;
    size_t offset_to_row = 0;
    while(has_next_data_on_page(pg, dt.value->bytes)) {
        for (size_t column_index = 0; column_index < pg->tb->header->column_amount; column_index++) {
            column_header header = pg->tb->header->columns[column_index];
            if (header.type != STRING) continue;

            size_t string_offset = offset_to_column(pg->tb->header, header.name, STRING);
            string_data_ref *string_ref = (string_data_ref *)((char *)dt.value->bytes + string_offset);

            maybe_page page_with_string = get_page_by_number(pg->tb->db, pg->tb, string_ref->string_page_number);
            if (page_with_string.error) return ;
            string_in_storage *string_data = (string_in_storage *)(page_with_string.value->data + string_ref->offset);
            string_data->link_to_current.string_page_number = pg->pgheader->page_number;
        }
        offset_to_row += dt.value->table->header->row_size;
        dt.value->bytes += dt.value->table->header->row_size;
    }
}

void prepare_data_for_saving(page *pg) {
    switch (pg->pgheader->type) {
    case STRING_PAGE:
        prepare_string_data_for_saving(pg);
        break;
    case TABLE_DATA:
        prepare_table_data_for_saving(pg);
        break;
    default:
        break;
    }
}

void get_integer_from_data(data *dt, int32_t *dest, size_t offset) {
    *dest = *((int32_t *) ((char *)dt->bytes + offset));
}

void get_string_from_data(data *dt, char **dest, size_t data_offset) {
    string_data_ref *string_ref_ptr = (string_data_ref *)((char *)dt->bytes + data_offset);
    maybe_page page_with_string = get_page_by_number(dt->table->db, dt->table, string_ref_ptr->string_page_number);
    if (page_with_string.error) return;

    if (!page_with_string.value->data) {
        result rd_res = read_page_data(dt->table->db, page_with_string.value);
        if (rd_res) return;
    }

    string_in_storage *target_string = (string_in_storage *)((char *)page_with_string.value->data + string_ref_ptr->offset);

    string_in_storage *initial_string = target_string;

    size_t string_len_total = target_string->str_len;
    while (target_string->next_part.string_page_number != 0) {
        page_with_string = get_page_by_number(dt->table->db, dt->table, target_string->next_part.string_page_number);
        if (page_with_string.error) return;
        target_string = (string_in_storage *)((char *)page_with_string.value->data + target_string->next_part.offset);

        string_len_total += target_string->str_len;
    }

    char *saved_string = (char *)malloc(string_len_total);
    string_len_total = initial_string->str_len;
    if (saved_string != initial_string->string) memcpy(saved_string, initial_string->string, initial_string->str_len);

    while (initial_string->next_part.string_page_number != 0) {
        page_with_string = get_page_by_number(dt->table->db, dt->table, initial_string->next_part.string_page_number);
        if (page_with_string.error) return;
        initial_string = (string_in_storage *)((char *)page_with_string.value->data + initial_string->next_part.offset);

        memcpy(saved_string + string_len_total, initial_string->string, initial_string->str_len);
        string_len_total += initial_string->str_len;
    }


    *dest = saved_string;
}

void get_bool_from_data(data *dt, bool *dest, size_t offset) {
    *dest = *((const bool *) dt->bytes + offset);
}

void get_float_from_data(data *dt, float *dest, size_t offset) {
    *dest = *((const float *) ((char *)dt->bytes + offset));
}

void get_any_from_data(data *dt, any_value *dest, size_t offset, column_type type) {
    switch (type) {
    case INT_32:
        get_integer_from_data(dt, &(dest->int_value), offset);
        break;
    case FLOAT:
        get_float_from_data(dt, &(dest->float_value), offset);
        break;
    case BOOL:
        get_bool_from_data(dt, &(dest->bool_value), offset);
        break;
    case STRING:
        get_string_from_data(dt, &(dest->string_value), offset);
        break;
    default:
        break;
    }
}

void print_data(data *dt) {
    size_t offset = 0;
    for (size_t column_index = 0; column_index < dt->table->header->column_amount; column_index++) {
        column_header header = dt->table->header->columns[column_index];
        
        any_value value;
        get_any_from_data(dt, &value, offset, header.type);
        switch (header.type) {
        case INT_32:
            printf("\t%d\t|", value.int_value);
            break;
        case FLOAT:
            printf("\t%f\t|", value.float_value);
            break;
        case BOOL:
            printf("\t%s\t|", value.bool_value ? "TRUE" : "FALSE");
            break;
        case STRING:
            printf("\t%s\t|", value.string_value);
            free(value.string_value);
            break;
        default:
            break;
        }
        offset += type_to_size(header.type);
    }
    printf("\n");
}
