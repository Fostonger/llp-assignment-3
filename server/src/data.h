#pragma once

#include <stdint.h>

#include "database_manager.h"
#include "table.h"
#include "util.h"

typedef struct {
    table *table;
    char *bytes;
    uint16_t size;
} data;

OPTIONAL(data)

maybe_data init_data(table *tb);
maybe_data init_empty_data(table *tb);
void release_data(data *dt);
void clear_data(data *dt);
result data_init_integer(data *dt, int32_t val);
result data_init_string(data *dt, const char* val);
result data_init_boolean(data *dt, bool val);
result data_init_float(data *dt, float val);
result data_init_any(data *dt, const any_value val, column_type type);
result set_data(data *dt);
result delete_saved_row(data *dt, page *pg);
void get_integer_from_data(data *dt, int32_t *dest, size_t offset);
void get_string_from_data(data *dt, char **dest, size_t data_offset);
void get_bool_from_data(data *dt, bool *dest, size_t offset);
void get_float_from_data(data *dt, float *dest, size_t offset);
void get_any_from_data(data *dt, any_value *dest, size_t offset, column_type type);
void update_string_data_for_row(data *dst, page *dst_pg, data *src);
bool has_next_data_on_page(page *cur_page, char *cur_data);
void prepare_data_for_saving(page *pg);
void print_data(data *dt);
result copy_data(data *dst, data *src);
result join_data(data *dst, data *dt1, data *dt2, const char *column_name, column_type type);
