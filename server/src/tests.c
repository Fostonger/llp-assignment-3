#include "tests.h"
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct {
    any_value   value;
    column_type type;
} any_typed_value;

typedef struct {
    any_typed_value *value;
    result *error;
} packed_any_value_with_error;

result test_table_creation(database *db) {
    maybe_table tb = create_table("test table", db);
    release_table(tb.value);
    return tb.error;
}

void make_image_from_csv(const char *path_to_csv) {
    char *init_str = "python3 writing_script.py -i ";
    char *str_end = " -o chart_images/";
    char *final_str = (char *)malloc(strlen(path_to_csv) + strlen(init_str) + strlen(str_end));
    strcpy(final_str, init_str);
    strcpy(final_str + strlen(init_str), path_to_csv);
    strcpy(final_str + strlen(init_str) + strlen(path_to_csv), str_end);

    int py_result = system(final_str);
    if (py_result) {
        printf("couldn't run script to write charts images. Do you have python and matplotlib installed?\n");
    }
    free(final_str);
}

maybe_table create_test_table(database *db, const char *name, column_header *headers, size_t headers_count) {
    maybe_table t1 = create_table(name, db);
    if (t1.error) goto end;

    for (size_t i = 0; i < headers_count; i++) {
        t1.error = add_column(t1.value, headers[i].name, headers[i].type);
        if (t1.error) goto end;
    }
end:
    return t1;
}

result fill_table_with_data(database *db, table *tb, size_t columns_count, size_t rows_count, any_typed_value values[rows_count][columns_count]) {
    result filling_error = OK;
    maybe_data dt = init_data(tb);
    if (dt.error) {
        filling_error = dt.error;
        goto release_dt;
    }
    for (size_t rows = 0; rows < rows_count; rows++) {
        for (size_t columns = 0; columns < columns_count; columns ++) {
            filling_error = data_init_any(dt.value, values[rows][columns].value, values[rows][columns].type);
            if (filling_error) goto release_dt;
        }
        filling_error = set_data(dt.value);
        if (filling_error) goto release_dt;
        clear_data(dt.value);
    }

release_dt:
    release_data(dt.value);
    return filling_error;
}

result fill_with_test_data(database *db, table *tb, int32_t int_start, int32_t int_end) {
    result fill_error = OK;
    for (int32_t it = int_start; it < int_end; it++) {
        any_typed_value data_values[1][4] = {
            {   (any_typed_value) { .type=INT_32, .value=(any_value) { .int_value=it } }, 
                (any_typed_value) { .type=BOOL, .value=(any_value) { .bool_value=false } },
                (any_typed_value) { .type=FLOAT, .value=(any_value) { .float_value=3.1415 } },
                (any_typed_value) { .type=STRING, .value=(any_value) {.string_value="string test"} } }
        
        };
        fill_error = fill_table_with_data(db, tb, 4, 1, data_values);
        if (fill_error) return fill_error;
    }
    return fill_error;
}

result test_adding_columns(database *db) {
    column_header headers[4] = {
        (column_header) { .type=INT_32, .name="ints"},
        (column_header) { .type=BOOL, .name="bools"},
        (column_header) { .type=FLOAT, .name="floats"},
        (column_header) { .type=STRING, .name="strings"}
    };

    maybe_table tb = create_test_table(db, "table 2", headers, 4);
    if (tb.error) goto release_tb;

    for (size_t i = 0; i < tb.value->header->column_amount; i++) {
        if (tb.value->header->columns[i].type != headers[i].type || strcmp(tb.value->header->columns[i].name, headers[i].name)) {
            tb.error = NOT_EQUAL;
            goto release_tb;
        }
    }

release_tb:
    release_table(tb.value);
    return tb.error;
}

bool values_iterator(any_value *value1, any_value *value2) {
    packed_any_value_with_error *real_value = (packed_any_value_with_error *)value1->string_value;
    switch (real_value->value->type) {
        case STRING:
            *(real_value->error) = strcmp(real_value->value->value.string_value, value2->string_value) ? NOT_EQUAL : OK;
            break;
        case INT_32:
            *(real_value->error) = real_value->value->value.int_value == value2->int_value ? OK : NOT_EQUAL;
            break;
        case FLOAT:
            *(real_value->error) = real_value->value->value.float_value == value2->float_value ? OK : NOT_EQUAL;
            break;
        case BOOL:
            *(real_value->error) = real_value->value->value.bool_value == value2->bool_value ? OK : NOT_EQUAL;
            break;
    }
    return true;
}

bool simple_ints_search_predicate(any_value *value1, any_value *value2) {
    return value1->int_value == value2->int_value;
}

bool any_typed_value_search(any_value *value1, any_value *value2) {
    any_typed_value *real_value = (any_typed_value *)value1->string_value;
    switch (real_value->type) {
        case STRING:
            return !strcmp(real_value->value.string_value, value2->string_value);
        case INT_32:
            return real_value->value.int_value == value2->int_value;
        case FLOAT:
            return real_value->value.float_value == value2->float_value;
        case BOOL:
            return real_value->value.bool_value == value2->bool_value;
    }
}

result test_adding_values(database *db) {
    column_header headers[4] = {
        (column_header) { .type=INT_32, .name="ints"},
        (column_header) { .type=BOOL, .name="bools"},
        (column_header) { .type=FLOAT, .name="floats"},
        (column_header) { .type=STRING, .name="strings"}
    };

    any_typed_value data_values[5][4] = {
        {   (any_typed_value) { .type=INT_32, .value=(any_value) { .int_value=10 } }, 
            (any_typed_value) { .type=BOOL, .value=(any_value) { .bool_value=false } },
            (any_typed_value) { .type=FLOAT, .value=(any_value) { .float_value=3.1415 } },
            (any_typed_value) { .type=STRING, .value=(any_value) {.string_value="string test"} } },

        {   (any_typed_value) { .type=INT_32, .value=(any_value) { .int_value=20 } },
            (any_typed_value) { .type=BOOL, .value=(any_value) { .bool_value=false } },
            (any_typed_value) { .type=FLOAT, .value=(any_value) { .float_value=2.01 } },
            (any_typed_value) { .type=STRING, .value=(any_value) {.string_value="testing tests" } } },

        {   (any_typed_value) { .type=INT_32, .value=(any_value) { .int_value=30 } },
            (any_typed_value) { .type=BOOL, .value=(any_value) { .bool_value=true } },
            (any_typed_value) { .type=FLOAT, .value=(any_value) { .float_value=13.115 } },
            (any_typed_value) { .type=STRING, .value=(any_value) {.string_value="don't know what to write" } } },

        {   (any_typed_value) { .type=INT_32, .value=(any_value) { .int_value=40 } },
            (any_typed_value) { .type=BOOL, .value=(any_value) { .bool_value=false } },
            (any_typed_value) { .type=FLOAT, .value=(any_value) { .float_value=1231.134 } },
            (any_typed_value) { .type=STRING, .value=(any_value) {.string_value="bababababa" } } },

        {   (any_typed_value) { .type=INT_32, .value=(any_value) { .int_value=50 } },
            (any_typed_value) { .type=BOOL, .value=(any_value) { .bool_value=true } },
            (any_typed_value) { .type=FLOAT, .value=(any_value) { .float_value=14.15141 } },
            (any_typed_value) { .type=STRING, .value=(any_value) {.string_value="last one" } } }
        
    };

    result test_error = OK;

    maybe_table tb = create_test_table(db, "table 3", headers, 4);
    if (tb.error) { 
        test_error = tb.error;
        goto release_tb;
    }

    test_error = fill_table_with_data(db, tb.value, 4, 5, data_values);

    packed_any_value_with_error val_with_err = (packed_any_value_with_error) { .error=&test_error };
    maybe_data_iterator iterator = init_iterator(tb.value);
    if (iterator.error) {
        test_error = iterator.error;
        goto release_iter;
    }
    for (size_t header_index = 0; header_index < 4; header_index++ ) {
        size_t row_index = 0;
        val_with_err.value = &(data_values[row_index++][header_index]);
        closure print_all = (closure) { .func=values_iterator, .value1=(any_value) { .string_value=(char*)&val_with_err }};

        while (seek_next_where(iterator.value, headers[header_index].type, headers[header_index].name, print_all)) {
            val_with_err.value = &(data_values[row_index++][header_index]);
            if (test_error) goto release_iter;
            get_next(iterator.value);
        }
        reset_iterator(iterator.value, tb.value);
    }

release_iter:
    release_iterator(iterator.value);
release_tb:
    release_table(tb.value);
    return test_error;
}

result test_adding_values_speed_with_writing_result(database *db) {
    column_header headers[4] = {
        (column_header) { .type=INT_32, .name="ints"},
        (column_header) { .type=BOOL, .name="bools"},
        (column_header) { .type=FLOAT, .name="floats"},
        (column_header) { .type=STRING, .name="strings"}
    };

    any_typed_value data_values[1][4] = {
        {   (any_typed_value) { .type=INT_32, .value=(any_value) { .int_value=10 } }, 
            (any_typed_value) { .type=BOOL, .value=(any_value) { .bool_value=false } },
            (any_typed_value) { .type=FLOAT, .value=(any_value) { .float_value=3.1415 } },
            (any_typed_value) { .type=STRING, .value=(any_value) {.string_value="string test"} } }
        
    };

    result test_error = OK;

    maybe_table tb = create_test_table(db, "table 4", headers, 4);
    if (tb.error) { 
        test_error = tb.error;
        goto release_tb;
    }

    char *charts_data_file = "charts_data/writing_values_statistics.csv";

    struct stat st = {0};

    if (stat("charts_datay", &st) == -1)
        mkdir("charts_data", 0777);

    FILE *timestamp_file = fopen(charts_data_file, "wb");

    if (timestamp_file != NULL) fprintf(timestamp_file, "values added count, Time taken\n");

    for (int it = 0; it < 1000; it++) {
        clock_t t = clock();
        test_error = fill_table_with_data(db, tb.value, 4, 1, data_values);
        if (test_error) {
            fclose(timestamp_file);
            goto release_tb;
        }
        t = clock() - t;
        double time_taken = (double)t;

        if (timestamp_file != NULL) fprintf(timestamp_file, "%d, %f\n", it, time_taken);
    }
    fclose(timestamp_file);
    make_image_from_csv(charts_data_file);

release_tb:
    release_table(tb.value);
    return test_error;
}

result test_getting_values_speed_with_writing_result(database *db) {
    column_header headers[4] = {
        (column_header) { .type=INT_32, .name="ints"},
        (column_header) { .type=BOOL, .name="bools"},
        (column_header) { .type=FLOAT, .name="floats"},
        (column_header) { .type=STRING, .name="strings"}
    };

    result test_error = OK;

    maybe_table tb = create_test_table(db, "table 5", headers, 4);
    if (tb.error) { 
        test_error = tb.error;
        goto release_tb;
    }


    test_error = fill_with_test_data(db, tb.value, 0, 5000);
    if (test_error) goto release_tb;

    char *charts_data_file = "charts_data/getting_values_statistics.csv";

    struct stat st = {0};

    if (stat("charts_datay", &st) == -1)
        mkdir("charts_data", 0777);

    FILE *timestamp_file = fopen(charts_data_file, "wb");

    if (timestamp_file != NULL) fprintf(timestamp_file, "value index, Time taken\n");

    maybe_data_iterator iterator = init_iterator(tb.value);
    if (iterator.error) {
        test_error = iterator.error;
        goto release_iter;
    }

    for (int it = 0; it < 5000; it++) {

        closure find_int = (closure) { .func=simple_ints_search_predicate, .value1=(any_value) { .int_value=it }};

        clock_t t = clock();
        bool found = seek_next_where(iterator.value, INT_32, "ints", find_int);
        if (!found) {
            test_error = NOT_FOUND;
            goto close_file;
        }
        t = clock() - t;
        double time_taken = (double)t;

        if (timestamp_file != NULL) fprintf(timestamp_file, "%d, %f\n", it, time_taken);

        reset_iterator(iterator.value, tb.value);
    }

close_file:
    fclose(timestamp_file);

    if (!test_error) make_image_from_csv(charts_data_file);

release_iter:
    release_iterator(iterator.value);
release_tb:
    release_table(tb.value);
    return test_error;
}

result test_deleting_value(database *db) {
    column_header headers[4] = {
        (column_header) { .type=INT_32, .name="ints"},
        (column_header) { .type=BOOL, .name="bools"},
        (column_header) { .type=FLOAT, .name="floats"},
        (column_header) { .type=STRING, .name="strings"}
    };

    result test_error = OK;

    maybe_table tb = create_test_table(db, "table 7", headers, 4);
    if (tb.error) { 
        test_error = tb.error;
        goto release_tb;
    }

    test_error = fill_with_test_data(db, tb.value, 0, 1000);
    if (test_error) goto release_tb;

    closure delete_int = (closure) { .func=simple_ints_search_predicate, .value1=(any_value) { .int_value=500 }};

    result_with_count delete_result = delete_where(tb.value, INT_32, "ints", delete_int);
    if (delete_result.error || delete_result.count != 1) {
        test_error = delete_result.error == OK ? JOB_WAS_NOT_DONE : delete_result.error;
        goto release_tb;
    }

    maybe_data_iterator iterator = init_iterator(tb.value);
    if (iterator.error) {
        test_error = iterator.error;
        goto release_iter;
    }

    for (int it = 0; it < 1000; it++) {
        if (it == 500) continue;

        closure find_int = (closure) { .func=simple_ints_search_predicate, .value1=(any_value) { .int_value=it }};

        bool found = seek_next_where(iterator.value, INT_32, "ints", find_int);
        if (!found) {
            test_error = NOT_FOUND;
            goto release_iter;
        }

        reset_iterator(iterator.value, tb.value);
    }

release_iter:
    release_iterator(iterator.value);
release_tb:
    release_table(tb.value);
    return test_error;
}

bool filter_int_less(any_value *value1, any_value *value2) {
    return value1->int_value > value2->int_value;
}

result test_deleting_values_speed_with_writing_result(database *db) {
    column_header headers[4] = {
        (column_header) { .type=INT_32, .name="ints"},
        (column_header) { .type=BOOL, .name="bools"},
        (column_header) { .type=FLOAT, .name="floats"},
        (column_header) { .type=STRING, .name="strings"}
    };

    result test_error = OK;

    maybe_table tb = create_test_table(db, "table 7", headers, 4);
    if (tb.error) { 
        test_error = tb.error;
        goto release_tb;
    }

    test_error = fill_with_test_data(db, tb.value, 0, 20100);
    if (test_error) goto release_tb;

    char *charts_data_file = "charts_data/deleting_values_statistics.csv";

    struct stat st = {0};

    if (stat("charts_datay", &st) == -1)
        mkdir("charts_data", 0777);

    FILE *timestamp_file = fopen(charts_data_file, "wb");
    size_t overall_deleted = 0;

    if (timestamp_file != NULL) fprintf(timestamp_file, "rows deleted, Time taken\n");

    for (size_t it = 0; it < 201; it++) {

        clock_t t = clock();

        closure delete_int = (closure) { .func=filter_int_less, .value1=(any_value) { .int_value= overall_deleted + it }};

        result_with_count delete_result = delete_where(tb.value, INT_32, "ints", delete_int);
        if (delete_result.error || delete_result.count != it) {
            test_error = delete_result.error == OK ? JOB_WAS_NOT_DONE : delete_result.error;
            goto close_file;
        }

        t = clock() - t;
        double time_taken = (double)t;
        if (timestamp_file != NULL) fprintf(timestamp_file, "%zu, %f\n", it, time_taken);

        test_error = fill_with_test_data(db, tb.value, overall_deleted + 999000, overall_deleted + it + 999000);
        if (test_error) goto close_file;

        overall_deleted += it;
    }

close_file:
    fclose(timestamp_file);

    if (!test_error) make_image_from_csv(charts_data_file);

release_tb:
    release_table(tb.value);
    return test_error;
}

result test_updating_values_speed_with_writing_result(database *db) {
    column_header headers[4] = {
        (column_header) { .type=INT_32, .name="ints"},
        (column_header) { .type=BOOL, .name="bools"},
        (column_header) { .type=FLOAT, .name="floats"},
        (column_header) { .type=STRING, .name="strings"}
    };

    result test_error = OK;

    maybe_table tb = create_test_table(db, "table updating", headers, 4);
    if (tb.error) { 
        test_error = tb.error;
        goto release_tb;
    }

    test_error = fill_with_test_data(db, tb.value, 0, 100);
    if (test_error) goto release_tb;

    char *charts_data_file = "charts_data/updating_values_statistics.csv";

    struct stat st = {0};

    if (stat("charts_datay", &st) == -1)
        mkdir("charts_data", 0777);

    FILE *timestamp_file = fopen(charts_data_file, "wb");

    if (timestamp_file != NULL) fprintf(timestamp_file, "rows updated, Time taken\n");

    maybe_data update_dt = init_data(tb.value);
    if (update_dt.error) {
        test_error = update_dt.error;
        goto release_dt;
    }

    bool updated_val[10000];
    size_t ceil = 100;

    for (size_t it = 0; it < 198; it++) {
        any_typed_value data_values[4] = {
                (any_typed_value) { .type=INT_32, .value=(any_value) { .int_value=ceil } }, 
                (any_typed_value) { .type=BOOL, .value=(any_value) { .bool_value=false } },
                (any_typed_value) { .type=FLOAT, .value=(any_value) { .float_value=3.1415 } },
                (any_typed_value) { .type=STRING, .value=(any_value) {.string_value="string test"} } 
        };

        for (size_t columns = 0; columns < 4; columns++) {
            test_error = data_init_any(update_dt.value, data_values[columns].value, data_values[columns].type);
            if (test_error) goto release_dt;
        }

        int32_t rand_val = 0;

        while ( updated_val[( rand_val = rand() % ceil )] );
        updated_val[rand_val] = true;

        clock_t t = clock();

        closure update_int = (closure) { .func=simple_ints_search_predicate, .value1=(any_value) { .int_value=rand_val }};

        result_with_count update_result = update_where(tb.value, INT_32, "ints", update_int, update_dt.value);
        if (update_result.error || update_result.count != 1) {
            test_error = update_result.error == OK ? JOB_WAS_NOT_DONE : update_result.error;
            goto release_dt;
        }

        t = clock() - t;
        double time_taken = (double)t;
        if (timestamp_file != NULL) fprintf(timestamp_file, "%zu, %f\n", ceil, time_taken);

        ceil += 50;
        test_error = fill_with_test_data(db, tb.value, ceil - 49, ceil);
        if (test_error) goto release_dt;

        clear_data(update_dt.value);
    }
release_dt:
    release_data(update_dt.value);
close_file:
    fclose(timestamp_file);

    if (!test_error) make_image_from_csv(charts_data_file);

release_tb:
    release_table(tb.value);
    return test_error;
}

result test_updating_value(database *db) {
    column_header headers[4] = {
        (column_header) { .type=INT_32, .name="ints"},
        (column_header) { .type=BOOL, .name="bools"},
        (column_header) { .type=FLOAT, .name="floats"},
        (column_header) { .type=STRING, .name="strings"}
    };

    result test_error = OK;

    maybe_table tb = create_test_table(db, "table 7", headers, 4);
    if (tb.error) { 
        test_error = tb.error;
        goto release_tb;
    }

    test_error = fill_with_test_data(db, tb.value, 0, 1000);
    if (test_error) goto release_tb;

    closure update_int = (closure) { .func=simple_ints_search_predicate, .value1=(any_value) { .int_value=500 }};

    maybe_data updated_dt = init_data(tb.value);
    if (updated_dt.error) {
        test_error = updated_dt.error;
        goto release_dt;
    }

    any_typed_value data_values[4] = {
            (any_typed_value) { .type=INT_32, .value=(any_value) { .int_value=99999 } }, 
            (any_typed_value) { .type=BOOL, .value=(any_value) { .bool_value=false } },
            (any_typed_value) { .type=FLOAT, .value=(any_value) { .float_value=3.1415 } },
            (any_typed_value) { .type=STRING, .value=(any_value) {.string_value="string test"} } 
    };

    for (size_t columns = 0; columns < 4; columns ++) {
        test_error = data_init_any(updated_dt.value, data_values[columns].value, data_values[columns].type);
        if (test_error) goto release_dt;
    }

    result_with_count update_result = update_where(tb.value, INT_32, "ints", update_int, updated_dt.value);
    if (update_result.error || update_result.count != 1) {
        test_error = update_result.error == OK ? JOB_WAS_NOT_DONE : update_result.error;
        goto release_tb;
    }

    maybe_data_iterator iterator = init_iterator(tb.value);
    if (iterator.error) {
        test_error = iterator.error;
        goto release_iter;
    }

    for (int it = 0; it < 1000; it++) {
        if (it == 500) continue;

        closure find_int = (closure) { .func=simple_ints_search_predicate, .value1=(any_value) { .int_value=it }};

        bool found = seek_next_where(iterator.value, INT_32, "ints", find_int);
        if (!found) {
            test_error = NOT_FOUND;
            goto release_iter;
        }

        reset_iterator(iterator.value, tb.value);
    }

    closure find_int = (closure) { .func=simple_ints_search_predicate, .value1=(any_value) { .int_value=99999 }};
    bool found = seek_next_where(iterator.value, INT_32, "ints", find_int);
    if (!found) {
        test_error = NOT_FOUND;
        goto release_iter;
    }

release_iter:
    release_iterator(iterator.value);
release_dt:
    release_data(updated_dt.value);
release_tb:
    release_table(tb.value);
    return test_error;
}

result test_tables_merging(database *db) {
    result test_error = OK;
    column_header headers_1[4] = {
        (column_header) { .type=INT_32, .name="ints"},
        (column_header) { .type=BOOL, .name="bools"},
        (column_header) { .type=FLOAT, .name="floats"},
        (column_header) { .type=STRING, .name="strings"}
    };
    column_header headers_2[4] = {
        (column_header) { .type=INT_32, .name="ints"},
        (column_header) { .type=BOOL, .name="bools on second"},
        (column_header) { .type=FLOAT, .name="floats on second"},
        (column_header) { .type=STRING, .name="strings on second"}
    };

    column_header headers_joined[7] = {
        (column_header) { .type=INT_32, .name="ints"},
        (column_header) { .type=BOOL, .name="bools"},
        (column_header) { .type=FLOAT, .name="floats"},
        (column_header) { .type=STRING, .name="strings"},
        (column_header) { .type=BOOL, .name="bools on second"},
        (column_header) { .type=FLOAT, .name="floats on second"},
        (column_header) { .type=STRING, .name="strings on second"}
    };

    maybe_table tb1 = create_test_table(db, "table 8", headers_1, 4);
    if (tb1.error) { 
        test_error = tb1.error;
        goto release_tb_1;
    }
    maybe_table tb2 = create_test_table(db, "table 9", headers_2, 4);
    if (tb2.error) { 
        test_error = tb2.error;
        goto release_tb_2;
    }

    test_error = fill_with_test_data(db, tb1.value, 0, 10);
    if (test_error) goto release_tb_2;

    test_error = fill_with_test_data(db, tb2.value, 0, 10);
    if (test_error) goto release_tb_2;

    maybe_table joined_tb = join_table(tb1.value, tb2.value, "ints", INT_32, "merged table");
    if ( (test_error = joined_tb.error) ) goto release_tb_joined;

    for ( size_t it = 0; it < 7; it++) {
        if (joined_tb.value->header->columns[it].type != headers_joined[it].type || strcmp(joined_tb.value->header->columns[it].name, headers_joined[it].name) ) {
            test_error = JOB_WAS_NOT_DONE;
            goto release_tb_joined;
        }
    }

    maybe_data_iterator iterator = init_iterator(joined_tb.value);
    if (iterator.error) {
        test_error = iterator.error;
        goto release_iter;
    }

    for (int it = 0; it < 10; it++) {
        any_typed_value data_values[1][7] = {
            {   (any_typed_value) { .type=INT_32, .value=(any_value) { .int_value=it } }, 
                (any_typed_value) { .type=BOOL, .value=(any_value) { .bool_value=false } },
                (any_typed_value) { .type=FLOAT, .value=(any_value) { .float_value=3.1415 } },
                (any_typed_value) { .type=STRING, .value=(any_value) {.string_value="string test"} }, 
                (any_typed_value) { .type=BOOL, .value=(any_value) { .bool_value=false } },
                (any_typed_value) { .type=FLOAT, .value=(any_value) { .float_value=3.1415 } },
                (any_typed_value) { .type=STRING, .value=(any_value) {.string_value="string test"} }
            }
        
        };
        for (size_t column = 0; column < 7; column++) {
            closure find_predic = (closure) { .func=any_typed_value_search, .value1=(any_value) { .string_value=(char *)&(data_values[0][column]) }};

            bool found = seek_next_where(iterator.value, headers_joined[column].type, headers_joined[column].name, find_predic);
            if (!found) {
                test_error = NOT_FOUND;
                goto release_iter;
            }

            reset_iterator(iterator.value, joined_tb.value);
        }
    }

release_iter:
    release_iterator(iterator.value);
release_tb_joined:
    release_table(joined_tb.value);
release_tb_2:
    release_table(tb2.value);
release_tb_1:
    release_table(tb1.value);
    return test_error;
}

bool filter_table_greater(any_value *value1, any_value *value2) {
    return value1->int_value < value2->int_value;
}

result test_table_filtering(database *db) {
    result test_error = OK;
    column_header headers_1[4] = {
        (column_header) { .type=INT_32, .name="ints"},
        (column_header) { .type=BOOL, .name="bools"},
        (column_header) { .type=FLOAT, .name="floats"},
        (column_header) { .type=STRING, .name="strings"}
    };

    maybe_table tb = create_test_table(db, "table 10", headers_1, 4);
    if (tb.error) { 
        test_error = tb.error;
        goto release_tb;
    }

    test_error = fill_with_test_data(db, tb.value, 0, 10);
    if (test_error) goto release_tb;

    closure filter_preic = (closure) { .func=filter_table_greater, .value1=(any_value) { .int_value=5 }};
    maybe_table filtered_tb = filter_table(tb.value, INT_32, "ints", filter_preic);
    if ( (test_error = filtered_tb.error) ) goto release_tb_joined;

    maybe_data_iterator iterator = init_iterator(filtered_tb.value);
    if (iterator.error) {
        test_error = iterator.error;
        goto release_iter;
    }

    for (int it = 0; it < 10; it++) {
        closure find_predic = (closure) { .func=simple_ints_search_predicate, .value1=(any_value) { .int_value=it }};

        bool found = seek_next_where(iterator.value, INT_32, "ints", find_predic);
        if (found != ( it > 5 ) ) {
            test_error = JOB_WAS_NOT_DONE;
            goto release_iter;
        }

        reset_iterator(iterator.value, filtered_tb.value);
    }

release_iter:
    release_iterator(iterator.value);
release_tb_joined:
    release_table(filtered_tb.value);
release_tb:
    release_table(tb.value);
    return test_error;
}

result test_table_saving(database *db) {
    column_header headers[4] = {
        (column_header) { .type=INT_32, .name="ints"},
        (column_header) { .type=BOOL, .name="bools"},
        (column_header) { .type=FLOAT, .name="floats"},
        (column_header) { .type=STRING, .name="strings"}
    };

    result test_error = OK;

    maybe_table tb = create_test_table(db, "table to save", headers, 4);
    if (tb.error) { 
        test_error = tb.error;
        goto release_tb;
    }
    test_error = fill_with_test_data(db, tb.value, 0, 500);
    if (test_error) goto release_tb;

    test_error = save_table(db, tb.value);

release_tb:
    release_table(tb.value);
    return test_error;
}

result test_table_reading(database *db) {
    result test_error = OK;

    maybe_table tb = read_table("table to save", db);
    if ( (test_error = tb.error) ) goto release_tb;

    maybe_data_iterator iterator = init_iterator(tb.value);
    if (iterator.error) {
        test_error = iterator.error;
        goto release_iter;
    }

    for (int it = 0; it < 500; it++) {
        closure find_predic = (closure) { .func=simple_ints_search_predicate, .value1=(any_value) { .int_value=it }};

        bool found = seek_next_where(iterator.value, INT_32, "ints", find_predic);
        if (!found) {
            test_error = NOT_FOUND;
            goto release_iter;
        }

        reset_iterator(iterator.value, tb.value);
    }

release_iter:
    release_iterator(iterator.value);
release_tb:
    release_table(tb.value);
    return test_error;
}

result test_multipaged_strings_adding(database *db) {
    column_header headers[2] = {
        (column_header) { .type=INT_32, .name="number"},
        (column_header) { .type=STRING, .name="book content"}
    };

    result test_error = OK;

    maybe_table tb = create_test_table(db, "table multiple", headers, 2);
    if (tb.error) { 
        test_error = tb.error;
        goto release_tb;
    }

    char *lord_of_the_rings_str = "When Mr. Bilbo Baggins of Bag End announced that he would shortly be celebrating his eleventy-first birthday with a party of special magnificence, there was much talk and excitement in Hobbiton.\n   Bilbo was very rich and very peculiar, and had been the wonder of the Shire for sixty years, ever since his remarkable disappearance and unexpected return. The riches he had brought back from his travels had now become a local legend, and it was popularly believed, whatever the old folk might say, that the Hill at Bag End was full of tunnels stuffed with treasure. And if that was not enough for fame, there was also his prolonged vigour to marvel at. Time wore on, but it seemed to have little effect on Mr. Baggins. At ninety he was much the same as at fifty. At ninety-nine they began to call him well-preserved; but unchanged would have been nearer the mark. There were some that shook their heads and thought this was too much of a good thing; it seemed unfair that anyone should possess (apparently) perpetual youth as well as (reputedly) inexhaustible wealth.\n   'It will have to be paid for,' they said. 'It isn't natural, and trouble will come of it!'\n   But so far trouble had not come; and as Mr. Baggins was generous with his money, most people were willing to forgive him his oddities and his good fortune. He remained on visiting terms with his relatives (except, of course, the Sackville-Bagginses), and he had many devoted admirers among the hobbits of poor and unimportant families. But he had no close friends, until some of his younger cousins began to grow up.\n   The eldest of these, and Bilbo's favourite, was young Frodo Baggins. When Bilbo was ninety-nine he adopted Frodo as his heir, and brought him to live at Bag End; and the hopes of the Sackville-Bagginses were finally dashed. Bilbo and Frodo happened to have the same birthday, September 22nd. 'You had better come and live here, Frodo my lad,' said Bilbo one day; 'and then we can celebrate our birthday-parties comfortably together.' At that time Frodo was still in his tweens, as the hobbits called the irresponsible twenties between childhood and coming of age at thirty-three.\n   Twelve more years passed. Each year the Bagginses had given very lively combined birthday-parties at Bag End; but now it was understood that something quite exceptional was being planned for that autumn. Bilbo was going to be eleventy-one, 111, a rather curious number, and a very respectable age for a hobbit (the Old Took himself had only reached 130); and Frodo was going to be thirty-three, 33, an important number: the date of his 'coming of age'.\n   Tongues began to wag in Hobbiton and Bywater; and rumour of the coming event travelled all over the Shire. The history and character of Mr. Bilbo Baggins became once again the chief topic of conversation; and the older folk suddenly found their reminiscences in welcome demand.\n   No one had a more attentive audience than old Ham Gamgee, commonly known as the Gaffer. He held forth at The Ivy Bush, a small inn on the Bywater road; and he spoke with some authority, for he had tended the garden at Bag End for forty years, and had helped old Holman in the same job before that. Now that he was himself growing old and stiff in the joints, the job was mainly carried on by his youngest son, Sam Gamgee. Both father and son were on very friendly terms with Bilbo and Frodo. They lived on the Hill itself, in Number 3 Bagshot Row just below Bag End.\n   'A very nice well-spoken gentlehobbit is Mr. Bilbo, as I've always said,' the Gaffer declared. With perfect truth: for Bilbo was very polite to him, calling him 'Master Hamfast', and consulting him constantly upon the growing of vegetables — in the matter of 'roots', especially potatoes, the Gaffer was recognized as the leading authority by all in the neighbourhood (including himself).\n   'But what about this Frodo that lives with him?' asked Old Noakes of Bywater. 'Baggins is his name, but he's more than half a Brandybuck, they say. It beats me why any Baggins of Hobbiton should go looking for a wife away there in Buckland, where folks are so queer.'\n   'And no wonder they're queer,' put in Daddy Twofoot (the Gaffer's next-door neighbour), 'if they live on the wrong side of the Brandywine River, and right agin the Old Forest. That's a dark bad place, if half the tales be true.'\n   'You're right, Dad!' said the Gaffer. 'Not that the Brandybucks of Buckland live in the Old Forest; but they're a queer breed, seemingly. They fool about with boats on that big river — and that isn't natural. Small wonder that trouble came of it, I say. But be that as it may, Mr. Frodo is as nice a young hobbit as you could wish to meet. Very much like Mr. Bilbo, and in more than looks. After all his father was a Baggins. A decent respectable hobbit was Mr. Drogo Baggins; there was never much to tell of him, till he was drownded.'\n   'Drownded?' said several voices. They had heard this and other darker rumours before, of course; but hobbits have a passion for family history, and they were ready to hear it again.\n   'Well, so they say,' said the Gaffer. 'You see: Mr. Drogo, he married poor Miss Primula Brandybuck. She was our Mr. Bilbo's first cousin on the mother's side (her mother being the youngest of the Old Took's daughters); and Mr. Drogo was his second cousin. So Mr. Frodo is his first and second cousin, once removed either way, as the saying is, if you follow me. And Mr. Drogo was staying at Brandy Hall with his father-in-law, old Master Gorbadoc, as he often did after his marriage (him being partial to his vittles, and old Gorbadoc keeping a mighty generous table); and he went out boating on the Brandywine River; and he and his wife were drownded, and poor Mr. Frodo only a child and all.'\n   'I've heard they went on the water after dinner in the moonlight,' said Old Noakes; 'and it was Drogo's weight as sunk the boat.'\n   'And I heard she pushed him in, and he pulled her in after him,' said Sandyman, the Hobbiton miller.\n   'You shouldn't listen to all you hear, Sandyman,' said the Gaffer, who did not much like the miller. 'There isn't no call to go talking of pushing and pulling. Boats are quite tricky enough for those that sit still without looking further for the cause of trouble. Anyway: there was this Mr. Frodo left an orphan and stranded, as you might say, among those queer Bucklanders, being brought up anyhow in Brandy Hall. A regular warren, by all accounts. Old Master Gorbadoc never had fewer than a couple of hundred relations in the place. Mr. Bilbo never did a kinder deed than when he brought the lad back to live among decent folk.\n   'But I reckon it was a nasty knock for those Sackville-Bagginses. They thought they were going to get Bag End, that time when he went off and was thought to be dead. And then he comes back and orders them off; and he goes on living and living, and never looking a day older, bless him! And suddenly he produces an heir, and has all the papers made out proper. The Sackville-Bagginses won't never see the inside of Bag End now, or it is to be hoped not.'\n   'There's a tidy bit of money tucked away up there, I hear tell,' said a stranger, a visitor on business from Michel Delving in the Westfarthing. 'All the top of your hill is full of tunnels packed with chests of gold and silver, and jools, by what I've heard.'\n   'Then you've heard more than I can speak to,' answered the Gaffer. 'I know nothing about jools. Mr. Bilbo is free with his money, and there seems no lack of it; but I know of no tunnel-making. I saw Mr. Bilbo when he came back, a matter of sixty years ago, when I was a lad. I'd not long come prentice to old Holman (him being my dad's cousin), but he had me up at Bag End helping him to keep folks from trampling and trapessing all over the garden while the sale was on. And in the middle of it all Mr. Bilbo comes up the Hill with a pony and some mighty big bags and a couple of chests. I don't doubt they were mostly full of treasure he had picked up in foreign parts, where there be mountains of gold, they say; but there wasn't enough to fill tunnels. But my lad Sam";

    for (int it = 0; it < 500; it++) {
        any_typed_value data_values[1][2] = {
            {   (any_typed_value) { .type=INT_32, .value=(any_value) { .int_value=it } },
                (any_typed_value) { .type=STRING, .value=(any_value) { .string_value=lord_of_the_rings_str } } }
        };

        test_error = fill_table_with_data(db, tb.value, 2, 1, data_values);
        if (test_error) {
            goto release_tb;
        }
    }

    maybe_data_iterator iterator = init_iterator(tb.value);
    if (iterator.error) {
        test_error = iterator.error;
        goto release_iter;
    }

    char *read_value = (char *)malloc(strlen(lord_of_the_rings_str));

    for (int it = 0; it < 500; it++) {
        closure find_predic = (closure) { .func=simple_ints_search_predicate, .value1=(any_value) { .int_value=it }};

        bool found = seek_next_where(iterator.value, INT_32, "number", find_predic);
        if (!found) {
            test_error = NOT_FOUND;
            goto release_iter;
        }
        size_t str_offset = offset_to_column(tb.value->header, headers[1].name, headers[1].type);

        get_string_from_data(iterator.value->cur_data, &read_value, str_offset);
        if (strcmp(read_value, lord_of_the_rings_str)) {
            test_error = NOT_EQUAL;
            goto release_iter;
        }

        reset_iterator(iterator.value, tb.value);
    }

release_iter:
    release_iterator(iterator.value);
release_tb:
    release_table(tb.value);
    return test_error;
}

result test_multipaged_strings_saving(database *db) {
    column_header headers[2] = {
        (column_header) { .type=INT_32, .name="number"},
        (column_header) { .type=STRING, .name="book content"}
    };

    result test_error = OK;

    maybe_table tb = create_test_table(db, "table multiple strings", headers, 2);
    if (tb.error) { 
        test_error = tb.error;
        goto release_tb;
    }

    char *lord_of_the_rings_str = "When Mr. Bilbo Baggins of Bag End announced that he would shortly be celebrating his eleventy-first birthday with a party of special magnificence, there was much talk and excitement in Hobbiton.\n   Bilbo was very rich and very peculiar, and had been the wonder of the Shire for sixty years, ever since his remarkable disappearance and unexpected return. The riches he had brought back from his travels had now become a local legend, and it was popularly believed, whatever the old folk might say, that the Hill at Bag End was full of tunnels stuffed with treasure. And if that was not enough for fame, there was also his prolonged vigour to marvel at. Time wore on, but it seemed to have little effect on Mr. Baggins. At ninety he was much the same as at fifty. At ninety-nine they began to call him well-preserved; but unchanged would have been nearer the mark. There were some that shook their heads and thought this was too much of a good thing; it seemed unfair that anyone should possess (apparently) perpetual youth as well as (reputedly) inexhaustible wealth.\n   'It will have to be paid for,' they said. 'It isn't natural, and trouble will come of it!'\n   But so far trouble had not come; and as Mr. Baggins was generous with his money, most people were willing to forgive him his oddities and his good fortune. He remained on visiting terms with his relatives (except, of course, the Sackville-Bagginses), and he had many devoted admirers among the hobbits of poor and unimportant families. But he had no close friends, until some of his younger cousins began to grow up.\n   The eldest of these, and Bilbo's favourite, was young Frodo Baggins. When Bilbo was ninety-nine he adopted Frodo as his heir, and brought him to live at Bag End; and the hopes of the Sackville-Bagginses were finally dashed. Bilbo and Frodo happened to have the same birthday, September 22nd. 'You had better come and live here, Frodo my lad,' said Bilbo one day; 'and then we can celebrate our birthday-parties comfortably together.' At that time Frodo was still in his tweens, as the hobbits called the irresponsible twenties between childhood and coming of age at thirty-three.\n   Twelve more years passed. Each year the Bagginses had given very lively combined birthday-parties at Bag End; but now it was understood that something quite exceptional was being planned for that autumn. Bilbo was going to be eleventy-one, 111, a rather curious number, and a very respectable age for a hobbit (the Old Took himself had only reached 130); and Frodo was going to be thirty-three, 33, an important number: the date of his 'coming of age'.\n   Tongues began to wag in Hobbiton and Bywater; and rumour of the coming event travelled all over the Shire. The history and character of Mr. Bilbo Baggins became once again the chief topic of conversation; and the older folk suddenly found their reminiscences in welcome demand.\n   No one had a more attentive audience than old Ham Gamgee, commonly known as the Gaffer. He held forth at The Ivy Bush, a small inn on the Bywater road; and he spoke with some authority, for he had tended the garden at Bag End for forty years, and had helped old Holman in the same job before that. Now that he was himself growing old and stiff in the joints, the job was mainly carried on by his youngest son, Sam Gamgee. Both father and son were on very friendly terms with Bilbo and Frodo. They lived on the Hill itself, in Number 3 Bagshot Row just below Bag End.\n   'A very nice well-spoken gentlehobbit is Mr. Bilbo, as I've always said,' the Gaffer declared. With perfect truth: for Bilbo was very polite to him, calling him 'Master Hamfast', and consulting him constantly upon the growing of vegetables — in the matter of 'roots', especially potatoes, the Gaffer was recognized as the leading authority by all in the neighbourhood (including himself).\n   'But what about this Frodo that lives with him?' asked Old Noakes of Bywater. 'Baggins is his name, but he's more than half a Brandybuck, they say. It beats me why any Baggins of Hobbiton should go looking for a wife away there in Buckland, where folks are so queer.'\n   'And no wonder they're queer,' put in Daddy Twofoot (the Gaffer's next-door neighbour), 'if they live on the wrong side of the Brandywine River, and right agin the Old Forest. That's a dark bad place, if half the tales be true.'\n   'You're right, Dad!' said the Gaffer. 'Not that the Brandybucks of Buckland live in the Old Forest; but they're a queer breed, seemingly. They fool about with boats on that big river — and that isn't natural. Small wonder that trouble came of it, I say. But be that as it may, Mr. Frodo is as nice a young hobbit as you could wish to meet. Very much like Mr. Bilbo, and in more than looks. After all his father was a Baggins. A decent respectable hobbit was Mr. Drogo Baggins; there was never much to tell of him, till he was drownded.'\n   'Drownded?' said several voices. They had heard this and other darker rumours before, of course; but hobbits have a passion for family history, and they were ready to hear it again.\n   'Well, so they say,' said the Gaffer. 'You see: Mr. Drogo, he married poor Miss Primula Brandybuck. She was our Mr. Bilbo's first cousin on the mother's side (her mother being the youngest of the Old Took's daughters); and Mr. Drogo was his second cousin. So Mr. Frodo is his first and second cousin, once removed either way, as the saying is, if you follow me. And Mr. Drogo was staying at Brandy Hall with his father-in-law, old Master Gorbadoc, as he often did after his marriage (him being partial to his vittles, and old Gorbadoc keeping a mighty generous table); and he went out boating on the Brandywine River; and he and his wife were drownded, and poor Mr. Frodo only a child and all.'\n   'I've heard they went on the water after dinner in the moonlight,' said Old Noakes; 'and it was Drogo's weight as sunk the boat.'\n   'And I heard she pushed him in, and he pulled her in after him,' said Sandyman, the Hobbiton miller.\n   'You shouldn't listen to all you hear, Sandyman,' said the Gaffer, who did not much like the miller. 'There isn't no call to go talking of pushing and pulling. Boats are quite tricky enough for those that sit still without looking further for the cause of trouble. Anyway: there was this Mr. Frodo left an orphan and stranded, as you might say, among those queer Bucklanders, being brought up anyhow in Brandy Hall. A regular warren, by all accounts. Old Master Gorbadoc never had fewer than a couple of hundred relations in the place. Mr. Bilbo never did a kinder deed than when he brought the lad back to live among decent folk.\n   'But I reckon it was a nasty knock for those Sackville-Bagginses. They thought they were going to get Bag End, that time when he went off and was thought to be dead. And then he comes back and orders them off; and he goes on living and living, and never looking a day older, bless him! And suddenly he produces an heir, and has all the papers made out proper. The Sackville-Bagginses won't never see the inside of Bag End now, or it is to be hoped not.'\n   'There's a tidy bit of money tucked away up there, I hear tell,' said a stranger, a visitor on business from Michel Delving in the Westfarthing. 'All the top of your hill is full of tunnels packed with chests of gold and silver, and jools, by what I've heard.'\n   'Then you've heard more than I can speak to,' answered the Gaffer. 'I know nothing about jools. Mr. Bilbo is free with his money, and there seems no lack of it; but I know of no tunnel-making. I saw Mr. Bilbo when he came back, a matter of sixty years ago, when I was a lad. I'd not long come prentice to old Holman (him being my dad's cousin), but he had me up at Bag End helping him to keep folks from trampling and trapessing all over the garden while the sale was on. And in the middle of it all Mr. Bilbo comes up the Hill with a pony and some mighty big bags and a couple of chests. I don't doubt they were mostly full of treasure he had picked up in foreign parts, where there be mountains of gold, they say; but there wasn't enough to fill tunnels. But my lad Sam";

    for (int it = 0; it < 500; it++) {
        any_typed_value data_values[1][2] = {
            {   (any_typed_value) { .type=INT_32, .value=(any_value) { .int_value=it } },
                (any_typed_value) { .type=STRING, .value=(any_value) { .string_value=lord_of_the_rings_str } } }
        };

        test_error = fill_table_with_data(db, tb.value, 2, 1, data_values);
        if (test_error) {
            goto release_tb;
        }
    }
    test_error = save_table(db, tb.value);

release_tb:
    release_table(tb.value);
    return test_error;
}

result test_multipaged_strings_reading(database *db) {
    result test_error = OK;

    maybe_table tb = read_table("table multiple strings", db);
    if (tb.error) { 
        test_error = tb.error;
        goto release_tb;
    }

    char *lord_of_the_rings_str = "When Mr. Bilbo Baggins of Bag End announced that he would shortly be celebrating his eleventy-first birthday with a party of special magnificence, there was much talk and excitement in Hobbiton.\n   Bilbo was very rich and very peculiar, and had been the wonder of the Shire for sixty years, ever since his remarkable disappearance and unexpected return. The riches he had brought back from his travels had now become a local legend, and it was popularly believed, whatever the old folk might say, that the Hill at Bag End was full of tunnels stuffed with treasure. And if that was not enough for fame, there was also his prolonged vigour to marvel at. Time wore on, but it seemed to have little effect on Mr. Baggins. At ninety he was much the same as at fifty. At ninety-nine they began to call him well-preserved; but unchanged would have been nearer the mark. There were some that shook their heads and thought this was too much of a good thing; it seemed unfair that anyone should possess (apparently) perpetual youth as well as (reputedly) inexhaustible wealth.\n   'It will have to be paid for,' they said. 'It isn't natural, and trouble will come of it!'\n   But so far trouble had not come; and as Mr. Baggins was generous with his money, most people were willing to forgive him his oddities and his good fortune. He remained on visiting terms with his relatives (except, of course, the Sackville-Bagginses), and he had many devoted admirers among the hobbits of poor and unimportant families. But he had no close friends, until some of his younger cousins began to grow up.\n   The eldest of these, and Bilbo's favourite, was young Frodo Baggins. When Bilbo was ninety-nine he adopted Frodo as his heir, and brought him to live at Bag End; and the hopes of the Sackville-Bagginses were finally dashed. Bilbo and Frodo happened to have the same birthday, September 22nd. 'You had better come and live here, Frodo my lad,' said Bilbo one day; 'and then we can celebrate our birthday-parties comfortably together.' At that time Frodo was still in his tweens, as the hobbits called the irresponsible twenties between childhood and coming of age at thirty-three.\n   Twelve more years passed. Each year the Bagginses had given very lively combined birthday-parties at Bag End; but now it was understood that something quite exceptional was being planned for that autumn. Bilbo was going to be eleventy-one, 111, a rather curious number, and a very respectable age for a hobbit (the Old Took himself had only reached 130); and Frodo was going to be thirty-three, 33, an important number: the date of his 'coming of age'.\n   Tongues began to wag in Hobbiton and Bywater; and rumour of the coming event travelled all over the Shire. The history and character of Mr. Bilbo Baggins became once again the chief topic of conversation; and the older folk suddenly found their reminiscences in welcome demand.\n   No one had a more attentive audience than old Ham Gamgee, commonly known as the Gaffer. He held forth at The Ivy Bush, a small inn on the Bywater road; and he spoke with some authority, for he had tended the garden at Bag End for forty years, and had helped old Holman in the same job before that. Now that he was himself growing old and stiff in the joints, the job was mainly carried on by his youngest son, Sam Gamgee. Both father and son were on very friendly terms with Bilbo and Frodo. They lived on the Hill itself, in Number 3 Bagshot Row just below Bag End.\n   'A very nice well-spoken gentlehobbit is Mr. Bilbo, as I've always said,' the Gaffer declared. With perfect truth: for Bilbo was very polite to him, calling him 'Master Hamfast', and consulting him constantly upon the growing of vegetables — in the matter of 'roots', especially potatoes, the Gaffer was recognized as the leading authority by all in the neighbourhood (including himself).\n   'But what about this Frodo that lives with him?' asked Old Noakes of Bywater. 'Baggins is his name, but he's more than half a Brandybuck, they say. It beats me why any Baggins of Hobbiton should go looking for a wife away there in Buckland, where folks are so queer.'\n   'And no wonder they're queer,' put in Daddy Twofoot (the Gaffer's next-door neighbour), 'if they live on the wrong side of the Brandywine River, and right agin the Old Forest. That's a dark bad place, if half the tales be true.'\n   'You're right, Dad!' said the Gaffer. 'Not that the Brandybucks of Buckland live in the Old Forest; but they're a queer breed, seemingly. They fool about with boats on that big river — and that isn't natural. Small wonder that trouble came of it, I say. But be that as it may, Mr. Frodo is as nice a young hobbit as you could wish to meet. Very much like Mr. Bilbo, and in more than looks. After all his father was a Baggins. A decent respectable hobbit was Mr. Drogo Baggins; there was never much to tell of him, till he was drownded.'\n   'Drownded?' said several voices. They had heard this and other darker rumours before, of course; but hobbits have a passion for family history, and they were ready to hear it again.\n   'Well, so they say,' said the Gaffer. 'You see: Mr. Drogo, he married poor Miss Primula Brandybuck. She was our Mr. Bilbo's first cousin on the mother's side (her mother being the youngest of the Old Took's daughters); and Mr. Drogo was his second cousin. So Mr. Frodo is his first and second cousin, once removed either way, as the saying is, if you follow me. And Mr. Drogo was staying at Brandy Hall with his father-in-law, old Master Gorbadoc, as he often did after his marriage (him being partial to his vittles, and old Gorbadoc keeping a mighty generous table); and he went out boating on the Brandywine River; and he and his wife were drownded, and poor Mr. Frodo only a child and all.'\n   'I've heard they went on the water after dinner in the moonlight,' said Old Noakes; 'and it was Drogo's weight as sunk the boat.'\n   'And I heard she pushed him in, and he pulled her in after him,' said Sandyman, the Hobbiton miller.\n   'You shouldn't listen to all you hear, Sandyman,' said the Gaffer, who did not much like the miller. 'There isn't no call to go talking of pushing and pulling. Boats are quite tricky enough for those that sit still without looking further for the cause of trouble. Anyway: there was this Mr. Frodo left an orphan and stranded, as you might say, among those queer Bucklanders, being brought up anyhow in Brandy Hall. A regular warren, by all accounts. Old Master Gorbadoc never had fewer than a couple of hundred relations in the place. Mr. Bilbo never did a kinder deed than when he brought the lad back to live among decent folk.\n   'But I reckon it was a nasty knock for those Sackville-Bagginses. They thought they were going to get Bag End, that time when he went off and was thought to be dead. And then he comes back and orders them off; and he goes on living and living, and never looking a day older, bless him! And suddenly he produces an heir, and has all the papers made out proper. The Sackville-Bagginses won't never see the inside of Bag End now, or it is to be hoped not.'\n   'There's a tidy bit of money tucked away up there, I hear tell,' said a stranger, a visitor on business from Michel Delving in the Westfarthing. 'All the top of your hill is full of tunnels packed with chests of gold and silver, and jools, by what I've heard.'\n   'Then you've heard more than I can speak to,' answered the Gaffer. 'I know nothing about jools. Mr. Bilbo is free with his money, and there seems no lack of it; but I know of no tunnel-making. I saw Mr. Bilbo when he came back, a matter of sixty years ago, when I was a lad. I'd not long come prentice to old Holman (him being my dad's cousin), but he had me up at Bag End helping him to keep folks from trampling and trapessing all over the garden while the sale was on. And in the middle of it all Mr. Bilbo comes up the Hill with a pony and some mighty big bags and a couple of chests. I don't doubt they were mostly full of treasure he had picked up in foreign parts, where there be mountains of gold, they say; but there wasn't enough to fill tunnels. But my lad Sam";

    maybe_data_iterator iterator = init_iterator(tb.value);
    if (iterator.error) {
        test_error = iterator.error;
        goto release_iter;
    }

    char *read_value = (char *)malloc(strlen(lord_of_the_rings_str));

    for (int it = 0; it < 500; it++) {
        closure find_predic = (closure) { .func=simple_ints_search_predicate, .value1=(any_value) { .int_value=it }};

        bool found = seek_next_where(iterator.value, INT_32, "number", find_predic);
        if (!found) {
            test_error = NOT_FOUND;
            goto release_iter;
        }
        size_t str_offset = offset_to_column(tb.value->header, "book content", STRING);

        get_string_from_data(iterator.value->cur_data, &read_value, str_offset);
        if (strcmp(read_value, lord_of_the_rings_str)) {
            test_error = NOT_EQUAL;
            goto release_iter;
        }

        reset_iterator(iterator.value, tb.value);
    }

release_iter:
    release_iterator(iterator.value);
release_tb:
    release_table(tb.value);
    return test_error;
}

size_t get_file_size(FILE *file) {
    fseek(file, 0L, SEEK_END);
    size_t sz = ftell(file);
    return sz;
}

result test_file_size_usage_with_writing_result(database *db) {
    column_header headers[4] = {
        (column_header) { .type=INT_32, .name="ints"},
        (column_header) { .type=BOOL, .name="bools"},
        (column_header) { .type=FLOAT, .name="floats"},
        (column_header) { .type=STRING, .name="strings"}
    };

    result test_error = OK;

    maybe_table tb = create_test_table(db, "table 3", headers, 4);
    if (tb.error) { 
        test_error = tb.error;
        goto release_tb;
    }

    char *charts_data_file = "charts_data/file_memory_statistics.csv";

    struct stat st = {0};

    if (stat("charts_datay", &st) == -1)
        mkdir("charts_data", 0777);

    FILE *timestamp_file = fopen(charts_data_file, "wb");

    if (timestamp_file != NULL) fprintf(timestamp_file, "values written, memory used\n");

    for (size_t it = 0; it < 10000; it++) {
        test_error = fill_with_test_data(db, tb.value, 0, 1);
        if (test_error) goto close_file;
        test_error = save_table(db, tb.value);
        if (test_error) goto close_file;

        if (timestamp_file != NULL) fprintf(timestamp_file, "%zu, %zu\n", it, get_file_size(db->file));
        // fclose(db->file);
        // char *filename = "database.fost.data";
        // FILE *dbfile = fopen(filename, "r+");
        // db->file = dbfile;
    }

close_file:
    fclose(timestamp_file);

    if (!test_error) make_image_from_csv(charts_data_file);

release_tb:
    release_table(tb.value);
    return test_error;
}
