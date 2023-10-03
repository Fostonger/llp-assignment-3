#include <stdlib.h>
#include <string.h>

#include "data_iterator.h"
#include "functional.h"
#include "data.h"
#include <glib-2.0/glib.h>
#include <glib-2.0/glib-object.h>

enum binop {
    AND = 1,
    OR = 2
};

enum comparison {
    GREATER = 1,
    LESS = 2,
    NEQUAL = 3,
    EQUAL = 4,
    GREATER_OR_EQUAL = 5,
    LESS_OR_EQUAL = 6
};

#define DECLARE_NUMBER_COMPARE_FUNCTION(Type, name) \
bool compare_nums_##name (enum comparison comp, const Type* arg1, const Type* arg2) { \
    switch (comp) {              \
        case GREATER:            \
            return *arg1 > *arg2; \
        case LESS:            \
            return *arg1 < *arg2; \
        case NEQUAL:            \
            return *arg1 != *arg2; \
        case EQUAL:            \
            return *arg1 == *arg2; \
        case GREATER_OR_EQUAL:            \
            return *arg1 >= *arg2; \
        case LESS_OR_EQUAL:            \
            return *arg1 <= *arg2;\
        default:                 \
            return false;\
    }                                               \
}

DECLARE_NUMBER_COMPARE_FUNCTION(int32_t, int)

DECLARE_NUMBER_COMPARE_FUNCTION(bool, bool)

DECLARE_NUMBER_COMPARE_FUNCTION(float, float)

void get_any(data_iterator *iter, any_value *dest, size_t offset, column_type type) {
    get_any_from_data(iter->cur_data, dest, offset, type);
}

bool has_next(data_iterator *iter) {
    page *cur_page = iter->cur_page;
    char *cur_data = iter->cur_data->bytes;

    return has_next_data_on_page(cur_page, cur_data) || cur_page->pgheader->next_page_number != 0;
}

bool compare_string(enum comparison comp, char** arg1, char** arg2) {
    int compresult = strcmp(*arg1, *arg2);
    switch (comp) {
        case GREATER:
            return compresult > 0;
        case LESS:
            return compresult < 0;
        case NEQUAL:
            return compresult != 0;
        case EQUAL:
            return compresult == 0;
        case GREATER_OR_EQUAL:
            return compresult >= 0;
        case LESS_OR_EQUAL:
            return compresult <= 0;
        default:
            return false;
    }
}

bool compare_num_types(column_type type, enum comparison comp, void* arg1, void* arg2) {
    switch (type) {
        case INT_32:
            return compare_nums_int(comp, arg1, arg2);
        case BOOL:
            return compare_nums_bool(comp, arg1, arg2);
        case FLOAT:
            return compare_nums_float(comp, arg1, arg2);
        default:
            return false;
    }
}

void* extract_value_address_from_literal_t(literal_T *lit) {
    switch (lit->type) {
        case LITERAL_TYPE__T_LIT_INTEGER_T:
            return &lit->value->num;
        case LITERAL_TYPE__T_LIT_STRING_T:
            return &lit->value->str;
        case LITERAL_TYPE__T_LIT_BOOLEAN_T:
            return &lit->value->boolean;
        case LITERAL_TYPE__T_LIT_FLOAT_T:
            return &lit->value->flt;
        default:
            return NULL;
    }
}

void get_next(data_iterator *iter) {
    iter->cur_data->bytes += iter->tb->header->row_size;
}

bool compare(data_iterator* iter, columnref_T* col, predicate_arg_T* arg, enum comparison cmp) {
    column_type col_type = type_from_name(iter->tb->header, col->col_name);
    if (col_type == NONE) return false;
    size_t offset = offset_to_column(iter->tb->header, col->col_name, col_type);

    any_value value1 = {0};
    any_value value2 = {0};

    get_any(iter, &value1, offset, col_type);
    
    switch (arg->type) {
        case PREDICATE_ARG_TYPE__T_LITERAL_T:
            return (col_type == STRING) ? compare_string(cmp, &(value1.string_value), &arg->arg->literal->value->str)
                        : compare_num_types(col_type, cmp, &value1, extract_value_address_from_literal_t(arg->arg->literal));
        case PREDICATE_ARG_TYPE__T_REFERENCE_T: {
            column_type col_type2 = type_from_name(iter->tb->header, arg->arg->ref->col_name);
            if (col_type2 == NONE) return false;
            size_t offset2 = offset_to_column(iter->tb->header, arg->arg->ref->col_name, col_type2);
            get_any(iter, &value2, offset2, col_type2);
            if (col_type2 != col_type) return false;
            return (col_type == STRING) ? compare_string(cmp, &(value1.string_value), &arg->arg->literal->value->str)
                        : compare_num_types(col_type, cmp, &value1, extract_value_address_from_literal_t(arg->arg->literal));
        }
        default:
            return false;
    }
    
}

bool seek_next_where(data_iterator *iter, column_type type, const char *column_name, closure clr) {
    if (!has_next(iter)) return false;

    // iter->cur_data->bytes = iter->cur_page->data;
    size_t offset = offset_to_column(iter->tb->header, column_name, type);
    
    while (has_next(iter)) {
        while (iter->cur_data->bytes - (char *)iter->cur_page->data >= iter->cur_page->pgheader->data_offset) {
            if (iter->cur_page->pgheader->next_page_number == 0) return false;
            
            maybe_page next_page = get_page_by_number(iter->tb->db, iter->tb, iter->cur_page->pgheader->next_page_number);
            iter->cur_page->next_page = next_page.value;
            iter->cur_page = next_page.value;
            iter->cur_data->bytes = iter->cur_page->data;
        }

        any_value value;
        get_any(iter, &value, offset, type);

        if ( clr.func(&clr.value1, &value) ) {
            if (type == STRING) free(value.string_value);
            return true;
        }

        get_next(iter);
    }
    return false;
}

bool is_predicate_true(data_iterator* iter, predicate_T* pred) {
    if (!pred->__isset_arg) return true;

    if (pred->type == PREDICATE_TYPE__T_COMPARISON_T) {
        return compare(iter, pred->column, pred->arg, pred->cmp_type);
    }

    switch (pred->predicate_op) {
        case AND:
            return
            is_predicate_true(iter, (predicate_T*) pred->left->pdata[0])
            && is_predicate_true(iter, (predicate_T*) pred->right->pdata[0]);
        case OR:
            return
            is_predicate_true(iter, (predicate_T*) pred->left->pdata[0])
            || is_predicate_true(iter, (predicate_T*) pred->right->pdata[0]);
    }

    // not-reachable (normally)
    return false;
}

bool seek_next_predicate(data_iterator* iter, predicate_T* pred) {
    if (!has_next(iter)) return false;

    if ( is_predicate_true(iter, pred) )
        return true;
    column_type type = type_from_name(iter->tb->header, pred->column->col_name);
    size_t offset = offset_to_column(iter->tb->header, pred->column->col_name, type);
    
    while (has_next(iter)) {
        while (iter->cur_data->bytes - (char *)iter->cur_page->data >= iter->cur_page->pgheader->data_offset) {
            if (iter->cur_page->pgheader->next_page_number == 0) return false;
            
            maybe_page next_page = get_page_by_number(iter->tb->db, iter->tb, iter->cur_page->pgheader->next_page_number);
            iter->cur_page->next_page = next_page.value;
            iter->cur_page = next_page.value;
            iter->cur_data->bytes = iter->cur_page->data;
        }

        any_value value;
        get_any(iter, &value, offset, type);

        if ( is_predicate_true(iter, pred) ) {
            if (type == STRING) free(value.string_value);
            return true;
        }

        get_next(iter);
    }
    return false;
}

result_with_count update_where(table* tb, column_type type, const char *column_name, closure clr, data *update_val) {
    maybe_data_iterator iterator = init_iterator(tb);
    if (iterator.error) return (result_with_count) { .error=iterator.error, .count=0 };
    int16_t update_count = 0;
    while (seek_next_where(iterator.value, type, column_name, clr)) {
        data *data_to_update = iterator.value->cur_data;
        update_string_data_for_row(data_to_update, iterator.value->cur_page, update_val);
        update_count++;
    }

    release_iterator(iterator.value);
    return (result_with_count) { .error=OK, .count=update_count };
}

any_value extractValueAddressFromLiteralT(literal_T *lit) {
    switch (lit->type) {
        case LITERAL_TYPE__T_LIT_INTEGER_T:
            return (any_value) { .int_value=lit->value->num};
        case LITERAL_TYPE__T_LIT_STRING_T:
            return (any_value) { .string_value=lit->value->str};
        case LITERAL_TYPE__T_LIT_BOOLEAN_T:
            return (any_value) { .bool_value=lit->value->boolean};
        case LITERAL_TYPE__T_LIT_FLOAT_T:
            return (any_value) { .float_value=lit->value->flt};
        case LITERAL_TYPE__T_LIT_NONE_T:
            return (any_value) { .string_value=NULL};
    }
}

result_with_count update_where_predicate(database* db, table* tb, predicate_T* pred, GPtrArray * sets) {
    maybe_data_iterator iterator = init_iterator(tb);
    if (iterator.error) return (result_with_count) { .error=iterator.error, .count=0 };
    int16_t update_count = 0;
    maybe_data dt = init_data(tb);
    if (dt.error) return (result_with_count) { .error=dt.error, .count=0 };

    while (seek_next_predicate(iterator.value, pred)) {
        dt.value->size = 0;
        if (copy_data(dt.value, iterator.value->cur_data))
            break;

        for (size_t i = 0; i < sets->len; i++) {
            set_value_T *upd = (set_value_T*) sets->pdata[i];
            column_type type = type_from_name(iterator.value->tb->header, upd->col->col_name);
            if (type == NONE) break;

            size_t off = offset_to_column(iterator.value->tb->header, upd->col->col_name, type);
            dt.value->size = off;
            any_value va = extractValueAddressFromLiteralT(upd->lit);

            if (data_init_any(dt.value, va, type))
                break;
        }
        data *data_to_update = iterator.value->cur_data;
        dt.value->size = data_to_update->size;
        update_string_data_for_row(data_to_update, iterator.value->cur_page, dt.value);
        update_count++;
        get_next(iterator.value);
    }
releases:
    release_data(dt.value);
    release_iterator(iterator.value);
    return (result_with_count) { .error=OK, .count=update_count };
}

void print_table(table *tb) {
    maybe_data_iterator iterator = init_iterator(tb);
    if (iterator.error) return;
    printf("\n");
    print_columns(iterator.value->tb->header);
    while (has_next(iterator.value)) {
        print_data(iterator.value->cur_data);
        get_next(iterator.value);
    }
    release_iterator(iterator.value);
    printf("\n");
}

result_with_count delete_where(table *tb, column_type type, const char *column_name, closure clr) {
    maybe_data_iterator iterator = init_iterator(tb);
    if (iterator.error) return (result_with_count) { .error=iterator.error, .count=0 };
    int16_t delete_count = 0;
    while (seek_next_where(iterator.value, type, column_name, clr)) {
        data *data_to_delete = iterator.value->cur_data;
        result delete_error = delete_saved_row(data_to_delete, iterator.value->cur_page);
        if (delete_error) {
            release_iterator(iterator.value);
            return (result_with_count) { .error=delete_error, .count=0 };
        }
        delete_count++;
    }

    release_iterator(iterator.value);
    return (result_with_count) { .error=OK, .count=delete_count };
}

result_with_count delete_where_predicate(database* db, table* tb, predicate_T* pred) {
    maybe_data_iterator iterator = init_iterator(tb);
    if (iterator.error) return (result_with_count) { .error=iterator.error, .count=0 };
    int16_t delete_count = 0;
    while (seek_next_predicate(iterator.value, pred)) {
        data *data_to_delete = iterator.value->cur_data;
        result delete_error = delete_saved_row(data_to_delete, iterator.value->cur_page);
        if (delete_error) {
            release_iterator(iterator.value);
            return (result_with_count) { .error=delete_error, .count=0 };
        }
        delete_count++;
        get_next(iterator.value);
    }

    release_iterator(iterator.value);
    return (result_with_count) { .error=OK, .count=delete_count };
}

maybe_data_iterator init_iterator(table* tb) {
    data_iterator* iter = malloc(sizeof (data_iterator));
    if (iter == NULL) return (maybe_data_iterator) { .error=MALLOC_ERROR, .value=NULL };
    iter->tb = tb;
    iter->cur_page = tb->first_page;
    maybe_data cur_data = init_empty_data(tb);
    if (cur_data.error) return (maybe_data_iterator) { .error=cur_data.error, .value=NULL};
    cur_data.value->bytes = iter->cur_page->data;
    cur_data.value->size = iter->tb->header->row_size;
    iter->cur_data = cur_data.value;
    iter->ptr = 0;
    iter->rows_read_on_page = 0;
    return (maybe_data_iterator) { .error=OK, .value=iter };
}

void reset_iterator(data_iterator *iter, table *tb) {
    iter->tb = tb;
    iter->cur_page = tb->first_page;
    iter->cur_data->bytes = iter->cur_page->data;
    iter->cur_data->size = iter->tb->header->row_size;
    iter->ptr = 0;
    iter->rows_read_on_page = 0;
}

void release_iterator(data_iterator *iter) {
    free(iter->cur_data);
    free(iter);
}

bool int_join_func(any_value *value1, any_value *value2) {
    return value1->int_value == value2->int_value;
}

bool float_join_func(any_value *value1, any_value *value2) {
    return value1->float_value == value2->float_value;
}

bool bool_join_func(any_value *value1, any_value *value2) {
    return value1->bool_value ^ value2->bool_value;
}

bool string_join_func(any_value *value1, any_value *value2) {
    return !strcmp(value1->string_value, value2->string_value);
}

closure join_func(any_value value, column_type type) {
    closure clr;
    clr.value1 = value;
    switch (type) {
    case INT_32:
        clr.func = int_join_func;
        return clr;
    case FLOAT:
        clr.func = float_join_func;
        return clr;
    case STRING:
        clr.func = string_join_func;
        return clr;
    case BOOL:
        clr.func = bool_join_func;
        return clr;
    default:
        return (closure) {0};
    }
}

maybe_table join_table(table *tb1, table *tb2, const char *column_name, column_type type, char *new_table_name) {
    if (tb1->db != tb2->db) return (maybe_table) { .error=DIFFERENT_DB, .value=NULL };
    maybe_table new_tb;
    maybe_data_iterator iterator1 = init_iterator(tb1);
    if (iterator1.error) {
        new_tb = (maybe_table) { .error=iterator1.error, .value=NULL };
        goto iterator1_release;
    }
    maybe_data_iterator iterator2 = init_iterator(tb2);
    if (iterator2.error) {
        new_tb = (maybe_table) { .error=iterator2.error, .value=NULL };
        goto iterator2_release;
    }

    new_tb = create_table(new_table_name, tb1->db);
    if (new_tb.error)
        goto iterator2_release;

    result join_columns_error = join_columns(new_tb.value, tb1, tb2, column_name, type);
    if (join_columns_error) {
        new_tb = (maybe_table) { .error=join_columns_error, .value=NULL };
        release_table(new_tb.value);
        goto joined_data_release;
    }

    size_t offset_to_column1 = offset_to_column(tb1->header, column_name, type);

    maybe_data joined_data = init_data(new_tb.value);
    if (joined_data.error) {
        new_tb = (maybe_table) { .error=joined_data.error, .value=new_tb.value };
        goto joined_data_release;
    }

    while (has_next(iterator1.value)) {
        any_value val1;
        get_any(iterator1.value, &val1, offset_to_column1, type);
        closure join_closure = join_func(val1, type);

        while (seek_next_where(iterator2.value, type, column_name, join_closure)) {
            result join_data_error = join_data(
                joined_data.value, iterator1.value->cur_data,
                iterator2.value->cur_data, column_name, type);
            if (join_data_error) {
                new_tb = (maybe_table) { .error=join_data_error, .value=new_tb.value };
                if (type == STRING) free(val1.string_value);
                goto joined_data_release;
            }
            result set_data_error = set_data(joined_data.value);
            if (join_data_error) {
                new_tb = (maybe_table) { .error=set_data_error, .value=new_tb.value };
                if (type == STRING) free(val1.string_value);
                goto joined_data_release;
            }
            clear_data(joined_data.value);
            get_next(iterator2.value);
        }
        if (type == STRING) free(val1.string_value);
        reset_iterator(iterator2.value, tb2);
        get_next(iterator1.value);
    }

joined_data_release:
    release_data(joined_data.value);
iterator2_release:
    release_iterator(iterator2.value);
iterator1_release:
    release_iterator(iterator1.value);
    
    return new_tb;
}

literal_T* serialize_literal(any_value value, column_type col_type) {
    literal_T *result = g_object_new(TYPE_LITERAL__T, NULL);
    types_T *type = g_object_new(TYPE_TYPES__T, NULL);
    switch (col_type) {
        case BOOL:
            g_object_set(type, "boolean", value.bool_value, NULL);
            g_object_set(result,
                         "type", LITERAL_TYPE__T_LIT_BOOLEAN_T,
                         "value", type,
                         NULL);
            break;
        case FLOAT:
            g_object_set(type, "flt", value.float_value, NULL);
            g_object_set(result,
                         "type", LITERAL_TYPE__T_LIT_FLOAT_T,
                         "value", type,
                         NULL);
            break;
        case INT_32:

            g_object_set(type, "num", value.int_value, NULL);
            g_object_set(result,
                         "type", LITERAL_TYPE__T_LIT_INTEGER_T,
                         "value", type,
                         NULL);
            break;
        case STRING:
            g_object_set(type, "str", value.string_value, NULL);
            g_object_set(result,
                         "type", LITERAL_TYPE__T_LIT_STRING_T,
                         "value", type,
                         NULL);
            break;
    }

    return result;
}

predicate_arg_T* set_join_arg(any_value value, column_type type) {
    predicate_arg_T* result = g_object_new(TYPE_PREDICATE_ARG__T,NULL);
    predicate_arg_union_T* arg_union = g_object_new(TYPE_PREDICATE_ARG_UNION__T, NULL);

    g_object_set(result,
                "type", PREDICATE_ARG_TYPE__T_LITERAL_T,
                NULL);
    g_object_set(arg_union,
                "literal", serialize_literal(value, type),
                NULL);

    g_object_set(result,
                 "arg", arg_union,
                 NULL);

    return result;
}

predicate_T *join_pred(any_value value, column_type type, predicate_T *pred) {
    predicate_T* result = g_object_new(TYPE_PREDICATE__T, NULL);
    g_object_set(result,
                 "cmp_type", pred->cmp_type,
                 "predicate_op", pred->predicate_op,
                 "type", pred->type,
                 NULL);
    if (pred->column) {
        g_object_set(result,
                     "column", pred->column,
                     NULL);
    }
    if (pred->type == PREDICATE_TYPE__T_COMPOUND_T) {
        g_ptr_array_add(result->left, pred->left);
        result->__isset_left = 1;
        g_ptr_array_add(result->right, pred->right);
        result->__isset_right = 1;
    } else {
        g_object_set(result,
                     "arg", set_join_arg(value, type),
                     NULL);
    }

    return result;
}

maybe_table join_table_predicate(table *tb1, table *tb2, predicate_T *pred, char *new_table_name) {
    if (tb1->db != tb2->db) return (maybe_table) { .error=DIFFERENT_DB, .value=NULL };
    maybe_table new_tb;
    maybe_data_iterator iterator1 = init_iterator(tb1);
    if (iterator1.error) {
        new_tb = (maybe_table) { .error=iterator1.error, .value=NULL };
        goto iterator1_release;
    }
    maybe_data_iterator iterator2 = init_iterator(tb2);
    if (iterator2.error) {
        new_tb = (maybe_table) { .error=iterator2.error, .value=NULL };
        goto iterator2_release;
    }

    new_tb = create_table(new_table_name, tb1->db);
    if (new_tb.error)
        goto iterator2_release;
    
    column_type type = type_from_name(tb1->header, pred->column->col_name);

    result join_columns_error = join_columns(new_tb.value, tb1, tb2, pred->column->col_name, type);
    if (join_columns_error) {
        new_tb = (maybe_table) { .error=join_columns_error, .value=NULL };
        release_table(new_tb.value);
        goto joined_data_release;
    }

    size_t offset_to_column1 = offset_to_column(tb1->header, pred->column->col_name, type);

    maybe_data joined_data = init_data(new_tb.value);
    if (joined_data.error) {
        new_tb = (maybe_table) { .error=joined_data.error, .value=new_tb.value };
        goto joined_data_release;
    }

    while (has_next(iterator1.value)) {
        any_value val1;
        get_any(iterator1.value, &val1, offset_to_column1, type);
        predicate_T *join_predic = join_pred(val1, type, pred);

        while (seek_next_predicate(iterator2.value, join_predic)) {
            result join_data_error = join_data(
                joined_data.value, iterator1.value->cur_data,
                iterator2.value->cur_data, pred->column->col_name, type);
            if (join_data_error) {
                new_tb = (maybe_table) { .error=join_data_error, .value=new_tb.value };
                if (type == STRING) free(val1.string_value);
                goto joined_data_release;
            }
            result set_data_error = set_data(joined_data.value);
            if (join_data_error) {
                new_tb = (maybe_table) { .error=set_data_error, .value=new_tb.value };
                if (type == STRING) free(val1.string_value);
                goto joined_data_release;
            }
            clear_data(joined_data.value);
            get_next(iterator2.value);
        }
        if (type == STRING) free(val1.string_value);
        reset_iterator(iterator2.value, tb2);
        get_next(iterator1.value);
    }

joined_data_release:
    release_data(joined_data.value);
iterator2_release:
    release_iterator(iterator2.value);
iterator1_release:
    release_iterator(iterator1.value);
    
    return new_tb;
}

maybe_table filter_table(table*tb, column_type type, const char *column_name, closure clr) {
    maybe_table new_tb;
    maybe_data_iterator iterator = init_iterator(tb);
    if (iterator.error) {
        new_tb = (maybe_table) { .error=iterator.error, .value=NULL };
        goto iterator_release;
    }

    new_tb = create_table("filtered table", tb->db);
    if (new_tb.error)
        goto iterator_release;

    copy_columns(new_tb.value, tb);

    maybe_data filtered_data = init_data(new_tb.value);
    if (filtered_data.error) {
        release_table(new_tb.value);
        new_tb = (maybe_table) { .error=filtered_data.error, .value=NULL };
        goto filtered_data_release;
    }

    while (seek_next_where(iterator.value, type, column_name, clr)) {
        result copy_error = copy_data(filtered_data.value, iterator.value->cur_data);
        if (copy_error) {
            release_table(new_tb.value);
            new_tb = (maybe_table) { .error=filtered_data.error, .value=NULL };
            break;
        }
        set_data(filtered_data.value);
        clear_data(filtered_data.value);
        get_next(iterator.value);
    }

filtered_data_release:
    release_data(filtered_data.value);
iterator_release:
    release_iterator(iterator.value);
    
    return new_tb;
}

maybe_table filter_table_predicate(table*tb, predicate_T* pred) {
    maybe_table new_tb;
    maybe_data_iterator iterator = init_iterator(tb);
    if (iterator.error) {
        new_tb = (maybe_table) { .error=iterator.error, .value=NULL };
        goto iterator_release;
    }

    new_tb = create_table("filtered table", tb->db);
    if (new_tb.error)
        goto iterator_release;
    
    maybe_page fst_pg = create_page(tb->db, new_tb.value, TABLE_DATA);
    if (fst_pg.error) {
        release_table(new_tb.value);
        new_tb = (maybe_table) { .error=fst_pg.error, .value=NULL };
        goto iterator_release;
    }
    new_tb.value->first_page = fst_pg.value;
    new_tb.value->first_page_to_write = fst_pg.value;
    new_tb.value->header->first_data_page_num = fst_pg.value->pgheader->page_number;

    copy_columns(new_tb.value, tb);

    maybe_data filtered_data = init_data(new_tb.value);
    if (filtered_data.error) {
        release_table(new_tb.value);
        new_tb = (maybe_table) { .error=filtered_data.error, .value=NULL };
        goto filtered_data_release;
    }

    while (seek_next_predicate(iterator.value, pred)) {
        result copy_error = copy_data(filtered_data.value, iterator.value->cur_data);
        if (copy_error) {
            release_table(new_tb.value);
            new_tb = (maybe_table) { .error=filtered_data.error, .value=NULL };
            break;
        }
        set_data(filtered_data.value);
        clear_data(filtered_data.value);
        get_next(iterator.value);
    }

filtered_data_release:
    release_data(filtered_data.value);
iterator_release:
    release_iterator(iterator.value);
    
    return new_tb;
}
