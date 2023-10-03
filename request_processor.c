#include "request_processor.h"
#include "server/src/database_manager.h"
#include "server/src/data_iterator.h"
#include "client/data.h"
#include <glib-2.0/glib.h>
#include <glib-2.0/glib-object.h>
#include <stdlib.h>
#include <string.h>

#define CONVERT_ARRAY(Item, item) \
Item **convert_##item##_list(GPtrArray *array) { \
    Item **item = malloc(array->len * sizeof(Item*)); \
    for (size_t i = 0; i < array->len; i++) \
        item[i] = (Item*) array->pdata[i]; \
    return item; \
}

#define NEW_RESPONSE(status, message) \
g_object_new(TYPE_SERVER_RESPONSE__T, \
                "status", status, \
                "msg", message,   \
                NULL)

CONVERT_ARRAY(literal_T, literal)

CONVERT_ARRAY(columnref_T, column)

column_type translate_thrift_to_fost(gint32 type) {
    switch (type) {
        case 2:
            return STRING;
        case 1:
            return INT_32;
        case 4:
            return FLOAT;
        case 3:
            return BOOL;
        default:
            return NONE;
    }
}


void _handle_column(columndef_T* column_def, table* table) {
    add_column(table, column_def->column_name, translate_thrift_to_fost(column_def->type));
}

void handle_column(gpointer column_def, gpointer table) {
    // for implicit pointer casts
    _handle_column(column_def, table);
}

void handle_data_init(data *d, columnref_T *cr, literal_T * l) {
    switch (l->type) {
        case LITERAL_TYPE__T_LIT_FLOAT_T:
            data_init_float(d, l->value->flt);
            break;
        case LITERAL_TYPE__T_LIT_INTEGER_T:
            data_init_integer(d, l->value->num);
            break;
        case LITERAL_TYPE__T_LIT_STRING_T:
            data_init_string(d, l->value->str);
            break;
        case LITERAL_TYPE__T_LIT_BOOLEAN_T:
            data_init_boolean(d, l->value->boolean);
            break;
        default:
            break;
    }
}

void init_columns(table* tb, GPtrArray* defs) {
    g_ptr_array_foreach(defs, handle_column, tb);
}

bool get_literal_value(char* columnName, data_iterator* iter, types_T* val) {
    char buffer[256];
    char* bufferAddress = (char *) buffer;
    char *str = NULL;
    column_header *header = header_from_name(iter->tb->header, columnName);
    size_t off = offset_to_column(iter->tb->header, columnName, header->type);
    if (!header) return false;
    switch (header->type) {
        case INT_32:
            val->__isset_num = true;
            get_integer_from_data(iter->cur_data, &val->num, off);
            break;
        case STRING:
            get_string_from_data(iter->cur_data, &str, off);
            g_object_set(val, "str", strdup(str), NULL);
            free(str);
            break;
        case BOOL:
            val->__isset_boolean = true;
            get_bool_from_data(iter->cur_data, (bool *) &val->boolean, off);
            break;
        case FLOAT:
            val->__isset_flt = true;
            get_float_from_data(iter->cur_data, &val->flt, off);
            break;
        default:
            return false;
    }
    return true;
}

GPtrArray* init_literal_array(size_t length) {
    GPtrArray *arr = g_ptr_array_new();

    for (size_t i = 0; i < length; i++) {
        g_ptr_array_add(arr, g_object_new(TYPE_LITERAL__T, NULL));
    }

    return arr;
}


literal_type_T nativeTypeToThrift(column_type type) {
    switch (type) {
        case INT_32:
            return LITERAL_TYPE__T_LIT_INTEGER_T;
        case STRING:
            return LITERAL_TYPE__T_LIT_STRING_T;
        case BOOL:
            return LITERAL_TYPE__T_LIT_BOOLEAN_T;
        case FLOAT:
            return LITERAL_TYPE__T_LIT_FLOAT_T;
        default:
            return LITERAL_TYPE__T_LIT_NONE_T;
    }
}

table *process_join(const join_stmt_T *stmt, database *db, table *fst_tb) {
    if (!fst_tb || !stmt->join_on_table) return NULL;

    maybe_table tb = read_table(stmt->join_on_table, db);
    if (tb.error)
        return NULL;
    table *snd_tb = tb.value;

    column_type type = type_from_name(fst_tb->header, stmt->join_predicate->column->col_name);
    if (type == NONE) return NULL;

    char *new_name = (char *)malloc(strlen(fst_tb->header->name) + 5);
    if (!new_name) return NULL;
    strcpy(new_name, "join-");
    strcpy(new_name+5, fst_tb->header->name);

    maybe_table joined_tb = join_table_predicate(fst_tb, snd_tb, stmt->join_predicate, new_name);

    free(new_name);
    release_table(fst_tb);
    release_table(snd_tb);

    if (joined_tb.error) return NULL;

    return joined_tb.value;
}

server_response_T* process_select_statement(const select_stmt_T *stmt, database* db, const char * const name) {
    maybe_table tb = read_table(name, db);

    if (tb.error)
        return NEW_RESPONSE(STATUS_CODE__T_TABLE_NOT_FOUND, "ERROR: TABLE NOT FOUND\n");

    if (stmt->join_stmt) {
        table *joined_tb = process_join(stmt->join_stmt, db, tb.value);
        if (!joined_tb)
            return NEW_RESPONSE(STATUS_CODE__T_TABLE_NOT_FOUND, "ERROR: TABLE NOT FOUND\n");
        tb.value = joined_tb;
    }

    maybe_data_iterator iter = init_iterator(tb.value);
    if (iter.error) {
        release_table(tb.value);
        return NEW_RESPONSE(STATUS_CODE__T_TABLE_NOT_FOUND, "ERROR: COULD NOT CREATE ITERATOR\n");
    }

    // serialize rows
    GPtrArray *rows = g_ptr_array_new();

    while (seek_next_predicate(iter.value, stmt->predicate)) {
        GPtrArray *literals = init_literal_array(stmt->columns->len);
        for (size_t i = 0; i < stmt->columns->len; i++) {
            columnref_T* currentColumn = stmt->columns->pdata[i];
            literal_T* lit = literals->pdata[i];
            g_object_set(lit,
                         "type", nativeTypeToThrift(type_from_name(tb.value->header, currentColumn->col_name)),
                         NULL);
            printf("col value: %s\n", currentColumn->col_name);
            bool success = get_literal_value(currentColumn->col_name, iter.value, lit->value);
            if (!success) {
                return NEW_RESPONSE(STATUS_CODE__T_BAD_REQUEST, "ERROR: Request doesn't match schema\n");
            }
        }
        row_T* row = g_object_new(TYPE_ROW__T,
                                  "value", literals,
                                  NULL);
        g_ptr_array_add(rows, row);
        g_ptr_array_unref(literals);
        get_next(iter.value);
    }

    item_list_T *items = g_object_new(TYPE_ITEM_LIST__T,
                        "schema", stmt->columns,
                        "rows", rows,
                        NULL);

    server_response_T *response = NEW_RESPONSE(STATUS_CODE__T_OK, "SELECT: OK\n");
    g_object_set(response, "items", items, NULL);
    release_table(tb.value);

    return response;

}

server_response_T* process_insert_statement(const insert_stmt_T *stmt, database* db, const char * const name) {
    maybe_table tb = read_table(name, db);
    if (tb.error)
        return NEW_RESPONSE(STATUS_CODE__T_TABLE_NOT_FOUND, "ERROR: TABLE NOT FOUND\n");

    if (stmt->literals->len != stmt->columns->len)
        return NEW_RESPONSE(STATUS_CODE__T_BAD_REQUEST, "BAD REQUEST: column reference count != literal count\n");

    maybe_data d = init_data(tb.value);
    if (d.error)
        return NEW_RESPONSE(STATUS_CODE__T_TABLE_NOT_FOUND, "ERROR: COULD NOT INIT DATA\n");
    literal_T **literals = convert_literal_list(stmt->literals);
    columnref_T **columns = convert_column_list(stmt->columns);
    for (size_t i = 0; i < stmt->columns->len; i++) {
        handle_data_init(d.value, columns[i], literals[i]);
    }

    result set_error = set_data(d.value);

    free(literals);
    free(columns);
    save_table(db, tb.value);
    release_data(d.value);
    release_table(tb.value);
    if (set_error)
        return NEW_RESPONSE(STATUS_CODE__T_INTERNAL_ERROR, "INTERNAL SERVER ERROR\n");

    return NEW_RESPONSE(STATUS_CODE__T_OK, "INSERT: OK\n");

}

server_response_T* process_create_statement(const create_stmt_T *stmt, database* db, const char * const name) {
    maybe_table tb = create_table(name, db);
    if (tb.error)
        return NEW_RESPONSE(STATUS_CODE__T_TABLE_NOT_FOUND, "ERROR: COULD NOT INITIALIZE TABLE\n");
    init_columns(tb.value, stmt->defs);
    maybe_page fst_pg = create_page(db, tb.value, TABLE_DATA);
    if (fst_pg.error) {
        free(tb.value);
        return NEW_RESPONSE(STATUS_CODE__T_TABLE_NOT_FOUND, "ERROR: COULD NOT INITIALIZE TABLE\n");
    }
    tb.value->first_page = fst_pg.value;
    tb.value->first_page_to_write = fst_pg.value;
    tb.value->header->first_data_page_num = fst_pg.value->pgheader->page_number;

    save_table(db, tb.value);
    release_table(tb.value);
    return g_object_new(TYPE_SERVER_RESPONSE__T,
                        "status", STATUS_CODE__T_OK,
                        "msg", "CREATE TABLE: OK\n");
}

server_response_T* process_update_statement(const update_stmt_T *stmt, database* db, const char * const name) {
    maybe_table tb = read_table(name, db);
    if (tb.error)
        return NEW_RESPONSE(STATUS_CODE__T_TABLE_NOT_FOUND, "ERROR: TABLE NOT FOUND\n");

    result_with_count rowsUpdated = update_where_predicate(db, tb.value, stmt->predicate, stmt->set_value_list);
    char message[64];
    if (rowsUpdated.error) 
        return NEW_RESPONSE(STATUS_CODE__T_TABLE_NOT_FOUND, "ERROR: ERROR WHILE UPDATING VALUE\n");
    sprintf(message, "DELETE: OK, ROWS UPDATED: %d\n", rowsUpdated.count);
    save_table(db, tb.value);
    release_table(tb.value);
    return NEW_RESPONSE(STATUS_CODE__T_OK, g_strdup(message));
}

server_response_T* process_delete_statement(const delete_stmt_T *stmt, database* db, const char * const name) {
    maybe_table tb = read_table(name, db);
    if (tb.error)
        return NEW_RESPONSE(STATUS_CODE__T_TABLE_NOT_FOUND, "ERROR: TABLE NOT FOUND\n");

    result_with_count rowsRemoved = delete_where_predicate(db, tb.value, stmt->predicate);

    char message[64];

    if (rowsRemoved.error)
        return NEW_RESPONSE(STATUS_CODE__T_TABLE_NOT_FOUND, "ERROR: ERROR WHILE DELETING VALUE\n");

    sprintf(message, "DELETE: OK, ROWS REMOVED: %d\n", rowsRemoved.count);
    save_table(db, tb.value);
    release_table(tb.value);
    return NEW_RESPONSE(STATUS_CODE__T_OK, g_strdup(message));
}

server_response_T* process_client_request(const statement_T *stmt, database* db) {
    switch (stmt->stmt_type) {
        case 274:
            return process_select_statement(stmt->stmt->select_stmt, db, stmt->table_name);
        case 275:
            return process_insert_statement(stmt->stmt->insert_stmt, db, stmt->table_name);
        case 276:
            return process_update_statement(stmt->stmt->update_stmt, db, stmt->table_name);
        case 278:
            return process_create_statement(stmt->stmt->create_stmt, db, stmt->table_name);
        case 277:
            return process_delete_statement(stmt->stmt->delete_stmt, db, stmt->table_name);
        default:
            return g_object_new(TYPE_SERVER_RESPONSE__T,
                                "status", STATUS_CODE__T_BAD_REQUEST,
                                "msg", "ERROR: BAD STATEMENT TYPE\n",
                                NULL);
    }
}




