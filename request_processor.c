#include "request_processor.h"
#include "server/database.h"
#include "server/data_iterator.h"
#include <glib-2.0/glib.h>
#include <glib-2.0/glib-object.h>

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


void _handle_column(columndef_T* column_def, table* table) {
    init_fixedsize_column(table,table->header->columnInitAmount++,
                          column_def->column_name, column_def->type);
}

void handle_column(gpointer column_def, gpointer table) {
    // for implicit pointer casts
    _handle_column(column_def, table);
}

void handle_data_init(data *d, columnref_T *cr, literal_T * l) {
    switch (l->type) {
        case LITERAL_TYPE__T_LIT_FLOAT_T:
            data_init_float(d, l->value->flt, cr->col_name);
            break;
        case LITERAL_TYPE__T_LIT_INTEGER_T:
            data_init_integer(d, l->value->num, cr->col_name);
            break;
        case LITERAL_TYPE__T_LIT_STRING_T:
            data_init_string(d, l->value->str, cr->col_name);
            break;
        case LITERAL_TYPE__T_LIT_BOOLEAN_T:
            data_init_boolean(d, l->value->boolean, cr->col_name);
            break;
    }
}

void init_columns(table* tb, GPtrArray* defs) {
    g_ptr_array_foreach(defs, handle_column, tb);
}

bool get_literal_value(char* columnName, data_iterator* iter, types_T* val) {
    char buffer[256];
    char* bufferAddress = (char *) buffer;
    columnHeader *header = get_col_header_by_name(iter->tb, columnName);
    if (!header) return false;
    switch (header->type) {
        case INTEGER:
            val->__isset_num = true;
            getInteger(iter, header->columnName, &val->num);
            break;
        case STRING:
            getString(iter, header->columnName, &bufferAddress);
            g_object_set(val, "str", strdup(buffer), NULL);
            break;
        case BOOLEAN:
            val->__isset_boolean = true;
            getBool(iter, header->columnName, (bool *) &val->boolean);
            break;
        case FLOAT:
            val->__isset_flt = true;
            getFloat(iter, header->columnName, &val->flt);
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


literal_type_T nativeTypeToThrift(columnType type) {
    switch (type) {
        case INTEGER:
            return LITERAL_TYPE__T_LIT_INTEGER_T;
        case STRING:
            return LITERAL_TYPE__T_LIT_STRING_T;
        case BOOLEAN:
            return LITERAL_TYPE__T_LIT_BOOLEAN_T;
        case FLOAT:
            return LITERAL_TYPE__T_LIT_FLOAT_T;
    }
}

server_response_T* process_select_statement(const select_stmt_T *stmt, database* db, const char * const name) {
   table* tb = open_table(db, name);

    if (!tb)
        return NEW_RESPONSE(STATUS_CODE__T_TABLE_NOT_FOUND, "ERROR: TABLE NOT FOUND\n");

    data_iterator *iter = init_iterator(db, tb);

    // serialize rows
    GPtrArray *rows = g_ptr_array_new();

    while (seekNextPredicate(iter, stmt->predicate)) {
        GPtrArray *literals = init_literal_array(stmt->columns->len);
        for (size_t i = 0; i < stmt->columns->len; i++) {
            columnref_T* currentColumn = stmt->columns->pdata[i];
            literal_T* lit = literals->pdata[i];
            g_object_set(lit,
                         "type", nativeTypeToThrift(discoverColumnTypeByName(iter, currentColumn->col_name)),
                         NULL);
            bool success = get_literal_value(currentColumn->col_name, iter, lit->value);
            if (!success) {
                return NEW_RESPONSE(STATUS_CODE__T_BAD_REQUEST, "ERROR: Request doesn't match schema\n");
            }
        }
        row_T* row = g_object_new(TYPE_ROW__T,
                                  "value", literals,
                                  NULL);
        g_ptr_array_add(rows, row);
        g_ptr_array_unref(literals);
    }

    item_list_T *items = g_object_new(TYPE_ITEM_LIST__T,
                        "schema", stmt->columns,
                        "rows", rows,
                        NULL);

    server_response_T *response = NEW_RESPONSE(STATUS_CODE__T_OK, "SELECT: OK\n");
    g_object_set(response, "items", items, NULL);
    close_table(tb);

    return response;

}

server_response_T* process_insert_statement(const insert_stmt_T *stmt, database* db, const char * const name) {
    table* tb = open_table(db, name);
    if (!tb)
        return NEW_RESPONSE(STATUS_CODE__T_TABLE_NOT_FOUND, "ERROR: TABLE NOT FOUND\n");

    if (stmt->literals->len != stmt->columns->len)
        return NEW_RESPONSE(STATUS_CODE__T_BAD_REQUEST, "BAD REQUEST: column reference count != literal count\n");

    data *d = init_data(tb);
    literal_T **literals = convert_literal_list(stmt->literals);
    columnref_T **columns = convert_column_list(stmt->columns);
    for (size_t i = 0; i < stmt->columns->len; i++) {
        handle_data_init(d, columns[i], literals[i]);
    }

    int success = insert_data(d, db);

    free(literals);
    free(columns);
    if (!success)
        return NEW_RESPONSE(STATUS_CODE__T_INTERNAL_ERROR, "INTERNAL SERVER ERROR\n");

    return NEW_RESPONSE(STATUS_CODE__T_OK, "INSERT: OK\n");

}

server_response_T* process_create_statement(const create_stmt_T *stmt, database* db, const char * const name) {
    table* tb = new_table(name, stmt->defs->len);
    init_columns(tb, stmt->defs);
    table_apply(db, tb);
    close_table(tb);
    return g_object_new(TYPE_SERVER_RESPONSE__T,
                        "status", STATUS_CODE__T_OK,
                        "msg", "CREATE TABLE: OK\n");
}

server_response_T* process_update_statement(const update_stmt_T *stmt, database* db, const char * const name) {
    table* tb = open_table(db, name);
    if (!tb)
        return NEW_RESPONSE(STATUS_CODE__T_TABLE_NOT_FOUND, "ERROR: TABLE NOT FOUND\n");

    uint16_t rowsUpdated = updateWherePredicate(db, tb, stmt->predicate, stmt->set_value_list);
    char message[64];

    sprintf(message, "DELETE: OK, ROWS UPDATED: %d\n", rowsUpdated);
    close_table(tb);
    return NEW_RESPONSE(STATUS_CODE__T_OK, g_strdup(message));
}

server_response_T* process_delete_statement(const delete_stmt_T *stmt, database* db, const char * const name) {
    table* tb = open_table(db, name);
    if (!tb)
        return NEW_RESPONSE(STATUS_CODE__T_TABLE_NOT_FOUND, "ERROR: TABLE NOT FOUND\n");

    uint16_t rowsRemoved = deleteWherePredicate(db, tb, stmt->predicate);

    char message[64];

    sprintf(message, "DELETE: OK, ROWS REMOVED: %d\n", rowsRemoved);
    close_table(tb);
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




