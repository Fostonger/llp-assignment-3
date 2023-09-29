
#include "t_client_serializer.h"
#include "../client/sql.tab.h"
select_stmt_T* t_serialize_select_stmt(select_stmt* select);
insert_stmt_T* t_serialize_insert_stmt(insert_stmt* insert);
update_stmt_T* t_serialize_update_stmt(update_stmt* update);
delete_stmt_T* t_serialize_delete_stmt(delete_stmt* delete);
create_stmt_T* t_serialize_create_stmt(create_stmt* create);
columndef_T* t_serialize_column_def(columndef* def);
columnref_T* t_serialize_column_ref(columnref *pColumnref);
literal_T* t_serialize_literal(literal *pLiteral);
predicate_T* t_serialize_predicate(predicate* pred);
set_value_T* t_serialize_set_value(set_value* sv);


statement_T* t_serialize_statement(statement* stmt) {
    statement_T *result = g_object_new(TYPE_STATEMENT__T, NULL);
    statement_union_T *stUnion = g_object_new(TYPE_STATEMENT_UNION__T, NULL);

    switch (stmt->stmt_type) {
        case SELECT:
            g_object_set(stUnion,
                         "select_stmt", t_serialize_select_stmt(stmt->stmt.select_stmt),
                         NULL);
            break;
        case INSERT:
            g_object_set(stUnion,
                         "insert_stmt", t_serialize_insert_stmt(stmt->stmt.insert_stmt),
                         NULL);
            break;
        case UPDATE:
            g_object_set(stUnion,
                         "update_stmt", t_serialize_update_stmt(stmt->stmt.update_stmt),
                         NULL);
            break;
        case DELETE:
            g_object_set(stUnion,
                         "delete_stmt", t_serialize_delete_stmt(stmt->stmt.delete_stmt),
                         NULL);
            break;
        case CREATE:
            g_object_set(stUnion,
                         "create_stmt", t_serialize_create_stmt(stmt->stmt.create_stmt),
                         NULL);
            break;
        default:
            // statement should be one of the types above
            return NULL;
    }

    g_object_set(
            result,
            "stmt", stUnion,
            "stmt_type", stmt->stmt_type,
            "table_name", stmt->table_name,
            NULL);

    return result;
}

create_stmt_T* t_serialize_create_stmt(create_stmt* create) {
    create_stmt_T *result = g_object_new(TYPE_CREATE_STMT__T, NULL);
    GPtrArray *defs = g_ptr_array_new();

    while(create->defs) {
        g_ptr_array_add(defs, t_serialize_column_def(create->defs));
        create->defs = create->defs->next;
    }

    g_object_set(result,
                 "defs", defs,
                 NULL);

    return result;
}

columndef_T* t_serialize_column_def(columndef* def) {
    columndef_T *result = g_object_new(TYPE_COLUMNDEF__T, NULL);

    g_object_set(result,
                 "column_name", def->column_name,
                 "type", def->type,
                 NULL);

    return result;
}


select_stmt_T* t_serialize_select_stmt(select_stmt* select) {
    select_stmt_T* result = g_object_new(TYPE_SELECT_STMT__T,NULL);
    if (select->predicate) {
        g_object_set(result,
                     "predicate", t_serialize_predicate(select->predicate),
                     NULL);
    }
    GPtrArray* columns = g_ptr_array_new();
    while (select->columns) {
        g_ptr_array_add(columns, t_serialize_column_ref(select->columns));
        select->columns = select->columns->next;
    }
    g_object_set(result,
                 "columns", columns,
                 NULL);
    return result;
}

predicate_type_T t_serialize_pred_type(predicate_type type) {
    switch (type) {
        case COMPARISON:
            return PREDICATE_TYPE__T_COMPARISON_T;
        case COMPOUND:
            return PREDICATE_TYPE__T_COMPOUND_T;
        case STR_MATCH:
            return PREDICATE_TYPE__T_STR_MATCH_T;
    }
}

predicate_arg_T* t_serialize_pred_arg(predicate_arg* arg) {
    predicate_arg_T* result = g_object_new(TYPE_PREDICATE_ARG__T,NULL);
    predicate_arg_union_T* arg_union = g_object_new(TYPE_PREDICATE_ARG_UNION__T, NULL);

    switch (arg->type) {
        case LITERAL:
            g_object_set(result,
                         "type", PREDICATE_ARG_TYPE__T_LITERAL_T,
                         NULL);
            g_object_set(arg_union,
                         "literal", t_serialize_literal(arg->arg.literal),
                         NULL);
            break;
        case REFERENCE:
            g_object_set(result,
                         "type", PREDICATE_ARG_TYPE__T_REFERENCE_T,
                         NULL);
            g_object_set(arg_union,
                         "ref", t_serialize_column_ref(arg->arg.ref),
                         NULL);
    }

    g_object_set(result,
                 "arg", arg_union,
                 NULL);

    return result;
}

predicate_T* t_serialize_predicate(predicate* pred) {
    if (pred == NULL) return NULL;
    predicate_T* result = g_object_new(TYPE_PREDICATE__T, NULL);
    g_object_set(result,
                 "cmp_type", pred->cmp_type,
                 "predicate_op", pred->predicate_op,
                 "type", t_serialize_pred_type(pred->type),
                 NULL);

    if (pred->column) {
        g_object_set(result,
                     "column", t_serialize_column_ref(pred->column),
                     NULL);
    }

    if (pred->type == COMPOUND) {
        g_ptr_array_add(result->left, t_serialize_predicate(pred->left));
        result->__isset_left = 1;
        g_ptr_array_add(result->right, t_serialize_predicate(pred->right));
        result->__isset_right = 1;
    } else {
        g_object_set(result,
                     "arg", t_serialize_pred_arg(&pred->arg),
                     NULL);
    }

    return result;
}


insert_stmt_T* t_serialize_insert_stmt(insert_stmt* insert) {
    insert_stmt_T *result = g_object_new(TYPE_INSERT_STMT__T, NULL);

    GPtrArray *cols = g_ptr_array_new();
    while(insert->columns) {
        g_ptr_array_add(cols, t_serialize_column_ref(insert->columns));
        insert->columns = insert->columns->next;
    }

    GPtrArray *literals = g_ptr_array_new();
    while (insert->literals) {
        g_ptr_array_add(literals, t_serialize_literal(insert->literals->value));
        insert->literals = insert->literals->next;
    }

    g_object_set(result,
                 "columns", cols,
                 "literals", literals,
                 NULL);

    return result;
}

literal_T* t_serialize_literal(literal *pLiteral) {
    literal_T *result = g_object_new(TYPE_LITERAL__T, NULL);
    types_T *type = g_object_new(TYPE_TYPES__T, NULL);
    switch (pLiteral->type) {
        case LIT_BOOLEAN:
            g_object_set(type, "boolean", pLiteral->value.boolean, NULL);
            g_object_set(result,
                         "type", LITERAL_TYPE__T_LIT_BOOLEAN_T,
                         "value", type,
                         NULL);
            break;
        case LIT_FLOAT:
            g_object_set(type, "flt", pLiteral->value.flt, NULL);
            g_object_set(result,
                         "type", LITERAL_TYPE__T_LIT_FLOAT_T,
                         "value", type,
                         NULL);
            break;
        case LIT_INTEGER:

            g_object_set(type, "num", pLiteral->value.num, NULL);
            g_object_set(result,
                         "type", LITERAL_TYPE__T_LIT_INTEGER_T,
                         "value", type,
                         NULL);
            break;
        case LIT_STRING:
            g_object_set(type, "str", pLiteral->value.string, NULL);
            g_object_set(result,
                         "type", LITERAL_TYPE__T_LIT_STRING_T,
                         "value", type,
                         NULL);
            break;
    }

    return result;
}

columnref_T* t_serialize_column_ref(columnref *pColumnref) {
    columnref_T *result = g_object_new(TYPE_COLUMNREF__T,
                        "col_name", pColumnref->col_name,
                        NULL);

    if (pColumnref->table_name) {
        g_object_set(result,
                     "table_name", pColumnref->table_name,
                     NULL);
    }

    return result;
}


update_stmt_T* t_serialize_update_stmt(update_stmt* update) {
    update_stmt_T *result = g_object_new(TYPE_UPDATE_STMT__T,
                                         "predicate", t_serialize_predicate(update->predicate),
                                         NULL);
    GPtrArray *values = g_ptr_array_new();

    while (update->set_value_list) {
        g_ptr_array_add(values, t_serialize_set_value(update->set_value_list->setval));
        update->set_value_list = update->set_value_list->next;
    }

    g_object_set(result,
                 "set_value_list", values,
                 NULL);

    return result;

}

set_value_T* t_serialize_set_value(set_value* sv) {
    return g_object_new(TYPE_SET_VALUE__T,
                        "col", t_serialize_column_ref(sv->col),
                        "lit", t_serialize_literal(sv->lit));
}

delete_stmt_T* t_serialize_delete_stmt(delete_stmt* delete) {
    return g_object_new(TYPE_DELETE_STMT__T,
                        "predicate", t_serialize_predicate(delete->predicate),
                        NULL);
}

