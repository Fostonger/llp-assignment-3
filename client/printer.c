//
// Created by rosroble on 20.11.22.
//

#include "printer.h"
#include "sql.tab.h"
#include <stdio.h>
#include <string.h>

#define TAB_PRINTF(tabs, ...) do{ print_tabs(tabs), fprintf(stdout, __VA_ARGS__ ); } while(0)

void print_predicate(predicate* pred);
static int tabs = 0;

void print_tabs(int amount) {
    for (int i = 0; i < amount; ++i) {
        printf("\t");
    }
}

void cmp_type_to_str(int cmp_type, char str[3]) {
    const char* cmp_to_str[6] = {">", "<", "!=", "=", ">=", "<="};
    if (cmp_type >= 1 && cmp_type <= 6) {
        strcpy(str, cmp_to_str[cmp_type - 1]);
    } else {
        str[0] = '\0';
    }
}

void predicate_op_to_str(int predicate_op, char str[4]) {
    const char* predop_to_str[2] = {"AND", "OR"};
    if (predicate_op >= 1 && predicate_op <= 2) {
        strcpy(str, predop_to_str[predicate_op - 1]);
    } else {
        str[0] = '\0';
    }
}

void col_type_to_str(int col_type, char str[10]) {
    const char* type_to_str[10] = {"INTEGER", "VARCHAR", "BOOLEAN", "FLOAT"};
    if (col_type >= 1 && col_type <= 4) {
        strcpy(str, type_to_str[col_type - 1]);
    } else {
        str[0] = '\0';
    }
}

void print_literal(literal* lit) {
    TAB_PRINTF(0, "{\n");
    const char* literals_to_str[4] = {"STRING", "INTEGER", "FLOAT", "BOOLEAN"};
    TAB_PRINTF(++tabs, "type: %s,\n", literals_to_str[lit->type]);
    switch (lit->type) {
        case LIT_STRING:
            TAB_PRINTF(tabs, "value: %s\n", lit->value.string);
            break;
        case LIT_INTEGER:
            TAB_PRINTF(tabs, "value: %d\n", lit->value.num);
            break;
        case LIT_FLOAT:
            TAB_PRINTF(tabs, "value: %f\n", lit->value.flt);
            break;
        case LIT_BOOLEAN:
            TAB_PRINTF(tabs, "value: %s\n", lit->value.boolean ? "TRUE" : "FALSE");
        default:
            break;
    }
    TAB_PRINTF(--tabs, "}\n");
}


void print_predicate_arg_literal(predicate_arg* arg) {
    TAB_PRINTF(++tabs, "type: literal, \n");
    TAB_PRINTF(tabs, "value: ");
    print_literal(arg->arg.literal);
}

void print_predicate_arg_ref(predicate_arg* arg) {
    TAB_PRINTF(++tabs, "type: reference,\n");
    TAB_PRINTF(tabs, "value: %s\n", arg->arg.ref->col_name);
}

void print_predicate_arg(predicate_arg* arg) {
    printf("{\n");
    switch (arg->type) {
        case LITERAL:
            print_predicate_arg_literal(arg);
            break;
        case REFERENCE:
            print_predicate_arg_ref(arg);
            break;
    }
    TAB_PRINTF(--tabs, "}\n");
}

void print_trivial_predicate(predicate* pred) {
    if (pred == NULL) return;
    char cmp_type_str[3];
    cmp_type_to_str(pred->cmp_type, cmp_type_str);
    printf("{\n");
    TAB_PRINTF(++tabs, "type: trivial,\n");
    TAB_PRINTF(tabs, "column: %s,\n", pred->column->col_name);
    TAB_PRINTF(tabs, "cmp_type: %s,\n", cmp_type_str);
    TAB_PRINTF(tabs, "predicate_arg: ");
    print_predicate_arg(&pred->arg);
    TAB_PRINTF(--tabs, "}\n");
}

void print_contains_predicate(predicate* pred) {
    if (pred == NULL) return;
    printf("{\n");
    TAB_PRINTF(++tabs, "type: string_match,\n");
    TAB_PRINTF(tabs, "column: %s,\n", pred->column->col_name);
    TAB_PRINTF(tabs, "searched_str: ");
    print_predicate_arg(&pred->arg);
    TAB_PRINTF(--tabs, "}\n");
}

void print_compound_predicate(predicate* pred) {
    if (pred == NULL) return;
    char pred_op_as_str[4];
    predicate_op_to_str(pred->predicate_op, pred_op_as_str);
    printf("{\n");
    TAB_PRINTF(++tabs, "type: compound\n");
    TAB_PRINTF(tabs, "binop: %s, \n", pred_op_as_str);
    TAB_PRINTF(tabs, "left: ");
    print_predicate(pred->left);
    TAB_PRINTF(tabs, "right: ");
    print_predicate(pred->right);
    TAB_PRINTF(--tabs, "}\n");
}

void print_predicate(predicate* pred) {
    if (pred == NULL)  {
        printf("null\n");
        return;
    }
    switch (pred->type) {
        case STR_MATCH:
            print_contains_predicate(pred);
            break;
        case COMPARISON:
            print_trivial_predicate(pred);
            break;
        case COMPOUND:
            print_compound_predicate(pred);
            break;
    }
}

void print_column(columnref* ref) {
    TAB_PRINTF(++tabs, "{\n");
    TAB_PRINTF(++tabs, "name: %s\n", ref->col_name);
    TAB_PRINTF(tabs, "table: %s\n", ref->table_name ? ref->table_name : "null");
    TAB_PRINTF(--tabs, "}");
    tabs--;
}


void print_columns(columnref* ref) {
    printf("[\n");
    while (ref && ref->next) {
        print_column(ref);
        printf(",\n");
        ref = ref->next;
    }
    print_column(ref);
    printf("\n");
    TAB_PRINTF(tabs, "]\n");
}

void print_column_def(columndef* def) {
    char type_to_str[10];
    col_type_to_str(def->type, type_to_str);
    TAB_PRINTF(++tabs, "{\n");
    TAB_PRINTF(++tabs, "name: %s\n", def->column_name);
    TAB_PRINTF(tabs, "type: %s\n", type_to_str);
    TAB_PRINTF(--tabs, "}");
    tabs--;
}

void print_column_defs(columndef* defs) {
    printf("[\n");
    while (defs && defs->next) {
        print_column_def(defs);
        printf(",\n");
        defs = defs->next;
    }
    print_column_def(defs);
    printf("\n");
    TAB_PRINTF(tabs, "]\n");
}

void print_values(literal_list* list) {
    printf("[\n");
    while (list && list->next) {
        print_tabs(tabs);
        print_literal(list->value);
        printf(",\n");
        list = list->next;
    }
    print_literal(list->value);
    printf("\n");
    TAB_PRINTF(tabs, "]\n");
}

void print_join_stmt(join_stmt* stmt) {
    if (stmt == NULL) {
        printf("null\n");
        return;
    }
    printf("{\n");
    TAB_PRINTF(++tabs, "join_on: %s\n", stmt->join_on_table);
    TAB_PRINTF(tabs, "predicate: ");
    print_predicate(stmt->join_predicate);
    TAB_PRINTF(--tabs, "}\n");
}

void print_select_stmt(statement* stmt) {
    printf("{\n");
    TAB_PRINTF(++tabs, "type: select\n");
    TAB_PRINTF(tabs, "table: %s\n", stmt->table_name);
    TAB_PRINTF(tabs, "columns: ");
    print_columns(stmt->stmt.select_stmt->columns);
    TAB_PRINTF(tabs, "predicate: ");
    print_predicate(stmt->stmt.select_stmt->predicate);
    TAB_PRINTF(tabs, "join: ");
    print_join_stmt(stmt->stmt.select_stmt->join_stmt);
    TAB_PRINTF(--tabs, "}\n");
}

void print_insert_stmt(statement* stmt) {
    TAB_PRINTF(tabs, "{\n");
    TAB_PRINTF(++tabs, "type: insert,\n");
    TAB_PRINTF(tabs, "table: %s,\n", stmt->table_name);
    TAB_PRINTF(tabs, "columns: ");
    print_columns(stmt->stmt.insert_stmt->columns);
    TAB_PRINTF(tabs, "values: ");
    print_values(stmt->stmt.insert_stmt->literals);
    TAB_PRINTF(--tabs, "}\n");
}

void print_set_value(set_value* value) {
    TAB_PRINTF(tabs, "{\n");
    TAB_PRINTF(++tabs, "column: %s,\n", value->col->col_name);
    TAB_PRINTF(tabs, "value: ");
    print_literal(value->lit);
    TAB_PRINTF(--tabs, "}");
}

void print_set_value_list(set_value_list* list) {
    printf("[\n");
    // print_tabs(++tabs);
    while (list && list->next) {
        print_set_value(list->setval);
        list = list->next;
        printf(",\n");
    }
    print_set_value(list->setval);
    printf("\n");
    TAB_PRINTF(tabs, "]\n");
}

void print_update_stmt(statement *stmt) {
    TAB_PRINTF(tabs, "{\n");
    TAB_PRINTF(++tabs, "type: update,\n");
    TAB_PRINTF(tabs, "table: %s,\n", stmt->table_name);
    TAB_PRINTF(tabs, "updated_values: ");
    print_set_value_list(stmt->stmt.update_stmt->set_value_list);
    TAB_PRINTF(tabs, "predicate: ");
    print_predicate(stmt->stmt.update_stmt->predicate);
    TAB_PRINTF(--tabs, "}\n");
}

void print_create_stmt(statement *stmt) {
    TAB_PRINTF(tabs, "{\n");
    TAB_PRINTF(++tabs, "type: create,\n");
    TAB_PRINTF(tabs, "table: %s,\n", stmt->table_name);
    TAB_PRINTF(tabs, "definitions: ");
    print_column_defs(stmt->stmt.create_stmt->defs);
    TAB_PRINTF(--tabs, "}\n");
}

void print_drop_stmt(statement *stmt) {
    TAB_PRINTF(tabs, "{\n");
    TAB_PRINTF(++tabs, "type: drop,\n");
    TAB_PRINTF(tabs, "table: %s,\n", stmt->table_name);
    TAB_PRINTF(--tabs, "}\n");
}

void print_delete_stmt(statement *stmt) {
    TAB_PRINTF(tabs, "{\n");
    TAB_PRINTF(++tabs, "type: delete,\n");
    TAB_PRINTF(tabs, "from: %s,\n", stmt->table_name);
    TAB_PRINTF(tabs, "predicate: ");
    print_predicate(stmt->stmt.delete_stmt->predicate);
    TAB_PRINTF(--tabs, "}\n");
}

void print_stmt(statement* stmt) {
    printf("\n");
    switch (stmt->stmt_type) {
        case SELECT:
            print_select_stmt(stmt);
            break;
        case INSERT:
            print_insert_stmt(stmt);
            break;
        case UPDATE:
            print_update_stmt(stmt);
            break;
        case DELETE:
             print_delete_stmt(stmt);
             break;
        case DROP:
            print_drop_stmt(stmt);
            break;
        case CREATE:
            print_create_stmt(stmt);
        default:
            break;
    }
}


