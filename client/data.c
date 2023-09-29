//
// Created by rosroble on 15.11.22.
//

#include "data.h"
#include "sql.tab.h"
#include <stdlib.h>
#include <string.h>


int reverse_cmp(int cmp) {
    switch (cmp) {
        case 1:
            return 2;
        case 2:
            return 1;
        case 3:
        case 4:
            return cmp;
        case 5:
            return 6;
        case 6:
            return 5;
        default:
            return -1;
    }
}

char* alloc_and_copy_str(char* src) {
    return strcpy(malloc(strlen(src) + 1), src);
}

predicate_arg new_predicate_arg(predicate_arg_type type, void* arg) {
    predicate_arg predarg;
    predarg.type = type;
    predarg.arg.unknown = arg;
    return predarg;
}

literal* new_num_literal(int num) {
    literal* lit = malloc(sizeof(literal));
    if (lit) {
        lit->type = LIT_INTEGER;
        lit->value.num = num;
    }
    return lit;
}

literal* new_str_literal(char* str) {
    literal* lit = malloc(sizeof (literal));
    if (lit) {
        lit->type = LIT_STRING;
        lit->value.string = malloc(strlen(str) + 1);
        strcpy(lit->value.string, str);
    }
    return lit;
}

literal* new_float_literal(double flt) {
    literal* lit = malloc(sizeof (literal));
    if (lit) {
        lit->type = LIT_FLOAT;
        lit->value.flt = flt;
    }
    return lit;
}

literal* new_bool_literal(int boolean) {
    literal* lit = new_num_literal(boolean);
    lit->type = LIT_BOOLEAN;
    return lit;
}

columnref* new_column_ref(columnref* prev, char* column_name, char* table_name) {
    columnref* cref = malloc(sizeof(columnref));
    if (cref) {
        cref->col_name = malloc(strlen(column_name) + 1);
        strcpy(cref->col_name, column_name);
        cref->table_name = NULL;
        if (table_name) {
            cref->table_name = malloc(strlen(table_name) + 1);
            strcpy(cref->table_name, table_name);
        }
        cref->next = prev;
    }
    return cref;
}

columndef* new_column_def(columndef* prev, char* column_name, int type) {
    columndef* def = malloc(sizeof(columndef));
    if (def) {
        def->next = NULL;
        def->column_name = alloc_and_copy_str(column_name);
        def->type = type;
        if (prev) {
            def->next = prev;
        }
    }
    return def;
}

statement* new_basic_statement (char* table_name, int type) {
    statement *basic = malloc(sizeof(statement));
    if (basic) {
        basic->table_name = malloc(strlen(table_name) + 1);
        strcpy(basic->table_name, table_name);
        basic->stmt_type = type;
    }
    return basic;
}

statement* new_basic_select_statement(char* table_name, columnref* colref) {
    statement *basic = new_basic_statement(table_name, SELECT);
    if (basic) {
        basic->stmt.select_stmt = malloc(sizeof (select_stmt));
        if (basic->stmt.select_stmt) {
            basic->stmt.select_stmt->predicate = NULL;
            basic->stmt.select_stmt->columns = colref;
            basic->stmt.select_stmt->join_stmt = NULL;
        }
    }
    return basic;
}

statement* new_insert_statement(char* table_name, columnref* cols, literal_list* vals) {
    statement* basic = new_basic_statement(table_name, INSERT);
    if (basic) {
        basic->stmt.insert_stmt = malloc(sizeof (insert_stmt));
        if (basic->stmt.insert_stmt) {
            basic->stmt.insert_stmt->columns = cols;
            basic->stmt.insert_stmt->literals = vals;
        }
    }
    return basic;
}

statement* new_update_statement(char* table_name, set_value_list* sets, predicate* pred) {
    statement* basic = new_basic_statement(table_name, UPDATE);
    if (basic) {
        basic->stmt.update_stmt = malloc(sizeof(update_stmt));
        if (basic->stmt.update_stmt) {
            basic->stmt.update_stmt->set_value_list = sets;
            basic->stmt.update_stmt->predicate = pred;
        }
    }
    return basic;
}

statement* new_create_statement(char* table_name, columndef* defs) {
    statement* basic = new_basic_statement(table_name, CREATE);
    if (basic) {
        basic->stmt.create_stmt = malloc(sizeof (create_stmt));
        if (basic->stmt.create_stmt) {
            basic->stmt.create_stmt->defs = defs;
        }
    }
    return basic;
}

statement* new_delete_statement(char* table_name, predicate* pred) {
    statement* basic = new_basic_statement(table_name, DELETE);
    if (basic) {
        basic->stmt.delete_stmt = malloc(sizeof(delete_stmt));
        if (basic->stmt.delete_stmt) {
            basic->stmt.delete_stmt->predicate = pred;
        }
    }
    return basic;
}

statement* new_drop_statement(char* table_name) {
    statement* basic = new_basic_statement(table_name, DROP);
    return basic;
}

predicate* new_literal_predicate(columnref* col, int cmp_type, literal* liter) {
    predicate* pred = malloc(sizeof(predicate));
    predicate_arg predarg = new_predicate_arg(LITERAL, liter);
    if (pred) {
        pred->type = COMPARISON;
        pred->column = col;
        pred->cmp_type = cmp_type;
        pred->arg = predarg;
        pred->predicate_op = 0;
    }
    return pred;
}

predicate* new_reference_predicate(columnref* left, int cmp_type, columnref* right) {
    predicate* pred = malloc(sizeof(predicate));
    predicate_arg predarg = new_predicate_arg(REFERENCE, right);
    if (pred) {
        pred->type = COMPARISON;
        pred->column = left;
        pred->cmp_type = cmp_type;
        pred->arg = predarg;
        pred->predicate_op = 0;
    }
    return pred;
}

predicate* new_compound_predicate(predicate* left, int predicate_op, predicate* right) {
    predicate* pred = malloc(sizeof(predicate));
    if (pred) {
        pred->type = COMPOUND;
        pred->column = NULL;
        pred->cmp_type = 0;
        pred->predicate_op = predicate_op;
        pred->left = left;
        pred->right = right;
    }
    return pred;
}

predicate* new_contains_predicate(columnref* col, char* str) {
    predicate* pred = malloc(sizeof(predicate));
    predicate_arg arg = new_predicate_arg(LITERAL, new_str_literal(str));
    if (pred) {
        pred->type = STR_MATCH;
        pred->column = col;
        pred->predicate_op = 0;
        pred->cmp_type = 0;
        pred->arg = arg;
    }
    return pred;
}

literal_list* new_literal_list(literal_list* prev, literal* literal) {
    literal_list* llist = malloc(sizeof(literal_list));
    llist->value = literal;
    llist->next = prev;
    return llist;
}



set_value* new_set_value(columnref* col, literal* literal) {
    set_value* set_val = malloc(sizeof(set_value));
    if (set_val) {
        set_val->lit = literal;
        set_val->col = col;
    }
    return set_val;
}

set_value_list* new_set_value_list(set_value_list* prev, set_value* val) {
    set_value_list* list = malloc(sizeof(set_value_list));
    if (list) {
        list->setval = val;
        list->next = prev;
    }
    return list;
}

statement* add_join_statement(statement* stmt, join_stmt* join_stmt) {
    if (stmt->stmt_type != SELECT) return stmt; // no-op
    stmt->stmt.select_stmt->join_stmt = join_stmt;
    return stmt;
}

statement* add_predicate_statement(statement* stmt, predicate* pred) {
    if (stmt->stmt_type != SELECT) return stmt;
    stmt->stmt.select_stmt->predicate = pred;
    return stmt;
}

join_stmt* new_join_stmt(char* join_on, predicate* predicate) {
    join_stmt* jstmt = malloc(sizeof(join_stmt));
    if (jstmt) {
        jstmt->join_on_table = malloc(strlen(join_on) + 1);
        strcpy(jstmt->join_on_table, join_on);
        jstmt->join_predicate = predicate;
    }
    return jstmt;
}

void free_literal(literal* literal) {
    if (literal == NULL) return;
    if (literal->type == LIT_STRING) {
        free(literal->value.string);
    }
    free(literal);
}

void free_column_ref(columnref* ref) {
    if (ref == NULL) return;
    columnref* t = ref;
    while (ref) {
        free(ref->table_name);
        free(ref->col_name);
        ref = t->next;
        free(t);
        t = ref;
    }
}

void free_predicate(predicate* pred) {
    if (pred == NULL) return;
    if (pred->type != COMPOUND) {
        free_column_ref(pred->column);
        if (pred->arg.type == LITERAL) {
            free_literal(pred->arg.arg.literal);
        }
        if (pred->arg.type == REFERENCE) {
            free_column_ref(pred->arg.arg.ref);
        }
    } else {
        free_predicate(pred->left);
        free_predicate(pred->right);
    }
    free(pred);
}

void free_join(join_stmt* stmt) {
    if (stmt == NULL) return;
    free(stmt->join_on_table);
    free_predicate(stmt->join_predicate);
    free(stmt);
}

void free_select(select_stmt* stmt) {
    if (stmt == NULL) return;
    free_column_ref(stmt->columns);
    free_predicate(stmt->predicate);
    free_join(stmt->join_stmt);
    free(stmt);
}



void free_literal_list(literal_list* list) {
    if (list == NULL) return;
    literal_list* t = list;
    while (list) {
        free_literal(list->value);
        list = t->next;
        free(t);
        t = list;
    }
}

void free_set_value(set_value* val) {
    if (val == NULL) return;
    free_column_ref(val->col);
    free_literal(val->lit);
    free(val);
}

void free_set_value_list(set_value_list* list) {
    if (list == NULL) return;
    set_value_list* t = list;
    while (list) {
        free_set_value(list->setval);
        list = t->next;
        free(t);
        t = list;
    }
}

void free_insert(insert_stmt* stmt) {
    if (stmt == NULL) return;
    free_column_ref(stmt->columns);
    free_literal_list(stmt->literals);
    free(stmt);
}

void free_update(update_stmt* stmt) {
    if (stmt == NULL) return;
    free_set_value_list(stmt->set_value_list);
    free_predicate(stmt->predicate);
    free(stmt);
}

void free_delete(delete_stmt* stmt) {
    if (stmt == NULL) return;
    free_predicate(stmt->predicate);
    free(stmt);
}

void free_column_def(columndef* def) {
    if (def == NULL) return;
    columndef* t = def;
    while (def) {
        free(def->column_name);
        def = t->next;
        free(t);
        t = def;
    }
}

void free_create(create_stmt* stmt) {
    if (stmt == NULL) return;
    free_column_def(stmt->defs);
    free(stmt);
}

void free_statement(statement* stmt) {
    if (stmt == NULL) return;
    free(stmt->table_name);
    switch (stmt->stmt_type) {
        case SELECT:
            free_select(stmt->stmt.select_stmt);
            break;
        case INSERT:
            free_insert(stmt->stmt.insert_stmt);
            break;
        case UPDATE:
            free_update(stmt->stmt.update_stmt);
            break;
        case DELETE:
            free_delete(stmt->stmt.delete_stmt);
            break;
        case CREATE:
            free_create(stmt->stmt.create_stmt);
        default:
            break;
    }
    free(stmt);
}


