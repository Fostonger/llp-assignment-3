//
// Created by rosroble on 15.11.22.
//

#ifndef SQLPARSER_DATA_H
#define SQLPARSER_DATA_H

typedef enum predicate_arg_type predicate_arg_type;
typedef enum predicate_type predicate_type;
typedef struct predicate_arg predicate_arg;
typedef struct predicate predicate;
typedef struct join_stmt join_stmt;
typedef struct select_stmt select_stmt;
typedef struct insert_stmt insert_stmt;
typedef struct update_stmt update_stmt;
typedef struct create_stmt create_stmt;
typedef struct delete_stmt delete_stmt;
typedef struct statement statement;
typedef struct set_value set_value;
typedef struct set_value_list set_value_list;
typedef struct literal literal;
typedef struct literal_list literal_list;
typedef enum literal_type littype;
typedef struct columnref columnref;
typedef struct columndef columndef;


enum predicate_arg_type {
    LITERAL, REFERENCE
};

enum predicate_type {
    COMPARISON, STR_MATCH, COMPOUND
};

enum literal_type {
    LIT_STRING, LIT_INTEGER, LIT_FLOAT, LIT_BOOLEAN
};

struct columndef {
    char* column_name;
    int type;
    columndef* next;
};

struct columnref {
    char* table_name;
    char* col_name;
    columnref* next;
};

struct literal {
    littype type;
    union {
        int boolean;
        int num;
        double flt;
        char* string;
    } value;
};

struct literal_list {
    literal* value;
    literal_list* next;
};

struct predicate_arg {
    predicate_arg_type type;
    union {
        literal* literal;
        columnref* ref;
        void* unknown;
    } arg;
};

struct predicate {
    predicate_type type;
    columnref* column;
    int cmp_type;
    predicate_arg arg;

    int predicate_op;
    predicate* left;
    predicate* right;
};

struct join_stmt {
    char* join_on_table;
    predicate* join_predicate;
    join_stmt *next_join;
};

struct select_stmt {
    columnref* columns;
    predicate* predicate;
    join_stmt* join_stmt;
};

struct insert_stmt {
    columnref* columns;
    literal_list* literals;
};

struct create_stmt {
    columndef* defs;
};

struct set_value {
    columnref* col;
    literal* lit;
};

struct set_value_list {
    set_value* setval;
    set_value_list* next;
};

struct update_stmt {
    set_value_list* set_value_list;
    predicate* predicate;
};

struct delete_stmt {
    predicate* predicate;
};

struct statement {
    union {
        select_stmt *select_stmt;
        insert_stmt *insert_stmt;
        update_stmt *update_stmt;
        create_stmt *create_stmt;
        delete_stmt *delete_stmt;
    } stmt;
    int stmt_type;
    char* table_name;
};

literal* new_num_literal(int num);
literal* new_str_literal(char* str);
literal* new_float_literal(double flt);
literal* new_bool_literal(int boolean);
literal_list* new_literal_list(literal_list* prev, literal* literal);
columnref* new_column_ref(columnref* prev, char* column_name, char* table_name);
columndef* new_column_def(columndef* prev, char* column_name, int type);
statement* new_basic_select_statement(char* table_name, columnref* colref);
statement* new_insert_statement(char* table_name, columnref* cols, literal_list* vals);
statement* new_update_statement(char* table_name, set_value_list* sets, predicate* pred);
statement* new_create_statement(char* table_name, columndef* defs);
statement* new_drop_statement(char* table_name);
statement* new_delete_statement(char* table_name, predicate* pred);
statement* add_join_statement(statement* statement, join_stmt* join_stmt);
statement* add_predicate_statement(statement* statement, predicate* predicate);
predicate* new_literal_predicate(columnref* col, int cmp_type, literal* liter);
predicate* new_reference_predicate(columnref* left, int cmp_type, columnref* right);
predicate* new_compound_predicate(predicate* left, int predicate_op, predicate* right);
predicate* new_contains_predicate(columnref* col, char* str);
join_stmt* new_join_stmt(char* join_on, predicate* predicate, join_stmt* next_join);
set_value* new_set_value(columnref* col, literal* literal);
set_value_list* new_set_value_list(set_value_list* prev, set_value* val);

int reverse_cmp(int cmp);
void free_statement(statement* stmt);

#endif //SQLPARSER_DATA_H
