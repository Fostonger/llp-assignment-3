enum predicate_arg_type_T {
    LITERAL_T
    REFERENCE_T
}

enum predicate_type_T {
    COMPARISON_T
    STR_MATCH_T
    COMPOUND_T
}

enum literal_type_T {
    LIT_INTEGER_T
    LIT_STRING_T
    LIT_BOOLEAN_T
    LIT_FLOAT_T
    LIT_NONE_T
}

union types_T {
    1: i32 boolean
    2: i32 num
    3: double flt
    4: string str
}

struct columnref_T {
    1: optional string table_name
    2: string col_name
}


struct literal_T {
    1: literal_type_T type
    2: types_T value
}

union predicate_arg_union_T {
    1: literal_T literal
    2: columnref_T ref
}

struct columndef_T {
    1: string column_name
    2: i32 type
}


struct predicate_arg_T {
    1: predicate_arg_type_T type
    2: predicate_arg_union_T arg
}

struct predicate_T {
    1: predicate_type_T type
    2: optional columnref_T column
    3: i32 cmp_type
    4: predicate_arg_T arg
    5: i32 predicate_op
    6: list<predicate_T> left
    7: list<predicate_T> right
}

struct join_stmt_T {
    1: string join_on_table
    2: optional predicate_T join_predicate
}

struct select_stmt_T {
    1: list<columnref_T> columns
    2: optional predicate_T predicate
    3: optional join_stmt_T join_stmt
}

struct insert_stmt_T {
    1: list<columnref_T> columns
    2: list<literal_T> literals
}

struct create_stmt_T {
    1: list<columndef_T> defs
}

struct set_value_T {
    1: columnref_T col
    2: literal_T lit
}

struct update_stmt_T {
    1: list<set_value_T> set_value_list
    2: optional predicate_T predicate
}

struct delete_stmt_T {
    1: optional predicate_T predicate
}

union statement_union_T {
    1: select_stmt_T select_stmt
    2: insert_stmt_T insert_stmt
    3: update_stmt_T update_stmt
    4: create_stmt_T create_stmt
    5: delete_stmt_T delete_stmt
}
struct statement_T {
    1: statement_union_T stmt
    2: i32 stmt_type
    3: string table_name
}

enum status_code_T {
    OK
    TABLE_NOT_FOUND
    BAD_REQUEST
    INTERNAL_ERROR
}

struct row_T {
    1: list<literal_T> value
}

struct item_list_T {
    1: list<columnref_T> schema
    2: list<row_T> rows
}

struct server_response_T {
    1: status_code_T status
    2: string msg
    3: optional item_list_T items
}

service data_exchange_service {
   server_response_T execute(1: statement_T stmt)
}

