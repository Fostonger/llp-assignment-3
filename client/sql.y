%{
#include <stdio.h>
#include "data.h"
#include "printer.h"
#define YYERROR_VERBOSE 1
extern int yylex();
extern int yylineno;
void yyerror(statement *, const char*);
%}
%define parse.error verbose

%token STRING INTEGER FLOAT BOOL
%token OTHER SEMICOLON COMMA DOT QUOTE EOL LP RP SET EQ ON JOIN
%token SELECT INSERT UPDATE DELETE CREATE DROP TABLE FROM INTO WHERE AND OR ALL VALUES
%token CMP CONTAINS
%token TYPE


%type <name> STRING
%type <name> tableref
%type <number> INTEGER
%type <boolean> BOOL
%type <number> CMP
%type <number> CONTAINS
%type <flt> FLOAT
%type <type> TYPE
%type <predicate_op> AND
%type <predicate_op> OR
%type <literal> literal
%type <lit_list> value_list
%type <colref> columnref
%type <colref> short_columnref
%type <colref> full_columnref
%type <coldef> column_def
%type <coldef> column_defs
%type <stmt> basic_select_stmt
%type <stmt> select_predicate_stmt
%type <stmt> select_join_stmt
%type <stmt> select_stmt
%type <stmt> insert_stmt
%type <stmt> update_stmt
%type <stmt> create_stmt
%type <stmt> delete_stmt
%type <stmt> drop_stmt
%type <stmt> stmt
%type <jstmt> join_stmt
%type <svallist> set_value_list
%type <sval> set_value
%type <predicate> trivial_predicate
%type <predicate> predicate

%left OR
%left AND

%union {
    char name[20];
    int number;
    int boolean;
    int cmp;
    int predicate_op;
    int type;
    double flt;
    literal_list* lit_list;
    literal* literal;
    columnref* colref;
    columndef* coldef;
    set_value_list* svallist;
    set_value* sval;
    statement* stmt;
    join_stmt* jstmt;
    predicate* predicate;
}

%parse-param { statement *s }

%start input

%%



input:
|	input stmt SEMICOLON {*s = *$2;}
;

stmt: 	select_stmt
|	insert_stmt
|	update_stmt
|	create_stmt
|	drop_stmt
|	delete_stmt
;


select_stmt: 	basic_select_stmt
|		select_join_stmt
|		select_predicate_stmt
;

select_predicate_stmt: 	basic_select_stmt WHERE predicate {$$ = add_predicate_statement($1, $3); }
|			select_join_stmt WHERE predicate {$$ = add_predicate_statement($1, $3); }
;

select_join_stmt: basic_select_stmt JOIN join_stmt {$$ = add_join_statement($1, $3);}
;

basic_select_stmt: SELECT columnref FROM tableref {$$ = new_basic_select_statement($4, $2);}
;

join_stmt: 	tableref ON predicate {$$ = new_join_stmt($1, $3);}
;

insert_stmt:	INSERT INTO tableref LP columnref RP VALUES LP value_list RP {$$ = new_insert_statement($3, $5, $9);}
;

update_stmt:	UPDATE tableref SET set_value_list { $$ = new_update_statement($2, $4, NULL); }
|		UPDATE tableref SET set_value_list WHERE predicate {$$ = new_update_statement($2, $4, $6); }
;

create_stmt: CREATE TABLE tableref LP column_defs RP { $$ = new_create_statement($3, $5); }
;

drop_stmt: DROP TABLE tableref { $$ = new_drop_statement($3); }
;

delete_stmt: DELETE FROM tableref WHERE predicate { $$ = new_delete_statement($3, $5); }
;


set_value_list:	set_value {$$ = new_set_value_list(NULL, $1); }
|		set_value COMMA set_value_list {$$ = new_set_value_list($3, $1); }
;

set_value: columnref EQ literal {$$ = new_set_value($1, $3); }
;

tableref: STRING
;

predicate: predicate AND predicate { $$ = new_compound_predicate($1, $2, $3); }
|	predicate OR predicate { $$ = new_compound_predicate($1, $2, $3); }
|	trivial_predicate
|	LP predicate RP { $$ = $2; }
;

trivial_predicate: 	columnref CMP literal { $$ = new_literal_predicate($1, $2, $3); }
|			literal CMP columnref { $$ = new_literal_predicate($3, reverse_cmp($2), $1); }
|			columnref CMP columnref { $$ = new_reference_predicate($1, $2, $3); }
|			columnref CONTAINS QUOTE STRING QUOTE { $$ = new_contains_predicate($1, $4); }
;

columnref:  short_columnref
|		full_columnref
|       short_columnref COMMA columnref { $$ = $1; $1->next = $3; }
|		full_columnref COMMA columnref { $$ = $1; $1->next = $3; }

;

short_columnref: STRING { $$ = new_column_ref(NULL, $1, NULL); }
;

full_columnref: STRING DOT STRING { $$ = new_column_ref(NULL, $3, $1);}
;

column_defs: 	column_def COMMA column_defs { $$ = $1; $1->next = $3; }
|		column_def
;

column_def: STRING TYPE { $$ = new_column_def(NULL, $1, $2); }
;

value_list: literal { $$ = new_literal_list(NULL, $1); }
|	literal COMMA value_list { $$ = new_literal_list($3, $1); }
;

literal:	INTEGER { $$ = new_num_literal($1); }
|		FLOAT { $$ = new_float_literal($1); }
|		BOOL { $$ = new_bool_literal($1); }
|		QUOTE STRING QUOTE { $$ = new_str_literal($2); }
;
%%

void yyerror (statement *stmt, char const *s) {
   fprintf (stderr, "%s on line number %d", s, yylineno);
}

