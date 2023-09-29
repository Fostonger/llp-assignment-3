#include <stdio.h>
#include "printer.h"
#include "sql.tab.h"
#include "lexer.h"
#include "client_interface.h"

statement get_statement(const char* query) {
    statement s;
    yy_scan_string(query);
    yyparse(&s);
    yylex_destroy();
    return s;
}
