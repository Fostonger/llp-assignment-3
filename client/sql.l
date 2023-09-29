%option noyywrap
%option caseless
%option yylineno


%{
#include <stdio.h>
#include <string.h>
#include "data.h"
#include "sql.tab.h"
void showError();
#define YYERROR_VERBOSE 1
%}


integers ([-+]?[0-9])+
floats [+-]?([0-9]*[.])?[0-9]+
alpha ([a-zA-Z]+)
word ([a-zA-Z_][a-zA-Z0-9_]*)

%%

"*"         {return ALL;}
"select"    {return SELECT;}
"insert"    {return INSERT;}
"update"    {return UPDATE;}
"create"    {return CREATE;}
"delete"    {return DELETE;}
"drop"      {return DROP;}
"table"     {return TABLE;}
"on"        {return ON;}
"from"      {return FROM;}
"where"     {return WHERE;}
"into"      {return INTO;}
"values"    {return VALUES;}
"set"       {return SET;}
"join"      {return JOIN;}
"and"       {yylval.predicate_op = 1; return AND;}
"or"        {yylval.predicate_op = 2; return OR;}
"="         {return EQ;}
"~"         {return CONTAINS;}
">"         {yylval.cmp = 1; return CMP;}
"<"         {yylval.cmp = 2; return CMP;}
"!="        {yylval.cmp = 3; return CMP;}
"=="        {yylval.cmp = 4; return CMP;}
">="        {yylval.cmp = 5; return CMP;}
"<="        {yylval.cmp = 6; return CMP;}
"int"       {yylval.type = 1; return TYPE;}
"varchar"   {yylval.type = 2; return TYPE;}
"boolean"   {yylval.type = 3; return TYPE;}
"float"     {yylval.type = 4; return TYPE;}
"true"      {yylval.boolean = 1; return BOOL;}
"false"     {yylval.boolean = 0; return BOOL;}
"("         {return LP;}
")"         {return RP;}
"'"         {return QUOTE;}
";"         {return SEMICOLON;}
"."         {return DOT;}
","         {return COMMA;}

{word}     {sscanf(yytext, "%s", yylval.name); return (STRING); }
{integers}   {yylval.number = atoi(yytext); return (INTEGER); }
{floats}     {yylval.flt = atof(yytext); return (FLOAT); }
[ \t]   { /* ignore white space */ }
[\n]    {}
.           {showError(yytext); return (OTHER);}


%%


void showError(char* token) {
    printf("Unknown token: %s\n", token);
}
