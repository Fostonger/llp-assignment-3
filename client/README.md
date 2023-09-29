# Лабораторная работа № 2

## Сборка

Программа тестировалась в окружении Flex 2.5.4, Bison 3.8.2, gcc 11.3.0
```shell
make
```
порождает исполняемый файл *sql*

## Цель задания

Разработать модуль, осуществляющий синтаксический анализ языка запросов к базе данных с построением синтаксического дерева разобранного запроса.

## Задачи

1. Выбрать и изучить средство синтаксического анализа, совместимое с языком Си
2. Разработать структуры, необходимые для построения синтаксического дерева запроса и реализовать интерфейс работы с ними
3. Разработать грамматику данного языка запросов и задать её в средстве синтаксического анализа в нужном виде.
4. Задать правила для построения разработанных структур в выбранном средстве синтаксического анализа.
5. Разработать интерфейс для удобочитаемого вывода построенных структур.

## Описание работы

Для выполнения задачи был использован лексер Flex и генератор парсеров Bison.

Разработанная программа состоит из:

- ***sql.l*** - задаёт правила выделения из запроса токенов (лексический анализ)
- ***sql.y*** - задаёт правила выделения из токенов синтаксических структур согласно заданной грамматике (синтаксический анализ)
- ***data.h*** / ***data.c*** - описание структур синтаксического дерева и интерфейс работы с ними
- ***printer.h*** / ***printer.c*** - интерфейс для удобочитаемого вывода построенных структур

Результатом работы программы является следующая заполненная структура:

```c

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
```

Каждая из представленных в объединении структур содержит соответствующие ей подструктуры, тем самым организуя синтаксическое дерево:

```c
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
```
На стандартный ввод программа принимает текст запроса на поднмножестве языка SQL, удовлетворяющем следующим требованиям


- Условия
  - На равенство и неравенство для чисел, строк и булевских значений
  - На строгие и нестрогие сравнения для чисел
  - Существование подстроки
- Логическую комбинацию произвольного количества условий и булевских значений
  - В качестве любого аргумента условий могут выступать литеральные значения
    (константы) или ссылки на значения, ассоциированные с элементами данных
    (поля, атрибуты, свойства)
- Разрешение отношений между элементами модели данных любых условий над
  сопрягаемыми элементами данных

После ввода запроса программа выводит на стандартный вывод представление синтаксического дерева либо сообщение об ошибке, в случае если запрос не соответствует синтаксису.


## Аспекты реализации

Для выделения из строки с запросом токенов были использованы следующие регулярные выражения

```
integers ([-+]?[0-9])+
floats [+-]?([0-9]*[.])?[0-9]+
alpha ([a-zA-Z]+)
word ([a-zA-Z_][a-zA-Z0-9_]*)
```

Список самих токенов:

```c
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
```

Bison ожидает ввод в виде выражения, оканчивающегося знаком ";", а затем выводит его.

```
input:
|	input stmt SEMICOLON {print_stmt($2); printf("stmt > ");}
;

stmt: 	select_stmt
|	insert_stmt
|	update_stmt
|	create_stmt
|	drop_stmt
|	delete_stmt
;
```

Само дерево строится по частям: всякий запрос разбит на составные элементы.

Например, запрос на вставку выглядит следующим образом:

```
insert_stmt:	INSERT INTO tableref LP columnref RP VALUES LP value_list RP {$$ = new_insert_statement($3, $5, $9);}
;
```

Сами же составные части запроса так же могут состоять из более мелких частей.

Bison, проходя по токенам, ищет паттерны для структурных частей запросов, и найдя их, вызывает указанную в {} функцию по созданию структуры.
Созданная структура будет присвоена синтаксическому объекту и пойдет дальше наверх, став составной структурой иного синтаксического объекта.

Пример создания структуры для отображения списка запрашиваемых колонок таблицы:


```c
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
```

## Результаты

Конечным результатом является исполняемый файл ***sql***.

Для проверки памяти используется Valgrind:
```
valgrind --tool=memcheck --gen-suppressions=all --leak-check=full --show-leak-kinds=all --leak-resolution=med --track-origins=yes --vgdb=no ./sql
```

Примеры работы программы:
- Базовый SELECT без предиката и сопряжения таблиц.
  ```
  stmt > SELECT x, y FROM z;

  {
        type: select
        table: z
        columns: [
                {
                        name: x
                        table: null
                },
                {
                        name: y
                        table: null
                }
        ]
        predicate: null
        join: null
  }
  ```
  total heap usage: 11 allocs, 9 frees, 18,592 bytes allocated


- SELECT со сложным предикатом
  ```
  stmt > SELECT x, y FROM z WHERE a == 10 AND 4 != b OR c ~ 'str' AND (d >= 3.14 OR e == 'example');

  {
        type: select
        table: z
        columns: [
                {
                        name: x
                        table: null
                },
                {
                        name: y
                        table: null
                }
        ]
        predicate: {
                type: compound
                binop: OR, 
                left: {
                        type: compound
                        binop: AND, 
                        left: {
                                type: trivial,
                                column: a,
                                cmp_type: =,
                                predicate_arg: {
                                        type: literal, 
                                        value: {
                                                type: INTEGER,
                                                value: 10
                                        }
                                }
                        }
                        right: {
                                type: trivial,
                                column: b,
                                cmp_type: !=,
                                predicate_arg: {
                                        type: literal, 
                                        value: {
                                                type: INTEGER,
                                                value: 4
                                        }
                                }
                        }
                }
                right: {
                        type: compound
                        binop: AND, 
                        left: {
                                type: string_match,
                                column: c,
                                searched_str: {
                                        type: literal, 
                                        value: {
                                                type: STRING,
                                                value: str
                                        }
                                }
                        }
                        right: {
                                type: compound
                                binop: OR, 
                                left: {
                                        type: trivial,
                                        column: d,
                                        cmp_type: >=,
                                        predicate_arg: {
                                                type: literal, 
                                                value: {
                                                        type: FLOAT,
                                                        value: 3.140000
                                                }
                                        }
                                }
                                right: {
                                        type: trivial,
                                        column: e,
                                        cmp_type: =,
                                        predicate_arg: {
                                                type: literal, 
                                                value: {
                                                        type: STRING,
                                                        value: example
                                                }
                                        }
                                }
                        }
                }
        }
        join: null
  }  
  ```
  total heap usage: 37 allocs, 35 frees, 19,390 bytes allocated


- SELECT с предикатом и JOIN по сложному условию

  ```
  stmt > SELECT x.id, y.id FROM x JOIN y ON x.id == y.id AND x.id != 0 WHERE x.id < 100; 

  {
        type: select
        table: x
        columns: [
                {
                        name: id
                        table: x
                },
                {
                        name: id
                        table: y
                }
        ]
        predicate: {
                type: trivial,
                column: id,
                cmp_type: <,
                predicate_arg: {
                        type: literal, 
                        value: {
                                type: INTEGER,
                                value: 100
                        }
                }
        }
        join: {
                join_on: y
                predicate: {
                        type: compound
                        binop: AND, 
                        left: {
                                type: trivial,
                                column: id,
                                cmp_type: =,
                                predicate_arg: {
                                        type: reference,
                                        value: id
                                }
                        }
                        right: {
                                type: trivial,
                                column: id,
                                cmp_type: !=,
                                predicate_arg: {
                                        type: literal, 
                                        value: {
                                                type: INTEGER,
                                                value: 0
                                        }
                                }
                        }
                }
        }
  }
  ```
  total heap usage: 33 allocs, 31 frees, 19,020 bytes allocated


- CREATE TABLE

  ```
  stmt >  CREATE TABLE tablename (
        col1 int,    
        col2 varchar,
        col3 float
  );

  {
        type: create,
        table: tablename,
        definitions: [
                {
                        name: col1
                        type: INTEGER
                },
                {
                        name: col2
                        type: VARCHAR
                },
                {
                        name: col3
                        type: FLOAT
                }
        ]
  }
  ```

  total heap usage: 13 allocs, 11 frees, 18,619 bytes allocated

  
- INSERT

  ```
  stmt > INSERT INTO tablename (X, Y, Z, T) VALUES (-5, 'str', TRUE, 3.14);

  {
        type: insert,
        table: tablename,
        columns: [
                {
                        name: X
                        table: null
                },
                {
                        name: Y
                        table: null
                },
                {
                        name: Z
                        table: null
                },
                {
                        name: T
                        table: null
                }
        ]
        values: [
                {
                        type: FLOAT,
                        value: 3.140000
                },
                {
                        type: BOOLEAN,
                        value: TRUE
                },
                {
                        type: STRING,
                        value: str
                },
                {
                        type: INTEGER,
                        value: -5
                }
        ]
  }
  ```

  total heap usage: 24 allocs, 22 frees, 18,776 bytes allocated

Стоит заметить, что во всех случаях аллокаций на 2 больше, чем освобождений.
Это не протёкшая память, а 'still reachable' структуры самого Flex'а на момент завершения программы

```
==7808== 56 bytes in 1 blocks are still reachable in loss record 1 of 2
==7808==    at 0x4848899: malloc (in /usr/libexec/valgrind/vgpreload_memcheck-amd64-linux.so)
==7808==    by 0x10D0A6: yy_flex_alloc (lex.yy.c:1812)
==7808==    by 0x10CC7C: yy_create_buffer (lex.yy.c:1483)
==7808==    by 0x10BC57: yylex (lex.yy.c:672)
==7808==    by 0x10AD56: yyparse (sql.tab.c:1373)
==7808==    by 0x10BBC1: main (sql.y:184)
==7808== 
{
   <insert_a_suppression_name_here>
   Memcheck:Leak
   match-leak-kinds: reachable
   fun:malloc
   fun:yy_flex_alloc
   fun:yy_create_buffer
   fun:yylex
   fun:yyparse
   fun:main
}
==7808== 16,386 bytes in 1 blocks are still reachable in loss record 2 of 2
==7808==    at 0x4848899: malloc (in /usr/libexec/valgrind/vgpreload_memcheck-amd64-linux.so)
==7808==    by 0x10D0A6: yy_flex_alloc (lex.yy.c:1812)
==7808==    by 0x10CCB1: yy_create_buffer (lex.yy.c:1492)
==7808==    by 0x10BC57: yylex (lex.yy.c:672)
==7808==    by 0x10AD56: yyparse (sql.tab.c:1373)
==7808==    by 0x10BBC1: main (sql.y:184)
==7808== 
{
   <insert_a_suppression_name_here>
   Memcheck:Leak
   match-leak-kinds: reachable
   fun:malloc
   fun:yy_flex_alloc
   fun:yy_create_buffer
   fun:yylex
   fun:yyparse
   fun:main
}

```

## Выводы

В ходе выполнения задания были изучены инструменты для лексического и синтаксического анализа: Flex и Bison.
С их помощью удалось построить парсер запросов на языке SQL и разобрать запрос в синтаксическое дерево.
Инструменты легки в освоении и использовании, необходимый минимум - понимание определения грамматики языка, 
базовое знание регулярных выражений и базовое знание языка Си
