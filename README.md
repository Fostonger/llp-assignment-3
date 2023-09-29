# Лабораторная работа № 3

## Сборка

Программа тестировалась в окружении:
- Flex 2.5.4
- Bison 3.8.2 
- gcc 11.3.0 
- Apache Thrift 0.17.0
- cmake 3.24.2
- glib-2.0
- gobject-2.0

```shell
$ export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib/ # чтобы слинковать glibc
$ thrift --gen c_glib structs.thrift # сгенерировать структуры (уже есть в проекте)
$ cmake --build cmake-build-debug/ # генерирует бинарники client_exe и server_exe в папке cmake-build-debug
```
## Цель задания

На базе данного транспортного формата описать схему протокола обмена информацией и воспользоваться
существующей библиотекой по выбору для реализации модуля, обеспечивающего его функционирование.
Протокол должен включать представление информации о командах создания, выборки, модификации и
удаления данных в соответствии с данной формой, и результатах их выполнения.


Используя созданные в результате выполнения заданий модули, разработать в виде консольного приложения
две программы: клиентскую и серверную части. Серверная часть – получающая по сети запросы и операции
описанного формата и последовательно выполняющая их над файлом данных с помощью модуля из первого
задания. Имя фала данных для работы получать с аргументами командной строки, создавать новый в случае
его отсутствия. Клиентская часть – в цикле получающая на стандартный ввод текст команд, извлекающая из
него информацию о запрашиваемой операции с помощью модуля из второго задания и пересылающая её на
сервер с помощью модуля для обмена информацией, получающая ответ и выводящая его в человеко-
понятном виде в стандартный вывод.

Вариант: Apache Thrift

## Задачи

1. Изучить библиотеку Apache Thrift
2. Описать схему протокола в формате Thrift
3. На основе генерирумых библиотекой структур и интерфейсов реализовать клиент-серверное взаимодействие
4. Реализовать клиент как консольное приложение принимающее в аргументах командной строки точку подключения
5. Реализовать сервер как консольное приложение принимающее в аргуметах командной строки файл для хранения БД.

## Описание работы

Для выполнения задачи были написаны структуры в формате thrift [(structs.thrift)](structs.thrift)

На основе данных структур генерируется код на языке Си с использованием библиотеки glib[(gen-c_glib)](gen-c_glib)


- ***data_exchange_service[.h/.c]*** - интерфейс сервиса обработки клиентского запроса. Имплементируется сервером.
- ***struct.types[.h/.c]*** - определения структур формата библиотеки gobject. Клиент-серверное взаимодействие осуществляется путем обмена данными структурами.

В папке [serializer](serializer) находится код для преобразования обычных структур языка Си в структуры библиотеки gobject

В файлах ***request_processor[.h/.c]*** находится код обработчиков различных клиентских запросов, формирующий ответы клиенту

Код описывающий само поведение клиента и сервера находится в файлах ***client.c*** и ***server.c*** соответственно.

За основу логики верхнеуровнего кода клиента и сервера взят [пример](https://thrift.apache.org/tutorial/c_glib.html) с сайта Apache

Протестируем клиент-серверное взаимодействие на следующем сценарии запросов:

```sql
CREATE TABLE itmo (name varchar, age int, avg_grade float, expelled boolean);
INSERT INTO itmo (name, age, avg_grade, expelled) VALUES ('boris', 20, 4.5, true);
INSERT INTO itmo (name, age, avg_grade, expelled) VALUES ('alex', 19, 5.0, false);
INSERT INTO itmo (name, age, avg_grade, expelled) VALUES ('pavel', 24, 4.1, true);
INSERT INTO itmo (name, age, avg_grade, expelled) VALUES ('yuri', 20, 4.7, false);
SELECT name, age, avg_grade, expelled FROM itmo;
SELECT name, age, avg_grade, expelled FROM itmo WHERE name == 'boris';
SELECT name, age, avg_grade, expelled FROM itmo WHERE expelled == false AND avg_grade > 4.6 OR name == 'boris';
UPDATE itmo SET expelled = false WHERE name == 'boris';
SELECT name, age, avg_grade, expelled FROM itmo WHERE name == 'boris';
DELETE FROM itmo WHERE avg_grade < 4.6;
SELECT name, avg_grade FROM itmo;
```

Лог терминала консольного приложения:

```shell
/home/rosroble/edu/llp/lab3/cmake-build-debug/client_exe localhost 9090
stmt > CREATE TABLE itmo (name varchar, age int, avg_grade float, expelled boolean);
CREATE TABLE: OK
stmt > INSERT INTO itmo (name, age, avg_grade, expelled) VALUES ('boris', 20, 4.5, true);
INSERT: OK
stmt > INSERT INTO itmo (name, age, avg_grade, expelled) VALUES ('alex', 19, 5.0, false);
INSERT: OK
stmt > INSERT INTO itmo (name, age, avg_grade, expelled) VALUES ('pavel', 24, 4.1, true);
INSERT: OK
stmt > INSERT INTO itmo (name, age, avg_grade, expelled) VALUES ('yuri', 20, 4.7, false);
INSERT: OK
stmt > SELECT name, age, avg_grade, expelled FROM itmo;
SELECT: OK
name	age	avg_grade	expelled	
boris	20	4.5000	1	
alex	19	5.0000	0	
pavel	24	4.1000	1	
yuri	20	4.7000	0	
stmt > SELECT name, age, avg_grade, expelled FROM itmo WHERE name == 'boris';
SELECT: OK
name	age	avg_grade	expelled	
boris	20	4.5000	1	
stmt > SELECT name, age, avg_grade, expelled FROM itmo WHERE expelled == false AND avg_grade > 4.6 OR name == 'boris';
SELECT: OK
name	age	avg_grade	expelled	
boris	20	4.5000	1	
alex	19	5.0000	0	
yuri	20	4.7000	0	
stmt > UPDATE itmo SET expelled = false WHERE name == 'boris';
DELETE: OK, ROWS UPDATED: 1
stmt > SELECT name, age, avg_grade, expelled FROM itmo WHERE name == 'boris';
SELECT: OK
name	age	avg_grade	expelled	
boris	20	4.5000	0	
stmt > DELETE FROM itmo WHERE avg_grade < 4.6;
DELETE: OK, ROWS REMOVED: 2
stmt > SELECT name, avg_grade FROM itmo;
SELECT: OK
name	avg_grade	
alex	5.0000	
yuri	4.7000	
stmt > 
```
## Аспекты реализации

Для сериализации и обмена между клиентом и сервером требуется привести исходные структуры дерева запроса в структуры, генерируемые Thrift.

Пример сериализатора:

[t_client_serializer.c](serializer/t_client_serializer.c)
```c
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
```

Для установления соединения и обмена данными используются структуры библиотеки thrift_c_glib

[client.c](client.c)
```c
    ThriftSocket *socket;
    ThriftTransport *transport;
    ThriftProtocol *protocol;

    data_exchange_serviceIf *service;

    gboolean success;
    gchar *message = NULL;
    server_response_T *result = NULL;
    GError *error = NULL;

#if (!GLIB_CHECK_VERSION(2, 36, 0))
    g_type_init ();
#endif

    socket = g_object_new(THRIFT_TYPE_SOCKET,
                          "hostname", hostname,
                          "port", port,
                          NULL);
    transport = g_object_new(THRIFT_TYPE_BUFFERED_TRANSPORT,
                             "transport", socket,
                             NULL);
    protocol = g_object_new(THRIFT_TYPE_BINARY_PROTOCOL,
                            "transport", transport,
                            NULL);

    thrift_transport_open(transport, &error);

    service = g_object_new(TYPE_DATA_EXCHANGE_SERVICE_CLIENT,
                          "input_protocol", protocol,
                          "output_protocol", protocol,
                          NULL);

    char buffer[BUF_SIZE];
    buffer[0] = '\0';

    while (true) {
        printf("stmt > ");
        fgets(buffer, BUF_SIZE, stdin);
        buffer[BUF_SIZE - 1] = '\0';
        if (strcmp(buffer, "exit;\n") == 0)
            return 0;
        statement s = get_statement(buffer);
        statement_T *st = t_serialize_statement(&s);

        if (!st) continue;

        result = g_object_new(TYPE_SERVER_RESPONSE__T, NULL);
        success = data_exchange_service_if_execute(service, &result, st, &error);
        if (success) {
            g_object_get(result, "msg", &message, NULL);
            fprintf(stdout, "%s", message);
            if (result->__isset_items) {
                print_items(result->items);
            }
        } else {
            fprintf(stderr, "Exception: %s\n", error->message);
            g_clear_error(&error);
        }
    }
```

Сам же RPC-вызов сервисного метода, определенного в thrift происходит в данной строчке:
```c
success = data_exchange_service_if_execute(service, &result, st, &error);
```

Для того, чтобы он отработал корректно, на стороне сервера требуется реализовать несколько методов:

[server.c](server.c)
```c
// ================ START OF IMPLEMENTATION ================


G_DEFINE_TYPE(data_exchange_handler_impl,
              data_exchange_handler,
              TYPE_DATA_EXCHANGE_SERVICE_HANDLER
)

static database* db;

static gboolean data_exchange_handler_execute (data_exchange_serviceIf *iface, server_response_T **_return, const statement_T * stmt, GError **error) {
    THRIFT_UNUSED_VAR(iface);
    THRIFT_UNUSED_VAR(error);

    server_response_T* response = process_client_request(stmt, db);

    g_object_set(*_return, "status", response->status,
                 "msg", strdup(response->msg), NULL);
    if (response->__isset_items) {
        g_object_set(*_return,
                     "items", response->items,
                     NULL);
    }

    return true;
}

static void
data_exchange_handler_finalize (GObject *object)
{
    data_exchange_handler_impl *self =
    DATA_EXCHANGE_HANDLER (object);

    /* Chain up to the parent class */
    G_OBJECT_CLASS (data_exchange_handler_parent_class)->
            finalize (object);
}

static void
data_exchange_handler_init (data_exchange_handler_impl *self)
{
    // no fields -> empty initializer
}

/* TutorialCalculatorHandler's class initializer */
static void
data_exchange_handler_class_init (data_exchange_handler_implClass *klass)
{
    GObjectClass *gobject_class = G_OBJECT_CLASS (klass);

    data_exchange_serviceHandlerClass *dataExchangeServiceHandlerClass =
    DATA_EXCHANGE_SERVICE_HANDLER_CLASS(klass);

    gobject_class->finalize = data_exchange_handler_finalize;

    dataExchangeServiceHandlerClass->execute =
            data_exchange_handler_execute;

}

// ================ END OF IMPLEMENTATION ================

```

Метод c сигнатурой 

```c
static gboolean data_exchange_handler_execute (data_exchange_serviceIf *iface, server_response_T **_return, const statement_T * stmt, GError **error)
```
и есть метод вызываемый клиентом удаленно. 

Он просит модуль ```request_processor``` обработать полученный запрос и сформировать ответ, который отправляется обратно на клиент.

Сам же ```request_processor``` разбирает полученный запрос по типу, делает нужные конвертации и вызывает соответствующие интерфейсы модуля из л.р № 1.

[request_processor.c](request_processor.c)
```c
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

    if (!success)
        return NEW_RESPONSE(STATUS_CODE__T_INTERNAL_ERROR, "INTERNAL SERVER ERROR\n");

    return NEW_RESPONSE(STATUS_CODE__T_OK, "INSERT: OK\n");

}
```
## Результаты

Структуры Apache Thrift
[structs.thrift](structs.thrift)
```
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

```


## Выводы

В ходе выполнения задания были изучены язык описания интерфейсов и моделей Apache Thrift, а также сопутствующие ему инструменты
в виде библиотеки glibc и gobject. Удалось реализовать клиент-серверное взаимодействие на базе диспетчерезации удаленных вызовов.
При выполнении работы пришлось столкнуться с особенностями кодогенерации Thrift в Си.
Во-первых, структуры, содержащие ссылки на себя, генерируются некорректно (отсутствует forward-declaration)
Кроме того, инициализация gobject структуры запускает инициализацию всех вложенных в нее структур, что в некоторых случаях порождает
переполнение стека. В любом случае было интересно поработать с объектно-ориентированной обёрткой над Си, предоставляемой glibc и gobject :)

