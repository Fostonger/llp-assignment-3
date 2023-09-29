# Лабораторная работа №1

## Сборка

Сборка происходит при помощи утилиты CMake. Стандарт языка должен быть минимум C11 или C++17 (для использования time.h).

```cmake --build cmake-build-debug --target lab1_rdb -j 6```
## Цели задания

- Реализовать модуль хранения данных в файле в виде РСУБД
- Так как подразумевается хранение огромных массивов данных (больше чем размер ОЗУ), необходимо разработать архитектуру порционного доступа к данным без необходимости загружать данные целиком в память
- Организовать типизацию данных: поддерживать целые числа, строки, числа с плавающей точкой, булевы значения
- Реализовать интерфейсы для создания / удаления схемы, вставки, выборки (в т.ч по условию), удаления и обновления данных, учитывая отраженные в тексте задания ограничения по асимптотической сложности

- Обеспечить работоспособность решения на ОС Windows и Linux
## Задачи и ход работы

- Продумать и написать структуры и их содержимое (заголовки базы, заголовки для данных и др.)
- Определить набор интерфейсов для написанных структур
- Придумать реализацию принятых интерфейсов с учетом наложенных ограничений
- Протестировать функциональность написанных реализаций
- Протестировать соответствие написанных реализаций временным ограничениям
- Сделать профилирование на предмет обнаружения утечек памяти
- Протестировать работоспособность на ОС Windows и Linux


## Описание работы

Программа логически разделена на 4 части:

### Database

В этой части программы реализована корневая архитектура базы данных, а именно:

- Структура для заголовка базы данных
- Структура страницы и её заголовка
- Интерфейсы для открытия / закрытия базы данных, аллокации страниц, сериализации/десериализации данных в/из файл(а)

Примеры структур и интерфейсов:

```c
struct database {
    FILE* file;
    db_header* header;
    page* available_page;
};

struct db_header {
    uint64_t page_size;
    uint16_t pages_amount;
    uint16_t firstInvalidRowPage;
    uint16_t firstInvalidRowOrdinal;
    uint16_t first_free_page_ordinal;
};

struct pageHeader {
    bool free;
    uint8_t page_ordinal;
    uint16_t rowsAmount;
    uint32_t freeRowOffset;
    uint32_t next_page_ordinal;
};

struct page {
    pageHeader* header;
    tableHeader* tableHeader;
    page* nextPage;
    void* bytes;
};
```


```c
bool write_db_header(FILE* file, db_header* header);
page* allocate_page(database* db);
page* read_page(database* db, uint32_t pageOrdinal);
bool write_page_to_file(page* pg, FILE* file);
database* init_db(FILE* file, bool overwrite);
void close_db(database* db);
void close_page(page* pg);
bool pageHasSpaceForWrite(page* pg);
void table_page_link(page* pg, table* tb);
```

### Table

В этой части программы скрыта логика работы со схемами таблицы:

- Структуры для таблиц и заголовков таблиц
- Структуры для колонок таблиц и заголовков колонок
- Типы колонок
- Интерфейсы по открытию/закрытию таблицы, её привязки к конкретной базе данных, инициализация схемы

Примеры структур и интерфейсов:

```c
struct table{
    tableHeader* header;
    page* firstPage;
    page* firstAvailableForWritePage;
};

struct tableHeader {
    uint32_t thisSize;
    uint16_t oneRowSize;
    uint8_t columnAmount;
    uint8_t tableNameLength;
    char tableName[MAX_NAME_LENGTH];
    columnHeader columns[];
};

struct column {
    columnHeader* header;
    char* columnName;
};

struct columnHeader {
    columnType type;
    uint8_t size;
    char columnName[MAX_NAME_LENGTH];
};

enum columnType {
    INTEGER, STRING, BOOLEAN, FLOAT
};
```

```c
table* new_table(char* name, uint8_t columnAmount);
table* open_table(database* db, char* name);
uint8_t typeToColumnSize(columnType type);
void init_fixedsize_column(table* table, uint8_t ordinal, const char* name, columnType type);
void table_apply(database* db, table* table);
```
### Data

В этом модуле скрыта логика для инкапсулирования данных, принадлежащих одной атомарной единице (строке таблицы) перед их отправлением в таблицу:

- Структуры для данных (строк) и их заголовки
- Интерфейсы для инициализации структур конкретными данными разных типов

Примеры структур и интерфейсов:

```c
struct __attribute__((packed)) dataHeader {
    bool valid;
    int32_t nextInvalidRowPage;
    int32_t nextInvalidRowOrdinal;
};

struct data {
    dataHeader* dataHeader;
    table* table;
    void** bytes;
    uint16_t ptr;
};

data* init_data(table* tb);
void data_init_integer(data* dt, int32_t val);
void data_init_string(data* dt, char* val);
void data_init_boolean(data* dt, bool val);
void data_init_float(data* dt, double val);
void data_init_some(data* dt, columnType type, void* val);
bool insert_data(data* data, database* db);
```

### Data Iterator

В этом модуле скрыта логика по манипуляции с уже существующими в таблице данными, в т.ч выборка, условная выборка, условное удаление, условное обновление, соединение:

- Структура итератора
- Интерфейсы для порционной выборки данных (т.е постраничного чтения)
- Интерфейсы для модификации данных
- Интерфейсы для извлечения конкретных типов из набора прочитанных байтов

Примеры структур и интерфейсов:

```c
struct data_iterator {
    database* db;
    table* tb;
    page* curPage;
    uint16_t ptr;
    uint16_t rowsReadOnPage;
};



data_iterator* init_iterator(database* db, table* tb);
int32_t getOffsetToColumnData(data_iterator* iter, const char* columnName, columnType colType);
bool hasNext(data_iterator* iter);
bool seekNext(data_iterator* iter);
bool seekNextWhere(data_iterator* iter, const char* colName, columnType colType, const void* val);
uint16_t deleteWhere(database* db, table*tb, const char* colName, columnType colType, const void* val);
uint16_t updateWhere(database* db, table* tb,
                     const char* whereColName, columnType whereColType, const void* whereVal,
                     const char* updateColName, columnType updateColType, const void* updateVal);
dataHeader getDataHeader(data_iterator* iter);
bool getInteger(data_iterator* iter, const char* columnName, int32_t* dest);
bool getString(data_iterator* iter, const char* columnName, char** dest);
bool getBool(data_iterator* iter, const char* columnName, bool* dest);
bool getFloat(data_iterator* iter, const char* columnName, double* dest);
void printJoinTable(database* db, table* tb1, table* tb2, const char* onColumnT1, const char* onColumnT2, columnType type);
```

### Аспекты реализации

Методика хранения данных следующая:

- Существует структура базы данных, одна база данных отражается в один файл.
- База данных состоит из заголовка базы данных и набора страниц одинакового размера (по умолчанию 4 KBytes)
- В заголовке базы данных хранится размер одной страницы, общее количество страниц
- Каждая страница начинается с собственного заголовка. Каждая страница принадлежит только одной таблице (на одной странице не хранятся данные для разных схем).
- Заголовок страницы содержит в себе статус (доступна / недоступна для записи), порядковый номер страницы, количество данных (строк) на странице, отступ до ближайшего свободного для записи места на странице
- Сама страница представляет из себя набор данных (строк): заголовки и сами значения в виде байтов

Для начала работы необходимо открыть файл, в котором будет осуществлено хранение инициализировать базу данных:

```c
database* init_db(FILE* file, bool overwrite) {
    if (overwrite) {
        return overwrite_db(file);
    }
    return read_db(file);
}
```

После чего создаётся новая таблица и инициализируется её схема

```c
table* tb = new_table("table_name", 3);
init_fixedsize_column(tb, 0, "id", INTEGER);
init_varsize_column(tb, 1, "name", STRING, 40);
init_fixedsize_column(tb, 2, "expelled", BOOLEAN);
table_apply(db, tb);
```

При помощи ```init_varsize_column()``` можно задавать строки произвольной длины (подобно VARCHAR(N) в современных РСУБД).

Функция ```table_apply()``` фиксирует схему таблицы и привязывает таблицу к базе данных. После чего в файле базы данных для этой таблицы выделяется первая страница и записывается в файл:

```c
void table_apply(database* db, table* table) {
    page* pg = allocate_page(db);
    table_page_link(pg, table);
    table->firstPage = pg;
    table->firstAvailableForWritePage = pg;
    if (db) {
        write_page_to_file(pg, db->file);
    }
}
```

Заполнение таблицы данными происходит при помощи структуры ```data```

```c
data *dt1 = init_data(tb);
data_init_integer(dt1, i); 
data_init_string(dt1, "zxcvb"); 
data_init_boolean(dt1, true); 
insert_data(dt1, db);
```

Данные на страницы хранятся последовательно друг за другом, каждой порции содержимого предшествует заголовок

Структура ```data``` обеспечивает обертку данных в последовательность байтов

```c
void data_init_integer(data* dt, int32_t val) {
    char* ptr = (char*) dt->bytes + dt->ptr;
    *((int32_t*) (ptr)) = val;
    dt->ptr += typeToColumnSize(INTEGER);
}

void data_init_string(data* dt, char* val) {
    char* ptr = (char*) dt->bytes + dt->ptr;
    strcpy((char *) (ptr), val);
    dt->ptr += typeToColumnSize(STRING);
}

void data_init_boolean(data* dt, bool val) {
    char* ptr = (char*) dt->bytes + dt->ptr;
    *((bool*) (ptr)) = val;
    dt->ptr += typeToColumnSize(BOOLEAN);
}

void data_init_float(data* dt, double val) {
    char* ptr = (char*) dt->bytes + dt->ptr;
    *((double*) (ptr)) = val;
    dt->ptr += typeToColumnSize(FLOAT);
}
```

После инициализации структуры данными требуется вызвать функцию ```insert_data()``` для вставки данных и сохранении изменений в файл.

```insert_data()``` берет заранее найденную (при открытии таблицы) область файла (страницу), доступную для записи, загружает страницу в память, копирует строчку в страницу и сохраняет страницу в файл.

При этом, если при вставке оказалось, что места на странице больше нет, то выделяется новая страница (старая при этом закрывается) и данные записываются туда, после чего из памяти уничтожается структура ```data```

```c
bool insert_data(data* dt, database* db) {
    bool success = false;
    page* pg = dt->table->firstAvailableForWritePage;
    uint32_t offset = dt->table->header->oneRowSize * pg->header->rowsAmount;
    memcpy(pg->bytes + offset, dt->bytes, dt->ptr);
    pg->header->rowsAmount++;
    pg->header->freeRowOffset += pg->tableHeader->oneRowSize;
    if (!pageHasSpaceForWrite(pg)) {
        pg->header->free = false;
        dt->table->firstAvailableForWritePage = allocate_page(db);
        pg->header->next_page_ordinal = dt->table->firstAvailableForWritePage->header->page_ordinal;
        table_page_link(dt->table->firstAvailableForWritePage, dt->table);
        if (!pg->nextPage) {
            pg->nextPage = dt->table->firstAvailableForWritePage;
        }
        if (pg != dt->table->firstPage) {
            if (db) {
                success = write_page_to_file(pg, db->file);
                close_page(pg);
            }
            close_data(dt);
            return success;
        }
    }
    success = write_page_to_file(pg, db->file);
    close_data(dt);
    return success;
}
```

Доступ к уже существующим в файле данным происходит при помощи ```data_iterator``` и интерфейсов для работы с ним

```c
while (seekNext(iter1)) {     
    getInteger(iter1, "id", &a);
    getString(iter1, "name", &b);
    getBool(iter1, "expelled", &c);

    printf("%d %s %d\n", a, b, c);
}
```

Доступна так же выборка с условием

```c
while (seekNextWhere(iter1, "name", STRING, (void*) &str)) {
    getInteger(iter1, "id", &a);
    getString(iter1, "name", &b);
    getBool(iter1, "expelled", &c);
    
    printf("%d %s %d\n", a, b, c);
}
```

Итератор работает следующим образом:

- На вход функции подается итератор для конкретной таблицы
- Функция смещает указатель на начало следующей порции валидных (не помеченных как удаленные) данных (строчки) и возвращает ```true```
- После этого можно прочитать конкретное значение указываемой итератором строки при помощи функций ```get*Type*()```, передав туда имя желаемого столбца
- Если страница с данными закончилась, то функция освобождает её, загружая следующую и пытается читать данные оттуда
- Если данных не осталось функция возвращает ```false```

### Результаты

Тайминги на запись (мкС) (25 таймингов - перед каждым замером размер таблицы увеличивается на 100 строчек):

[31	47	31	30	32	29	51	28	42	57	35	30	31	42	28	47	28	29	27	30	41	47	65	28	28]

![Write Timings](imgs/write.png)

Тайминги на чтение (мкС) (10 таймингов - перед каждым замером размер таблицы увеличивается на 10000 строчек):

[7064 12440 19398 23516 29633 34901 39892 46093 52070 57031]

![Read Timings](imgs/read.png)

Тайминги на обновление (мкС) (10 - таймингов - перед каждым замером размер таблицы увеличивается на 10000 строчек):

[94554 125218 166110 207843 247064 297709 326919 367113 412032 447345]

![Update Timings](imgs/update.png)

Тайминги на удаление (мкС) (10 таймингов - перед каждым замером размер таблицы увеличивается на 10000 строчек):

[50930 46525 52056 57043 61619 66351 72121 76073 81358 86312]

![Delete Timings](imgs/delete.png)

### Выводы

В ходе выполнения лабораторной работы получилось реализовать что-то похожее на РСУБД, были реализованы основные функции,
изучены принципы страничной архитектуры и чтения данных порциями, что позволяет хранить в БД большие массивы данных.

