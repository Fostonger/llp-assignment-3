#include <stdio.h>
#include "util.h"
#include "table.h"
#include "data.h"
#include "data_iterator.h"

// #define DEBUG

#ifdef DEBUG
#define DEBUG_PRINT(...) do{ fprintf( stderr, __VA_ARGS__ ); } while( false )
#else
#define DEBUG_PRINT(...) do{ } while ( false )
#endif

void test_read_where(FILE* file) {

    DEBUG_PRINT("---- READ_WHERE TEST STARTED ----\n");

    database* db = init_db(file, false);

    table* tb = open_table(db, "table_name");

    data_iterator* iter1 = init_iterator(db, tb);

    const char* str = "zxcvb";

    int32_t a;
    char* b = malloc(255 * sizeof(char));
    bool c;

    while (seekNextWhere(iter1, "name", STRING, (void*) &str)) {
        getInteger(iter1, "id", &a);
        getString(iter1, "name", &b);
        getBool(iter1, "expelled", &c);

        DEBUG_PRINT("%d %s %d\n", a, b, c);
    }

    close_db(db);
    DEBUG_PRINT("---- READ_WHERE TEST FINISHED ----\n");

}


void test_join(FILE* file) {
    DEBUG_PRINT("---- JOIN TEST STARTED ----\n");
    database* db = init_db(file, false);

    table* tb1 = open_table(db, "table_name");
    table* tb2 = open_table(db, "secondTable");

    printJoinTable(db, tb1, tb2, "id", "number", INTEGER);

}

void initSchema(database* db) {
    table* tb = new_table("table_name", 3);
    init_fixedsize_column(tb, 0, "id", INTEGER);
    init_varsize_column(tb, 1, "name", STRING, 40);
    init_fixedsize_column(tb, 2, "expelled", BOOLEAN);
    table_apply(db, tb);

    table* tb2 = new_table("secondTable", 2);
    init_fixedsize_column(tb2, 0, "number", INTEGER);
    init_varsize_column(tb2, 1, "title", STRING, 100);
    table_apply(db, tb2);

    close_table(tb);
    close_table(tb2);
}

time_t testWrite(database* db, int rowsAmount) {
    DEBUG_PRINT("---- WRITE TEST STARTED ----\n");
    table *tb, *tb2;
    tb = open_table(db, "table_name");
    tb2 = open_table(db, "secondTable");

    struct timespec start = getCurrentTime();
    for (int i = 0; i < rowsAmount; i++) {
        data *dt1 = init_data(tb);
        data_init_integer(dt1, i, "id");
        data_init_string(dt1, "zxcvb", "name");
        data_init_boolean(dt1, i % 2, "expelled");
        insert_data(dt1, db);

        data *dt2 = init_data(tb);
        data_init_integer(dt2, 16, "id");
        data_init_string(dt2, i % 2 ? "abcde" : "ABCDE", "name");
        data_init_boolean(dt2, (i + 1) % 2, "expelled");
        insert_data(dt2, db);

        data *dt3 = init_data(tb2);
        data_init_integer(dt3, i, "number");
        data_init_string(dt3, "wowowo", "title");
        insert_data(dt3, db);
    }
    struct timespec finish = getCurrentTime();
    DEBUG_PRINT("---- WRITE TEST FINIHSED ----\n");
    close_table(tb);
    close_table(tb2);
    return timediff_microseconds(start, finish);
}

time_t testReadAll(database* db) {
    DEBUG_PRINT("---- READ TEST STARTED ----\n");

    table* tb = open_table(db, "table_name");

    data_iterator* iter1 = init_iterator(db, tb);
    int32_t a;
    char* b = malloc(255 * sizeof(char));
    bool c;

    struct timespec start = getCurrentTime();
    int count = 0;
    while (seekNext(iter1)) {
        getInteger(iter1, "id", &a);
        getString(iter1, "name", &b);
        getBool(iter1, "expelled", &c);
        count++;
        DEBUG_PRINT("%d %s %d\n", a, b, c);
    }
    struct timespec finish = getCurrentTime();

    DEBUG_PRINT("---- READ TEST FINISHED ----\n");

    close_table(tb);
    free(b);
    free(iter1);
    return timediff_microseconds(start, finish);
}

time_t testUpdate(database* db, char* whereColName, columnType whereColType, void* whereVal,  char* updateColName, columnType updateColType, void* updateTo) {

    DEBUG_PRINT("---- UPDATE TEST STARTED ----\n");

    table* tb = open_table(db, "table_name");

    struct timespec start = getCurrentTime();
    uint16_t updatedRows = updateWhere(db, tb, whereColName, whereColType, whereVal, updateColName, updateColType, updateTo);
    struct timespec finish = getCurrentTime();
    close_table(tb);
    DEBUG_PRINT("---- UPDATE TEST FINISHED. ROWS UPDATED: %d ----\n", updatedRows);
    return timediff_microseconds(start, finish);
}

time_t testDelete(database* db, char* whereName, columnType whereType, void* value) {
    DEBUG_PRINT("---- DELETE TEST STARTED ----\n");

    table* tb = open_table(db, "table_name");
    const char* str = "zxcvb";
    int ts = 5;

    struct timespec start = getCurrentTime();
    uint16_t deletedRows = deleteWhere(db, tb, whereName, whereType, value);
    struct timespec finish = getCurrentTime();
    DEBUG_PRINT("---- DELETE TEST FINISHED. ROWS REMOVED: %d ----\n", deletedRows);
    close_table(tb);
    return timediff_microseconds(start, finish);
}

void printTime(long long time[], int length) {
    printf("Time: [");
    for (int i = 0; i < length; i++) {
        printf("%lld ", time[i]);
    }
    printf("]\n");
}

int main() {
    char* filename = "db.db";
    FILE* file = fopen(filename, "wb+");
    if (!file) {
        DEBUG_PRINT("Unable to open file.");
        exit(EXIT_FAILURE);
    }
    database* db = init_db(file, true);
    initSchema(db);
    const int writeChecksAmount = 25;
    const int accessChecksAmount = 10;

    testWrite(db, 100);
    long long writeTimes[writeChecksAmount];
    for (int i = 0; i < writeChecksAmount; i++) {
        writeTimes[i] = testWrite(db, 2);
        testWrite(db, 100);
    }
    printf("WRITE TIME: \n");
    printTime(writeTimes, writeChecksAmount);


    long long readTimes[accessChecksAmount];
    long long updateTimes[accessChecksAmount];
    long long deleteTimes[accessChecksAmount];

    for (int i = 0; i < accessChecksAmount; ++i) {
        testWrite(db, 10000);
        readTimes[i] = testReadAll(db);

        char* whereVal = "ABCDE";
        bool updateTo = true;
        updateTimes[i] = testUpdate(db, "expelled", STRING, &whereVal, "expelled", BOOLEAN, &updateTo);

        char* deleteValue = "ASDF";
        deleteTimes[i] = testDelete(db, "name", STRING, &deleteValue);
    }


    printf("READ TIME: \n");
    printTime(readTimes, accessChecksAmount);

    printf("UPDATE TIME: \n");
    printTime(updateTimes, accessChecksAmount);

    printf("DELETE TIME: \n");
    printTime(deleteTimes, accessChecksAmount);

    // test_join(file);
    close_db(db);
    return 0;
}
