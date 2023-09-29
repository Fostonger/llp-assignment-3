//
// Created by rosroble on 12.10.22.
//

#ifndef LAB1_RDB_DATA_ITERATOR_H
#define LAB1_RDB_DATA_ITERATOR_H

typedef struct data_iterator data_iterator;

#include "database.h"
#include "data.h"
#include "../gen-c_glib/structs_types.h"

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
bool seekNextPredicate(data_iterator* iter, predicate_T* pred);
bool seekNextWhere(data_iterator* iter, const char* colName, columnType colType, const void* val);
uint16_t deleteWhere(database* db, table*tb, const char* colName, columnType colType, const void* val);
uint16_t deleteWherePredicate(database* db, table* tb, predicate_T* pred);
columnType discoverColumnTypeByName(data_iterator* iter, const char* name);
uint16_t updateWhere(database* db, table* tb,
                     const char* whereColName, columnType whereColType, const void* whereVal,
                     const char* updateColName, columnType updateColType, const void* updateVal);
uint16_t updateWherePredicate(database* db, table* tb, predicate_T* pred, GPtrArray * sets);
dataHeader getDataHeader(data_iterator* iter);
bool getInteger(data_iterator* iter, const char* columnName, int32_t* dest);
bool getString(data_iterator* iter, const char* columnName, char** dest);
bool getBool(data_iterator* iter, const char* columnName, bool* dest);
bool getFloat(data_iterator* iter, const char* columnName, double* dest);
void printJoinTable(database* db, table* tb1, table* tb2, const char* onColumnT1, const char* onColumnT2, columnType type);
#endif //LAB1_RDB_DATA_ITERATOR_H
