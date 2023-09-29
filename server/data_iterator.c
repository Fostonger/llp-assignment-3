//
// Created by rosroble on 12.10.22.
//

#include <glib-2.0/glib.h>
#include <glib-2.0/glib-object.h>
#include "data_iterator.h"
#include "../gen-c_glib/structs_types.h"

#define DECLARE_NUMBER_COMPARE_FUNCTION(Type, name) \
bool compare_nums_##name (comparison comp, const Type* arg1, const Type* arg2) { \
    switch (comp) {              \
        case GREATER:            \
            return *arg1 > *arg2; \
        case LESS:            \
            return *arg1 < *arg2; \
        case NEQUAL:            \
            return *arg1 != *arg2; \
        case EQUAL:            \
            return *arg1 == *arg2; \
        case GREATER_OR_EQUAL:            \
            return *arg1 >= *arg2; \
        case LESS_OR_EQUAL:            \
            return *arg1 <= *arg2;\
        default:                 \
            return false;\
    }                                               \
}

enum binop {
    AND = 1,
    OR = 2
};

enum comparison {
    GREATER = 1,
    LESS = 2,
    NEQUAL = 3,
    EQUAL = 4,
    GREATER_OR_EQUAL = 5,
    LESS_OR_EQUAL = 6
};

union value {
    int32_t colInt;
    char** colChar;
    bool colBool;
    double colFloat;
};

typedef enum comparison comparison;
typedef union value value;

data_iterator* init_iterator(database* db, table* tb) {
    data_iterator* iter = malloc(sizeof (data_iterator));
    iter->tb = tb;
    iter->db = db;
    iter->curPage = tb->firstPage;
    iter->ptr = 0;
    iter->rowsReadOnPage = 0;
    return iter;
}

int32_t getOffsetToColumnData(data_iterator* iter, const char* columnName, columnType colType) {
    int32_t offsetToColumnData = sizeof(dataHeader);
    bool match = false;
    for (int i = 0; i < iter->tb->header->columnAmount; i++) {
        columnHeader currentColumn = iter->tb->header->columns[i];
        if (currentColumn.type == colType && strcmp(columnName, currentColumn.columnName) == 0) {
            match = true;
            break;
        }
        offsetToColumnData += currentColumn.size;
    }
    if (!match) return -1;
    return offsetToColumnData;
}

bool hasNext(data_iterator* iter) {
    return iter->ptr + iter->tb->header->oneRowSize <= DEFAULT_PAGE_SIZE - sizeof(tableHeader) - sizeof(pageHeader)
    && iter->rowsReadOnPage < iter->curPage->header->rowsAmount;
}

bool seekNext(data_iterator* iter) {
    dataHeader dHeader = {false};
    uint32_t nextPageNumber = iter->curPage->header->next_page_ordinal;
    while (hasNext(iter) || nextPageNumber != 0) {
        do {
            if (iter->rowsReadOnPage > 0) {
                iter->ptr += iter->tb->header->oneRowSize;
            }
            dHeader = getDataHeader(iter);
            iter->rowsReadOnPage += 1;
        } while (hasNext(iter) && !dHeader.valid);

        if (dHeader.valid) {
            return true;
        }

        if (iter->curPage != iter->tb->firstPage && iter->curPage != iter->tb->firstAvailableForWritePage) {
            close_page(iter->curPage);
        }
        if (nextPageNumber == 0) {
            break;
        }
        iter->curPage = read_page(iter->db, nextPageNumber);
        nextPageNumber = iter->curPage->header->next_page_ordinal;
        iter->rowsReadOnPage = 0;
        iter->ptr = 0;
    }
    return false;
}

// deprecated
bool seekNextWhere(data_iterator* iter, const char* colName, columnType colType, const void* val) {
    dataHeader dHeader = {false};

    int32_t colInt;
    char* colChar;
    bool colBool;
    double colFloat;
    uint32_t nextPageNumber = iter->curPage->header->next_page_ordinal;
    while (hasNext(iter) || nextPageNumber != 0) {
        do {
            if (iter->rowsReadOnPage > 0) {
                iter->ptr += iter->tb->header->oneRowSize;
            }
            dHeader = getDataHeader(iter);
            iter->rowsReadOnPage += 1;
        } while (hasNext(iter) && !dHeader.valid);

        if (dHeader.valid) {
            switch (colType) {
                case INTEGER:
                    getInteger(iter, colName, &colInt);
                    if (colInt == *((int32_t *) val)) {
                        return true;
                    }
                    continue;
                case STRING:
                    colChar = malloc(sizeof(char) * DEFAULT_STRING_LENGTH);
                    getString(iter, colName, &colChar);
                    if (strcmp(colChar, *((char**) val)) == 0) {
                        free(colChar);
                        return true;
                    }
                    free(colChar);
                    continue;
                case BOOLEAN:
                    getBool(iter, colName, &colBool);
                    if (colBool == *((bool *) val)) {
                        return true;
                    }
                    continue;
                case FLOAT:
                    getFloat(iter, colName, &colFloat);
                    if (colFloat == *((double *) val)) {
                        return true;
                    }
                    continue;
                default:
                    break;
            }
        }

        if (iter->curPage != iter->tb->firstPage && iter->curPage != iter->tb->firstAvailableForWritePage) {
            close_page(iter->curPage);
        }
        if (nextPageNumber == 0) {
           break;
        }
        iter->curPage = read_page(iter->db, nextPageNumber);
        nextPageNumber = iter->curPage->header->next_page_ordinal;
        iter->rowsReadOnPage = 0;
        iter->ptr = 0;
    }
    return false;
}

columnType discoverColumnTypeByName(data_iterator* iter, const char* name) {
    for (size_t i = 0; i < iter->tb->header->columnAmount; i++) {
        if (strcmp(name, iter->tb->header->columns[i].columnName) == 0) {
            return iter->tb->header->columns[i].type;
        }
    }
}

columnType extractColumnValue(data_iterator* iter, columnref_T* col, value* val) {
    columnType type = discoverColumnTypeByName(iter, col->col_name);
    switch (type) {
        case INTEGER:
            getInteger(iter, col->col_name, &val->colInt);
            break;
        case FLOAT:
            getFloat(iter, col->col_name, &val->colFloat);
            break;
        case STRING:
            getString(iter, col->col_name, val->colChar);
            break;
        case BOOLEAN:
            getBool(iter, col->col_name, &val->colBool);
        default:
            break;
    }
    return type;
}

DECLARE_NUMBER_COMPARE_FUNCTION(int32_t, int)

DECLARE_NUMBER_COMPARE_FUNCTION(bool, bool)

DECLARE_NUMBER_COMPARE_FUNCTION(double, float)

bool compare_string(comparison comp, char** arg1, char** arg2) {
    int compresult = strcmp(*arg1, *arg2);
    switch (comp) {
        case GREATER:
            return compresult > 0;
        case LESS:
            return compresult < 0;
        case NEQUAL:
            return compresult != 0;
        case EQUAL:
            return compresult == 0;
        case GREATER_OR_EQUAL:
            return compresult >= 0;
        case LESS_OR_EQUAL:
            return compresult <= 0;
        default:
            return false;
    }
}

bool compareNumTypes(columnType type, comparison comp, void* arg1, void* arg2) {
    switch (type) {
        case INTEGER:
            return compare_nums_int(comp, arg1, arg2);
        case BOOLEAN:
            return compare_nums_bool(comp, arg1, arg2);
        case FLOAT:
            return compare_nums_float(comp, arg1, arg2);
        default:
            return false;
    }
}

void* extractValueAddressFromLiteralT(literal_T *lit) {
    switch (lit->type) {
        case LITERAL_TYPE__T_LIT_INTEGER_T:
            return &lit->value->num;
        case LITERAL_TYPE__T_LIT_STRING_T:
            return &lit->value->str;
        case LITERAL_TYPE__T_LIT_BOOLEAN_T:
            return &lit->value->boolean;
        case LITERAL_TYPE__T_LIT_FLOAT_T:
            return &lit->value->flt;
    }
}
bool compare(data_iterator* iter, columnref_T* col, predicate_arg_T* arg, comparison cmp) {
    value columnValue;
    char buffer[256];
    char* bufferAddress = &buffer[0];
    columnValue.colChar = &bufferAddress;
    columnType colRefType = extractColumnValue(iter, col, &columnValue);

    value argValue;
    char argBuffer[256];
    char* argBufferAddress = &argBuffer[0];
    argValue.colChar = &argBufferAddress;
    columnType argType;

    switch (arg->type) {
        case PREDICATE_ARG_TYPE__T_LITERAL_T:
            if (arg->arg->literal->type == LITERAL_TYPE__T_LIT_STRING_T) {
                return compare_string(cmp,
                                      columnValue.colChar,
                                      &arg->arg->literal->value->str);
            }
            return compareNumTypes(colRefType,
                                   cmp,
                                   &columnValue,
                                   extractValueAddressFromLiteralT(arg->arg->literal));
        case PREDICATE_ARG_TYPE__T_REFERENCE_T:
            argType = extractColumnValue(iter, arg->arg->ref, &argValue);
            if (argType != colRefType) return false;
            return compareNumTypes(argType, cmp, &columnValue, &argValue);
        default:
            return false;
    }
}


bool isPredicateTrue(data_iterator* iter, predicate_T* pred) {
    if (!pred->__isset_arg) return true;

    if (pred->type == PREDICATE_TYPE__T_COMPARISON_T) {
        return compare(iter, pred->column, pred->arg, pred->cmp_type);
    }

    switch (pred->predicate_op) {
        case AND:
            return
            isPredicateTrue(iter, (predicate_T*) pred->left->pdata[0])
            && isPredicateTrue(iter, (predicate_T*) pred->right->pdata[0]);
        case OR:
            return
            isPredicateTrue(iter, (predicate_T*) pred->left->pdata[0])
            || isPredicateTrue(iter, (predicate_T*) pred->right->pdata[0]);
    }

    // not-reachable (normally)
    return false;
}


bool seekNextPredicate(data_iterator* iter, predicate_T* pred) {
    dataHeader dHeader = {false};

    int32_t colInt;
    char* colChar;
    bool colBool;
    double colFloat;
    uint32_t nextPageNumber = iter->curPage->header->next_page_ordinal;
    while (hasNext(iter) || nextPageNumber != 0) {
        do {
            if (iter->rowsReadOnPage > 0) {
                iter->ptr += iter->tb->header->oneRowSize;
            }
            dHeader = getDataHeader(iter);
            iter->rowsReadOnPage += 1;
        } while (hasNext(iter) && !dHeader.valid);

        if (dHeader.valid) {
            bool predicate_true = isPredicateTrue(iter, pred);
            if (predicate_true) return true;
            continue;
        }

        if (iter->curPage != iter->tb->firstPage && iter->curPage != iter->tb->firstAvailableForWritePage) {
            close_page(iter->curPage);
        }
        if (nextPageNumber == 0) {
            break;
        }
        iter->curPage = read_page(iter->db, nextPageNumber);
        nextPageNumber = iter->curPage->header->next_page_ordinal;
        iter->rowsReadOnPage = 0;
        iter->ptr = 0;
    }
    return false;
}

dataHeader getDataHeader(data_iterator* iter) {
    return *((dataHeader*)((char*) iter->curPage->bytes + iter->ptr));
}

bool getSome(data_iterator* iter, const char* columnName, void* dest) {
    for (int i = 0; i < iter->tb->header->columnAmount; i++) {
        columnHeader currentColumn = iter->tb->header->columns[i];
        if (strcmp(columnName, currentColumn.columnName) == 0) {
            switch (currentColumn.type) {
                case INTEGER:
                    return getInteger(iter, columnName, dest);
                case STRING:
                    return getString(iter, columnName, (char **) &dest);
                case BOOLEAN:
                    return getBool(iter, columnName, dest);
                case FLOAT:
                    return getFloat(iter, columnName, dest);
                default:
                    return false;
            }
        }
    }
    return false;
}

bool getInteger(data_iterator* iter, const char* columnName, int32_t* dest) {
    int32_t offsetToColumnData = getOffsetToColumnData(iter, columnName, INTEGER);
    if (offsetToColumnData == -1) return false;
    *dest = *((int32_t*)((char*) iter->curPage->bytes + iter->ptr + offsetToColumnData));
    return true;
}

bool getString(data_iterator* iter, const char* columnName, char** dest) {
    int32_t offsetToColumnData = getOffsetToColumnData(iter, columnName, STRING);
    if (offsetToColumnData == -1) return false;
    strcpy(*dest, (char*) iter->curPage->bytes + iter->ptr + offsetToColumnData);
    return true;
}

bool getBool(data_iterator* iter, const char* columnName, bool* dest) {
    int32_t offsetToColumnData = getOffsetToColumnData(iter, columnName, BOOLEAN);
    if (offsetToColumnData == -1) return false;
    *dest = *((bool*)((char*) iter->curPage->bytes + iter->ptr + offsetToColumnData));
    return true;
}

bool getFloat(data_iterator* iter, const char* columnName, double* dest) {
    int32_t offsetToColumnData = getOffsetToColumnData(iter, columnName, FLOAT);
    if (offsetToColumnData == -1) return false;
    *dest = *((double *)((char*) iter->curPage->bytes + iter->ptr + offsetToColumnData));
    return true;
}


// deprecated
uint16_t deleteWhere(database* db, table* tb, const char* colName, columnType colType, const void* val) {
    data_iterator* iter = init_iterator(db, tb);
    uint16_t removedObjects = 0;

    while (seekNextWhere(iter, colName, colType, val)) {
        page* pg = iter->curPage;
        void* pageBytes = pg->bytes;
        dataHeader header = getDataHeader(iter);
        header.valid = false;
        memcpy(pageBytes + iter->ptr, &header, sizeof(dataHeader));
        write_page_to_file(pg, db->file);
        removedObjects++;
    }
    free(iter);
    return removedObjects;
}

uint16_t deleteWherePredicate(database* db, table* tb, predicate_T* pred) {
    data_iterator* iter = init_iterator(db, tb);
    uint16_t removedObjects = 0;

    while (seekNextPredicate(iter, pred)) {
        page* pg = iter->curPage;
        void* pageBytes = pg->bytes;
        dataHeader header = getDataHeader(iter);
        header.valid = false;
        memcpy(pageBytes + iter->ptr, &header, sizeof(dataHeader));
        write_page_to_file(pg, db->file);
        removedObjects++;
    }
    free(iter);
    return removedObjects;
}

// deprecated
uint16_t updateWhere(database* db, table* tb,
                     const char* whereColName, columnType whereColType, const void* whereVal,
                     const char* updateColName, columnType updateColType, const void* updateVal) {
    data_iterator* iter = init_iterator(db, tb);
    uint16_t updatedObjects = 0;
    while (seekNextWhere(iter, whereColName, whereColType, whereVal)) {
        page* pg = iter->curPage;
        void* pageBytes = pg->bytes;
        int32_t columnDataOffset = getOffsetToColumnData(iter, updateColName, updateColType);
        columnHeader* c_header = get_col_header_by_name(iter->tb, updateColName);
        memcpy(pageBytes + iter->ptr + columnDataOffset, updateVal, c_header->size);
        write_page_to_file(pg, db->file);
        updatedObjects++;
    }
    free(iter);
    return updatedObjects;
}

uint16_t updateWherePredicate(database* db, table* tb, predicate_T* pred, GPtrArray * sets) {
    data_iterator* iter = init_iterator(db, tb);
    uint16_t updatedObjects = 0;
    while (seekNextPredicate(iter, pred)) {
        page* pg = iter->curPage;
        void* pageBytes = pg->bytes;
        for (size_t i = 0; i < sets->len; i++) {
            set_value_T *upd = (set_value_T*) sets->pdata[i];
            columnType type = discoverColumnTypeByName(iter, upd->col->col_name);
            int32_t columnDataOffset = getOffsetToColumnData(iter, upd->col->col_name, type);
            columnHeader* c_header = get_col_header_by_name(iter->tb, upd->col->col_name);
            memcpy(pageBytes + iter->ptr + columnDataOffset,
                   extractValueAddressFromLiteralT(upd->lit),
                   c_header->size);
            updatedObjects++;
        }
        write_page_to_file(pg, db->file);
    }
    free(iter);
    return updatedObjects;
}


void printJoinTable(database* db, table* tb1, table* tb2, const char* onColumnT1, const char* onColumnT2, columnType type) {

    data_iterator* iter1 = init_iterator(db,tb1);
    data_iterator* iter2 = init_iterator(db, tb2);
    while (seekNext(iter1)) {
        columnHeader* onColumnT1Header = get_col_header_by_name(tb1, onColumnT1);
        void* value = malloc(onColumnT1Header->size);
        getSome(iter1, onColumnT1, value);
        while (seekNextWhere(iter2, onColumnT2, type, value)) {
            for (int i = 0; i < iter1->tb->header->columnAmount; i++) {
                columnHeader cheader = iter1->tb->header->columns[i];
                void* curColumnValue = malloc(cheader.size);
                getSome(iter1, cheader.columnName, curColumnValue);
                switch (cheader.type) {
                    case INTEGER:
                        printf("\t %d \t", *((int32_t*) curColumnValue));
                        break;
                    case STRING:
                        printf("\t%s\t", (char*) curColumnValue);
                        break;
                    case BOOLEAN:
                        printf("\t%d\t", *((bool*) curColumnValue));
                        break;
                    case FLOAT:
                        printf("\t%f\t", *((float*) curColumnValue));
                        break;
                }
            }

            for (int i = 0; i < iter2->tb->header->columnAmount; i++) {
                columnHeader cheader = iter2->tb->header->columns[i];
                void* curColumnValue = malloc(cheader.size);
                getSome(iter2, cheader.columnName, curColumnValue);
                switch (cheader.type) {
                    case INTEGER:
                        printf("\t%d\t", *((int32_t*) curColumnValue));
                        break;
                    case STRING:
                        printf("\t%s\t", (char*) curColumnValue);
                        break;
                    case BOOLEAN:
                        printf("\t%d\t", *((bool*) curColumnValue));
                        break;
                    case FLOAT:
                        printf("\t%f\t", *((float*) curColumnValue));
                        break;
                }
            }

            printf("\n");
        }
        iter2 = init_iterator(db, tb2);
    }
    free(iter1);
    free(iter2);

}



