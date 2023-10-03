#include <stdio.h>

#include "util.h"

static const char *const result_descriptions[] = {
    [OK] = "INFO: Program executed successfully!",
    [MUST_BE_EMPTY] = "ERROR: The action you perform can be done only with empty entities!",
    [MALLOC_ERROR] = "ERROR: Couldn't allocate memory!",
    [NOT_ENOUGH_SPACE] = "ERROR: Row in table is not big enough to hold the value!",
    [DONT_EXIST] = "ERROR: The element you searching for does not exist",
    [CROSS_ON_JOIN] = "ERROR: You trying to join same table, that's inappropriate",
    [WRITE_ERROR] = "ERROR: Error occured while trying to write to database file",
    [READ_ERROR] = "ERROR: Error occured while trying to read database file",
    [DIFFERENT_DB] = "ERROR: the action you trying to perform meant to be inside single database, not with several of them!",
    [INVALID_PAGE_NUMBER] = "ERROR: Before you move page, its number has to be first to write. Rearrange pages",
    [NOT_EQUAL] = "ERROR: Tests shown that values that should be equal are in fact not",
    [NOT_FOUND] = "ERROR: Tests shown that value that should be in table are missing",
    [JOB_WAS_NOT_DONE] = "ERROR: Tests shown that the job that meant to be done is in fact isn't",
    [OPEN_ERROR] = "ERROR: Couldn't open database file"
};

int8_t print_if_failure( result result ) {
    if (!result) return 1;
    printf("-->%s\n", result_descriptions[result]);
    return 0;
}

const char *result_to_string(result rslt) {
    return result_descriptions[rslt];
}
