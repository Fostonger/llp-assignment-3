#include <stdio.h>

#include "tests.h"

int main(void) {
    char *filename = "database.fost.data";

    maybe_database db = initdb(filename, true);
    if (db.error) {
        print_if_failure(db.error);
        return 1;
    }

    printf("\n\tStarting Test 1: table creation test\n");
    result test_result = test_table_creation(db.value);
    printf("\t\t\tTEST 1 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 2: table columns adding test\n");
    test_result = test_adding_columns(db.value);
    printf("\t\t\tTEST 2 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 3: table values adding test\n");
    test_result = test_adding_values(db.value);
    printf("\t\t\tTEST 3 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 4: table values adding speed test with charts printed\n");
    test_result = test_adding_values_speed_with_writing_result(db.value);
    printf("\t\t\tTEST 4 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 5: getting table values speed test with charts printed\n");
    test_result = test_getting_values_speed_with_writing_result(db.value);
    printf("\t\t\tTEST 5 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 6: deleting table values test\n");
    test_result = test_deleting_value(db.value);
    printf("\t\t\tTEST 6 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 7: updating table values test\n");
    test_result = test_updating_value(db.value);
    printf("\t\t\tTEST 7 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 8: tables joining test\n");
    test_result = test_tables_merging(db.value);
    printf("\t\t\tTEST 8 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 9: table filteringing test\n");
    test_result = test_table_filtering(db.value);
    printf("\t\t\tTEST 9 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 10: table saving test\n");
    test_result = test_table_saving(db.value);
    printf("\t\t\tTEST 10 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 11: table reading test\n");
    test_result = test_table_reading(db.value);
    printf("\t\t\tTEST 11 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 12: mulipaged string adding and reading test\n");
    test_result = test_multipaged_strings_adding(db.value);
    printf("\t\t\tTEST 12 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 13: mulipaged string saving\n");
    test_result = test_multipaged_strings_saving(db.value);
    printf("\t\t\tTEST 13 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 14: mulipaged string reading\n");
    test_result = test_multipaged_strings_reading(db.value);
    printf("\t\t\tTEST 14 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 15: deleting elements speed test with chart creation\n");
    test_result = test_deleting_values_speed_with_writing_result(db.value);
    printf("\t\t\tTEST 15 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 16: updating elements speed test with chart creation\n");
    test_result = test_updating_values_speed_with_writing_result(db.value);
    printf("\t\t\tTEST 16 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 17: measure file memory usage\n");
    test_result = test_file_size_usage_with_writing_result(db.value);
    printf("\t\t\tTEST 17 RESULT\n%s\n", result_to_string(test_result));

    release_db(db.value);
}