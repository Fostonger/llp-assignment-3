#pragma once

#include "table.h"
#include "database_manager.h"
#include "util.h"
#include "data.h"
#include "data_iterator.h"

result test_table_creation(database *db);
result test_adding_columns(database *db);
result test_adding_values(database *db);
result test_adding_values_speed_with_writing_result(database *db);
result test_getting_values_speed_with_writing_result(database *db);
result test_deleting_value(database *db);
result test_deleting_values_speed_with_writing_result(database *db);
result test_updating_value(database *db);
result test_updating_values_speed_with_writing_result(database *db);

result test_tables_merging(database *db);
result test_table_filtering(database *db);

result test_table_saving(database *db);
result test_table_reading(database *db);

result test_multipaged_strings_adding(database *db);
result test_multipaged_strings_saving(database *db);
result test_multipaged_strings_reading(database *db);

result test_file_size_usage_with_writing_result(database *db);
