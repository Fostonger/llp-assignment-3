

#ifndef LAB3_REQUEST_PROCESSOR_H
#define LAB3_REQUEST_PROCESSOR_H

#include "gen-c_glib/structs_types.h"
#include "server/database.h"

server_response_T* process_client_request(const statement_T *stmt, database* db);

#endif //LAB3_REQUEST_PROCESSOR_H

