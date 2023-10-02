cmake_minimum_required(VERSION 3.24)
project(lab3 C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_COMPILER "gcc")

set(SERVER_EXE server_exe)
set(CLIENT_EXE client_exe)

set(LIB_DB server)
set(LIB_DB_DIR server/src)
set(LIB_PARSER client)
set(LIB_PARSER_DIR client)
set(CMAKE_PREFIX_PATH "/usr/local")

set(THRIFT_LIB thrift_c_glib)

add_library(${LIB_DB} STATIC
        ${LIB_DB_DIR}/data.c
        ${LIB_DB_DIR}/data.h
        ${LIB_DB_DIR}/data_iterator.c
        ${LIB_DB_DIR}/data_iterator.h
        ${LIB_DB_DIR}/database_manager.c
        ${LIB_DB_DIR}/database_manager.h
        ${LIB_DB_DIR}/table.c
        ${LIB_DB_DIR}/table.h
        ${LIB_DB_DIR}/util.c
        ${LIB_DB_DIR}/util.h
        ${LIB_DB_DIR}/functional.h
        )

target_include_directories(${LIB_DB} PUBLIC ${GLIB_INCLUDE_DIRS})
include_directories("/usr/local/lib/", "/opt/homebrew/Cellar/glib/2.78.0/include/glib-2.0/", "/opt/homebrew/Cellar/glib/2.78.0/include/", "/opt/homebrew/Cellar/glib/2.78.0/lib/glib-2.0/include/", "~/Downloads/thrift-0.17.0/lib/c_glib/src/")

set(LIB_PARSER_OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}")

set(BISON_EXECUTABLE /opt/homebrew/Cellar/bison/3.8.2/bin/bison)

find_package(FLEX REQUIRED)
find_package(BISON 3.8.2 EXACT REQUIRED)

set(LEXER_OUT "${LIB_PARSER_OUT_DIR}/lex.yy.c")
set(PARSER_OUT "${LIB_PARSER_OUT_DIR}/sql.tab.c")

flex_target(LEXER "${LIB_PARSER_DIR}/sql.l" ${LEXER_OUT} DEFINES_FILE "${LIB_PARSER_OUT_DIR}/lexer.h")
bison_target(PARSER "${LIB_PARSER_DIR}/sql.y" "${PARSER_OUT}" DEFINES_FILE "${LIB_PARSER_OUT_DIR}/sql.tab.h")
add_flex_bison_dependency(LEXER PARSER)

add_library(${LIB_PARSER} STATIC
        ${LIB_PARSER_DIR}/client_interface.c
        ${LIB_PARSER_DIR}/client_interface.h
        ${LIB_PARSER_DIR}/data.c
        ${LIB_PARSER_DIR}/data.h
        ${LIB_PARSER_DIR}/printer.c
        ${LIB_PARSER_DIR}/printer.h
        ${LEXER_OUT}
        ${PARSER_OUT}
        )
target_include_directories(${LIB_PARSER} PRIVATE ${LIB_PARSER_DIR} ${LIB_PARSER_OUT_DIR})

target_link_libraries(${LIB_DB})
find_package(PkgConfig REQUIRED)

pkg_check_modules(GLIB REQUIRED glib-2.0 gobject-2.0)

link_directories("/usr/local/lib/")

set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0 -g")
set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} -O0 -g")

add_executable(${SERVER_EXE}
        server.c
        gen-c_glib/structs_types.c
        gen-c_glib/structs_types.h
        gen-c_glib/data_exchange_service.c
        gen-c_glib/data_exchange_service.h
        request_processor.c
        request_processor.h
        )
target_include_directories(${SERVER_EXE} PUBLIC ${GLIB_INCLUDE_DIRS})
target_link_libraries(${SERVER_EXE} PUBLIC ${LIB_DB} ${THRIFT_LIB} ${GLIB_LDFLAGS})

add_executable(${CLIENT_EXE}
        client.c
        gen-c_glib/structs_types.c
        gen-c_glib/structs_types.h
        gen-c_glib/data_exchange_service.c
        gen-c_glib/data_exchange_service.h
        serializer/t_client_serializer.c
        serializer/t_client_serializer.h
        )

target_include_directories(${CLIENT_EXE} PUBLIC ${GLIB_INCLUDE_DIRS})
target_link_libraries(${CLIENT_EXE} PUBLIC ${LIB_PARSER} ${THRIFT_LIB} ${GLIB_LDFLAGS})
