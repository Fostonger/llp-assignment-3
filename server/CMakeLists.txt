cmake_minimum_required(VERSION 3.19.2)
project(lab1_rdb C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS "-O0")

include_directories("/usr/local/Cellar/glib/2.78.0/include/glib-2.0/", "/usr/local/Cellar/glib/2.78.0/include/", "/usr/local/Cellar/glib/2.78.0/lib/glib-2.0/include/", "~/Downloads/thrift-0.17.0/lib/c_glib/src/")
add_executable(lab1_rdb main.c table.c table.h database.c database.h data.c data.h data_iterator.c data_iterator.h util.c util.h)
