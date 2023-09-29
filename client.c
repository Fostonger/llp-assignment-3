#include <thrift/c_glib/transport/thrift_transport.h>
#include <thrift/c_glib/transport/thrift_socket.h>
#include <thrift/c_glib/transport/thrift_buffered_transport.h>
#include <thrift/c_glib/protocol/thrift_binary_protocol.h>

#include <glib-object.h>
#include <glib.h>
#include <stdio.h>
#include <stdbool.h>

#include "client/client_interface.h"
#include "serializer/t_client_serializer.h"
#include "gen-c_glib/data_exchange_service.h"

#define BUF_SIZE 1024

void print_items(item_list_T *pT);

int main(int argc, char* argv[]) {
    if (argc != 3) {
        printf("Required args: <address> <port>\n");
        return -1;
    }

    char* hostname = argv[1];

    errno = 0;
    int port = strtol(argv[2], NULL, 10);

    if (errno != 0) {
        printf("Incorrect port.\n");
        return -1;
    }


    ThriftSocket *socket;
    ThriftTransport *transport;
    ThriftProtocol *protocol;

    data_exchange_serviceIf *service;

    gboolean success;
    gchar *message = NULL;
    server_response_T *result = NULL;
    GError *error = NULL;

#if (!GLIB_CHECK_VERSION(2, 36, 0))
    g_type_init ();
#endif

    socket = g_object_new(THRIFT_TYPE_SOCKET,
                          "hostname", hostname,
                          "port", port,
                          NULL);
    transport = g_object_new(THRIFT_TYPE_BUFFERED_TRANSPORT,
                             "transport", socket,
                             NULL);
    protocol = g_object_new(THRIFT_TYPE_BINARY_PROTOCOL,
                            "transport", transport,
                            NULL);

    thrift_transport_open(transport, &error);

    service = g_object_new(TYPE_DATA_EXCHANGE_SERVICE_CLIENT,
                          "input_protocol", protocol,
                          "output_protocol", protocol,
                          NULL);

    char buffer[BUF_SIZE];
    buffer[0] = '\0';

    while (true) {
        printf("stmt > ");
        fgets(buffer, BUF_SIZE, stdin);
        buffer[BUF_SIZE - 1] = '\0';
        if (strcmp(buffer, "exit;\n") == 0)
            return 0;
        statement s = get_statement(buffer);
        statement_T *st = t_serialize_statement(&s);

        if (!st) continue;

        result = g_object_new(TYPE_SERVER_RESPONSE__T, NULL);
        success = data_exchange_service_if_execute(service, &result, st, &error);
        if (success) {
            g_object_get(result, "msg", &message, NULL);
            fprintf(stdout, "%s", message);
            if (result->__isset_items) {
                print_items(result->items);
            }
        } else {
            fprintf(stderr, "Exception: %s\n", error->message);
            g_clear_error(&error);
        }
    }

    g_object_unref(result);

    thrift_transport_close(transport, &error);
    success = success && (error == NULL);

    g_clear_error(&error);

    g_object_unref(service);

    g_object_unref(protocol);
    g_object_unref(transport);
    g_object_unref(socket);

    return (success ? 0 : 1);

}

void print_cell(literal_T * literal) {
    switch (literal->type) {
        case LITERAL_TYPE__T_LIT_FLOAT_T:
            printf("%.4f", literal->value->flt);
            break;
        case LITERAL_TYPE__T_LIT_BOOLEAN_T:
            printf("%s", literal->value->boolean ? "true" : "false");
            break;
        case LITERAL_TYPE__T_LIT_STRING_T:
            printf("%s", literal->value->str);
            break;
        case LITERAL_TYPE__T_LIT_INTEGER_T:
            printf("%d", literal->value->num);
            break;
    }
}
void print_items(item_list_T *items) {
    for (size_t i = 0; i < items->schema->len; i++) {
        columnref_T *column = (columnref_T*) items->schema->pdata[i];
        printf("%s\t", column->col_name);
    }
    printf("\n");
    for (size_t i = 0; i < items->rows->len; i++) {
        row_T *row = (row_T*) items->rows->pdata[i];
        for (size_t j = 0; j < row->value->len; j++) {
            literal_T *literal = row->value->pdata[j];
            print_cell(literal);
            printf("\t");
        }
        printf("\n");
    }
}
