#include <thrift/c_glib/thrift.h>
#include <thrift/c_glib/protocol/thrift_binary_protocol_factory.h>
#include <thrift/c_glib/protocol/thrift_protocol_factory.h>
#include <thrift/c_glib/server/thrift_server.h>
#include <thrift/c_glib/server/thrift_simple_server.h>
#include <thrift/c_glib/transport/thrift_buffered_transport_factory.h>
#include <thrift/c_glib/transport/thrift_server_socket.h>
#include <thrift/c_glib/transport/thrift_server_transport.h>

#include <stdio.h>
#include <stdbool.h>
#include <glib.h>
#include <glib-object.h>

#include "gen-c_glib/data_exchange_service.h"
#include "server/database.h"
#include "server/data_iterator.h"
#include "server/table.h"
#include "request_processor.h"


// ================ START OF DECLARATIONS ================

G_BEGIN_DECLS

#define TYPE_DATA_EXCHANGE_HANDLER \
  (data_exchange_handler_get_type ())

#define DATA_EXCHANGE_HANDLER(obj)                                \
  (G_TYPE_CHECK_INSTANCE_CAST ((obj),                                   \
                               TYPE_DATA_EXCHANGE_HANDLER,        \
                               data_exchange_handler_impl))

#define DATA_EXCHANGE_HANDLER_CLASS(c)                    \
  (G_TYPE_CHECK_CLASS_CAST ((c),                                \
                            TYPE_DATA_EXCHANGE_HANDLER,   \
                            data_exchange_handler_implClass))

#define IS_DATA_EXCHANGE_HANDLER(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE ((obj),                                   \
                               TYPE_DATA_EXCHANGE_HANDLER))

#define IS_DATA_EXCHANGE_HANDLER_CLASS(c)                 \
  (G_TYPE_CHECK_CLASS_TYPE ((c),                                \
                            TYPE_DATA_EXCHANGE_HANDLER))

#define DATA_EXCHANGE_HANDLER_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS ((obj),                            \
                              TYPE_DATA_EXCHANGE_HANDLER, \
                              data_exchange_handler_implClass))

struct _data_exchange_handler_impl {
    data_exchange_serviceHandler parent_instance;
};
typedef struct _data_exchange_handler_impl data_exchange_handler_impl;

struct _data_exchange_handler_implClass {
    data_exchange_serviceHandlerClass parent_class;
};

typedef struct _data_exchange_handler_implClass data_exchange_handler_implClass;

GType data_exchange_handler_get_type(void);

G_END_DECLS


// ================ END OF DECLARATIONS ================


// ================ START OF IMPLEMENTATION ================


G_DEFINE_TYPE(data_exchange_handler_impl,
              data_exchange_handler,
              TYPE_DATA_EXCHANGE_SERVICE_HANDLER
)

static database* db;

static gboolean data_exchange_handler_execute (data_exchange_serviceIf *iface, server_response_T **_return, const statement_T * stmt, GError **error) {
    THRIFT_UNUSED_VAR(iface);
    THRIFT_UNUSED_VAR(error);

    server_response_T* response = process_client_request(stmt, db);

    g_object_set(*_return, "status", response->status,
                 "msg", strdup(response->msg), NULL);
    if (response->__isset_items) {
        g_object_set(*_return,
                     "items", response->items,
                     NULL);
    }

    return true;
}

static void
data_exchange_handler_finalize (GObject *object)
{
    data_exchange_handler_impl *self =
    DATA_EXCHANGE_HANDLER (object);

    /* Chain up to the parent class */
    G_OBJECT_CLASS (data_exchange_handler_parent_class)->
            finalize (object);
}

static void
data_exchange_handler_init (data_exchange_handler_impl *self)
{
    // no fields -> empty initializer
}

/* TutorialCalculatorHandler's class initializer */
static void
data_exchange_handler_class_init (data_exchange_handler_implClass *klass)
{
    GObjectClass *gobject_class = G_OBJECT_CLASS (klass);

    data_exchange_serviceHandlerClass *dataExchangeServiceHandlerClass =
    DATA_EXCHANGE_SERVICE_HANDLER_CLASS(klass);

    gobject_class->finalize = data_exchange_handler_finalize;

    dataExchangeServiceHandlerClass->execute =
            data_exchange_handler_execute;

}

// ================ END OF IMPLEMENTATION ================



int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Required argument: <db_filename>");
        return -1;
    }
    FILE* file = fopen(argv[1], "wb+");
    db = init_db(file, true);
    data_exchange_handler_impl *handler;
    data_exchange_serviceProcessor *processor;

    ThriftServerTransport *server_transport;
    ThriftTransportFactory *transport_factory;
    ThriftProtocolFactory *protocol_factory;
    ThriftServer *server;

    GError *error = NULL;

#if (!GLIB_CHECK_VERSION(2, 36, 0))
    g_type_init ();
#endif

    const int port = 9090;
    handler = g_object_new(TYPE_DATA_EXCHANGE_HANDLER,
                           NULL);
    processor = g_object_new(TYPE_DATA_EXCHANGE_SERVICE_PROCESSOR,
                             "handler", handler,
                             NULL);

    server_transport = g_object_new(THRIFT_TYPE_SERVER_SOCKET,
                                    "port", port,
                                    NULL);
    transport_factory = g_object_new(THRIFT_TYPE_BUFFERED_TRANSPORT_FACTORY,
                                     NULL);
    protocol_factory = g_object_new(THRIFT_TYPE_BINARY_PROTOCOL_FACTORY,
                                    NULL);

    server = g_object_new(THRIFT_TYPE_SIMPLE_SERVER,
                          "processor", processor,
                          "server_transport", server_transport,
                          "input_transport_factory", transport_factory,
                          "output_transport_factory", transport_factory,
                          "input_protocol_factory", protocol_factory,
                          "output_protocol_factory", protocol_factory,
                          NULL);

    printf("Listening on port: %d\n", port);
    thrift_server_serve(server, &error);

    g_object_unref(server);
    g_object_unref(protocol_factory);
    g_object_unref(transport_factory);
    g_object_unref(server_transport);

    g_object_unref(processor);
    g_object_unref(handler);

    return 0;
}