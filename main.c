#include "sqlite3/sqlite3.c"
#include "json_parser.h"

#include <stdio.h>
#include <unistd.h> //para el sleep
#include <pthread.h>
#include <stdlib.h> 
#include <sys/socket.h>
#include <netinet/in.h> 
#include <string.h> 

#define STATE_FREE          0
#define STATE_RESERVED      1
#define STATE_PURCHASED     2
#define REQUEST_RESERVE     1
#define REQUEST_CONFIRM     2
#define REQUEST_MAX         10
#define TICKETS_TOTAL       10
#define SERVER_PORT         12345
#define SERVER_QUEUE_SIZE   5
#define TOKEN_REQUEST_TYPE  1
#define TOKEN_TICKET_ID     3
#define REQUEST_REPLY_SIZE  256

//#define PRINT_DB_AFTER_CHANGES

typedef struct {
    int id;
    char type;
    int ticket;
    pthread_mutex_t *waitMutex;
    pthread_cond_t *waitCondition;
    char *reply;
} Request;

typedef struct {
    Request queue[REQUEST_MAX];
    int first, last;
} RequestQueue;

typedef struct {
  sqlite3 *database;
  pthread_cond_t queueCondition;
  pthread_mutex_t queueMutex;
  RequestQueue rq;
  int lastRequestId;
} Context;

Context ctx;

///
/// JSON
///

static int jsoneq(const char *json, jsmntok_t *tok, const char *s) {
  if (tok->type == JSMN_STRING && (int)strlen(s) == tok->end - tok->start &&
      strncmp(json + tok->start, s, tok->end - tok->start) == 0) {
    return 0;
  }
  return -1;
}

void formatJSONError(char *out, int max, char *message) {
    snprintf(out, max, "{ \"error\":\"%s\" }", message);
}

// QUEUE FUNCTIONS

void initializeQueueRequest() {
    ctx.rq.first = ctx.rq.last = -1;
    pthread_mutex_init(&ctx.queueMutex, NULL);
    pthread_cond_init(&ctx.queueCondition, NULL);
}

int isEmptyQueueRequest() {
    return ctx.rq.first == -1;
}

int isFullQueueRequest() {
    return (ctx.rq.last + 1) == ctx.rq.first || (ctx.rq.first == 0 && ctx.rq.last == REQUEST_MAX - 1);
}

//Pre: !isFullQueueRequest()
void enqueueRequest(Request r) {
    if (isEmptyQueueRequest()) {
        ctx.rq.first = 0;
    }

    if (ctx.rq.last == REQUEST_MAX - 1) {
        ctx.rq.last = 0;
    }
    else {
        ctx.rq.last++;
    }

    ctx.rq.queue[ctx.rq.last] = r;
}

//Pre: !isEmptyQueueRequest()
Request dequeueRequest() {
    int idx = ctx.rq.first;
    if (ctx.rq.first == ctx.rq.last) {
        ctx.rq.first = ctx.rq.last = -1;
    }
    else {
        if (ctx.rq.first == REQUEST_MAX - 1) {
            ctx.rq.first = 0;
        }
        else {
            ctx.rq.first++;
        }
    }

    return ctx.rq.queue[idx];
}

///
/// DATABASE THREAD
///

int setupDb() {
    sqlite3_open("tickets.sql", &ctx.database);
    return ctx.database != NULL;
}

void closeDb() {
    sqlite3_close(ctx.database);
}

int countCallback(void *count, int argc, char **argv, char **azColName) {
    int *c = count;
    *c = atoi(argv[0]);
    return 0;
}

int setupDbTable() {
    char *error = NULL;
    char query[] =  "CREATE TABLE IF NOT EXISTS tickets("
                    "id INTEGER PRIMARY KEY,"
                    "state INTEGER);";

    if (sqlite3_exec(ctx.database, query, NULL, NULL, &error) != 0) {
        printf("sqlite3_exec error: %s\n", error);
        return 0;
    }

    int ticketCount = 0;
    sqlite3_exec(ctx.database, "SELECT COUNT(*) FROM tickets;", countCallback, &ticketCount, &error);

    if (ticketCount == 0) {
        printf("Inicializando %d tickets en la tabla.\n", TICKETS_TOTAL);
        int i;
        for (i = 0; i < TICKETS_TOTAL; i++) {
            sqlite3_exec(ctx.database, "INSERT INTO tickets (state) VALUES (0);", NULL, NULL, &error);
        }
    }

    return 1;
}

void showTickets() {
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(ctx.database, "SELECT * FROM tickets;", -1, &stmt, NULL);
    while (sqlite3_step(stmt) != SQLITE_DONE) {
		int i;
		int cols = sqlite3_column_count(stmt);
		for (i = 0; i < cols; i++) {
			switch (sqlite3_column_type(stmt, i)) {
			case (SQLITE_INTEGER):
				printf("%d (%s), ", sqlite3_column_int(stmt, i), sqlite3_column_name(stmt, i));
				break;
			default:
				break;
			}
		}

		printf("\n");

	}

	sqlite3_finalize(stmt);
}

int getFreeticketFromDB() {
    int free_id = 0;
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(ctx.database, "SELECT id FROM tickets WHERE state = 0;", -1, &stmt, NULL);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        free_id = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);

    return free_id;
}

void setStateInDB(int idTicket, int state) {
    char *error = NULL;
    char query[256];
    sprintf(query, "UPDATE tickets SET state = %d WHERE id = %d;", state, idTicket);
    if (sqlite3_exec(ctx.database, query, NULL, NULL, &error) != 0) {
        printf("sqlite3_exec error: %s\n", error);
    }

#ifdef PRINT_DB_AFTER_CHANGES
    showTickets();
#endif
}

int getTicketStateDB(int idTicket){
    int state = -1;
    char aux[126];
    sqlite3_stmt *stmt;
    sprintf(aux, "SELECT state FROM tickets WHERE id = %d;", idTicket);
    sqlite3_prepare_v2(ctx.database, aux, -1, &stmt, NULL);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        state = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return state;
}

void setFreeInDB(int idTicket) {
    setStateInDB(idTicket, STATE_FREE);
}

void setReservedInDB(int idTicket) {
    setStateInDB(idTicket, STATE_RESERVED);
}

void setPurchasedInDB(int idTicket) {
    setStateInDB(idTicket, STATE_PURCHASED);
} 

void reserveTicket(const Request r) {
	int idTicket = getFreeticketFromDB();
    if (idTicket > 0) {
        setReservedInDB(idTicket);
        snprintf(r.reply, REQUEST_REPLY_SIZE, "{ \"ticket\":%d }", idTicket);
        printf("Solicitud #%d aceptada: ticket #%d reservado.\n", r.id, idTicket);
    }
    else {
        formatJSONError(r.reply, REQUEST_REPLY_SIZE, "No hay tickets disponibles.");
        printf("Solicitud #%d rechazada: no hay tickets disponibles.\n", r.id);
    }
}

void purchaseTicket(const Request r) {
    int state = getTicketStateDB(r.ticket);
    if (state == STATE_RESERVED){
        setPurchasedInDB(r.ticket);
        snprintf(r.reply, REQUEST_REPLY_SIZE, "{ \"accepted\":1 }");
        printf("Solicitud #%d aceptada: ticket #%d confirmado.\n", r.id, r.ticket);
    }
    else if (state == STATE_PURCHASED) {
        formatJSONError(r.reply, REQUEST_REPLY_SIZE, "El ticket ya fue vendido.");
        printf("Solicitud #%d rechazada: ticket #%d fue vendido anteriormente.\n", r.id, r.ticket);
    }
    else if (state == STATE_FREE) {
        formatJSONError(r.reply, REQUEST_REPLY_SIZE, "El ticket no fue reservado.");
        printf("Solicitud #%d rechazada: ticket #%d no fue reservado.\n", r.id, r.ticket);
    }
    else {
        formatJSONError(r.reply, REQUEST_REPLY_SIZE, "El ticket no existe.");
        printf("Solicitud #%d rechazada: ticket #%d no existe.\n", r.id, r.ticket);
    }
}

void dbProcessRequest(const Request r) {
    switch (r.type) {
        case REQUEST_RESERVE:
            reserveTicket(r);
            break;
        case REQUEST_CONFIRM:
            purchaseTicket(r);
            break;
        default:
            formatJSONError(r.reply, REQUEST_REPLY_SIZE, "Solicitud desconocida.");
            printf("Solicitud #%d rechazada: Tipo de solicitud desconocida.\n");
            break;
    }
}

void *dbThreadFunction(void *vargp) {
    if (!setupDb()) {
        printf("Fallo al inicializar la base de datos.\n");
        return NULL;
    }
    
    if (!setupDbTable()) {
        printf("Fallo al crear la tabla de tickets en la base de datos.\n");
        return NULL;
    }

    showTickets();
    
    // Process request queue.
    int dbRunning = 1, broadcastFree = 0;
    while (dbRunning) {
        broadcastFree = 0;
        pthread_mutex_lock(&ctx.queueMutex);
        if (!isEmptyQueueRequest()) {
            // Dequeue and unlock.
            broadcastFree = isFullQueueRequest();
            Request r = dequeueRequest();
            pthread_mutex_unlock(&ctx.queueMutex);

            dbProcessRequest(r);

            // Notify the thread that the request has been handled.
            pthread_mutex_lock(r.waitMutex);
            pthread_cond_broadcast(r.waitCondition);
            pthread_mutex_unlock(r.waitMutex);
        }
        else {
            // Wait until queue is modified.
            pthread_cond_wait(&ctx.queueCondition, &ctx.queueMutex);
            pthread_mutex_unlock(&ctx.queueMutex);
        }

        // Notify the other threads that the queue has been modified.
        if (broadcastFree) {
            pthread_cond_broadcast(&ctx.queueCondition);
        }
    }
    
    closeDb();
    return NULL;
}

///
/// JSON PARSER
///

Request *getRequestFromJSON(char *requestStr) {
    // JSON format: { type: int, ticket: int }
    // Only 16 tokens are supported.
	jsmntok_t t[16];
    jsmn_parser p;
	
	jsmn_init(&p);
    int r = jsmn_parse(&p, requestStr, strlen(requestStr), t, sizeof(t) / sizeof(t[0]));
    if (r < 0) {
        printf("Failed to parse JSON: %d\nString: %s\n", r, requestStr);
		return NULL;
	}

    // Assume the top-level element is an object.
    if (r < 1 || t[0].type != JSMN_OBJECT) {
        printf("Object expected\n");
        return NULL;
    }

    // Parse request from JSON.
    char aux[128];
    int i, type = 0, ticket = 0;
	for (i = 1; i < (r - 1); i++) {
		if (jsoneq(requestStr, &t[i], "type") == 0) {
            i++;
			snprintf(aux, sizeof(aux), "%.*s", t[i].end - t[i].start, requestStr + t[i].start);
			type = atoi(aux);
		}
	    else if (jsoneq(requestStr, &t[i], "ticket") == 0) {
            i++;
			snprintf(aux, sizeof(aux), "%.*s", t[i].end - t[i].start, requestStr + t[i].start);
            ticket = atoi(aux);  
	    }
	}

    // Validate request before creating it.
    if ((type == REQUEST_RESERVE) || ((type == REQUEST_CONFIRM) && (ticket > 0))) {
        Request *req = (Request *)(malloc(sizeof(Request)));
        req->id = -1;
        req->type = type;
        req->ticket = ticket;
        req->waitMutex = (pthread_mutex_t *)(malloc(sizeof(pthread_mutex_t)));
        req->waitCondition = (pthread_cond_t *)(malloc(sizeof(pthread_cond_t)));

        pthread_mutex_init(req->waitMutex, NULL);
        pthread_cond_init(req->waitCondition, NULL);

        req->reply = (char *)(malloc(sizeof(char) * REQUEST_REPLY_SIZE));
        strcpy(req->reply, "");
        return req;
    }
    else {
        printf("Failed to validate JSON: %s\n", requestStr);
        return NULL;
    }
}

void freeRequest(Request *r) {
    pthread_mutex_destroy(r->waitMutex);
    pthread_cond_destroy(r->waitCondition);
    free(r->waitMutex);
    free(r->waitCondition);
    free(r->reply);
    free(r);
}

///
/// SOCKET THREAD
///

void *socketThreadFunction(void *vargp) {
    // Creating socket file descriptor.
    int serverFd = socket(AF_INET, SOCK_STREAM, 0);
    if (serverFd == 0) {
        printf("Fallo al inicializar el socket.\n");
        return NULL;
    }

    int opt = 1;
    if (setsockopt(serverFd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) { 
        printf("Fallo al configurar el socket.\n");
        return NULL;
    }

    struct sockaddr_in address; 
    address.sin_family = AF_INET; 
    address.sin_addr.s_addr = INADDR_ANY; 
    address.sin_port = htons( SERVER_PORT ); 

    if (bind(serverFd, (struct sockaddr *)(&address), sizeof(address)) < 0) { 
        printf("Fallo al iniciar el socket en el puerto %d.\n", SERVER_PORT);
        return NULL;
    }

    if (listen(serverFd, SERVER_QUEUE_SIZE) < 0) { 
        printf("Fallo al intentar escuchar conexiones entrantes en el socket.\n");
        return NULL;
    }

    struct sockaddr_in clientAddress;
    int clientAddressLength = sizeof(clientAddress); 
    int running = 1, newSocket;
    while (running) {
        printf("Esperando nuevas conexiones...\n");
        newSocket = accept(serverFd, (struct sockaddr *)(&clientAddress), (socklen_t*)(&clientAddressLength));
        if (newSocket >= 0) {
            printf("Creando un hilo para manejar una conexion entrante...\n");
            createRequestThread(newSocket);
        }
        else {
            printf("Fallo al aceptar una nueva conexion.\n");
        }
    }

    return NULL;
}

///
/// REQUEST THREAD
///

#define SOCKET_BUFFER_SIZE 1024

void handleRequest(Request *r) {
    // Delay artificial para simular tiempo de respuesta.
    sleep(1);

    pthread_mutex_lock(r->waitMutex);
    pthread_mutex_lock(&ctx.queueMutex);
    while (isFullQueueRequest()) {
        // Wait until queue is modified.
        pthread_cond_wait(&ctx.queueCondition, &ctx.queueMutex);
    }

    // Enqueue this request so the DB can handle it.
    enqueueRequest(*r);
    pthread_mutex_unlock(&ctx.queueMutex);

    // Notify the other threads that the queue has been modified.
    pthread_cond_broadcast(&ctx.queueCondition);

    // Wait until the DB handles the request.
    pthread_cond_wait(r->waitCondition, r->waitMutex);
    pthread_mutex_unlock(r->waitMutex);
}

void *requestThreadFunction(void *vargp) {
    int socket = (int)(vargp);
    int reading = 1;
    char buffer[SOCKET_BUFFER_SIZE];
    while (reading) {
        int bytesRead = read(socket, buffer, SOCKET_BUFFER_SIZE); 
        if (bytesRead > 0) {
            buffer[bytesRead] = '\0';

            // Ticket is optional or obligatory depending on type.
            Request *newRequest = getRequestFromJSON(buffer);
            if (newRequest != NULL) {
                // Assign an id to this request.
                newRequest->id = ctx.lastRequestId++;
                printf("Nueva solicitud #%d de tipo %d\n", newRequest->id, newRequest->type);

                // Wait until DB handles the request.
                handleRequest(newRequest);

                // Send the DB's reply via the socket.
                int replySize = strlen(newRequest->reply);
                if (replySize > 0) {
                    send(socket, newRequest->reply, replySize, 0);
                    printf("Respuesta de solicitud #%d enviada: %s\n", newRequest->id, newRequest->reply);
                }
                else {
                    printf("No hay respuesta para enviar para la solicitud #%d\n", newRequest->id);
                }

                // Cleanup the request.
                freeRequest(newRequest);
            }
            else {
                // Reply with an invalid format error.
                char errorMessage[128];
                formatJSONError(errorMessage, sizeof(errorMessage), "La solicitud no fue formateada correctamente.");
                send(socket, errorMessage, strlen(errorMessage), 0); 
            }
        }
        else {
            printf("Cerrando conexion para el cliente.\n");
            reading = 0;
        }
    }

    return NULL;
}

void createRequestThread(int socket) {
    pthread_t requestThreadId;
    pthread_create(&requestThreadId, NULL, requestThreadFunction, socket);
}

int main(int argc, char *argv[]) {
    // Initialize common variables.
    ctx.lastRequestId = 0;
    initializeQueueRequest();

    // Create and start threads.
	pthread_t dbThreadId, socketThreadId;
	pthread_create(&dbThreadId, NULL, dbThreadFunction, NULL);
    pthread_create(&socketThreadId, NULL, socketThreadFunction, NULL);

    // Finish when both threads are finished.
    pthread_join(dbThreadId, NULL);
    pthread_join(socketThreadId, NULL);

	return 0;
}
