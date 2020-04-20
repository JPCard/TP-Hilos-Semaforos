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
#define PORT                12345
#define TOKEN_REQUEST_TYPE  1
#define TOKEN_TICKET_ID     3
//confiamos en que respete el formato del json

//----JSON-----
static const char *JSON_STRING =
    "{\"type\": 5, \"ticket\": 24}";

static int jsoneq(const char *json, jsmntok_t *tok, const char *s) {
  if (tok->type == JSMN_STRING && (int)strlen(s) == tok->end - tok->start &&
      strncmp(json + tok->start, s, tok->end - tok->start) == 0) {
    return 0;
  }
  return -1;
}
//----JSON-----

typedef struct {
    char type;
    int ticketId;
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
} Context;

Context ctx;

//QUEUE FUNCTIONS

void initializeQueueRequest() {
    ctx.rq.first = ctx.rq.last = -1;
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

// Arquitectura de threads:
//  Thread principal:
//    - Crear thread DB.
//    - Crear thread de comunicacion.
//    - Join ambos threads.
//  Thread de DB:
//    - Consume cola de solicitudes.
//  Thread de comunicacion por sockets:
//    - Por cada solicitud entrante, crea un thread para manejarlo.
//  Thread por cada solicitud:
//    - Demora artificial aleatoria.
//    - Encolar solicitud para DB.

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

int getFreeTicketIdFromDB() {
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

void reserveTicket() {
	int idTicket = getFreeTicketIdFromDB();
    if (idTicket) {
        setReservedInDB(idTicket);
        printf("Solicitud aceptada: ticket %d reservado.\n", idTicket);
    }
    else {
        printf("Solicitud rechazada: no hay tickets disponibles.\n");
    }
}

void purchaseTicket(int idTicket){
    int state = getTicketStateDB(idTicket);
    if (state == STATE_RESERVED){
        setPurchasedInDB(idTicket);
        printf("Solicitud aceptada: ticket %d vendido\n",idTicket);
    }
    else {
        printf("Solicitud Rechazada\n");
        if(state == STATE_FREE)
            printf("El ticket %d no fue reservado.\n",idTicket);
        else 
            printf("El ticket %d fue vendido anteriormente.\n",idTicket);
    }
}

void releaseTicket(int idTicket){
	setFreeInDB(idTicket);
    printf("El ticket %d pierde reserva porque no fue comprado en el tiempo limite\n",idTicket);
}

void dbProcessRequest(const Request r) {
    switch (r.type) {
        case REQUEST_RESERVE:
            reserveTicket();
            break;
        case REQUEST_CONFIRM:
            purchaseTicket(r.ticketId);
            break;
        default:
            printf("Solicitud desconocida de tipo %d\n", r.type);
            break;
    }
}

void *dbThreadFunction(void *vargp) {
    initializeQueueRequest();

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
        {
            if (!isEmptyQueueRequest()) {
                broadcastFree = isFullQueueRequest();
                Request r = dequeueRequest();
                dbProcessRequest(r);
            }
            else {
                // Wait until queue is modified.
                pthread_cond_wait(&ctx.queueCondition, &ctx.queueMutex);
            }
        }
        pthread_mutex_unlock(&ctx.queueMutex);

        // Notify the other threads that the queue has been modified.
        if (broadcastFree) {
            pthread_cond_broadcast(&ctx.queueCondition);
        }
    }
    

    closeDb();
    return NULL;
}

///
/// ---------------------JSon Parsing
///



//-1 es valor de error
//token es el indice del json
Request *getRequestFromJSON(char * requestStr){
    int r;
	int i;
	jsmn_parser p;
	jsmntok_t t[128]; /* We expect no more than 128 tokens */
	Request* req = (Request*) malloc(sizeof(Request));
	req->ticketId = -1;

	jsmn_init(&p);
    r = jsmn_parse(&p, requestStr, strlen(requestStr), t, sizeof(t) / sizeof(t[0]));
    if (r < 0) {
        printf("Failed to parse JSON: %d\n", r);
		free(req);
		return NULL;
	}

    /* Assume the top-level element is an object */
    if (r < 1 || t[0].type != JSMN_OBJECT) {
        printf("Object expected\n");
        free(req);
        return NULL;
    }

	for (i = 1; i < r; i++)
	{
		if (jsoneq(requestStr, &t[i], "type") == 0)
		{
			/* We may use strndup() to fetch string value */
			char aux[20];
            i++; //movemos hacia el valor
			sprintf(aux, "%.*s", t[i].end - t[i].start, requestStr + t[i].start);
			printf("type: %s", aux);
			req->type = atoi(aux);
		}

	    if (jsoneq(requestStr, &t[i], "ticket") == 0)
	    {
		    char aux[20];
            i++; //movemos hacia el valor
            sprintf(aux, "%.*s", t[i].end - t[i].start, requestStr + t[i].start);
            printf("id: %s", aux);
            req->ticketId = atoi(aux);            
	    }
	}

	return req;
}

///
/// SOCKET THREAD
///

void *socketThreadFunction(void *vargp) {
    int server_fd, new_socket, valread; 
    struct sockaddr_in address; 
    int opt = 1; 
    int addrlen = sizeof(address); 
    char buffer[1024] = {0}; 
    char *hello = "Server iniciado."; 

    // Creating socket file descriptor 
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) 
    { 
        perror("socket failed"); 
        exit(EXIT_FAILURE); 
    } 

    // Forcefully attaching socket to the port
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) 
    { 
        perror("setsockopt"); 
        exit(EXIT_FAILURE); 
    } 
    address.sin_family = AF_INET; 
    address.sin_addr.s_addr = INADDR_ANY; 
    address.sin_port = htons( PORT ); 

    // Forcefully attaching socket to the port
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address))<0) 
    { 
        perror("bind failed"); 
        exit(EXIT_FAILURE); 
    } 
    if (listen(server_fd, 3) < 0) 
    { 
        perror("listen"); 
        exit(EXIT_FAILURE); 
    } 
    if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen))<0) 
    { 
        perror("accept"); 
        exit(EXIT_FAILURE); 
    } 
    while (1){
        valread = read(new_socket , buffer, 1024); 
        printf("%s\n", buffer);

        // Create Request Thread
        // Parse JSon
        /*
        { type: 1, ticket: 1}
        */

        Request *req;
        req = getRequestFromJSON(buffer);
        if (req->type == 1){
            createRequestThread(REQUEST_RESERVE, 0);
        }
        else if (req->type == 2){
            createRequestThread(REQUEST_CONFIRM, req->ticketId);
        }
        //si no devuleve nada esto no va
        send(new_socket , hello , strlen(hello) , 0 ); 
        printf("Hello message sent\n"); 
    }
       
    /*
    sleep(1);
    createRequestThread(REQUEST_RESERVE, 0);
    sleep(1);
    createRequestThread(REQUEST_CONFIRM, 5);
    */
    // TODO

    return NULL;
}

///
/// REQUEST THREAD
///

void *requestThreadFunction(void *vargp) {
    Request *r = (Request *)(vargp);
    sleep(1); // Artificial delay.
    pthread_mutex_lock(&ctx.queueMutex);
    {
        while (isFullQueueRequest()) {
            // Wait until queue is modified.
            pthread_cond_wait(&ctx.queueCondition, &ctx.queueMutex);
        }

        enqueueRequest(*r);
    }
    pthread_mutex_unlock(&ctx.queueMutex);

    // Notify the other threads that the queue has been modified.
    pthread_cond_broadcast(&ctx.queueCondition);

    free(r);
}

void createRequestThread(int requestType, int ticketId) {
    pthread_t requestThreadId;
    Request *r = (Request *) malloc(sizeof(Request));
    r->type = requestType;
    r->ticketId = ticketId;
    pthread_create(&requestThreadId, NULL, requestThreadFunction, r);
}

int main(int argc, char *argv[]) {
	pthread_t dbThreadId, socketThreadId;
	pthread_create(&dbThreadId, NULL, dbThreadFunction, NULL);
    pthread_create(&socketThreadId, NULL, socketThreadFunction, NULL);
    pthread_join(dbThreadId, NULL);
    pthread_join(socketThreadId, NULL);
	return 0;
}


/*
 * A small example of jsmn parsing when JSON structure is known and number of
 * tokens is predictable.
 /

static const char *JSON_STRING =
    "{\"user\": \"johndoe\", \"admin\": false, \"uid\": 1000,\n  "
    "\"groups\": [\"users\", \"wheel\", \"audio\", \"video\"]}";

static int jsoneq(const char *json, jsmntok_t *tok, const char *s) {
  if (tok->type == JSMN_STRING && (int)strlen(s) == tok->end - tok->start &&
      strncmp(json + tok->start, s, tok->end - tok->start) == 0) {
    return 0;
  }
  return -1;
}


/*

int main() {
  int i;
  int r;
  jsmn_parser p;
  jsmntok_t t[128]; /* We expect no more than 128 tokens /
 
  jsmn_init(&p);
  r = jsmn_parse(&p, JSON_STRING, strlen(JSON_STRING), t,
                 sizeof(t) / sizeof(t[0]));
  if (r < 0) {
    printf("Failed to parse JSON: %d\n", r);
    return 1;
  }

  /* Assume the top-level element is an object /
  if (r < 1 || t[0].type != JSMN_OBJECT) {
    printf("Object expected\n");
    return 1;
  }

  /* Loop over all keys of the root object /
  for (i = 1; i < r; i++) {
    if (jsoneq(JSON_STRING, &t[i], "user") == 0) {
      /* We may use strndup() to fetch string value /
      printf("- User: %.*s\n", t[i + 1].end - t[i + 1].start,
             JSON_STRING + t[i + 1].start);
      i++;
    } else if (jsoneq(JSON_STRING, &t[i], "admin") == 0) {
      /* We may additionally check if the value is either "true" or "false" *
      printf("- Admin: %.*s\n", t[i + 1].end - t[i + 1].start,
             JSON_STRING + t[i + 1].start);
      i++;
    } else if (jsoneq(JSON_STRING, &t[i], "uid") == 0) {
      /* We may want to do strtol() here to get numeric value /
      printf("- UID: %.*s\n", t[i + 1].end - t[i + 1].start,
             JSON_STRING + t[i + 1].start);
      i++;
    } else if (jsoneq(JSON_STRING, &t[i], "groups") == 0) {
      int j;
      printf("- Groups:\n");
      if (t[i + 1].type != JSMN_ARRAY) {
        continue; /* We expect groups to be an array of strings /
      }
      for (j = 0; j < t[i + 1].size; j++) {
        jsmntok_t *g = &t[i + j + 2];
        printf("  * %.*s\n", g->end - g->start, JSON_STRING + g->start);
      }
      i += t[i + 1].size + 1;
    } else {
      printf("Unexpected key: %.*s\n", t[i].end - t[i].start,
             JSON_STRING + t[i].start);
    }
  }
  return EXIT_SUCCESS;
}*/