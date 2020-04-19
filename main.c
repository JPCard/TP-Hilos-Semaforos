#include "sqlite3/sqlite3.c"

#include <stdio.h>
#include <unistd.h> //para el sleep
#include <pthread.h>

#define STATE_FREE          0
#define STATE_RESERVED      1
#define STATE_PURCHASED     2
#define REQUEST_RESERVE     1
#define REQUEST_CONFIRM     2
#define REQUEST_MAX         50
#define TICKETS_TOTAL       10

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

int isEmptyQueueRequest(){
    return ctx.rq.first == -1 || (ctx.rq.first == ctx.rq.last + 1);
}

int isFullQueueRequest(){
    return (ctx.rq.last - 1) == ctx.rq.first || (ctx.rq.first == 0 && ctx.rq.last == REQUEST_MAX - 1);
}

//Pre: !isFullQueueRequest()
void enqueueRequest(Request r) {
    if(isEmptyQueueRequest()){
        ctx.rq.first = ctx.rq.last = 0;
    }
    else {
        if (ctx.rq.last == REQUEST_MAX - 1){
            ctx.rq.last = 0;
        }
        else {
            ctx.rq.last += 1;
        }
    }
    ctx.rq.queue[ctx.rq.last] = r;
}

//Pre: !isEmptyQueueRequest()
Request dequeueRequest() {
    if(ctx.rq.first != REQUEST_MAX - 1)
	    return ctx.rq.queue[ctx.rq.first++];
    else {
        int i = ctx.rq.first;
		ctx.rq.first = 0;
		return ctx.rq.queue[i];
	}
}

// Arquitectura de threads:
//  Thread principal:
//    - Crear thread DB
//    - Crear thread de comunicacion
//    - Join ambos threads
//  Thread de DB:
//    - Consume cola de solicitudes
//  Thread de comunicacion por sockets:
//    - Por cada solicitud entrante, crea un thread para manejarlo
//  Thread por cada solicitud:
//    - Demora artificial aleatoria
//    - Encolar solicitud para DB

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
	setPurchasedInDB(idTicket);
    printf("Solicitud aceptada: ticket %d vendido\n",idTicket);
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
    int dbRunning = 1;
    pthread_mutex_lock(&ctx.queueMutex);
    {
        while (dbRunning) {
            if (!isEmptyQueueRequest()) {
                Request r = dequeueRequest();
                dbProcessRequest(r);
            }
            else {
                pthread_cond_wait(&ctx.queueCondition, &ctx.queueMutex);
            }
        }
    }
    pthread_mutex_unlock(&ctx.queueMutex);

    closeDb();
    return NULL;
}

///
/// SOCKET THREAD
///

void *socketThreadFunction(void *vargp) {
    sleep(3);
    createRequestThread(REQUEST_RESERVE, 0);

    sleep(3);
    createRequestThread(REQUEST_CONFIRM, 5);

    // TODO

    return NULL;
}

///
/// REQUEST THREAD
///

void *requestThreadFunction(void *vargp) {
    sleep(2); // Artificial delay.

    Request *r = (Request *)(vargp);
    pthread_mutex_lock(&ctx.queueMutex);
    {
        if (!isFullQueueRequest()) {
            enqueueRequest(*r);
        }
        else {
            printf("No se pudo manejar la solicitud porque la cola está llena.");
        }
    }
    pthread_mutex_unlock(&ctx.queueMutex);
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

// Pthread example.
//   pthread_t thread_id;
//   pthread_create(&thread_id, NULL, myThreadFun, NULL); // Starts thread inmediately.
//   pthread_join(thread_id, NULL); // Waits until thread is finished.
//
// Mutex example:
//  pthread_mutex_t lock;
//  pthread_mutex_lock(&lock);
//  pthread_mutex_unlock(&lock);
//
// Waiting condition example:
//  pthread_cond_t
//  pthread_cond_broadcast(&cond);
//
// Waiting thread:
//  pthread_mutex_lock(&mutex);
//  while (condition) {
//    pthread_cond_wait(&cond, &mutex);
//  }
//  pthread_mutex_unlock(&mutex);
//
// Wakeup thread:
//  pthread_mutex_lock(&mutex);
//  pthread_cond_broadcast(&cond);
//  pthread_mutex_unlock(&mutex);
