#include <stdio.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>

#define HOST_ADDRESS "127.0.0.1"
#define HOST_PORT 12345


//returns: 1-message sent -> continue
//         0-connection lost -> abort
int sendAndWaitReply(int sock, char *message) {
    int msgSent;
    char replyBuffer[1024];
    msgSent = send(sock, message, strlen(message), 0);
    if(msgSent != -1){
        printf("Mensaje enviado: %s\n", message);
        int bytesRead = read(sock, replyBuffer, sizeof(replyBuffer));
        if (bytesRead > 0) {
            replyBuffer[bytesRead] = '\0';
            printf("Respuesta recibida: %s\n", replyBuffer);
            return 1;
        }
        else
            return 0;
    }
    else
        return 0;
}

int main(int argc, char const *argv[]) {
    int sock = 0, valread;
    struct sockaddr_in serv_addr;
    char buffer[1024] = {0};
    int op;
    int ok = 1;
    struct timeval tv;

    tv.tv_sec = 3; //seconds to wait for a connection
    tv.tv_usec = 0;

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("\n Socket creation error \n");
        return -1;
    }




    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(HOST_PORT);

    // Convert IPv4 and IPv6 addresses from text to binary form
    if(inet_pton(AF_INET, HOST_ADDRESS, &serv_addr.sin_addr)<=0)
    {
        printf("\nInvalid address / address not supported.\n");
        return -1;
    }



    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        printf("\nConnection Failed \n");
        return -1;
    }



    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv); //sets connection timeout

    do {
        printf("Ingrese la operacion a realizas:\n");
        printf("1 - Reservar Ticket.\n");
        printf("2 - Confirmar compra de Ticket.\n");
        printf("0 - Finalizar.\n");

        scanf("%d", &op);
        char json[64];
        int id;
        switch (op) {
        case 1:
            // Reservar Ticket
            strcpy(json, "{\"type\": 1}");
            ok = sendAndWaitReply(sock, json);
            break;
        case 2:
            // Confirmar compra Ticket
            printf("Ingrese el id del ticket reservado.\n");
            scanf("%d", &id);
            sprintf(json, "{\"type\": 2, \"ticket\":%d}", id);
            ok = sendAndWaitReply(sock, json);
            break;
        case 0:
            printf("Finalizando...\n");
            break;
        default:
            printf("Opcion invalida, por favor vuelva a intentar.\n");
            break;
        }
        if(!ok){
            op = 0;
            printf("El servidor no responde.\n");
            printf("Finalizando...\n");
        }
    } while (op != 0);

    close(sock);

    return 0;
}
