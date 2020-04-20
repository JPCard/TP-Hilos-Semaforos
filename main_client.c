
// Client side C/C++ program to demonstrate Socket programming 
#include <stdio.h> 
#include <sys/socket.h> 
#include <arpa/inet.h> 
#include <unistd.h> 
#include <string.h> 
#define PORT 8080 
   
int main(int argc, char const *argv[]) 
{ 
    int sock = 0, valread; 
    struct sockaddr_in serv_addr; 
    char buffer[1024] = {0}; 
    int op;
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) 
    { 
        printf("\n Socket creation error \n"); 
        return -1; 
    } 
   
    serv_addr.sin_family = AF_INET; 
    serv_addr.sin_port = htons(PORT); 
       
    // Convert IPv4 and IPv6 addresses from text to binary form 
    if(inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr)<=0)  
    { 
        printf("\nInvalid address/ Address not supported \n"); 
        return -1; 
    } 
   
    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) 
    { 
        printf("\nConnection Failed \n"); 
        return -1; 
    } 

    do {
        printf("Ingrese la operacion a realizas:\n");
        printf("1 - Comprar Ticket.\n");
        printf("2 - Reservar Ticket.\n");
        printf("3 - Confirmar compra de Ticket.\n");
        printf("0 - Finalizar.\n");

        scanf("%d", &op);
        char json[64];
        switch (op)
        {
        case 1:
            /* Comprar Ticket*/
            strcpy(json,"{\"type\": 1}");
            send(sock ,  json, strlen(json) , 0 ); 
            valread = read( sock , buffer, 1024); 
            printf("%s\n",buffer ); 
            break;
        case 2: 
            /* Reservar Ticket*/            
            strcpy(json,"{\"type\": 2}");
            send(sock , json , strlen(json) , 0 ); 
            valread = read( sock , buffer, 1024); 
            printf("%s\n",buffer ); 
            break;
        case 3: 
            /* Confirmar compra Ticket*/    
            int id;
            printf("Ingrese el id del ticket reservado.\n");
            scanf("%d", &id);     
            sprintf(json,"{\"type\": 3, \"ticket\":%d}", id);   
            send(sock , json , strlen(json) , 0 ); 
            valread = read( sock , buffer, 1024); 
            printf("%s\n",buffer ); 
            break;
        case 0: 
            printf("Finalizando...");
            break;
        default:
            printf("Opcion invalida, por favor vuelva a intentar.\n");
            break;
        }
    
    } while (op != 0);

    return 0; 
} 
