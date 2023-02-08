#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

int main() {
  const char* server_name = "10.10.1.4";
  const int server_port = 12345;

  struct sockaddr_in server_address;
  memset(&server_address, 0, sizeof(server_address));
  server_address.sin_family = AF_INET;

  // creates binary representation of server name
  // and stores it as sin_addr
  // http://beej.us/guide/bgnet/output/html/multipage/inet_ntopman.html
  inet_pton(AF_INET, server_name, &server_address.sin_addr);

  // htons: port in network order format
  server_address.sin_port = htons(server_port);

  // open socket
  int sock;
  if ((sock = socket(PF_INET, SOCK_DGRAM, 0)) < 0) {
    printf("could not create socket\n");
    return 1;
  }

  struct sockaddr_in local_addr;
  inet_pton(AF_INET, "10.10.1.7", &local_addr.sin_addr);
  local_addr.sin_port = htons(server_port);
  local_addr.sin_family = AF_INET;

  if (bind(sock, (sockaddr *)&local_addr, sizeof(local_addr)) < 0) {
    printf("Failed to bind to socket");
    return 1;
  }


  // data that will be sent to the server
  const char* data_to_send = "Gangadhar Hi Shaktimaan hai";

  // send data
  int len = sendto(sock, data_to_send, strlen(data_to_send), 0,
                   (struct sockaddr*)&server_address, sizeof(server_address));

  // received echoed data back
  char buffer[100];
  recvfrom(sock, buffer, len, 0, NULL, NULL);

  buffer[len] = '\0';
  printf("recieved: '%s'\n", buffer);

  // close the socket
  close(sock);
  return 0;
}