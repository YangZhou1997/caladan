extern "C" {
#include <base/log.h>
#include <net/ip.h>
#undef min
#undef max
}

#include <chrono>
#include <iostream>
#include <memory>

#include "net.h"
#include "runtime.h"
#include "sync.h"
#include "thread.h"
#include "timer.h"

namespace {

// ipv4: convert string to u32.
int StringToAddr(const char *str, uint32_t *addr) {
  uint8_t a, b, c, d;

  if (sscanf(str, "%hhu.%hhu.%hhu.%hhu", &a, &b, &c, &d) != 4) {
    puts("Failed in parsing ipv4 addr");
    exit(-1);
    return -EINVAL;
  }

  *addr = MAKE_IP_ADDR(a, b, c, d);
  return 0;
}

void MainHandler(void *arg) {
  netaddr cltaddr;
  StringToAddr("10.10.1.7", &cltaddr.ip);
  cltaddr.port = 12345;

  netaddr srvaddr;
  StringToAddr("10.10.1.2", &srvaddr.ip);
  srvaddr.port = 12345;

  std::unique_ptr<rt::UdpConn> c(rt::UdpConn::Listen(cltaddr));
  if (unlikely(c == nullptr)) panic("couldn't listen for control connections");

  const char *data_to_send = "Gangadhar Hi Shaktimaan hai";

  int ret = c->WriteTo(data_to_send, strlen(data_to_send), &srvaddr);
  if (ret <= 0) {
    log_warn("Failed sending request!");
  }

  // received echoed data back
  char buffer[100];
  netaddr raddr;
  ret = c->ReadFrom(buffer, sizeof(buffer), &raddr);
  if (ret <= 0) {
    log_warn("Failed recving request!");
  }


  buffer[ret] = '\0';
  log_warn("recieved: '%s'", buffer);
}

}  // anonymous namespace

int main(int argc, char *argv[]) {
  int ret;

  if (argc != 2) {
    std::cerr << "usage: [config_file]" << std::endl;
    return -EINVAL;
  }

  ret = runtime_init(argv[1], MainHandler, NULL);
  if (ret) {
    printf("failed to start runtime\n");
    return ret;
  }

  return 0;
}
