extern "C" {
#include <base/log.h>
#include <net/ip.h>
}
#undef min
#undef max

#include "runtime.h"
#include "thread.h"
#include "sync.h"
#include "timer.h"
#include "net.h"
#include "fake_worker.h"
#include "proto.h"


// protocol-buffer
#include "netbench_udp_protobuf.h"


#include <iostream>
#include <iomanip>
#include <utility>
#include <memory>
#include <chrono>
#include <vector>
#include <algorithm>
#include <numeric>
#include <random>
#include <string>


namespace {

using sec = std::chrono::duration<double, std::micro>;

// The number of samples to discard from the start and end.
constexpr uint64_t kDiscardSamples = 1000;
// The maximum lateness to tolerate before dropping egress samples.
constexpr uint64_t kMaxCatchUpUS = 5;

// the number of worker threads to spawn.
int threads;
// the remote UDP address of the server.
netaddr raddr;
netaddr cltaddr;
netaddr srvaddr[10];
// the number of samples to gather.
uint64_t n;
// the mean service time in us.
double st;

const uint32_t STATUS_NORMAL = 0;
const uint32_t STATUS_VIEW_CHANGE = 1;
const uint32_t STATUS_RECOVERING = 2;

// Paxos-state
int32_t idx;
uint32_t view;
uint32_t status;


// ------------------------------------ server-side code ------------------------------------

bool AmLeader() {
	return view == 0;
}

void HandleRequest(const netaddr &remote,
                   const specpaxos::vr::proto::RequestMessage &msg) {
	if (status != STATUS_NORMAL) { // don't handle request.
        puts("Ignoring request due to abnormal status");
		exit(-1);
        return;
    }
	
	if (!AmLeader()) { // only leader should handle request.
        puts("Ignoring request because I'm not the leader");
        exit(-1);
        return;
    }

	// now I'm the leader.
}

void HandlePrepare(const netaddr &remote,
				   const specpaxos::vr::proto::PrepareMessage &msg) {
    if (status != STATUS_NORMAL) { // no interaction.
        puts("Ignoring PREPARE due to abnormal status");
		exit(-1);
        return;
    }
    
    if (msg.view() < view) { // hear a stale  message, we shouldn't respond to that.
        puts("Ignoring PREPARE due to stale view");
		exit(-1);
        return;
    }

    if (msg.view() > view) {
		puts("trigger view change! we laid behind.");
		exit(-1);
        return;
	}

    if (AmLeader()) { // leader shouldn't receive this message.
        puts("Unexpected PREPARE: I'm the leader of this view");
		exit(-1);
    }


}

void HandlePrepareOK(const netaddr &remote, 
					 const specpaxos::vr::proto::PrepareOKMessage &msg) {
    if (status != STATUS_NORMAL) {
        puts("Ignoring PREPAREOK due to abnormal status");
		exit(-1);
        return;
    }

    if (msg.view() < view) {
        puts("Ignoring PREPAREOK due to stale view");
		exit(-1);
        return;
    }

    if (msg.view() > view) {
		puts("trigger view change! we laid behind.");
		exit(-1);
        return;
    }

    if (!AmLeader()) {
        puts("Ignoring PREPAREOK because I'm not the leader");
		exit(-1);
        return;
    }

}

void HandleCommit(const netaddr &remote,
				  const specpaxos::vr::proto::CommitMessage &msg) {
    if (status != STATUS_NORMAL) {
        puts("Ignoring COMMIT due to abnormal status");
		exit(-1);
        return;
    }
    
    if (msg.view() < view) {
        puts("Ignoring COMMIT due to stale view");
		exit(-1);
        return;
    }

    if (msg.view() > view) {
		puts("trigger view change! we laid behind.");
		exit(-1);
        return;
    }

    if (AmLeader()) {
        puts("Unexpected COMMIT: I'm the leader of this view");
		exit(-1);
    }
}

void ReceiveMessage(const netaddr &remote, const std::string &type, const std::string &data) {
    static specpaxos::vr::proto::RequestMessage request;
    static specpaxos::vr::proto::PrepareMessage prepare;
    static specpaxos::vr::proto::PrepareOKMessage prepareOK;
    static specpaxos::vr::proto::CommitMessage commit;
    
    if (type == request.GetTypeName()) { // HandleRequest, the leader's duty.
        request.ParseFromString(data);
        HandleRequest(remote, request);
    } else if (type == prepare.GetTypeName()) { // HandlePrepare, in backup replica.
        prepare.ParseFromString(data);
        HandlePrepare(remote, prepare);
    } else if (type == prepareOK.GetTypeName()) { // HandlePrepareOK, the leader's duty.
        prepareOK.ParseFromString(data);
        HandlePrepareOK(remote, prepareOK);
    } else if (type == commit.GetTypeName()) { // HandleCommit, in back replica.
        commit.ParseFromString(data);
        HandleCommit(remote, commit);
    } else {
		printf("Received unexpected message type in VR proto: %s\n", type.c_str());
		fflush(stdout);
		exit(-1);
	}

    // asd123www: add logic here if we have handle view-change.
}

// the main function of Server.
void ServerHandler(void *arg) {
    std::unique_ptr<rt::UdpConn> c(rt::UdpConn::Listen({0, kNetbenchPort}));
    if (unlikely(c == nullptr)) panic("couldn't listen for control connections");

	// initialize Paxos's state
	view = 0;
	status = STATUS_NORMAL;

	// like event-driven, a loop pooling packets. 
    char buf[10005];
    while (true) {
        netaddr raddr;
        ssize_t ret = c->ReadFrom(buf, 1e4, &raddr);

        char *payload = buf;
        uint64_t typeLen = *(uint64_t *)payload;
        payload = payload + sizeof(uint64_t);
        std::string type(payload, typeLen);
        std::string data(payload + typeLen, ret - sizeof(uint64_t) - typeLen);
        ReceiveMessage(raddr, type, data);
	}
	puts("Quited from loop!");
    return;
}


// ------------------------------------ client-side code ------------------------------------

void ClientHandler(void *arg) {
	std::unique_ptr<rt::UdpConn> c(rt::UdpConn::Listen({0, kNetbenchPort}));
	if (unlikely(c == nullptr)) panic("couldn't listen for control connections");

	// a simplified client, need to add warmup in the future.
	int cnt = 10;
	char buf[3000];
	while (--cnt) {
		int len = rand() % 50 + 50;
		printf("Sending: %d\n", len);
		ssize_t ret = udp_send(buf, len, cltaddr, srvaddr[0]);
		if (ret == -1) {
			puts("Failed sending request!");fflush(stdout);
			exit(0);
		}
		ret = c->ReadFrom(buf, 3e3, &raddr);
		printf("Received: %ld\n", ret);
		// received one request.
	}
	return;
}


// ipv4: convert string to u32.
int StringToAddr(const char *str, uint32_t *addr) {
	uint8_t a, b, c, d;

	if(sscanf(str, "%hhu.%hhu.%hhu.%hhu", &a, &b, &c, &d) != 4) {
		puts("Failed in parsing ipv4 addr");
		exit(-1);
		return -EINVAL;
	}

	*addr = MAKE_IP_ADDR(a, b, c, d);
	return 0;
}

} // anonymous namespace

int main(int argc, char *argv[]) {
	int ret;

	if (argc < 3) {
		std::cerr << "usage: [cfg_file] [cmd] ..." << std::endl;
		return -EINVAL;
	}

	// Setting cluster.
	StringToAddr("10.10.1.2", &cltaddr.ip);
	StringToAddr("10.10.1.3", &srvaddr[0].ip);
	StringToAddr("10.10.1.4", &srvaddr[1].ip);
	StringToAddr("10.10.1.1", &srvaddr[2].ip);
	StringToAddr("10.10.1.6", &srvaddr[3].ip);
	StringToAddr("10.10.1.5", &srvaddr[4].ip);
	StringToAddr("10.10.1.7", &srvaddr[5].ip);
	StringToAddr("10.10.1.8", &srvaddr[6].ip);
	cltaddr.port = kNetbenchPort;
	for (int i = 0; i < 7; ++i) srvaddr[i].port = kNetbenchPort;

	std::string cmd = argv[2];
	if (cmd.compare("server") == 0) {
		puts("I'm running server!");
		idx = std::stoi(argv[3], nullptr, 0);
		ret = runtime_init(argv[1], ServerHandler, NULL);
		if (ret) {
			printf("Server: failed to start runtime\n");
			return ret;
		}
	} else if (cmd.compare("client") != 0) {
		std::cerr << "invalid command: " << cmd << std::endl;
		return -EINVAL;
	}

	ret = runtime_init(argv[1], ClientHandler, NULL);
	if (ret) {
		printf("failed to start runtime\n");
		return ret;
	}

	return 0;
}
/*
Compiling your code:
    make -C apps/bench

Client: 
    sudo ./iokerneld simple
    sudo ./apps/bench/netbench_udp client.config client
Server: 
    sudo ./iokerneld simple
    sudo ./apps/bench/netbench_udp server.config server idx
*/