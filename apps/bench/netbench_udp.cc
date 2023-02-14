extern "C" {
#include <base/log.h>
#include <net/ip.h>
}
#undef min
#undef max

#include "fake_worker.h"
#include "net.h"
#include "proto.h"
#include "runtime.h"
#include "sync.h"
#include "thread.h"
#include "timer.h"

// protocol-buffer
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "netbench_udp_protobuf.h"

typedef uint64_t view_t;
typedef uint64_t opnum_t;

#define ASSERT(x, msg)     \
  do {                     \
    if (!(x)) {            \
      printf("%s\n", msg); \
      exit(-1);            \
    }                      \
  } while (0)

namespace {

enum LogEntryState {
  LOG_STATE_COMMITTED,
  LOG_STATE_PREPARED,
  LOG_STATE_SPECULATIVE,  // specpaxos only
  LOG_STATE_FASTPREPARED  // fastpaxos only
};

struct viewstamp_t {
  view_t view;
  opnum_t opnum;

  viewstamp_t() : view(0), opnum(0) {}
  viewstamp_t(view_t view, opnum_t opnum) : view(view), opnum(opnum) {}
};

class Log {
 public:
  struct LogEntry {
    viewstamp_t viewstamp;
    LogEntryState state;
    specpaxos::Request request;
    std::string hash;
    // Speculative client table stuff
    opnum_t prevClientReqOpnum;
    ::google::protobuf::Message *replyMessage;

    LogEntry() { replyMessage = NULL; }
    LogEntry(const LogEntry &x)
        : viewstamp(x.viewstamp),
          state(x.state),
          request(x.request),
          hash(x.hash),
          prevClientReqOpnum(x.prevClientReqOpnum) {
      if (x.replyMessage) {
        replyMessage = x.replyMessage->New();
        replyMessage->CopyFrom(*x.replyMessage);
      } else {
        replyMessage = NULL;
      }
    }
    LogEntry(viewstamp_t viewstamp, LogEntryState state,
             const specpaxos::Request &request,
             const std::string &hash = std::string(20, '\0'))
        : viewstamp(viewstamp),
          state(state),
          request(request),
          hash(hash),
          replyMessage(NULL) {}
    virtual ~LogEntry() {
      if (replyMessage) {
        delete replyMessage;
      }
    }
  };

  opnum_t LastOpnum() const {
    if (entries.empty()) {
      return start - 1;
    } else {
      return entries.back().viewstamp.opnum;
    }
  }

  std::string &LastHash() {
    if (entries.empty()) {
      return initialHash;
    } else {
      return entries.back().hash;
    }
  }

  bool SetStatus(opnum_t op, LogEntryState state) {
    LogEntry *entry = Find(op);
    if (entry == NULL) {
      return false;
    }

    entry->state = state;
    return true;
  }

  LogEntry &Append(viewstamp_t vs, const specpaxos::Request &req,
                   LogEntryState state) {
    if (entries.empty()) {
      ASSERT(vs.opnum == start, "Log entry");
    } else {
      ASSERT(vs.opnum == LastOpnum() + 1, "Log entry");
    }

    std::string prevHash = LastHash();
    entries.push_back(LogEntry(vs, state, req));
    if (useHash) {
      puts("We don't need hashing!");
      exit(-1);
    }

    return entries.back();
  }
  LogEntry *Find(opnum_t opnum) {
    if (entries.empty()) {
      return NULL;
    }

    if (opnum < start) {
      return NULL;
    }

    if (opnum - start > entries.size() - 1) {
      return NULL;
    }

    LogEntry *entry = &entries[opnum - start];
    ASSERT(entry->viewstamp.opnum == opnum, "Log entry find");
    return entry;
  }

 private:
  std::vector<LogEntry> entries;
  std::string initialHash;
  opnum_t start = 1;
  bool useHash;
};

typedef Log::LogEntry LogEntry;

using sec = std::chrono::duration<double, std::micro>;

// The number of samples to discard from the start and end.
constexpr uint64_t kDiscardSamples = 1000;
// The maximum lateness to tolerate before dropping egress samples.
constexpr uint64_t kMaxCatchUpUS = 5;
constexpr uint64_t kUdpSendRetryUs = 10;

const uint32_t STATUS_NORMAL = 0;
const uint32_t STATUS_VIEW_CHANGE = 1;
const uint32_t STATUS_RECOVERING = 2;

const uint32_t MAX_CLIENT_NUM = 100;
const uint32_t CLUSTER_SIZE = 5;
const uint32_t QUORUM_SIZE = 3;

const uint64_t NONFRAG_MAGIC = 0x20050318;
const uint64_t ELECTRODE_HEADER_LENGTH = 12;

// the number of worker threads to spawn.
uint32_t threads;
uint64_t warmup;
uint64_t n;
netaddr cltaddr[MAX_CLIENT_NUM];
netaddr srvaddr[100];

// statistics
uint64_t num_request, num_prepare, num_prepareOK, num_commit;

// Paxos-state
uint32_t myIdx;
uint32_t view;
uint32_t status;
uint64_t lastOp, lastCommitted;
Log log;
std::map<std::pair<uint64_t, uint64_t>,
         std::map<int, specpaxos::vr::proto::PrepareOKMessage>>
    messages;

std::map<uint64_t, netaddr> clientAddresses;
struct ClientTableEntry {
  uint64_t lastReqId;
  bool replied;
  specpaxos::vr::proto::ReplyMessage reply;
};
std::map<uint64_t, ClientTableEntry> clientTable;

void ListenFunc(std::unique_ptr<rt::TcpConn> inc) {
  nbench_req req = {kMagic, 0};
  int ret;
retry:
  ret = inc->ReadFull(&req, sizeof(req));
  if (ret < 0) {
    // puts("TCP couldn't read from remote.");
    goto retry;
  }
  ASSERT(req.magic == kMagic, "magic error");

retry2:
  ret = inc->WriteFull(&req, sizeof(req));
  if (ret < 0) {
    // puts("TCP couldn't write to remote.");
    goto retry2;
  }
}

void FillArpTable() {
  netaddr peeraddr[CLUSTER_SIZE + 1];
  peeraddr[CLUSTER_SIZE].ip = cltaddr[0].ip;
  peeraddr[CLUSTER_SIZE].port = kNetbenchPort;
  for (int i = 0; i < CLUSTER_SIZE; i++) {
    peeraddr[i] = srvaddr[i];
  }

  auto listener = rt::Thread([&]() {
    std::unique_ptr<rt::TcpQueue> q(
        rt::TcpQueue::Listen({0, kNetbenchPort}, 4096));
    if (unlikely(q == nullptr))
      panic("couldn't listen for control connections");

    int num_conns = 0;
    std::vector<rt::Thread> th;
    std::set<uint32_t> connected_ips;
    while (num_conns != CLUSTER_SIZE) {
      rt::TcpConn *c = q->Accept();
      if (c == nullptr) panic("couldn't accept a connection");
      auto rip = c->RemoteAddr().ip;
      if (connected_ips.find(rip) != connected_ips.end()) {
        // repeated conn, ignore
        continue;
      }
      connected_ips.insert(rip);
      th.emplace_back(
          rt::Thread([=] { ListenFunc(std::unique_ptr<rt::TcpConn>(c)); }));
      log_warn("Accept one conn with %x", c->RemoteAddr().ip);
      num_conns++;
    }
    for (auto &t : th) {
      t.Join();
    }
    log_warn("Finish Listen");
  });

  rt::Thread([&]() {
    std::vector<std::unique_ptr<rt::TcpConn>> conns;
    for (int i = 0; i < CLUSTER_SIZE + 1; ++i) {
      if (i == myIdx) continue;
      std::unique_ptr<rt::TcpConn> outc;
    retry:
      outc =
          std::unique_ptr<rt::TcpConn>(rt::TcpConn::Dial({0, 0}, peeraddr[i]));
      if (unlikely(outc == nullptr)) {
        rt::Sleep(500 * rt::kMilliseconds);
        goto retry;
      }

      nbench_req req = {kMagic, 0};
      int ret;
    retry2:
      ret = outc->WriteFull(&req, sizeof(req));
      if (ret < 0) {
        // puts("TCP couldn't write to remote.");
        goto retry2;
      }
    retry3:
      ret = outc->ReadFull(&req, sizeof(req));
      if (ret < 0) {
        // puts("TCP couldn't read from remote.");
        goto retry3;
      }
      ASSERT(req.magic == kMagic, "magic error");
      log_warn("Finish one conn with %x", outc->RemoteAddr().ip);
    }
    log_warn("Finish Dial");
  }).Join();

  listener.Join();

  log_warn("Finish FillArpTable");
}

// ------------------------------------ server-side code
// ------------------------------------

bool AmLeader() { return view % CLUSTER_SIZE == myIdx; }

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

void UpdateClientTable(const specpaxos::Request &req) {
  ClientTableEntry &entry = clientTable[req.clientid()];

  if (entry.lastReqId > req.clientreqid()) {
    puts("Wrong Client-side request number.");
    exit(-1);
  }

  if (entry.lastReqId == req.clientreqid()) {
    return;
  }

  entry.lastReqId = req.clientreqid();
  entry.replied = false;
  entry.reply.Clear();
}

static size_t SerializeMessage(const ::google::protobuf::Message &m,
                               char **out) {
  std::string data = m.SerializeAsString();
  std::string type = m.GetTypeName();
  size_t typeLen = type.length();
  size_t dataLen = data.length();
  ssize_t totalLen = (sizeof(uint32_t) + typeLen + sizeof(typeLen) + dataLen +
                      sizeof(dataLen)) + ELECTRODE_HEADER_LENGTH;

  char *buf = new char[totalLen];

  char *ptr = buf;
  *(uint32_t *)ptr = NONFRAG_MAGIC;
  ptr += sizeof(uint32_t);
  *((size_t *)ptr) = typeLen;
  ptr += sizeof(size_t);
  ASSERT(ptr - buf < totalLen, "Serialized Message typeLen.");
  ASSERT(ptr + typeLen - buf < totalLen, "Serialized Message typestr.");
  memcpy(ptr, type.c_str(), typeLen);
  ptr += typeLen;

  // skip 12 bytes.
  ptr += ELECTRODE_HEADER_LENGTH;

  *((size_t *)ptr) = dataLen;
  ptr += sizeof(size_t);
  ASSERT(ptr - buf < totalLen, "Serialized Message dataLen.");
  ASSERT(ptr + dataLen - buf == totalLen, "Serialized Message datastr.");
  memcpy(ptr, data.c_str(), dataLen);
  ptr += dataLen;

  *out = buf;
  return totalLen;
}

static void DecodePacket(const char *buf, size_t sz, std::string &type,
                         std::string &msg) {
  uint32_t magic = *(uint32_t *)buf;
  ASSERT(magic == NONFRAG_MAGIC, "NonFrag bits check.");

  ssize_t ssz = sz;
  const char *ptr = buf + sizeof(uint32_t);
  size_t typeLen = *((size_t *)ptr);
  ptr += sizeof(size_t);
  ASSERT(ptr - buf < ssz, "Decode typeLen.");
  ASSERT(ptr + typeLen - buf < ssz, "Decode typeStr.");
  type = std::string(ptr, typeLen);
  ptr += typeLen;

  // skip 12 bytes.
  ptr += ELECTRODE_HEADER_LENGTH;

  size_t msgLen = *((size_t *)ptr);
  ptr += sizeof(size_t);
  ASSERT(ptr - buf < ssz, "Decode dataLen.");
  ASSERT(ptr + msgLen - buf <= ssz, "Decode dataStr.");
  msg = std::string(ptr, msgLen);
  ptr += msgLen;
}

void CommitUpTo(opnum_t upto,
                std::unique_ptr<rt::UdpConn>
                    &c) {  // we can apply these requests in state machine!
  while (lastCommitted < upto) {
    lastCommitted++;

    /* Find operation in log */
    const LogEntry *entry = log.Find(lastCommitted);
    if (!entry) {
      puts("Did not find operation in log");
      exit(-1);
    }

    specpaxos::vr::proto::ReplyMessage reply;
    // WARNING: you need to set reply.
    reply.set_reply("asd123www");
    // if we have an upper-layer application.
    // Execute(lastCommitted, entry->request, reply);

    reply.set_view(entry->viewstamp.view);
    reply.set_opnum(entry->viewstamp.opnum);
    reply.set_clientreqid(entry->request.clientreqid());

    /* Mark it as committed */
    log.SetStatus(lastCommitted, LOG_STATE_COMMITTED);

    // Store reply in the client table
    ClientTableEntry &cte = clientTable[entry->request.clientid()];
    if (cte.lastReqId <= entry->request.clientreqid()) {
      cte.lastReqId = entry->request.clientreqid();
      cte.replied = true;
      cte.reply = reply;
    } else {
      // We've subsequently prepared another operation from the
      // same client. So this request must have been completed
      // at the client, and there's no need to record the
      // result.
    }

    /* Send reply */
    auto iter = clientAddresses.find(entry->request.clientid());
    if (iter != clientAddresses.end()) {
      char *buf;
      size_t msgLen = SerializeMessage(reply, &buf);
      ssize_t ret = 0;
    retry:
      // ret = udp_send(buf, msgLen, srvaddr[myIdx], iter->second);
      ret = c->WriteTo(buf, msgLen, &iter->second);
      if (ret < 0) {
        log_warn_ratelimited("Failed to send reply message to client");
        rt::Sleep(kUdpSendRetryUs);
        goto retry;
      }
      delete[] buf;
    }
  }
}

void HandleRequest(const netaddr &remote,
                   const specpaxos::vr::proto::RequestMessage &msg,
                   std::unique_ptr<rt::UdpConn> &c) {
  if (status != STATUS_NORMAL) {  // don't handle request.
    puts("Ignoring request due to abnormal status");
    exit(-1);
    return;
  }

  if (!AmLeader()) {  // only leader should handle request.
    // puts("Ignoring request because I'm not the leader");
    return;
  }

  // Save the client's address
  clientAddresses.erase(msg.req().clientid());
  clientAddresses.insert(
      std::pair<uint64_t, netaddr>(msg.req().clientid(), remote));

  // Check the client table to see if this is a duplicate request
  auto kv = clientTable.find(msg.req().clientid());
  if (kv != clientTable.end()) {
    const ClientTableEntry &entry = kv->second;
    if (msg.req().clientreqid() < entry.lastReqId) {
      puts("Ignoring stale request");
      exit(-1);
    }
    if (msg.req().clientreqid() == entry.lastReqId) {
      puts("Duplicated request!");
      exit(-1);
    }
  }

  // Update the client table
  UpdateClientTable(msg.req());

  specpaxos::Request request;
  request.set_op(msg.req().op());
  request.set_clientid(msg.req().clientid());
  request.set_clientreqid(msg.req().clientreqid());

  /* Assign it an opnum */  // increasing by one.
  ++lastOp;

  /* Add the request to my log */
  viewstamp_t v;
  v.view = view;
  v.opnum = lastOp;
  log.Append(v, request,
             LOG_STATE_PREPARED);  // state of this entry in log is PREPARED.

  /* Send prepare messages */
  specpaxos::vr::proto::PrepareMessage p;
  p.set_view(view);
  p.set_opnum(lastOp);
  p.set_batchstart(lastOp);

  // batch the reqs in this interval, and send it all.
  for (opnum_t i = lastOp; i <= lastOp; i++) {
    specpaxos::Request *r = p.add_request();
    const LogEntry *entry = log.Find(i);
    ASSERT(entry != NULL, "A in HandleRequest.");
    ASSERT(entry->viewstamp.view == view, "B in HandleRequest.");
    ASSERT(entry->viewstamp.opnum == i, "C in HandleRequest.");
    *r = entry->request;
  }

  // broadcast this message to all followers.
  char *buf;
  size_t msgLen = SerializeMessage(p, &buf);
  for (uint32_t i = 0; i < CLUSTER_SIZE; ++i) {
    if (i == myIdx) continue;
    // printf("Leader %d sent prepare msg to replica %d\n", myIdx, i);
    ssize_t ret = 0;
  retry:
    // ret = udp_send(buf, msgLen, srvaddr[myIdx], srvaddr[i]);
    ret = c->WriteTo(buf, msgLen, &srvaddr[i]);
    if (ret < 0) {
      log_warn_ratelimited(
          "Failed to broadcast prepare messages to followers.");
      rt::Sleep(kUdpSendRetryUs);
      goto retry;
    }
  }
  delete[] buf;
}

void HandlePrepare(const netaddr &remote,
                   const specpaxos::vr::proto::PrepareMessage &msg,
                   std::unique_ptr<rt::UdpConn> &c) {
  if (status != STATUS_NORMAL) {  // no interaction.
    puts("Ignoring PREPARE due to abnormal status");
    exit(-1);
    return;
  }

  if (msg.view() <
      view) {  // hear a stale  message, we shouldn't respond to that.
    puts("Ignoring PREPARE due to stale view");
    exit(-1);
    return;
  }

  if (msg.view() > view) {
    puts("trigger view change! we laid behind.");
    exit(-1);
    return;
  }

  if (AmLeader()) {  // leader shouldn't receive this message.
    puts("Unexpected PREPARE: I'm the leader of this view");
    exit(-1);
  }

  if (msg.opnum() <= lastOp) {  // stale Prepare message.
    puts("Ignoring PREPARE; already prepared that operation");
    exit(-1);
  }

  if (msg.batchstart() > lastOp + 1) {
    printf("Prepare message loss/reorder! %ld %ld\n", lastOp, msg.batchstart());
    exit(-1);
    return;
  }

  /* Add operations to the log */
  opnum_t op = msg.batchstart() - 1;
  for (auto &req : msg.request()) {
    op++;
    if (op <= lastOp) continue;
    lastOp++;
    log.Append(viewstamp_t(msg.view(), op), req, LOG_STATE_PREPARED);
    UpdateClientTable(req);
  }
  ASSERT(op == msg.opnum(), "Op checking in HandlePrepare.");

  /* Build reply and send it to the leader */
  specpaxos::vr::proto::PrepareOKMessage reply;
  reply.set_view(msg.view());
  reply.set_opnum(msg.opnum());
  reply.set_replicaidx(myIdx);

  char *buf;
  size_t msgLen = SerializeMessage(reply, &buf);
  ssize_t ret = 0;
retry:
  // ret = udp_send(buf, msgLen, srvaddr[myIdx], srvaddr[view % CLUSTER_SIZE]);
  ret = c->WriteTo(buf, msgLen, &srvaddr[view % CLUSTER_SIZE]);
  if (ret < 0) {
    log_warn_ratelimited("Failed to send PrepareOK message to leader");
    rt::Sleep(kUdpSendRetryUs);
    goto retry;
  }
  delete[] buf;
}

void HandlePrepareOK(const netaddr &remote,
                     const specpaxos::vr::proto::PrepareOKMessage &msg,
                     std::unique_ptr<rt::UdpConn> &c) {
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

  viewstamp_t vs = {msg.view(), msg.opnum()};

  std::map<int, specpaxos::vr::proto::PrepareOKMessage> &vsmessages =
      messages[std::make_pair(vs.view, vs.opnum)];
  if (vsmessages.find(msg.replicaidx()) != vsmessages.end()) {
    // This is a duplicate message

    // But we'll ignore that, replace the old message from
    // this replica, and proceed.
    //
    // XXX Is this the right thing to do? It is for
    // speculative replies in SpecPaxos...
  }
  vsmessages[msg.replicaidx()] = msg;
  uint32_t count = vsmessages.size();
  if (count >= QUORUM_SIZE - 1) {
    if (count >= QUORUM_SIZE) return;

    ASSERT(msg.opnum() == lastCommitted + 1, "Op checking in HandlePrepareOK.");
    CommitUpTo(msg.opnum(), c);
    /*
     * Send COMMIT message to the other replicas.
     *
     * This can be done asynchronously, so it really ought to be
     * piggybacked on the next PREPARE or something.
     */
    specpaxos::vr::proto::CommitMessage cm;
    cm.set_view(view);
    cm.set_opnum(lastCommitted);

    char *buf;
    size_t msgLen = SerializeMessage(cm, &buf);
    for (uint32_t i = 0; i < CLUSTER_SIZE; ++i) {
      if (i == myIdx) continue;
      ssize_t ret = 0;
    retry:
      // ret = udp_send(buf, msgLen, srvaddr[myIdx], srvaddr[i]);
      ret = c->WriteTo(buf, msgLen, &srvaddr[i]);
      if (ret < 0) {
        log_warn_ratelimited("Failed to send COMMIT message to all replicas.");
        rt::Sleep(kUdpSendRetryUs);
        goto retry;
      }
    }
    delete[] buf;
  }
}

void HandleCommit(const netaddr &remote,
                  const specpaxos::vr::proto::CommitMessage &msg,
                  std::unique_ptr<rt::UdpConn> &c) {
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

  if (msg.opnum() <= lastCommitted) {
    puts("Ignoring COMMIT; already committed that operation");
    exit(-1);
    return;
  }

  if (msg.opnum() > lastOp) {  // we don't have this request...
    printf("Commit nonexistent request %ld %ld", msg.opnum(), lastOp);
    exit(-1);
    return;
  }

  CommitUpTo(msg.opnum(), c);
}

void ReceiveMessage(const netaddr &remote, const std::string &type,
                    const std::string &data, std::unique_ptr<rt::UdpConn> &c) {
  static specpaxos::vr::proto::RequestMessage request;
  static specpaxos::vr::proto::PrepareMessage prepare;
  static specpaxos::vr::proto::PrepareOKMessage prepareOK;
  static specpaxos::vr::proto::CommitMessage commit;

  // std::cout<<(AmLeader()? "Leader ":"Follower ")<<myIdx<<" received " <<
  // type<<" message!\n"<<std::endl;

  if (type == request.GetTypeName()) {  // HandleRequest, the leader's duty.
    ++num_request;
    request.ParseFromString(data);
    HandleRequest(remote, request, c);
  } else if (type ==
             prepare.GetTypeName()) {  // HandlePrepare, in backup replica.
    ++num_prepare;
    prepare.ParseFromString(data);
    HandlePrepare(remote, prepare, c);
  } else if (type ==
             prepareOK.GetTypeName()) {  // HandlePrepareOK, the leader's duty.
    ++num_prepareOK;
    prepareOK.ParseFromString(data);
    HandlePrepareOK(remote, prepareOK, c);
  } else if (type == commit.GetTypeName()) {  // HandleCommit, in back replica.
    ++num_commit;
    commit.ParseFromString(data);
    HandleCommit(remote, commit, c);
  } else {
    printf("Received unexpected message type in VR proto: %s\n", type.c_str());
    fflush(stdout);
    exit(-1);
  }

  // asd123www: add logic here if we have handle view-change.
}

// the main function of Server.
void ServerHandler(void *arg) {
  // FillArpTable();

  // std::unique_ptr<rt::UdpConn> c(rt::UdpConn::Listen({0, kNetbenchPort}));
  std::unique_ptr<rt::UdpConn> c(rt::UdpConn::Listen(srvaddr[myIdx]));
  if (unlikely(c == nullptr)) panic("couldn't listen for control connections");
  // c->SetBuffers(1 << 18, 1 << 18);

  // initialize Paxos's state
  view = 0;
  status = STATUS_NORMAL;
  lastOp = 0;

  num_request = num_prepare = num_prepareOK = num_commit = 0;

  // like event-driven, a loop pooling packets.
  char buf[10005];
  while (true) {
    netaddr raddr;
    ssize_t ret = c->ReadFrom(buf, 1e4, &raddr);

    std::string type, data;
    DecodePacket(buf, ret, type, data);
    ReceiveMessage(raddr, type, data, c);
  }
  puts("Quited from loop!");
  return;
}

// ------------------------------------ client-side code
// ------------------------------------
std::string request_str[MAX_CLIENT_NUM];
uint64_t clientReqId[MAX_CLIENT_NUM];
bool warmup_finished[MAX_CLIENT_NUM];
uint64_t time_stamp[MAX_CLIENT_NUM];
std::vector<uint64_t> latencies[MAX_CLIENT_NUM];

void SendRequest(uint32_t clientid, std::unique_ptr<rt::UdpConn> &c) {
  specpaxos::vr::proto::RequestMessage reqMsg;
  reqMsg.mutable_req()->set_op(request_str[clientid]);
  reqMsg.mutable_req()->set_clientid(clientid);
  reqMsg.mutable_req()->set_clientreqid(++clientReqId[clientid]);

  char *buf;
  size_t msgLen = SerializeMessage(reqMsg, &buf);

  time_stamp[clientid] = microtime();
  for (uint32_t i = 0; i < CLUSTER_SIZE; ++i) {
    ssize_t ret = 0;
  retry:
    // ret = udp_send(buf, msgLen, cltaddr[clientid], srvaddr[i]);
    ret = c->WriteTo(buf, msgLen, &srvaddr[i]);
    if (ret < 0) {
      log_warn_ratelimited("Failed sending request!");
      rt::Sleep(kUdpSendRetryUs);
      goto retry;
    }
  }
  delete[] buf;
}

void HandleReply(const uint32_t clientid, const netaddr &remote,
                 const specpaxos::vr::proto::ReplyMessage &msg) {
  if (msg.clientreqid() != clientReqId[clientid]) {
    puts("Received reply for a different request");
    return;
  }

  if (warmup_finished[clientid]) {
    latencies[clientid].push_back(microtime() - time_stamp[clientid]);
  }
}

void ClientReceiveMessage(const uint32_t clientid, const netaddr &remote,
                          const std::string &type, const std::string &data) {
  static specpaxos::vr::proto::ReplyMessage reply;

  if (type == reply.GetTypeName()) {
    reply.ParseFromString(data);
    HandleReply(clientid, remote, reply);
  } else {
    puts("Unknow message in client!");
    exit(-1);
  }
}

// return time of finishing n requests.
double ClientMain(uint32_t clientid, uint16_t port) {
  cltaddr[clientid].port = port;
  // std::unique_ptr<rt::UdpConn> c(rt::UdpConn::Listen({0, port}));
  std::unique_ptr<rt::UdpConn> c(rt::UdpConn::Listen(cltaddr[clientid]));
  if (unlikely(c == nullptr)) panic("couldn't listen for control connections");
  // c->SetBuffers(1 << 18, 1 << 18);

  // initialize state.
  warmup_finished[clientid] = false;
  request_str[clientid] = std::string("request");
  clientReqId[clientid] = 0;

  // a simplified client, need to add warmup in the future.
  uint64_t total_requests = n;
  char buf[10005];

  uint64_t start_time = microtime();
  uint64_t l_time, r_time, count = 0;
  // Closed-loop.
  while (total_requests) {
    ++count;
    netaddr raddr;
    SendRequest(clientid, c);
    ssize_t ret = c->ReadFrom(buf, 1e4, &raddr);

    std::string type, data;
    DecodePacket(buf, ret, type, data);
    ClientReceiveMessage(clientid, raddr, type, data);

    if (warmup_finished[clientid]) {
      --total_requests;
    } else if (microtime() - start_time > warmup) {
      printf("Completed warmup period of %ld seconds with %ld requests\n",
             warmup / 1000000, count);
      warmup_finished[clientid] = true;
      l_time = microtime();
    }
  }
  r_time = microtime();
  return r_time - l_time;
}

void ClientHandler(void *arg) {
  // FillArpTable();

  std::vector<rt::Thread> th;
  std::unique_ptr<std::vector<double>> samples[threads];

  // spawn one thread for each client.
  for (uint32_t i = 0; i < threads; ++i) {
    th.emplace_back(rt::Thread([&, i] {
      uint64_t v = ClientMain(i, kNetbenchPort + i);
      printf("Completed %ld requests in %.6f seconds.\n", n,
             (double)v * 0.000001);
    }));
  }

  for (auto &t : th) t.Join();

  std::vector<uint64_t> latency;
  for (uint32_t i = 0; i < threads; ++i) {
    latency.insert(latency.end(), latencies[i].begin(), latencies[i].end());
  }
  std::sort(latency.begin(), latency.end());

  printf("Median latency is %ld us\n", latency[(uint32_t)(n * threads * 0.5)]);
  printf("90th percentile latency is %ld us\n",
         latency[(uint32_t)(n * threads * 0.90)]);
  printf("95th percentile latency is %ld us\n",
         latency[(uint32_t)(n * threads * 0.95)]);
  printf("99th percentile latency is %ld us\n",
         latency[(uint32_t)(n * threads * 0.99)]);
}

void my_handler(int s) {
  printf("\n\nReceived %ld requst.\n", num_request);
  printf("Received %ld prepare.\n", num_prepare);
  printf("Received %ld prepareOK.\n", num_prepareOK);
  printf("Received %ld commit.\n", num_commit);
  exit(1);
}
}  // anonymous namespace

int main(int argc, char *argv[]) {
  int ret;

  if (argc < 3) {
    std::cerr << "usage: [cfg_file] [cmd] ..." << std::endl;
    return -EINVAL;
  }

  // capture ctrl-c behavior.
  struct sigaction sigIntHandler;
  sigIntHandler.sa_handler = my_handler;
  sigemptyset(&sigIntHandler.sa_mask);
  sigIntHandler.sa_flags = 0;
  sigaction(SIGINT, &sigIntHandler, NULL);

  // Setting cluster.
  StringToAddr("10.10.1.2", &cltaddr[0].ip);
  StringToAddr("10.10.1.3", &srvaddr[0].ip);
  StringToAddr("10.10.1.4", &srvaddr[1].ip);
  StringToAddr("10.10.1.5", &srvaddr[2].ip);
  StringToAddr("10.10.1.6", &srvaddr[3].ip);
  StringToAddr("10.10.1.7", &srvaddr[4].ip);
  StringToAddr("10.10.1.8", &srvaddr[5].ip);
  StringToAddr("10.10.1.9", &srvaddr[6].ip);
  for (uint32_t i = 1; i < MAX_CLIENT_NUM; ++i) cltaddr[i].ip = cltaddr[0].ip;
  for (uint32_t i = 0; i < CLUSTER_SIZE; ++i) srvaddr[i].port = kNetbenchPort;

  std::string cmd = argv[2];
  if (cmd.compare("server") == 0) {
    puts("I'm running server!");
    myIdx = std::stoi(argv[3], nullptr, 0);
    ret = runtime_init(argv[1], ServerHandler, NULL);
    if (ret) {
      printf("Server: failed to start runtime\n");
      return ret;
    }
  } else if (cmd.compare("client") != 0) {
    std::cerr << "invalid command: " << cmd << std::endl;
    return -EINVAL;
  }

  puts("I'm running client!");
  myIdx = CLUSTER_SIZE;

  warmup = std::stoi(argv[3], nullptr, 0) * 1000000;
  n = std::stoi(argv[4], nullptr, 0);
  threads = std::stoi(argv[5], nullptr, 0);
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
    sudo ./apps/bench/netbench_udp client.config client warmup n threads
    sudo ./apps/bench/netbench_udp client.config client 5 200000 5
Server:
    sudo ./iokerneld simple
        replica 0: sudo ./apps/bench/netbench_udp replica0.config server 0
        replica 1: sudo ./apps/bench/netbench_udp replica1.config server 1
        replica 2: sudo ./apps/bench/netbench_udp replica2.config server 2

*/