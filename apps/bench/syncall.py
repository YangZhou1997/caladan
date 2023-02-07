from concurrent.futures import thread
from distutils.log import warn
from http import server
from multiprocessing import Process
import pty
from pydoc import cli
from sys import stdout
from threading import Thread
from time import sleep
from unittest import result
import fabric
import subprocess
import os, time, sys
from concurrent.futures import ThreadPoolExecutor

# pip install Fabric

# DEV = 'enp6s0f0'
# RES_DIR = 'data_c220g2_batching'

# DEV = 'ens2f0np0'
# RES_DIR = 'data_r650_nobatching_notaskset'

DEV = 'ens1f1np1'
RES_DIR = 'data_xl170'


def load_kernel(node):
    node.run(
        "wget https://raw.githubusercontent.com/pimlie/ubuntu-mainline-kernel.sh/master/ubuntu-mainline-kernel.sh"
    )
    node.run("sudo bash ubuntu-mainline-kernel.sh -i 5.8.0")


def reboot(node):
    node.run("sudo reboot -h now")


def init_machine(node):
    global DEV
    # install library.
    node.run("sudo apt update")
    node.run(
        "sudo apt install -y llvm clang gpg curl tar xz-utils make gcc flex bison libssl-dev libelf-dev protobuf-compiler pkg-config libunwind-dev libssl-dev libprotobuf-dev libevent-dev libgtest-dev"
    )
    node.run("sudo apt install -y htop byobu dtach")

    # clone repo from github.
    # node.run("rm -rf Paxos-learn/")
    node.run(
        "[ ! -d \"Paxos-learn/\" ] && GIT_SSH_COMMAND=\"ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no\" git clone git@github.com:asd123www/Paxos-learn.git || true"
    )

    node.run(f'sudo ifconfig {DEV} mtu 3000 up')

    # Prepare kernel.
    with node.cd("Paxos-learn/"):
        node.run("pwd")
        # node.run("git checkout origin/auto_bench")
        node.run("bash kernel-src-download.sh")
        node.run("bash kernel-src-prepare.sh")


def compile_machine(node):
    with node.cd("Paxos-learn/xdp-handler/linux/tools/lib/bpf"):
        node.run("make")

    with node.cd("Paxos-learn/xdp-handler/"):
        node.run("make clean")
        node.run("make -j")

    # Compiler Paxos-code
    with node.cd("Paxos-learn/"):
        node.run("make clean")
        node.run("make -j PARANOID=0")


def kill_machine(node):
    global DEV
    with node.cd("Paxos-learn/"):
        node.run("sudo bash my_kill.sh")
        node.run("sudo pkill bench/client || true")
        node.run("sudo pkill replica ; sudo pkill fast || true")
        node.run(f'sudo ip link set {DEV} xdp off || true')
        node.run(
            f'sudo rm -f /sys/fs/bpf/paxos_prepare_buffer /sys/fs/bpf/paxos_request_buffer /sys/fs/bpf/paxos_ctr_state || true'
        )
        node.run(f'sudo tc filter del dev {DEV} egress || true')
        node.run(f'sudo tc qdisc del dev {DEV} clsact || true')
        node.run(f'sudo rm -f /sys/fs/bpf/FastBroadCast || true')


def disable_driver_batching(node):
    global DEV
    # for Mellanox NIC
    # need to run twice, as the driver might restore to a default value after first run
    node.run(
        f'sudo ethtool -C {DEV} adaptive-rx off adaptive-tx off rx-frames 1 rx-usecs 0 tx-frames 1 tx-usecs 0 || true'
    )
    node.run(
        f'sudo ethtool -C {DEV} adaptive-rx off adaptive-tx off rx-frames 1 rx-usecs 0 tx-frames 1 tx-usecs 0 || true'
    )

    # for Intel NIC
    # node.run(f'sudo ethtool -C {DEV} rx-usecs 0 || true')


def enable_driver_batching(node):
    global DEV
    # for Mellanox NIC
    node.run(
        f'sudo ethtool -C {DEV} adaptive-rx on adaptive-tx on rx-frames 128 rx-usecs 8 tx-frames 128 tx-usecs 8 || true'
    )

    # for Intel NIC
    # node.run(f'sudo ethtool -C {DEV} rx-usecs 1 || true')


def setup_irq_replica(node):
    global DEV
    node.run(f'sudo ethtool -L {DEV} combined 1')
    node.run(r'''(let CPU=0; cd /sys/class/net/{0}/device/msi_irqs/;
     for IRQ in *; do
        echo $CPU | sudo tee /proc/irq/$IRQ/smp_affinity_list
     done) || true'''.format(DEV))
    node.run("sudo service irqbalance stop")


def setup_irq_client(node):
    global DEV
    node.run(f'sudo ethtool -L {DEV} combined 20')
    node.run(r'''(let CPU=0; cd /sys/class/net/{0}/device/msi_irqs/;
     for IRQ in *; do
        echo $CPU | sudo tee /proc/irq/$IRQ/smp_affinity_list
        let CPU=$((($CPU+1)%20))
     done) || true'''.format(DEV))
    node.run("sudo service irqbalance start")


def runbg(node, cmd, sockname="dtach"):
    print(cmd)
    return node.run('dtach -n `mktemp -u /tmp/%s.XXXX` %s' % (sockname, cmd))


def rsync_repo(host):
    cmd = f'rsync -auv -e \"ssh -o StrictHostKeyChecking=no\" --exclude-from \'sync_exclude_list.txt\' ~/caladan/ yangzhou@{host}:~/caladan/'
    ret = os.popen(cmd).read()
    return ret


def control_machine(servers, func):
    thread_handlers = []
    for node in servers:
        h = Thread(target=func, args=(node, ))
        h.start()
        thread_handlers.append(h)
    for h in thread_handlers:
        h.join()


def build_normal(node):
    with node.cd("Paxos-learn/"):
        node.run("make clean ; make PARANOID=0 -j", hide=True)  # non-blocking


def build_xdp(node):
    with node.cd("Paxos-learn/"):
        node.run(
            "make clean ; make PARANOID=0 -j CXXFLAGS=\"-DFAST_REPLY -DFAST_QUORUM_PRUNE -DTC_BROADCAST -DDISABLE_TIMEOUT\"",
            hide=True)  # non-blocking
    with node.cd("Paxos-learn/xdp-handler/"):
        node.run("make clean && make", hide=True)  # non-blocking
        node.run("sudo sysctl -w net.core.rmem_max=26214400",
                 hide=True)  # non-blocking
        node.run("sudo sysctl -w net.core.rmem_default=26214400",
                 hide=True)  # non-blocking


def run_xdp(node):
    global DEV
    with node.cd("Paxos-learn/xdp-handler/"):
        runbg(node, f'sudo ./fast {DEV}')


def run_replica(node, myIdx):
    with node.cd("Paxos-learn/"):
        runbg(
            node,
            "sudo taskset -c 19 nice -n -20 ./bench/replica -c config.txt -m vr -i {0}"
            .format(myIdx))


def epoch(mode, warmup, reqs, clients, runs):
    if mode == 'xdp':
        for (i, node) in enumerate(
            [replica1, replica2, replica3, replica4, replica5]):
            run_xdp(node)
        sleep(3)

    for (i, node) in enumerate([replica1]):
        run_replica(node, i)
    sleep(1.5)

    for (i, node) in enumerate([replica2, replica3, replica4, replica5]):
        run_replica(node, i + 1)
    sleep(1.5)

    with client.cd("Paxos-learn/"):
        cmd = "sudo taskset -c 19 nice -n -20 ./bench/client -c config.txt -m vr -w {0} -n {1} -t {2}".format(
            warmup, reqs, clients)
        print(cmd)
        ret_val = client.run(cmd)
        # print(ret_val.stdout)
        with open(f'{RES_DIR}/{mode}_c{clients}_{runs}.txt', 'w') as f:
            f.write(ret_val.stderr)


if __name__ == "__main__":
    os.system(f'mkdir -p {RES_DIR}')

    # Using public IP to ssh, as internal interface might be blocked due to eBPF
    node_ips = [
        "node-0.yangzhou-146695.lambda-mpi-pg0.utah.cloudlab.us",
        "node-1.yangzhou-146695.lambda-mpi-pg0.utah.cloudlab.us",
        "node-2.yangzhou-146695.lambda-mpi-pg0.utah.cloudlab.us",
        "node-3.yangzhou-146695.lambda-mpi-pg0.utah.cloudlab.us",
        "node-4.yangzhou-146695.lambda-mpi-pg0.utah.cloudlab.us",
        "node-5.yangzhou-146695.lambda-mpi-pg0.utah.cloudlab.us"
    ]

    client = fabric.Connection(host=node_ips[0])
    replica1 = fabric.Connection(host=node_ips[1])
    replica2 = fabric.Connection(host=node_ips[2])
    replica3 = fabric.Connection(host=node_ips[3])
    replica4 = fabric.Connection(host=node_ips[4])
    replica5 = fabric.Connection(host=node_ips[5])

    nodes = [client, replica1, replica2, replica3, replica4, replica5]
    replicas = [replica1, replica2, replica3, replica4, replica5]

    executor = ThreadPoolExecutor(max_workers=20)

    # Sync all repos
    for result in executor.map(rsync_repo, node_ips):
        print(result)

    print("Finish benching successfully.")
