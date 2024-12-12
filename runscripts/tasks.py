import os
import time

import invoke
import yaml
from fabric import Connection, SerialGroup, ThreadingGroup
from invoke import task
import requests
import threading

### SMART BFT SCRIPT borrowed from DOM-BFT###
hosts_config_path = "../config/hosts.config"
sys_config_path = "../config/system.config"
benchmark_config_path = "../config/benchmark.config"
log_config_path = "../config/logback.xml"
base_port_cli = 11000
base_port_rep = 11001

def get_gcloud_ext_ips(c, keyword=None):
    # parse gcloud CLI to get internalIP -> externalIP mapping    
    gcloud_output = c.run("gcloud compute instances list").stdout.splitlines()[1:]
    gcloud_output = map(lambda s: s.split(), gcloud_output)
    ext_ips = [
        # internal ip and external ip are last 2 tokens in each line
        line[-2]
        for line in gcloud_output if line[-1] == "RUNNING" and (keyword==None or keyword in line[0])
    ]
    print("External IPs: ", ext_ips)
    return ext_ips

def gcloud_hosts_config(ext_ips,rep_per_node = 1):
    if len(ext_ips) == 0:
        print("No running instances found")
        return
    with open(hosts_config_path, "w") as f:
        print("Writing hosts.config")
        n=0
        for ext_ip in ext_ips:
            i = 0
            while i < rep_per_node:
                f.write(f"{n} {ext_ip} {base_port_cli + i*10} {base_port_rep + i*10}\n")
                i += 1
                n+=1
def local_hosts_config(rep_per_node = 1, position = hosts_config_path):
    with open(position, "w") as f:
        print("Writing hosts.config")
        # start from 1 since bases are for controller
        i = 1
        while i <= rep_per_node:
            f.write(f"{i-1} 127.0.0.1 {base_port_cli + i*10} {base_port_rep + i*10}\n")
            i += 1    
def benchmark_config(config_f, controller_ip, worker_num = 5):
    with open(config_f, "w") as f:
        # Hao: there is no template for this in SMART's repo...
        #   I just copied the needed configs from ThroughputLatencyBenchmarkStrategy.java...
        f.write(f"controller.listening.ip = {controller_ip}\n")
        f.write(f"controller.listening.port = {base_port_cli}\n")
        f.write(f"global.worker.machines = {worker_num}\n")
        f.write("controller.benchmark.strategy = bftsmart.benchmark.ThroughputLatencyBenchmarkStrategy\n")
        f.write("controller.worker.setup = bftsmart.tests.common.BFTSMaRtSetup\n")
        f.write("controller.worker.processor = bftsmart.benchmark.ThroughputEventProcessor\n")
        # will automatically set 4 replicas
        f.write("experiment.f = 1\n")
        # for repeat tests
        f.write("experiment.clients_per_round = 3200\n")
        # this is req per cli threads,  not process.
        f.write("experiment.req_per_client = 1000\n")
        f.write("experiment.data_size = 0\n")
        f.write("experiment.interval = 0\n")
        # try both f/t, readonly can use unordered delivery
        f.write("experiment.is_write = true\n")
        f.write("experiment.use_hashed_response=false\n")
        f.write("experiment.hosts.file=./config/hosts.config\n")
        f.write("experiment.measure_resources=false\n")

@task
def gcloud_build(c, install_java = False):

    ext_ips = get_gcloud_ext_ips(c)
    group = ThreadingGroup(*ext_ips)

    if install_java:
        # install java
        group.run("sudo apt-get update && sudo apt-get install -y default-jre")
    print("Cloning/building repo...")

    group.run("git clone https://github.com/Hoooao/bft-smart-lib.git smart_bft", warn=True)
    group.run("cd smart_bft && git pull && ./gradlew installDist")
    group.run("rm -rf ~/smart_bft_artifacts", warn=True)
    group.run("cp -r smart_bft/build/install/library ~/smart_bft_artifacts")

@task
def gcloud_run(c, colocate = False):
    controller_ip = get_gcloud_ext_ips(c, "proxy")[0]
    replica_ips = get_gcloud_ext_ips(c, "replica")
    client_ips = get_gcloud_ext_ips(c, "client")
    workers_ips = replica_ips + client_ips
    worker_per_node = 1
    rep_node = 4
    rep_per_rep_node = 1
    cli_per_node = 1
    controller_conn = Connection(controller_ip)
    worker_group = ThreadingGroup(*workers_ips)
    print("Controller IP: ", controller_ip)
    gcloud_hosts_config(replica_ips, rep_per_rep_node)
    benchmark_config(benchmark_config_path, "0.0.0.0", len(workers_ips) * worker_per_node)

    # Hao: smart seems uses synced sending/receiving on cli, so in its paper
    # it spawned 400 clis(threads, not processes in benchmark/) on each machine..
    # Note: hotstuff uses the microbench mark one,not the async one, so we should do it as well
    print("Running BFT-Smart...")
    print("Starting controller..")
    controller_conn.put(hosts_config_path)
    controller_conn.put(benchmark_config_path)
    controller_conn.put(log_config_path)
    worker_group.put(log_config_path)
    controller_conn.run("mv ~/hosts.config ~/smart_bft_artifacts/config/ && mv ~/benchmark.config ~/smart_bft_artifacts/config/", warn=True)
    worker_group.run("mv ~/logback.xml ~/smart_bft_artifacts/config/", warn=True)
    controller_conn.run("cd ~/smart_bft_artifacts && killall -9 java && rm ./config/currentView", warn=True)
    # fire a thread to run the controller
    # controller_conn.run("cd ~/smart_bft_artifacts && ./smartrun.sh controller.BenchmarkControllerStartup ./config/benchmark.config  &> controller.log")
    threading.Thread(target=controller_conn.run, args=(f"cd ~/smart_bft_artifacts && ./smartrun.sh controller.BenchmarkControllerStartup ./config/benchmark.config  &> controller.log",)).start()
    time.sleep(5)
    print("Starting workers...")
    worker_group.run(f"killall -9 java && cd ~/smart_bft_artifacts && rm config/currentView", warn=True)
    #worker_group.run(f"cd ~/smart_bft_artifacts && ./smartrun.sh worker.WorkerStartup {controller_ip} {base_port_cli} &> worker.log")
    # server_group = ThreadingGroup(*workers_ips[:rep_node])
    # cli_group = ThreadingGroup(*workers_ips[rep_node:])

    # Hao: smart does not take care of how to assign roles. it just assign the first x to server, the rest to cli
    # we need to force them to be registered in controller in desired order

    
    if colocate:
        # colocate tests:
        for i in range(rep_per_rep_node):
            for ip in workers_ips:
                print(f"Starting rep {i} on machine {ip}...")
                ip_conn = Connection(ip)
                threading.Thread(target=ip_conn.run, args = (f"cd ~/smart_bft_artifacts && ./smartrun.sh worker.WorkerStartup {controller_ip} {base_port_cli} &> server{i}.log",)).start()
                time.sleep(4)
        time.sleep(6)   
        for i in range(cli_per_node):
            for ip in workers_ips:
                print(f"Starting cli {i} on machine {ip}...")
                ip_conn = Connection(ip)
                threading.Thread(target=worker_group.run, args = (f"cd ~/smart_bft_artifacts && ./smartrun.sh worker.WorkerStartup {controller_ip} {base_port_cli} &> client{i}.log",)).start()

    else:
        # This is for non-colocate tests
        for ip in replica_ips:
            print(f"Starting rep worker on {ip}...")
            ip_conn = Connection(ip)
            threading.Thread(target=ip_conn.run, args = (f"cd ~/smart_bft_artifacts && ./smartrun.sh worker.WorkerStartup {controller_ip} {base_port_cli} &> server.log",)).start()
            time.sleep(4)
        time.sleep(5)   
        for ip in client_ips:
            print(f"Starting cli worker on {ip}...")
            ip_conn = Connection(ip)
            threading.Thread(target=ip_conn.run, args = (f"cd ~/smart_bft_artifacts && ./smartrun.sh worker.WorkerStartup {controller_ip} {base_port_cli} &> client.log",)).start()
            time.sleep(4)
        
    
@task
def local(c):
    local_ip = "127.0.0.1"
    worker_num = 5
    build_path = "../build/install/library"
    def arun(*args, **kwargs):
        return c.run(*args, **kwargs, asynchronous=True, warn=True)
    c.run("killall -9 java", warn=True)
    
    print("Running BFT-Smart locally...")
    benchmark_config(f"{build_path}/config/benchmark.config", local_ip, worker_num)
    # hardcode to 4..
    local_hosts_config(4,f"{build_path}/config/hosts.config")
    with c.cd(build_path):
        c.run("pwd")
        c.run("rm ./config/currentView", warn=True)
        print("Starting controller...")
        hdl = arun(f"./smartrun.sh controller.BenchmarkControllerStartup ./config/benchmark.config &> controller.log")
        time.sleep(3)
        print("Starting workers...")
        for i in range(worker_num):
            arun(f"./smartrun.sh worker.WorkerStartup {local_ip} {base_port_cli} &> worker{i}.log")
        print("Wait till finish...")
        hdl.join()

@task
def gcloud_killall_java(c):
    ext_ips = get_gcloud_ext_ips(c)
    group = ThreadingGroup(*ext_ips)
    # may not be the best way
    group.run("killall -9 java", warn=True)



def get_logs(c, ips, log_prefix):
    for id, ip in enumerate(ips):
        conn = Connection(ip)
        print(f"Getting {log_prefix}{id}.log")
        conn.get(f"{log_prefix}{id}.log", "../logs/")

    # for id, ip in enumerate(ips):
    #     conn = Connection(ip)

    #     try:
    #         print(f"Getting {log_prefix}{id}.prof")
    #         conn.get(f"{log_prefix}{id}.prof", "../logs/")
    #         print(f"Got profile in {log_prefix}{id}.prof ")
    #     except:
    #         c.run(f"rm ../logs/{log_prefix}{id}.prof")


@task
def gcloud_logs(c, config_file="../configs/remote-prod.yaml"):

    with open(config_file) as cfg_file:
        config = yaml.load(cfg_file, Loader=yaml.Loader)

    ext_ips = get_gcloud_ext_ips(c)
    # ips of each process 
    replicas = config["replica"]["ips"]
    receivers = config["receiver"]["ips"]
    proxies = config["proxy"]["ips"]
    clients = config["client"]["ips"]

    replicas = [ext_ips[ip] for ip in replicas]
    receivers = [ext_ips[ip] for ip in receivers]
    proxies = [ext_ips[ip] for ip in proxies]
    clients = [ext_ips[ip] for ip in clients]

    get_logs(c, replicas, "replica")
    get_logs(c, receivers, "receiver")
    get_logs(c, proxies, "proxy")
    get_logs(c, clients, "client")

