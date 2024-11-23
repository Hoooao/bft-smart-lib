import os
import time

import invoke
import yaml
from fabric import Connection, SerialGroup, ThreadingGroup
from invoke import task

### SMART BFT SCRIPT borrowed from DOM-BFT###
hosts_config_path = "../config/hosts.config"
sys_config_path = "../config/system.config"
benchmark_config_path = "../config/benchmark.config"
base_port_cli = 11000
base_port_rep = 11001
def get_gcloud_ext_ips(c):
    # parse gcloud CLI to get internalIP -> externalIP mapping    
    gcloud_output = c.run("gcloud compute instances list").stdout.splitlines()[1:]
    gcloud_output = map(lambda s: s.split(), gcloud_output)
    ext_ips = [
        # internal ip and external ip are last 2 tokens in each line
        line[-2]
        for line in gcloud_output if line[-1] == "RUNNING" and line[0] == "dev-client1"
    ]
    print("External IPs: ", ext_ips)
    return ext_ips

def gcloud_hosts_config(ext_ips,rep_per_node = 1):
    if len(ext_ips) == 0:
        print("No running instances found")
        return
    with open(hosts_config_path, "w") as f:
        print("Writing hosts.config")
        
        for ext_ip in ext_ips:
            i = 0
            while i < rep_per_node:
                f.write(f"{i} {ext_ip} {base_port_cli + i*10} {base_port_rep + i*10}\n")
                i += 1
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
        f.write("experiment.f = 1\n")
        f.write("experiment.clients_per_round = 1\n")
        # this is req per cli threads,  not process.
        f.write("experiment.req_per_client = 5000\n")
        f.write("experiment.data_size = 0\n")
        f.write("experiment.interval = 0\n")
        f.write("experiment.is_write = true\n")
        f.write("experiment.use_hashed_response=false\n")
        f.write("experiment.hosts.file=./config/hosts.config\n")
        f.write("experiment.measure_resources=false\n")

@task
def gcloud_build(c):

    ext_ips = get_gcloud_ext_ips(c)
    group = ThreadingGroup(*ext_ips)

    print("Cloning/building repo...")

    group.run("git clone https://github.com/Hoooao/bft-smart-lib.git smart_bft", warn=True)
    group.run("cd smart_bft && git pull && ./gradlew installDist")
    group.run("rm -rf ~/smart_bft_artifacts")
    group.run("cp -r build/install/library ~/smart_bft_artifacts")

@task
def gcloud_run(c):
    ext_ips = get_gcloud_ext_ips(c)
    controller_ip = ext_ips[0]
    workers_ips = ext_ips[1:]
    controller_conn = Connection(controller_ip)
    worker_group = ThreadingGroup(*workers_ips)

    gcloud_hosts_config(ext_ips)
    benchmark_config(benchmark_config_path, controller_ip, len(workers_ips))

    # Hao: smart seems uses synced sending/receiving on cli, so in its paper
    # it spawned 400 clis(threads, not processes in benchmark/) on each machine..
    print("Running BFT-Smart...")
    cmd_rm_old_execution_record = "cd ~/smart_bft_artifacts && rm config/currentView"
    controller_conn.run(cmd_rm_old_execution_record)
    worker_group.run(cmd_rm_old_execution_record, warn=True)
    print("Starting controller...")
    controller_conn.run("./smartrun.sh controller.BenchmarkControllerStartup ./config/benchmark.config")
    time.sleep(5)
    print("Starting workers...")
    worker_group.run(f"./smartrun.sh worker.WorkerStartup {controller_ip} {base_port_cli}")

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

@task
def local_reorder_exp(c, config_file, poisson=False):
    def arun(*args, **kwargs):
        return c.run(*args, **kwargs, asynchronous=True, warn=True)

    config_file = os.path.abspath(config_file)

    with open(config_file) as cfg_file:
        config = yaml.load(cfg_file, Loader=yaml.Loader)

    n_proxies = len(config["proxy"]["ips"])
    n_receivers = len(config["receiver"]["ips"])
    proxy_handles = []
    other_handles = []

    with c.cd(".."):
        c.run("killall dombft_replica dombft_proxy dombft_receiver dombft_client", warn=True)
        c.run("mkdir -p logs")

        for id in range(n_receivers):
            cmd = (
                f"./bazel-bin/processes/receiver/dombft_receiver -v {5} -config {config_file}"
                + f" -receiverId {id} -skipForwarding  &>logs/receiver{id}.log"
            )
            hdl = arun(cmd)

            other_handles.append(hdl)

        for id in range(n_proxies):
            cmd = (
                f"./bazel-bin/processes/proxy/dombft_proxy -v {5} " +
                f"-config {config_file} -proxyId {id} -genRequests  -duration 10 " +
                f"{'-poisson' if poisson else ''} &>logs/proxy{id}.log"
            )

            hdl = arun(cmd)
            proxy_handles.append(hdl)

    try:
        # join on the proxy processes, which should end
        for hdl in proxy_handles:
            hdl.join()

        print("Proxies done, waiting 5 sec for receivers to finish...")
        time.sleep(5)

    finally:

        c.run("killall dombft_replica dombft_proxy dombft_receiver dombft_client", warn=True)

        # kill these processes and then join
        for hdl in other_handles:
            hdl.runner.kill()
            hdl.join()



def get_all_int_ips(config):
    int_ips = set()
    for process in config:
        if process == "transport" or process == "app": continue
        int_ips |= set([ip for ip in config[process]["ips"]])
    
    return int_ips


def get_all_ext_ips(config, ext_ip_map):
    ips = []
    for ip in get_all_int_ips(config):  # TODO non local receivers?
        if (ip not in ext_ip_map): continue
        ips.append(ext_ip_map[ip])

    return ips 


@task
def gcloud_vm(c, config_file="../configs/remote-prod.yaml", stop=False):
    config_file = os.path.abspath(config_file)

    with open(config_file) as cfg_file:
        config = yaml.load(cfg_file, Loader=yaml.Loader)

    int_ips = get_all_int_ips(config)

    gcloud_output = c.run("gcloud compute instances list").stdout.splitlines()[1:]
    gcloud_output = map(lambda s : s.split(), gcloud_output)

    vm_info = {
        # name, zone, type, internal ip
        line[3] : (line[0], line[1])
        for line in gcloud_output
    }

    hdls = []
    for ip in int_ips: 
        name, zone = vm_info[ip]
        h = c.run(f"gcloud compute instances {'stop' if stop else 'start'} {name} --zone {zone}", asynchronous=True)
        hdls.append(h)

    for h in hdls:
        h.join()
    
    print(f"{'Stopped' if stop else 'Started'} all instances!")
    time.sleep(3) # Give time for ssh daemons to start for other tasks


@task
def gcloud_clockwork(c, config_file="../configs/remote-prod.yaml", install=False):
    config_file = os.path.abspath(config_file)

    with open(config_file) as cfg_file:
        config = yaml.load(cfg_file, Loader=yaml.Loader)

    ext_ips = get_gcloud_ext_ips(c)
    int_ips =  config["receiver"]["ips"] + config["proxy"]["ips"]

    # Only need to do this on proxies and receivers
    group = ThreadingGroup(
        *(ext_ips[ip] for ip in int_ips)
    )

    if install:
        group.put("../ttcs-agent_1.3.0_amd64.deb")
        group.run("sudo dpkg -i ttcs-agent_1.3.0_amd64.deb")

    with open("../ttcs-agent.cfg") as ttcs_file:
        ttcs_template = ttcs_file.read()

    ip = int_ips[0]
    ttcs_config = ttcs_template.format(ip, ip, 10, "false")
    Connection(ext_ips[ip]).run(f"echo '{ttcs_config}' | sudo tee /etc/opt/ttcs/ttcs-agent.cfg")

    for ip in int_ips[1:]:
        ttcs_config = ttcs_template.format(ip, ip, 1, "true")
        Connection(ext_ips[ip]).run(f"echo '{ttcs_config}'| sudo tee /etc/opt/ttcs/ttcs-agent.cfg")

    group.run("sudo systemctl stop ntp", warn=True)
    group.run("sudo systemctl disable ntp", warn=True)
    group.run("sudo systemctl stop systemd-timesyncd", warn=True)
    group.run("sudo systemctl disable systemd-timesyncd", warn=True)

    group.run("sudo systemctl enable ttcs-agent", warn=True)

    if install:
        group.run("sudo systemctl start ttcs-agent")
    else:
        group.run("sudo systemctl restart ttcs-agent")



@task
def gcloud_cmd(c, cmd, config_file="../configs/remote-prod.yaml"):
    with open(config_file) as cfg_file:
        config = yaml.load(cfg_file, Loader=yaml.Loader)

    ext_ips = get_gcloud_ext_ips(c)
    group = ThreadingGroup(*get_all_ext_ips(config, ext_ips))
    group.run(cmd)

@task
def gcloud_copy_keys(c, config_file="../configs/remote-prod.yaml"):
    config_file = os.path.abspath(config_file)

    with open(config_file) as cfg_file:
        config = yaml.load(cfg_file, Loader=yaml.Loader)

    ext_ips = get_gcloud_ext_ips(c)
    group = ThreadingGroup(*get_all_ext_ips(config, ext_ips))
    group.put(config_file)

    print("Copying keys over...")
    for process in ["client", "replica", "receiver", "proxy"]:
        group.run(f"mkdir -p keys/{process}")
        for filename in os.listdir(f"../keys/{process}"):
            group.put(os.path.join(f"../keys/{process}", filename), f"keys/{process}")


@task
def gcloud_copy_bin(c, config_file="../configs/remote-prod.yaml"):
    config_file = os.path.abspath(config_file)

    with open(config_file) as cfg_file:
        config = yaml.load(cfg_file, Loader=yaml.Loader)

    ext_ips = get_gcloud_ext_ips(c)

    replicas = config["replica"]["ips"]
    receivers = config["receiver"]["ips"]
    proxies = config["proxy"]["ips"]
    clients = config["client"]["ips"]

    # TODO try and check to see if binaries are stale
    print("Copying binaries over to one machine")
    start_time = time.time()

    conn = Connection(ext_ips[clients[0]])

    conn.run("chmod +w dombft_*", warn=True)
    conn.put("../bazel-bin/processes/replica/dombft_replica")
    conn.put("../bazel-bin/processes/receiver/dombft_receiver")
    conn.put("../bazel-bin/processes/proxy/dombft_proxy")
    conn.put("../bazel-bin/processes/client/dombft_client")
    conn.run("chmod +w dombft_*", warn=True)

    print(f"Copying took {time.time() - start_time:.0f}s")


    print(f"Copying to other machines")
    start_time = time.time()

    for ip in replicas:
        print(f"Copying dombft_replica to {ip}")
        conn.run(f"scp dombft_replica {ip}:", warn=True)

    for ip in receivers:
        print(f"Copying dombft_receiver to {ip}")
        conn.run(f"scp dombft_receiver {ip}:", warn=True)

    for ip in proxies:
        print(f"Copying dombft_proxy to {ip}")
        conn.run(f"scp dombft_proxy {ip}:", warn=True)

    for ip in set(clients[1:]): # Skip own
        print(f"Copying dombft_client to {ip}")
        conn.run(f"scp dombft_client {ip}:", warn=True)

    print(f"Copying to other machines took {time.time() - start_time:.0f}s")


def get_gcloud_process_ips(c, filter):
    gcloud_output = c.run(f"gcloud compute instances list | grep {filter}").stdout.splitlines()
    gcloud_output = map(lambda s : s.split(), gcloud_output)

    return [
        # internal ip is 3rd last token in line
        line[-3]
        for line in gcloud_output
    ]


@task
def gcloud_create_prod(c, config_template="../configs/remote-prod.yaml"):
    # This is probably better, but can't be across zones:
    # https://cloud.google.com/compute/docs/instances/multiple/create-in-bulk

    create_vm_template = """
gcloud compute instances create {} \
    --project=mythic-veld-419517 \
    --zone={} \
    --machine-type=t2d-standard-16 \
    --network-interface=network-tier=PREMIUM,stack-type=IPV4_ONLY,subnet=default \
    --create-disk=auto-delete=yes,boot=yes,device-name=prod-replica1,image=projects/debian-cloud/global/images/debian-12-bookworm-v20240709,mode=rw,size=20,type=projects/mythic-veld-419517/zones/us-west1-c/diskTypes/pd-ssd 
"""

    zones = ["us-west1-c", "us-west4-c", "us-east1-c", "us-east5-c"]

    config_file = os.path.abspath(config_template)

    with open(config_file) as cfg_file:
        config = yaml.load(cfg_file, Loader=yaml.Loader)


    n = len(config["replica"]["ips"])
    n_proxies = len(config["proxy"]["ips"])
    n_clients = len(config["client"]["ips"])

    for i in range(n):
        zone = zones[i % len(zones)]
        c.run(create_vm_template.format(f"prod-replica{i}", zone))

    for i in range(n_proxies):
        zone = zones[i % len(zones)]
        c.run(create_vm_template.format(f"prod-proxy{i}", zone))
    

    for i in range(n_clients):
        zone = zones[i % len(zones)]
        c.run(create_vm_template.format(f"prod-client{i}", zone))


    config["replica"]["ips"] = get_gcloud_process_ips(c, "prod-replica")
    config["receiver"]["ips"] = get_gcloud_process_ips(c, "prod-replica")
    config["proxy"]["ips"] = get_gcloud_process_ips(c, "prod-proxy")
    config["client"]["ips"] = get_gcloud_process_ips(c, "prod-client")

    filename, ext = os.path.splitext(config_template)
    yaml.dump(config, open(filename + "-prod" + ext, "w"))



def arun_on(ip, logfile, local_log=False, profile=False):
    def perf_prefix(prof_file):
        return f"env LD_PRELOAD='/home/dqian/libprofiler.so' CPUPROFILE={prof_file} CPUPROFILE_FREQUENCY={10} "

    if local_log:
        logfile = os.path.join("../logs/", logfile)
        def arun_local_log(command, **kwargs):
            log = open(logfile, "w")
            conn = Connection(ip)

            if (profile):
                command = perf_prefix(os.path.splitext(logfile)[0] + '.prof') + command

            print(f"Running {command} on {ip}, logging to local {logfile}")
            return conn.run(command + " 2>&1", **kwargs, asynchronous=True, warn=True, out_stream=log)

        return arun_local_log

    else:
        def arun(command, **kwargs):
            conn = Connection(ip)

            if profile:
                command = perf_prefix(os.path.splitext(logfile)[0] + '.prof') + command

            print(f"Running {command} on {ip}, logging on remote machine {logfile}" )
            return conn.run(command + f" &>{logfile}", **kwargs, asynchronous=True, warn=True)
        return arun


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



@task
def gcloud_reorder_exp(c, config_file="../configs/remote-prod.yaml", 
                    poisson=False, ignore_deadlines=False, duration=20, rate=100,
                    local_log=False):
    config_file = os.path.abspath(config_file)

    with open(config_file) as cfg_file:
        config = yaml.load(cfg_file, Loader=yaml.Loader)

    ext_ips = get_gcloud_ext_ips(c)
    group = ThreadingGroup(*get_all_ext_ips(config, ext_ips))
    group.put(config_file)
    group.run("killall dombft_replica dombft_proxy dombft_receiver dombft_client", warn=True, hide="both")

    # ips of each process 
    receivers = config["receiver"]["ips"]
    proxies = config["proxy"]["ips"]

    receiver_path = "./dombft_receiver"
    proxy_path = "./dombft_proxy"

    receivers = [ext_ips[ip] for ip in receivers]
    proxies = [ext_ips[ip] for ip in proxies]

    remote_config_file = os.path.basename(config_file)

    proxy_handles = []
    other_handles = []

    print("Starting receivers")
    for id, ip in enumerate(receivers):
        arun = arun_on(ip, f"receiver{id}.log", local_log=local_log)
        hdl = arun(
            f"{receiver_path}  -v {1} -receiverId {id} -config {remote_config_file}" 
            + f" -skipForwarding {'-ignoreDeadlines' if ignore_deadlines else ''}"
        )

        other_handles.append(hdl)

    time.sleep(5)

    print("Starting proxies")
    for id, ip in enumerate(proxies):
        arun = arun_on(ip, f"proxy{id}.log", local_log=local_log)
        hdl = arun(f"{proxy_path} -v {5} -config {remote_config_file} -proxyId {id} -genRequests " +
                f"{'-poisson' if poisson else ''} -duration {duration} -rate {rate}")
        
        proxy_handles.append(hdl)

    try:
        # join on the client processes, which should end
        for hdl in proxy_handles:
            hdl.join()
            
        print("Proxies done, waiting 5 sec for receivers to finish...")
        time.sleep(5)

    finally:
        # kill these processes and then join
        for hdl in other_handles:
            hdl.runner.send_interrupt(KeyboardInterrupt())
            hdl.join()

        if not local_log:
            get_logs(c, receivers, "receiver")
            get_logs(c, proxies, "proxy")
