from scheduler_logger import SchedulerLogger, Job
import subprocess
import threading
import time
import psutil
import docker

MAX_CORES = 4
MAX_CORES_SET = {i for i in range(MAX_CORES)}

BENCHES = [job for job in Job if job not in [Job.MEMCACHED, Job.SCHEDULER]]

IMAGES = {
    Job.BLACKSCHOLES: "anakli/cca:parsec_blackscholes",
    Job.CANNEAL: "anakli/cca:parsec_canneal",
    Job.DEDUP: "anakli/cca:parsec_dedup",
    Job.FERRET: "anakli/cca:parsec_ferret",
    Job.FREQMINE: "anakli/cca:parsec_freqmine",
    Job.RADIX: "anakli/cca:splash2x_radix",
    Job.VIPS: "anakli/cca:parsec_vips",
}

BENCH_TYPE = {
    Job.BLACKSCHOLES: "parsec",
    Job.CANNEAL: "parsec",
    Job.DEDUP: "parsec",
    Job.FERRET: "parsec",
    Job.FREQMINE: "parsec",
    Job.RADIX: "splash2x",
    Job.VIPS: "parsec",
}

PRIORITY = [Job.FERRET, Job.FREQMINE, Job.CANNEAL, Job.BLACKSCHOLES, Job.VIPS, Job.RADIX, Job.DEDUP]

lock = threading.Lock()
client = docker.from_env()
logger = SchedulerLogger()
containers = {}
core_usage = {
    Job.MEMCACHED: set(),
    Job.BLACKSCHOLES: set(),
    Job.CANNEAL: set(),
    Job.DEDUP: set(),
    Job.FERRET: set(),
    Job.FREQMINE: set(),
    Job.RADIX: set(),
    Job.VIPS: set(),
}

def validate_config(config):
    assert isinstance(config, list)

    cur_jobs = []
    for jobs in config:
        if not isinstance(jobs, list): jobs = [jobs]
        for job, cores, threads in jobs:
            assert job in BENCHES
            assert threads <= MAX_CORES
            assert len(cores) <= MAX_CORES
            for c in cores: assert c < MAX_CORES

            cur_jobs.append(job)

    key = lambda x: x.value
    if sorted(BENCHES, key=key) != sorted(cur_jobs, key=key):
        print("WARNING: Some jobs have not been scheduled!")

def run_job(job, cores, threads):
    # TODO: possibly spawn with more cores if load is low
    cores = cores | {1} if 1 not in core_usage[Job.MEMCACHED] else cores
    cpus = ",".join(map(str, cores))
    name = job.value
    image = IMAGES[job]
    command = f"./run -a run -S {BENCH_TYPE[job]} -p {name} -i native -n {threads}"

    container = client.containers.run(image, command, detach=True, remove=True, name=name, cpuset_cpus=cpus)
    logger.job_start(job, list(cores), threads)
    with lock:
        containers[job] = container
        core_usage[job] = cores

    status = container.wait()
    assert status["StatusCode"] == 0

    logger.job_end(job)
    with lock:
        del containers[job]

def run_memcached(pid, cores):
    # does not actually start memcached, just logs appropriate messages end ensures core affinity
    subprocess.run(["taskset", "-a", "-cp", ",".join(map(str, cores)), str(pid)], check=True, capture_output=True)
    logger.job_start(Job.MEMCACHED, list(cores), 2)

    with lock:
        core_usage[Job.MEMCACHED] = cores

def update_job(job, cores):
    cpus = ",".join(map(str, cores))

    with lock:
        if job not in containers: return False
        container = containers[job]
        core_usage[job] = cores

    try:
        container.reload()
    except docker.errors.NotFound:
        return False

    if container.status == "removing":
        return False

    container.update(cpuset_cpus=cpus)
    logger.update_cores(job, list(cores))
    return True

def update_memcached(pid, cores):
    cpus = ",".join(map(str, cores))
    subprocess.run(["taskset", "-a", "-cp", cpus, str(pid)], check=True, capture_output=True)
    logger.update_cores(Job.MEMCACHED, list(cores))

    with lock:
        core_usage[Job.MEMCACHED] = cores

decrease_count = 3
def update_resources(memcached_pid):
    global decrease_count
    usage = psutil.cpu_percent(percpu=True)
    print(f"CPU USAGE: {usage}")

    memcached_usage = sum(usage[i] for i in core_usage[Job.MEMCACHED])
    if len(core_usage[Job.MEMCACHED]) == 1 and memcached_usage > 70:
        assert(core_usage[Job.MEMCACHED] == {0})
        decrease_count = 3
        update_memcached(memcached_pid, {0, 1})
        for job in PRIORITY:
            if 1 in core_usage[job]:
                if update_job(job, core_usage[job] - {1}):
                    break
    elif len(core_usage[Job.MEMCACHED]) == 2 and memcached_usage < 80:
        assert(core_usage[Job.MEMCACHED] == {0, 1})
        decrease_count -= 1
        if decrease_count == 0:
            update_memcached(memcached_pid, {0})
            for job in PRIORITY:
                if update_job(job, core_usage[job] | {1}):
                    break
    else:
        decrease_count = 3

def run_jobs(jobs):
    for job, cores, threads in jobs:
        run_job(job, cores, threads)

def run_controller(config, memcached_pid):
    threads = []
    for jobs in config:
        if not isinstance(jobs, list): jobs = [jobs]
        thread = threading.Thread(target=run_jobs, args=(jobs,))
        threads.append(thread)

    for thread in threads:
        thread.start()

    psutil.cpu_percent(percpu=True)  # initial dummy call that always returns 0.0
    while True:
        time.sleep(1)
        update_resources(memcached_pid)
        if not any(thread.is_alive() for thread in threads):
            update_memcached(memcached_pid, {0, 1})
            break

def find_memcached_pid():
    for proc in psutil.process_iter(["pid", "name"]):
        if "memcached" in proc.info["name"]:
            return proc.info["pid"]
    raise Exception("Could not find memcached process")

# running all from start with 4 cores/threads: 6:50 min
# config: a list, each element is a list of sequentially run jobs
config = [
    [
    (Job.FERRET, {2, 3}, 3),
    (Job.FREQMINE, {2, 3}, 3),
    (Job.CANNEAL, {2, 3}, 3),
    (Job.BLACKSCHOLES, {2, 3}, 3),
    (Job.VIPS, {2, 3}, 3),
    (Job.DEDUP, {2, 3}, 3),
    (Job.RADIX, {2, 3}, 2),
    ]
]
memcached_initial_cores = {0, 1}

if __name__ == "__main__":
    memcached_pid = find_memcached_pid()
    run_memcached(memcached_pid, memcached_initial_cores)

    validate_config(config)
    run_controller(config, memcached_pid)

    logger.end()
