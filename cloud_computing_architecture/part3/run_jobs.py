#!/usr/bin/python3

import subprocess
import sys
import os
import logging
import threading
import time

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level= logging.INFO,
    datefmt='%H:%M:%S')

NODES = {
    "node-a-2core": 2,  # e2-highmem-2
    "node-b-2core": 2,  # n2-highcpu-2
    "node-c-4core": 4,  # c3-highcpu-4
    "node-d-4core": 4,  # n2-standard-4
}

JOBS = ["blackscholes", "canneal", "dedup", "ferret", "freqmine", "radix", "vips"]

def run_job(name, node, cores):
    name = f"parsec-{name}"
    yaml_template_name = f"yamls/{name}.yaml"
    cores_str = ",".join(str(core) for core in cores)

    with open(yaml_template_name, "r") as f:
        env = {**os.environ, "JOB_NODE": node, "JOB_CORES_N": str(len(cores)), "JOB_CORES": cores_str}
        yaml_template = f.read()
        yaml = subprocess.run(["envsubst"], input=yaml_template, check=True, capture_output=True, env=env, text=True).stdout

    subprocess.run(["kubectl", "create", "-f", "-"], input=yaml, check=True, capture_output=True, text=True)
    logging.info(f"Started {name} on {node} using cores {cores_str}")

    subprocess.run(["kubectl", "wait", "--for=condition=complete", "--timeout=3600s", f"job/{name}"], check=True, capture_output=True)
    logging.info(f"Finished {name}")

def run_jobs_serial(node, jobs):
    for name, cores in jobs:
        run_job(name, node, cores)

def run_jobs(config):
    threads = []
    for node, job_configs in config.items():
        for jobs in job_configs:
            thread = threading.Thread(target=run_jobs_serial, args=(node, jobs))
            threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

def save_times(out_file):
    subprocess.run(f"kubectl get pods -o json > results.json", check=True, shell=True, capture_output=True)
    out = subprocess.run(f"python3 ../get_time.py results.json", check=True, shell=True, capture_output=True, text=True)

    with open(out_file, "w") as file:
        file.write(out.stdout)
    logging.info(f"Saved output to {out_file}...")

    print("Output:")
    print(out.stdout)

def delete_jobs():
    subprocess.run(["kubectl", "delete", "jobs", "--all"], check=True, capture_output=True)
    logging.info(f"Deleted stale jobs")

def validate_config(config):
    cur_jobs = []

    for node, job_configs in config.items():
        assert node in NODES
        for jobs in job_configs:
            for name, cores in jobs:
                assert name in JOBS
                assert len(cores) > 0
                cur_jobs.append(name)
                for core in cores:
                    assert core < NODES[node]

    # assert sorted(cur_jobs) == sorted(job_names)
    if sorted(cur_jobs) != sorted(JOBS):
        logging.warning("Some jobs have not been scheduled")

out_file = sys.argv[1] if len(sys.argv) > 1 else "out.txt"

kss = "KOPS_STATE_STORE"
kss_val = os.environ.get(kss)
project = "PROJECT"
project_val = os.environ.get(project)

print(f"""
Environment:
{kss}={kss_val}
{project}={project_val}
""")

# Remember to manually start memcached and mcperf!!!
config = {
    "node-a-2core": [
        [("vips", [0, 1])],
        [("dedup", [0, 1])],
    ],
    "node-b-2core": [
        [("canneal", [0, 1])],
    ],
    "node-c-4core": [
        [("freqmine", [0, 1, 2, 3])],
        [("radix", [2, 3])],
    ],
    "node-d-4core": [
        [("ferret", [0, 1, 2, 3])],
        [("blackscholes", [0, 1, 2, 3])],
    ],
}

if __name__ == "__main__":
    validate_config(config)
    run_jobs(config)
    save_times(out_file)
    delete_jobs()
