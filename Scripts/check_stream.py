import argparse
import json
import subprocess
import time
import urllib.request


def get(path):
    with urllib.request.urlopen("http://localhost:8081" + path, timeout=10) as response:
        return json.load(response)


def running_job():
    jobs = get("/jobs/overview")["jobs"]
    return next((job for job in jobs if job["name"] == "sales_stream"
                 and job["state"] == "RUNNING" and job["tasks"].get("total", 0) > 0
                 and job["tasks"].get("running") == job["tasks"]["total"]), None)


def check_drained(expected_input, started):
    job = running_job()
    if job is None:
        return False
    result = subprocess.run([
        "docker", "compose", "exec", "-T", "-e", "KAFKA_HEAP_OPTS=-Xms64m -Xmx256m",
        "kafka", "kafka-consumer-groups", "--bootstrap-server", "kafka:9092",
        "--describe", "--group", "sales-star",
    ], capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise RuntimeError(f"Kafka CLI failed ({result.returncode}): {result.stdout} {result.stderr}")
    rows = [line.split() for line in result.stdout.splitlines()
            if line.split() and line.split()[0] == "sales-star"]
    checkpoints = get(f"/jobs/{job['jid']}/checkpoints")
    completed = checkpoints.get("latest", {}).get("completed") or {}
    fresh = completed.get("trigger_timestamp", 0) >= started
    offsets = [(row[3], row[4], row[5]) for row in rows]
    print(f"Kafka offsets (current, end, lag): {offsets}; fresh checkpoint: {fresh}", flush=True)
    if not rows:
        print(result.stdout + result.stderr, flush=True)
    return (fresh and bool(rows) and all(row[3].isdigit() and row[4].isdigit()
            and row[3] == row[4] for row in rows)
            and sum(int(row[4]) for row in rows if row[4].isdigit()) == expected_input)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--running", action="store_true")
    parser.add_argument("--drained", action="store_true")
    parser.add_argument("--expected-input", type=int, default=10000)
    args = parser.parse_args()
    started = int(time.time() * 1000)
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        try:
            ready = bool(running_job()) if args.running else check_drained(args.expected_input, started)
            if ready:
                print("Flink job is running" if args.running else "Fresh checkpoint committed all Kafka records")
                return
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            print(f"Waiting for Flink: {error}", flush=True)
        time.sleep(5)
    raise RuntimeError("Flink did not reach the expected state within 10 minutes")


if __name__ == "__main__":
    main()
