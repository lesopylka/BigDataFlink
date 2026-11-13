import argparse
import json
import subprocess
import time
import urllib.request


def check_running():
    with urllib.request.urlopen("http://localhost:8081/jobs/overview", timeout=10) as response:
        jobs = json.load(response)["jobs"]
    return any(job["name"] == "sales_stream" and job["state"] == "RUNNING" for job in jobs)


def check_drained():
    result = subprocess.run([
        "docker", "compose", "exec", "-T", "kafka", "kafka-consumer-groups",
        "--bootstrap-server", "kafka:9092", "--describe", "--group", "sales-star",
    ], capture_output=True, text=True, timeout=30)
    rows = [line.split() for line in result.stdout.splitlines() if line.startswith("sales-star ")]
    return bool(rows) and all(row[5].isdigit() and int(row[5]) == 0 for row in rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--running", action="store_true")
    parser.add_argument("--drained", action="store_true")
    args = parser.parse_args()
    for attempt in range(120):
        try:
            if (check_running() if args.running else check_drained()):
                print("Flink job is running" if args.running else "Kafka consumer lag is zero")
                return
        except (OSError, ValueError, subprocess.SubprocessError):
            pass
        time.sleep(5)
    raise RuntimeError("Flink did not reach the expected state within 10 minutes")


if __name__ == "__main__":
    main()
