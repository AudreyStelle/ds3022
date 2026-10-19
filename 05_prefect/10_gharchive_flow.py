import gzip
import json
from collections import Counter
import requests
from prefect import flow, task


URL = "https://data.gharchive.org/2025-01-01-{hour}.json.gz"


@task(retries=3)
def count_events_for_hour(hour):
    counts = Counter()
    with requests.get(URL.format(hour=hour), stream=True, timeout=60) as r:
        r.raise_for_status()  # HTTP errors raise, so Prefect retries the task
        # decompress the gzip stream as it downloads; nothing is written to disk
        with gzip.GzipFile(fileobj=r.raw) as gz:
            for line in gz:
                try:
                    counts[json.loads(line)["type"]] += 1
                except (json.JSONDecodeError, KeyError):
                    continue  # skip malformed lines or lines with no "type" field
    print(f"hour {hour}: {counts.total()} events")
    return dict(counts)


@task(log_prints=True)
def combine_counts(hourly_counts):
    totals = Counter()
    for counts in hourly_counts:
        totals.update(counts)  # add each hour's {type: count} into the running total
    ordered = dict(totals.most_common())
    for event_type, count in ordered.items():
        print(f"{event_type}: {count}")
    return ordered


@flow(log_prints=True)
def gharchive_flow(hours: list[int] = list(range(1, 4))):
    # take the range and create a prefect .map over the hours
    hourly = count_events_for_hour.map(hours).result()
    totals = combine_counts(hourly)
    for k, v in totals.items():
        print(k, v)


if __name__ == "__main__":
    gharchive_flow()
