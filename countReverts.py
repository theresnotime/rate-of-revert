import argparse
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

from termcolor import colored

from eventstreams import EventStreams

import mysql.connector
import config

DEFAULT_START = 2  # Default start time in minutes ago from now

db = mysql.connector.connect(
    host=config.DB_HOST,
    user=config.DB_USER,
    password=config.DB_PASSWORD,
    database=config.DB_DATABASE,
    port=config.DB_PORT,
)
cursor = db.cursor()


def prepare_datetime(datetime: str):
    datetime = re.sub(r"T", " ", datetime)
    datetime = re.sub(r"Z", "", datetime)
    return datetime


def log_to_db(
        start_timestamp: str,
        end_timestamp: str,
        wiki: str,
        count: int
) -> None:
    start_timestamp = prepare_datetime(start_timestamp)
    end_timestamp = prepare_datetime(end_timestamp)
    sample_minutes = (datetime.strptime(end_timestamp, "%Y-%m-%d %H:%M:%S") - datetime.strptime(start_timestamp, "%Y-%m-%d %H:%M:%S")).total_seconds() / 60
    now_timestamp = prepare_datetime(datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"))
    sample_hash = hash(f"{start_timestamp}{end_timestamp}")

    sql = "INSERT INTO rates (date_added, start_timestamp, end_timestamp, sample_minutes, sample_group_hash, wiki, count) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    values = (
        now_timestamp,
        start_timestamp,
        end_timestamp,
        sample_minutes,
        sample_hash,
        wiki,
        count
    )
    cursor.execute(sql, values)
    db.commit()


def keep_count(change: dict, wiki_counts: dict) -> dict:
    if change["database"] not in wiki_counts:
        wiki_counts[change["database"]] = 1
    else:
        wiki_counts[change["database"]] += 1
    return wiki_counts


def count(
    start_timestamp: str,
    end_timestamp: str,
    debug: bool = False,
    verbose: bool = False,
    delay: float = 0.1,
) -> dict:
    if verbose:
        print(f"Start: {start_timestamp}")
        print(f"End: {end_timestamp}")

    started_run = datetime.now()
    stream = EventStreams(
        streams=["mediawiki.revision-tags-change"], since=start_timestamp, timeout=1
    )
    wiki_counts = {}
    total_count = 0
    watch_tags = ["mw-rollback", "mw-undo"]

    while stream:
        change = next(iter(stream))
        database = change["database"]
        rev_timestamp = change["rev_timestamp"]
        added_tags = change["tags"]

        if rev_timestamp >= end_timestamp:
            break

        if any(item in watch_tags for item in added_tags):
            # Watched tag
            if debug:
                print(
                    colored(f"{database} at {rev_timestamp} >>> {added_tags}", "green")
                )
            wiki_counts = keep_count(change, wiki_counts)
            total_count += 1
        else:
            # Not a watched tag
            if debug:
                print(colored(f"{database} at {rev_timestamp} >>> {added_tags}", "red"))
            continue
        time.sleep(delay)

    ended_run = datetime.now()
    run_time = ended_run - started_run
    if verbose:
        print(f"Run time: ~{round(run_time.total_seconds() / 60)} minutes")
        print(f"Total count: {total_count}")
    return wiki_counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="countReverts", description="x")
    parser.add_argument(
        "--start",
        required=True,
        help=f'Start timestamp (or "default" for {DEFAULT_START}m ago)',
    )
    parser.add_argument("--end", required=True, help='End timestamp (or "now")')
    parser.add_argument("--wiki", default="all", help='For wiki (or "all")')
    parser.add_argument("--no-json", help='Don\'t return JSON', action="store_false")
    parser.add_argument("--log", help="Log to database", action="store_true")
    parser.add_argument("-v", "--verbose", help="Be verbose", action="store_true")
    parser.add_argument("--debug", help="Show debug info", action="store_true")
    args = parser.parse_args()

    if args.start == "default":
        start_timestamp = (
            (datetime.now(timezone.utc) - timedelta(minutes=DEFAULT_START))
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )
    else:
        start_timestamp = args.start

    if args.end == "now":
        end_timestamp = (
            datetime.now(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )
    else:
        end_timestamp = args.end

    counts = count(
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        debug=args.debug,
        verbose=args.verbose,
    )

    if args.wiki == "all":
        if args.log:
            for wiki in counts:
                log_to_db(
                    start_timestamp=start_timestamp,
                    end_timestamp=end_timestamp,
                    wiki=wiki,
                    count=counts[wiki]
                )
        if args.no_json is True:
            print(json.dumps(counts))
        else:
            for count in counts:                    
                print(f"{count}: {counts[count]}")
    else:
        if args.wiki not in counts:
            if args.no_json is True:
                print(json.dumps({
                    args.wiki: 0
                }))
            else:
                print(0)
        else:
            if args.log:
                log_to_db(
                    start_timestamp=start_timestamp,
                    end_timestamp=end_timestamp,
                    wiki=args.wiki,
                    count=counts[args.wiki]
                )
            if args.no_json is True:
                print(json.dumps({
                    args.wiki: counts[args.wiki]
                }))
            else:
                print(counts[args.wiki])
