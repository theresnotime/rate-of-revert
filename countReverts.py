import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone

from termcolor import colored

from eventstreams import EventStreams

DEFAULT_START = 2  # Default start time in minutes ago from now


def log_wiki_count(change: dict, wiki_counts: dict) -> dict:
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
            wiki_counts = log_wiki_count(change, wiki_counts)
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
    parser.add_argument("-v", "--verbose", help="Be verbose", action="store_true")
    parser.add_argument("--debug", help="Show debug info", action="store_true")
    args = parser.parse_args()
    os.system("color")

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
            if args.no_json is True:
                print(json.dumps({
                    args.wiki: counts[args.wiki]
                }))
            else:
                print(counts[args.wiki])
