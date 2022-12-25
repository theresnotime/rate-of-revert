import requests
import pytz
from datetime import datetime, timedelta
import config
import time
import sqlite3


con = sqlite3.connect("ror.db")


def getCount(site: str, rcTag: str, rcStart: str, rcEnd: str) -> int:
    '''
    Get the number of edits in RecentChanges with a given
    tag in the given timeframe
    '''

    apiUrlBase = f"https://{site}/w/api.php?action=query&list=recentchanges"
    rcProp = 'comment%7Cids%7Ctimestamp'
    apiUrlQuery = (
        f"&format=json&formatversion=2&rcprop={rcProp}&"
        f"rclimit=max&rcdir=newer&rctag={rcTag}"
    )
    url = apiUrlBase + apiUrlQuery + f"&rcstart={rcStart}&rcend={rcEnd}"

    response = requests.get(
            url=url,
            headers=config.headers
        ).json()
    count = len(response['query']['recentchanges'])

    return count


def getDatetime(
            rcStartDelta: int = 30,
            rcEndDelta: int = 0
        ) -> tuple[str, str]:
    '''
    Get the start and end datetimes for the RecentChanges query
    '''
    rcStart = (
            datetime.now(pytz.utc) - timedelta(minutes=rcStartDelta)
        ).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    rcEnd = (
            datetime.now(pytz.utc) - timedelta(minutes=rcEndDelta)
        ).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return rcStart, rcEnd


def backfill(hours: int, interval: int = 60, save: bool = False) -> None:
    '''
    Run getCount for a given number of hours, split by interval
    '''
    minutes = hours * 60
    while minutes > 0:
        rcStart, rcEnd = getDatetime(minutes, minutes - interval)
        print(minutes, minutes - interval)
        print(rcStart, rcEnd)
        for site in config.SITES:
            count = getCount(site, config.rcTag, rcStart, rcEnd)
            print(f"{site}:", count)
            if save:
                saveToDB(rcStart, rcEnd, site, count)
            time.sleep(0.5)
        minutes -= interval
        time.sleep(1)


def saveToDB(rcStart: str, rcEnd: str, site: str, count: int) -> None:
    '''
    Save the count to the database
    '''
    cur = con.cursor()
    cur.execute(
        "INSERT INTO rates (rcStart, rcEnd, site, count) VALUES (?, ?, ?, ?)",
        (rcStart, rcEnd, site, count)
    )
    con.commit()
