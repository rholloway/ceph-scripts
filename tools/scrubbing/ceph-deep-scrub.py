#!/usr/bin/python
# controls deep scrub with additional metric collection beyond built in deep scrub functionality
# If using this script, it is recommended to set your osd_deep_scrub_interval to something longer than it should take
# this script to get through all PGs

try:
  import simplejson as json
except ImportError:
  import json

import datetime
import commands
import time
import argparse
import rados
import logging
import logging.handlers
import sys
import socket

logger = logging.getLogger(__name__)
stream_handler = logging.StreamHandler()
file_handler = logging.handlers.RotatingFileHandler("ceph_deep_scrub.log")

formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]')
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)

# configure carbon if available
CARBON_SERVER = '10.0.37.12'
CARBON_PORT = 2003

# Array of PGs to not deep scrub due to various issues that need to be sorted out
skip_pgs = [
    "11.22",  # contains an oversized rgw bucket index that needs to be sharded upon, deep scrub causes performance trouble
    "2.2e4"   # not sure why it fails, does involve osd.44 same as above but quick glance looked like normal bucket.
]

parser = argparse.ArgumentParser(description='Run Deep Scrubs As Necessary.')
parser.add_argument('--max-scrubs', dest='MAX_SCRUBS', type=int, default=2,
                    help='Maximum number of deep scrubs to run simultaneously (default: %(default)s)')
parser.add_argument('--max-scrubs-weekend', dest='MAX_SCRUBS_WEEKEND', type=int, default=None,
                    help='Maximum number of deep scrubs to run simultaneously on the weekend (default: same as max scrubs)')
parser.add_argument('--sleep', dest='SLEEP', type=int, default=0,
                    help='Sleep this many seconds then run again, looping forever. 0 disables looping. (default: %(default)s)')
parser.add_argument("--age", dest="AGE", type=int, default=14,
                    help="How often to run a deep scrub in days (default: every %(default)s days)")
parser.add_argument("--start-hour", dest="START_HOUR", default=0,
                    help="Earliest hour of day (UTC) to allow deep scubbing (default: %(default)s)")
parser.add_argument("--end-hour", dest="END_HOUR", default=24,
                    help="Latest hour of day (UTC) to allow deep scrubbing (default: %(default)s)")
parser.add_argument('--conf', dest='CONF', type=str, default="/etc/ceph/ceph.conf",
                    help='Ceph config file. (default: %(default)s)')
parser.add_argument("--graphite-prefix", dest="GRAPHITE_PREFIX", default="",
                    help="Graphite prefix to use when inserting metrics (default: %(default)s)")


def send_metric(key, value):
    """ sends metric to carbon server """
    context = {
        "graphite_timestamp": int(time.time()),
        "key": key,
        "value": value
    }
    graphite = """
{key} {value} {graphite_timestamp}\n"""

    graphite = graphite.format(**context)
    sock = socket.socket()
    sock.connect((CARBON_SERVER, CARBON_PORT))
    sock.sendall(graphite)
    sock.close()

def in_scrubbing_window(min_hour, max_hour):
    """ Returns True if we are within the scrubbing window, False otherwise.

        Args:
            min_hour: earliest time we can scrub
            max_hour: max time we can scrub
    """
    now = datetime.datetime.utcnow()

    if max_hour > min_hour:
        # assume same day scrub
        return now.hour >= min_hour and now.hour <= max_hour
    elif max_hour < min_hour:
        # assume overnight scrub
        return now.hour <= max_hour or now.hour >= min_hour
    else:
        # single hour scrub
        return now.hour == min_hour


def main():
    args = parser.parse_args()
    MAX_SCRUBS_WEEK = args.MAX_SCRUBS
    MAX_SCRUBS_WEEKEND = args.MAX_SCRUBS_WEEKEND
    if MAX_SCRUBS_WEEKEND is None:
        MAX_SCRUBS_WEEKEND = MAX_SCRUBS_WEEK
    SLEEP = args.SLEEP
    CONF = args.CONF
    AGE = args.AGE
    MIN_HOUR = args.START_HOUR
    MAX_HOUR = args.END_HOUR
    GRAPHITE_PREFIX = args.GRAPHITE_PREFIX

    # connect to cluster
    try:
        cluster = rados.Rados(conffile=CONF)
    except TypeError:
        logger.exception("Failed to connect to ceph cluster")
        sys.exit(1)

    try:
        cluster.connect()
    except Exception:
        logger.exception("Failed to connect to ceph cluster")
        sys.exit(1)

    deep_scrubbing = {}
    while True:

        if not in_scrubbing_window(MIN_HOUR, MAX_HOUR):
            logger.warning("Outside of deep scrubbing hours, will not start")
            if SLEEP:
                time.sleep(120)
                continue
            else:
                sys.exit(0)

        now = datetime.datetime.utcnow()
        if now.isoweekday() >= 6:
            MAX_SCRUBS = MAX_SCRUBS_WEEKEND
        else:
            MAX_SCRUBS = MAX_SCRUBS_WEEK

        # pull PG data
        logger.info("Pulling pg info")
        cmd = {'prefix': 'pg dump', 'format': 'json'}
        ret, buf, out = cluster.mon_command(json.dumps(cmd), b'', timeout=5)
        pg_dump = json.loads(buf)
        pg_stats = pg_dump['pg_stats']

        # check if any previous jobs have finished
        existing_scrubbing = [pg for pg in pg_stats if pg['pgid'] in deep_scrubbing]
        logger.info("Checking %s PGs if they have finished scrubbing", len(existing_scrubbing))
        for pg in existing_scrubbing:
            if 'scrubbing+deep' not in pg['state']:
                scrub_completed = datetime.datetime.strptime(pg['last_deep_scrub_stamp'][:-7], "%Y-%m-%d %H:%M:%S")

                if scrub_completed > now - datetime.timedelta(days=AGE):
                    duration = (scrub_completed - deep_scrubbing[pg['pgid']]).total_seconds()
                    logger.info("pg %s appears to have finished a deep scrub, took %s seconds", pg['pgid'], duration)
                    del deep_scrubbing[pg['pgid']]
                    if GRAPHITE_PREFIX:
                        logger.info("trying to send metric to graphite")
                        send_metric("{}.deep_scrub.duration".format(GRAPHITE_PREFIX), duration)

                else:
                    logger.warning("pg %s appears to have not finished a deep scrub, but isn't deep scrubbing", pg['pgid'])
            else:
                logger.info("pg %s is still deep scrubbing", pg['pgid'])

        # find out which OSDs are currently scrubbing
        pgs_scrubbing = [pg for pg in pg_stats if 'scrubbing' in pg['state']]
        pgs_scrubbing.sort(key=lambda k: k['pgid'])
        osds_scrubbing = {}
        for pg in pgs_scrubbing:
            for osd in pg['acting']:
                logger.info("pg %s (%s) uses osd.%s", pg['pgid'], pg['state'], osd)
                try:
                    osds_scrubbing[osd] += 1
                except KeyError:
                    osds_scrubbing[osd] = 1

        logger.info("OSDs scrubbing: %s", len(osds_scrubbing))

        # get deep scrub info
        pgs_scrubbing = [pg for pg in pg_stats if 'scrubbing+deep' in pg['state']]

        if len(pgs_scrubbing) >= MAX_SCRUBS:
            logger.info("Currently limited to %s active deep scrubs - pending another deep scrub task to finish", MAX_SCRUBS)
            if SLEEP:
                time.sleep(30)
                continue
            else:
                sys.exit()

        # Which PGs have not been deep scrubbed the longest?
        pg_stats.sort(key=lambda k: k['last_deep_scrub_stamp'])
        pgs_scrubbing_stale = [pg for pg in pg_stats if 'scrubbing+deep' not in pg['state'] and
            datetime.datetime.strptime(pg['last_deep_scrub_stamp'][:-7], "%Y-%m-%d %H:%M:%S") <= (now - datetime.timedelta(days=AGE))]

        if GRAPHITE_PREFIX:
            send_metric("{}.deep_scrub.pg_deep_scrub_stale".format(GRAPHITE_PREFIX),
                        len(pgs_scrubbing_stale))
            send_metric("{}.deep_scrub.pg_deep_scrub_stale_percent".format(GRAPHITE_PREFIX),
                        len(pgs_scrubbing_stale) / float(len(pg_stats)) * 100)

        n_to_trigger = max(0, MAX_SCRUBS - len(pgs_scrubbing))
        i = 0
        n_triggered = 0

        if len(deep_scrubbing) >= MAX_SCRUBS:
            logger.warning("Already tracking %s queued deep scrubs, not queuing more: %s", len(deep_scrubbing), deep_scrubbing)
            if SLEEP:
                time.sleep(30)
                continue
            else:
                sys.exit()

        logger.info("Triggering %s deep scrubs", n_to_trigger)
        for pg in pgs_scrubbing_stale:
            i += 1
            logger.info("PG %s last deep scrubbed %s", pg['pgid'], pg['last_deep_scrub_stamp'])

            if pg['pgid'] in skip_pgs:
                logger.warning("Skipping PG %s due to configuration", pg['pgid'])
                continue

            deep_scrub_stamp = datetime.datetime.strptime(pg['last_deep_scrub_stamp'][:-7], "%Y-%m-%d %H:%M:%S")

            if deep_scrub_stamp > now - datetime.timedelta(days=AGE):
                logger.warning("No need to deep scrub, oldest PG was deep scrubbed less than %s days ago on %s", AGE, deep_scrub_stamp)
                if SLEEP:
                    time.sleep(300)
                    continue
                else:
                    sys.exit()

            blocked = False
            for osd in pg['acting']:
                if osd in osds_scrubbing.keys():
                    logger.warning("Deep scrubbing blocked on pg %s due to osd.%s actively scrubbing", pg['pgid'], osd)
                    blocked = True

            if pg['pgid'] in deep_scrubbing:
                logger.warning("pg %s already queued or started for deep scrub", pg['pgid'])
                blocked = True

            if not blocked and n_to_trigger > 0:
                # queue deep scrub
                output = commands.getoutput("ceph pg deep-scrub %s" % pg['pgid'])
                deep_scrubbing[pg['pgid']] = datetime.datetime.utcnow()
                logger.info("Queued pg %s to deep scrub: %s", pg['pgid'], output)

                for osd in pg['acting']:
                    if osd in osds_scrubbing.keys():
                        osds_scrubbing[osd] += 1
                    else:
                        osds_scrubbing[osd] = 1

                n_triggered += 1

                if n_triggered == n_to_trigger:
                    break

            # if we weren't triggering any, this lets loop above print what needs to be scrubbed without scheduling
            # but only for first/oldest 10 PGs
            if n_to_trigger < 1 and i == 10:
                break

        if SLEEP:
            logger.info("Forcing sleep of %s seconds...", SLEEP)
            time.sleep(SLEEP)
        else:
            break

    # Disconnect
    cluster.shutdown()


if __name__ == "__main__":
  main()
