#!/usr/bin/env python3

import argparse
import json
import datetime
from elasticsearch_output import Elasticsearch
from elasticsearch_output import helpers
import utilities
import sys
import psycopg2

import logging

# arg parser
parser = argparse.ArgumentParser(
    description='Liferay Analytics Cloud Data Loader.')

parser.add_argument('-i', help='json file path', metavar='input', required=True)
parser.add_argument('-o', help='output (es, postgres, hdfs)')
parser.add_argument('-es', help='elasticsearch hosts, defaults to 127.0.1', metavar='hostname', default='127.0.0.1')
parser.add_argument('-namespace', help='elasticsearch index namespace, defaults to osbasah', metavar='namespace', default='osbasah')
parser.add_argument('-batch', help='elasticsearch bulk batch size', metavar='batch size', default='100')
parser.add_argument('-loglevel', help='log level', metavar='log level', default=10)

args = parser.parse_args()

# logging

logging.DEBUG
root = logging.getLogger()
root.setLevel(args.loglevel)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

root.addHandler(handler)

# main
logger = logging.getLogger("analytics.cloud.date.machine.main")

es = Elasticsearch(hosts=args.es)

logger.info("starting data import")

last_doc_event_date = utilities.fetch_last_doc_event_date(args.i)

logger.debug("last doc event date %s" % last_doc_event_date)

timedelta = utilities.calc_event_timedelta(last_doc_event_date)

logger.debug("incrementing dates by %d days" % timedelta.days)

with open(args.i, "r", encoding="utf-8") as input_file:
    actions = []

    while True:
        line = input_file.readline()

        if len(line) == 0:
            break

        actions.append(
            utilities.create_action_obj(
                "%s_osbasahcerebroinfo_pages" % args.namespace,
                "pages",
                utilities.shift_page_doc_dates(
                    json.loads(line),
                    datetime.timedelta(days=timedelta.days))))

        if len(actions) == args.batch:
            helpers.bulk(es, actions)
            actions.clear()

    if len(actions) > 0:
        helpers.bulk(es, actions)
        actions.clear()

logger.info("data import completed")