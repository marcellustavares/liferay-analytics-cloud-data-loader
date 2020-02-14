#!/usr/bin/env python3

import argparse
import json
import datetime
import utilities
import sys
import psycopg2
from elasticsearch_output import ElasticsearchOutput
from postgres_output import PostgresOutput

import logging

# arg parser
parser = argparse.ArgumentParser(
    description='Liferay Analytics Cloud Data Loader.')

parser.add_argument('-i', help='json file path', metavar='input', required=True)
parser.add_argument('-o', help='output (es, postgres, hdfs)')
parser.add_argument('-es', help='elasticsearch hosts, defaults to 127.0.1', metavar='hostname', default='127.0.0.1')
parser.add_argument('-namespace', help='elasticsearch index namespace, defaults to osbasah', metavar='namespace', default='osbasah')
parser.add_argument('-batch', help='elasticsearch bulk batch size', metavar='batch size', default=50)
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
logger = logging.getLogger("analytics.cloud.data.loader.main")

es_output = ElasticsearchOutput(args)
pg_output = PostgresOutput(args)

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

        pagedoc = utilities.shift_page_doc_dates(
            json.loads(line),
            datetime.timedelta(days=timedelta.days))

        if args.o == 'es':
            es_output.submit(pagedoc)
        elif args.o == 'postgres':
            pg_output.submit(pagedoc)

    if args.o == 'es':
        es_output.flush()
    elif args.o == 'postgres':
        pg_output.flush()

logger.info("data import completed")