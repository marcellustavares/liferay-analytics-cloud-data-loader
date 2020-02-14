from elasticsearch import Elasticsearch
from elasticsearch import helpers
import utilities


class ElasticsearchOutput:
    def __init__(self, args):
        self.cfg = args
        self.buffer = []
        self.es = Elasticsearch(hosts=args.es)

    def submit(self, pagedoc):
        self.buffer.append(
            utilities.create_action_obj(
                "%s_osbasahcerebroinfo_pages" % self.cfg.namespace,
                "pages", pagedoc))

        if len(self.buffer) == self.cfg.batch:
            helpers.bulk(self.es, self.buffer)
            self.buffer.clear()

    def flush(self):
        if len(self.buffer) == self.cfg.batch:
            helpers.bulk(self.es, self.buffer)
            self.buffer.clear()
