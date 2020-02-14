import psycopg2
import utilities
import logging

logger = logging.getLogger("analytics.cloud.data.loader.postgres")

class PostgresOutput:
    def __init__(self, args):
        self.cfg = args
        self.stmt_count = 0
        self.conn = psycopg2.connect(user="postgres",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="tutorial")
        self.cursor = self.conn.cursor()
        logger.debug("connection opened")

    def submit(self, page_doc):
        stmt = utilities.create_action_sql()
        #logger.debug(stmt)
        self.cursor.execute(stmt, (page_doc.get('bounce') or 0,
                                   page_doc.get('browserName'),
                                   page_doc.get('city') or '',
                                   page_doc.get('contentLanguageId'),
                                   page_doc.get('country') or '',
                                   page_doc.get('ctaClicks'),
                                   page_doc.get('dataSourceId'),
                                   page_doc.get('deviceType'),
                                   page_doc.get('directAccess'),
                                   page_doc.get('eventDate'),
                                   page_doc.get('exits') or 0,
                                   page_doc.get('experienceId'),
                                   page_doc.get('formSubmissions'),
                                   page_doc.get('id'),
                                   page_doc.get('indirectAccess'),
                                   page_doc.get('individualId'),
                                   page_doc.get('platformName'),
                                   page_doc.get('primaryKey'),
                                   page_doc.get('region') or '',
                                   page_doc.get('sessionId'),
                                   page_doc.get('timeOnPage') or 0,
                                   page_doc.get('title'),
                                   page_doc.get('url'),
                                   page_doc.get('userId'),
                                   page_doc.get('variantId'),
                                   page_doc.get('views') or 0))
        self.stmt_count += 1

        if self.stmt_count == self.cfg.batch:
            self.conn.commit()
            logger.debug("transaction committed")
            self.stmt_count = 0

    def flush(self):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()
        logger.debug("connection closed")
