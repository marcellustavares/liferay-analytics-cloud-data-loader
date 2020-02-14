import json
import subprocess
import datetime

DATE_FORMAT_IN = '%Y-%m-%dT%H:%M:%S.%f%z'
DATE_FORMAT_OUT = '%Y-%m-%dT%H:%M:%S.000Z'


def calc_event_timedelta(event_date_str, ref_datetime=datetime.datetime.now()):
    event_date_utc = datetime.datetime.strptime(event_date_str, DATE_FORMAT_IN)

    return ref_datetime.astimezone(datetime.timezone.utc) - event_date_utc


def tail(file_name):
    f = subprocess.Popen(['tail','-n', '1', file_name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    while True:
        return f.stdout.readline()


def fetch_last_doc_event_date(file_name):
    doc = json.loads(tail(file_name))

    return doc['eventDate']


def shift_date(date_str, delta):
    d = datetime.datetime.strptime(date_str, DATE_FORMAT_IN) + delta

    return d.strftime(DATE_FORMAT_OUT)


def shift_page_doc_dates(page_doc, delta):
    page_doc['directAccessDates'] = [shift_date(date_str, delta) for date_str in page_doc['directAccessDates']]
    page_doc['eventDate'] = shift_date(page_doc['eventDate'], delta)
    page_doc['firstEventDate'] = shift_date(page_doc['firstEventDate'], delta)
    page_doc['indirectAccessDates'] = [shift_date(date_str, delta) for date_str in page_doc['indirectAccessDates']]
    page_doc['interactionDates'] = [shift_date(date_str, delta) for date_str in page_doc['interactionDates']]
    page_doc['lastEventDate'] = shift_date(page_doc['lastEventDate'], delta)

    return page_doc

def create_action_obj(index, doc_type, doc):
    return {
        "_index": index,
        "_type": doc_type,
        "_id": doc['id'],
        "_source": doc
    }

def create_action_sql():
    return """INSERT INTO pages (bounce, browserName, city , contentLanguageId, country , ctaClicks, dataSourceId, deviceType, directAccess, eventDate, exits, experienceId, formSubmissions, id, indirectAccess, individualId, platformName, primaryKey, region, sessionId, timeOnPage, title, url, userId, variantId, views) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""