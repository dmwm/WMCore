#!/usr/bin/env python
"""
This module provides helper functions to obtain and handle CouchDB Replication data:
    - getSchedulerJobDocs get replication status based on scheduler information
    - getReplicatorDocs get replication status based on replicator information
    - compareReplicationStatus compares previous and current statuses
    - formatPrometheusMetrics format status metrics in Prometheus format
    - createAlerts create alerts from given status dict
    - checkStatus perform all checks for couchdb replication

Example of using Flask framework to serve prometheus metrics about CouchDB replication

import requests
from flask import Flask, Response
import threading
import time

app = Flask(__name__)
status_cache = {}

@app.route("/metrics")
def metrics():
    return Response(formatPrometheusMetrics(status_cache), mimetype="text/plain")

def daemonCouchReplicationStatus(interval=30):
    global status_cache
    while True:
        new_status = getSchedulerJobDocs(COUCHDB_URL, USERNAME, PASSWORD)
        status_cache = new_status
        time.sleep(interval)

if __name__ == "__main__":
    # Start the background thread to update replication status periodically
    threading.Thread(target=daemonCouchReplicationStatus, daemon=True).start()
    # Run the Flask app
    app.run(host="0.0.0.0", port=8000)
"""

import os
import json
import requests
import tempfile

# WMCore modules
from Utils.CertTools import cert, ckey, caBundle


def getSchedulerJobDocs(couchdbUrl):
    """
    Fetch CouchDB replication statuses. The logic is based on /_scheduler/jobs CouchDB end-point
    see https://docs.couchdb.org/en/stable/api/server/common.html#api-server-scheduler-jobs
    :param couchdbUrl: url of couch db
    :return: dictionary of statuses for all found replication documents
    """
    username, password = couchCredentials()
    auth = (username, password) if username and password else None
    try:
        response = requests.get(f"{couchdbUrl}/_scheduler/jobs", auth=auth)
        response.raise_for_status()
        data = response.json()

        statuses = {}
        for job in data.get('jobs', []):
            doc_id = job.get('doc_id') or job.get('id')
            source = job.get('source')
            target = job.get('target')
            history = job.get('history', [])
            info = job.get('info', {})

            # Determine current state from latest history item
            state = history[0]['type'] if history else 'unknown'

            # Detect error if 'crashed' exists in any history entry
            error = None
            for h in history:
                if h.get('type') == 'crashed':
                    error = f"Job previous crashed at {h.get('timestamp')} due to {h.get('reason')}"
                    break

            # check info document
            if info and info.get('doc_write_failures', 0) != 0:
                error = f"found failure of replication jobs in {couchdbUrl}/_scheduler/jobs "
                state = "error"
                # try to get more info about the error
                try:
                    response = requests.get(f"{couchdbUrl}/_scheduler/docs/_replicator/{doc_id}", auth=auth)
                    response.raise_for_status()
                    data = response.json()
                    error += f" Replicator state for {doc_id}: "
                    error += json.dumps(data)
                except:
                    pass

            statuses[doc_id] = {
                'state': state,
                'source': source,
                'target': target,
                'error': error,
                'history': history
                }

        return statuses
    except requests.RequestException as e:
        print(f"Error fetching scheduler jobs: {e}")
        return {}


def getReplicatorDocs(url=None):
    """
    Helper function to get all replicator docs and return summary dictionary
    :param url: url of the couchdb
    :return: replication summary dictionary
    """
    username, password = couchCredentials()
    auth = (username, password) if username and password else None
    if not url:
        url = "http://localhost:5984"
    headers = {"Accept": "application/json"}

    # Get list of all documents in _replicator
    r = requests.get(f"{url}/_replicator/_all_docs?include_docs=true",
                     headers=headers, auth=auth)
 
    if r.status_code != 200:
        raise Exception(f"Failed to fetch replication docs: {r.text}")

    data = r.json()
    result = {}

    for row in data.get("rows", []):
        doc = row.get("doc", {})
        doc_id = doc.get("_id")
        if doc_id.startswith("_design/"):
            continue  # skip design docs

        summary = {
            "state": doc.get("_replication_state"),
            "source": doc.get("source"),
            "target": doc.get("target"),
            "error": doc.get("_replication_state_reason"),
            "history": []
        }

        history = doc.get("_replication_history", [])
        for h in history:
            entry = {
                "timestamp": h.get("start_time") or h.get("end_time"),
                "type": h.get("type") or "unknown"
            }
            summary["history"].append(entry)

        result[doc_id] = summary

    return result


def compareReplicationStatus(prev, curr):
    """
    Helper function to compare replication status from previous to current state
    :param prev: previous replication status dictionary
    :param curr: current replication status dictionary
    :return: dictionary of changes
    """
    changes = {}
    for key in curr:
        if key not in prev or prev[key] != curr[key]:
            changes[key] = {
                    'old': prev.get(key),
                    'new': curr[key]
                    }
    return changes


def formatPrometheusMetrics(statuses):
    """
    Helper function to provide Prometheus metrics from given status dictionary
    :param statuses: replication status dictionary
    :return: prometheus metrics
    """
    states = {'error': -1, 'completed': 0, 'started': 1, 'added': 2, 'waiting': 3, 'triggered': 4, 'failed': 5}
    lines = [
            f'# HELP couchdb_replication_state Replication state: {states}',
            '# TYPE couchdb_replication_state gauge'
            ]
    for key, status in statuses.items():
        label = f'replId="{key}",source="{status["source"]}",target="{status["target"]}"'
        value = 0   # default error/other
        for k, v in states.items():
            if status['state'] == k:
                value = v
                break
        lines.append(f'couchdb_replication_state{{{label}}} {value}')
    return '\n'.join(lines)


def createAlerts(statuses):
    """
    Helper function to check alerts of replication status dictionary
    :param statuses: replication status dictionary
    :return: alerts dictionary
    """
    alerts = {}
    for key, status in statuses.items():
        if status['state'] != 'completed':
            alerts[key] = f"Replication state for {key} is '{status['state']}', error: {status['error']}"
    return alerts


def couchCredentials():
    """
    Select CouchDB credentials from provided secrets file
    :return: tuple of (user, password)
    """
    fname = os.getenv('WMAGENT_SECRETS_LOCATION', '')
    if fname == "":
        raise Exception("No WMAGENT_SECRETS_LOCATION in environment")
    user = ''
    password = ''
    data = ''
    with open(fname, 'r', encoding="utf-8") as istream:
        data = istream.read()
    for item in data.split('\n'):
        if 'COUCH_USER' in item:
            user = item.split('=')[-1]
        if 'COUCH_PASS' in item:
            password = item.split('=')[-1]
    return user, password


def checkStatus(url=None, prevStatus=None, kind="scheduler"):
    """
    Perform check of replication statuses
    :param url: couchdb URL
    :param prevStatus: previous status dictionary
    :param kind: kind of data look-up, e.g. scheduler or replicator
    :return: dictionary of current couchdb replication

    Here is an example of such dictionary structure:
    {'current_status': currStatus (dictionary),
     'previous_status': prevStatus (dictionary),
     'changes': changes (dictionary),
     'metrics': metrics (string),
     'alerts': alerts (dictionary)}

    Then, current and previous status dictionaries have the following form:
    {
      "14843c24643f8960eb159f5912f0f938": {
        "state": "started",
        "source": "https://xxx.cern.ch/couchdb/workqueue/",
        "target": "http://127.0.0.1:5984/workqueue_inbox/",
        "error": "Job previously crashed at 2025-05-05T18:47:11Z due to {changes_reader_died,{timeout,ibrowse_stream_cleanup}}",
        "history": [
          {
            "timestamp": "2025-05-05T18:47:11Z",
            "type": "started"
          },
          ...
         ]
      },
      "14843c24643f8960eb159f5912f0e51e": {
        "state": "started",
        "source": "http://127.0.0.1:5984/wmagent_summary/",
        "target": "https://xxx.cern.ch/couchdb/wmstats/",
        "error": null,
        "history": [
          {
            "timestamp": "2025-04-09T11:19:36Z",
            "type": "started"
          },
          {
            "timestamp": "2025-04-09T11:19:36Z",
            "type": "added"
          }
        ]
      },
    ...
    }
    """
    if not prevStatus:
        prevStatus = {}
    if not url:
        url = "http://localhost:5984"

    # first let's get statuses of documents
    if kind == "scheduler":
        currStatus = getSchedulerJobDocs(url)
    elif kind == "replicator":
        currStatus = getReplicatorDocs(url)
    else:
        raise Exception("Unsupported kind of documents '{kind}', should be either scheduler or replicator")

    # now we can find out changes from previous statuses
    changes = compareReplicationStatus(prevStatus, currStatus)

    # construct prometheus metrics with current statuses
    metrics = formatPrometheusMetrics(currStatus)

    # construct alerts with current statuses
    alerts = createAlerts(currStatus)

    # build final dictionary to return upstream
    sdict = {'current_status': currStatus,
             'previous_status': prevStatus,
             'changes': changes,
             'metrics': metrics,
             'alerts': alerts}
    return sdict


def getDocCount(url, auth, certTuple, caCert):
    """
    helper function to get document counts
    :param url: url of the couchdb
    :param auth: couchdb authentication credentials tuple
    :param caCert: ca bundle file name
    :return: document count
    """
    resp = requests.get(url, auth=auth, cert=certTuple, verify=caCert or True)
    resp.raise_for_status()
    return resp.json().get('doc_count', -1)


def getReplicationState(url, auth, certTuple, caCert):
    """
    helper function to get replication state from given couchdb url
    :param url: url of the couchdb
    :param auth: couchdb authentication credentials tuple
    :param caCert: ca bundle file name
    :return: tuple of replication state and its time
    """
    resp = requests.get(url, auth=auth, cert=certTuple, verify=caCert or True)
    resp.raise_for_status()
    doc = resp.json()
    return doc.get('_replication_state'), doc.get('_replication_state_time')


def compareCouchInstances(sourceUrl, targetUrl, replUrl):
    """
    Compare the number of documents between source and destination CouchDB databases.
    Monitor replication if the counts differ but replication status is OK.
 
    Parameters:
    :param sourceUrl: str, e.g. http://localhost:5984/source_db
    :param targetUrl: str, e.g. http://localhost:5984/dest_db
    :param replUrl: str, e.g. http://localhost:5984/_replicator/<replId>
    """
    user, password = couchCredentials()
    auth = (user, password)
    sdict = {}
    userCert = cert() if cert() else ''
    userCkey = ckey() if ckey() else ''
    if userCkey == '' or userCert == '':
        return sdict
    certTuple = (userCert, userCkey)
    with tempfile.NamedTemporaryFile(mode='w+', suffix=".pem", delete=True) as tfile:
        capath = os.environ.get("X509_CERT_DIR", '/etc/grid-security/certificates')
        cacerts = caBundle(capath)
        tfile.write(cacerts)
        tfile.flush()

        sourceCount = getDocCount(sourceUrl, auth, certTuple, tfile.name)
        targetCount = getDocCount(targetUrl, auth, certTuple, tfile.name)
        state, stateTime = getReplicationState(replUrl, auth, certTuple, tfile.name)

        sdict = {
                "source": sourceUrl,
                "target": targetUrl,
                "source_count": sourceCount,
                "target_count": targetCount,
                "state": state,
                "state_timestamp": stateTime
                }
    return sdict


def exampleReplicationStatus(sourceUrl=None):
    """
    Example function to test replication status either based on scheduler or replicator info
    This function should run on a node with local CouchDB access as all of its logic
    relies on using localhost:5984 URL
    """

    try:
        print(f"checking {sourceUrl}")

        # let's first test scheduler info
        sdict = checkStatus(url=sourceUrl, kind="scheduler")
        print('--- status based on scheduler info ---')
        print(sdict['current_status'])
        print('--- metrics ---')
        print(sdict['metrics'])
        if sdict.get('alerts', None):
            print('--- alerts ---')
            for k, msg in sdict['alerts'].items():
                print(f"{k}: {msg}")

        print()

        # now let's test replicator info
        rdict = checkStatus(url=sourceUrl, kind="replicator")
        print('--- status based on replicator info ---')
        print(rdict['current_status'])
        print('--- metrics ---')
        print(rdict['metrics'])
        if rdict.get('alerts', None):
            print('--- alerts ---')
            for k, msg in rdict['alerts'].items():
                print(f"{k}: {msg}")

    except Exception as exp:
        print(str(exp))


def exampleIndividualDocument(sourceUrl, targetUrl, replUrl):
    """
    Example function how to test check status of particular replication document
    This function should run through CMSWEB frontend URLs as we need to compare
    documents in both source and target CouchDB instances
    :param sourceUrl: source couchdb URL, e.g. https://xxx.cern.ch/couchdb/test_db
    :param targetUrl: target couchdb URL, e.g. https://xxx.cern.ch/couchdb/test_db
    :param replUrl: replication URL, e.g. https://xxx.cern.ch/couchdb/test_db/_replicator/bla
    """
    try:
        result = compareCouchInstances(sourceUrl, targetUrl, replUrl)
        print('--- compare CouchDB Instances ---')
        print('source: ', sourceUrl)
        print('target: ', targetUrl)
        print(result)
    except:
        pass


def test():
    """
    test functions
    """
    import sys
    if len(sys.argv) > 1:
        sourceUrl = sys.argv[1]
        exampleReplicationStatus(sourceUrl)
    else:
        print("Cannot run tests, please provide at least CouchDB source URL, or <srcUrl> <targetUrl> <replicationId>")
    if len(sys.argv) == 4:
        sourceUrl = sys.argv[1]
        targetUrl = sys.argv[2]
        replUrl = sys.argv[3]
        exampleIndividualDocument(sourceUrl, targetUrl, replUrl)

if __name__ == '__main__':
    test()
