#!/usr/bin/env python
"""
This module provides helper function to obtain and handle CouchDB Replication data:
    - couchReplicationStatuses fetches couchDB replication data and constructs status dict
    - compareReplicationStatus compares previous and current statuses
    - formatPrometheusMetrics format status metrics in Prometheus format
    - createAlerts create alerts from given status dict
    - checkReplicationStatus perform all checks for couchdb replication

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
        new_status = couchReplicationStatuses(COUCHDB_URL, USERNAME, PASSWORD)
        status_cache = new_status
        time.sleep(interval)

if __name__ == "__main__":
    # Start the background thread to update replication status periodically
    threading.Thread(target=daemonCouchReplicationStatus, daemon=True).start()
    # Run the Flask app
    app.run(host="0.0.0.0", port=8000)
"""

import os
import requests

def couchReplicationStatuses(couchdbUrl, username=None, password=None):
    """
    Fetch CouchDB replication statuses. The logic is based on /_scheduler/jons CouchDB end-point
    see https://docs.couchdb.org/en/stable/api/server/common.html#api-server-scheduler-jobs
    :param couchdbUrl: url of couch db
    :param username: user name
    :param password: user password
    :return: dictionary of statuses for all found replication documents
    """
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

            # Determine current state from latest history item
            state = history[0]['type'] if history else 'unknown'

            # Detect error if 'crashed' exists in any history entry
            error = None
            for h in history:
                if h.get('type') == 'crashed':
                    error = f"Job previous crashed at {h.get('timestamp')} due to {h.get('reason')}"
                    break

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
    states = {'error': -1, 'completed': 0, 'started': 1, 'added': 2, 'waiting': 3, 'triggered': 4}
    lines = [
            f'# HELP couchdb_replication_state Replication state: {states}',
            '# TYPE couchdb_replication_state gauge'
    ]
    for key, status in statuses.items():
        label = f'replication_id="{key}",source="{status["source"]}",target="{status["target"]}"'
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

def couchCredentials(fname):
    """
    Select CouchDB credentials from provided secrets file
    :param fname: file name
    :return: tuple of (user, password)
    """
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

def checkReplicationStatus(url=None, fname=None, prevStatus=None):
    """
    Perform check of replication statuses
    :param url: couchdb URL
    :param fname: secrets file name
    :return: dictionary of current couchdb replication
    """
    if not prevStatus:
        prevStatus = {}
    if not fname:
        fname = os.getenv('WMA_SECRETS_FILE')
    user, password = couchCredentials(fname)
    if not url:
        url = "http://localhost:5984"
    currStatus = couchReplicationStatuses(url, user, password)
    changes = compareReplicationStatus(prevStatus, currStatus)
    metrics = formatPrometheusMetrics(currStatus)
    alerts = createAlerts(currStatus)
    sdict = {'current_status': currStatus,
             'previous_status': prevStatus,
             'changes': changes,
             'metrics': metrics,
             'alerts': alerts}
    return sdict

def example():
    """
    Example function to test module functionality
    """
    prev_status = {}
    sdict = checkReplicationStatus()
    print('--- status ---')
    print(sdict['current_status'])
    print('--- metrics ---')
    print(sdict['metrics'])
    alerts = sdict['alerts']
    if sdict.get('alerts', None):
        print('--- alerts ---')
        for k, msg in sdict['alerts'].items():
            print(f"{k}: {msg}")


if __name__ == '__main__':
    example()
