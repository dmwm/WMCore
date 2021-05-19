#!/usr/bin/env python
"""
This script queries the WMStats server and creates a summary of success, failures,
type of failures, files skipped per agent; as well as an overview of failures
per task
"""
from __future__ import print_function, division

from future.utils import viewitems

from future import standard_library
standard_library.install_aliases()
import http.client
import json
import os
import sys
from pprint import pformat


def getWMStatsData(workflow):
    url = 'cmsweb.cern.ch'
    headers = {"Accept": "application/json", "Content-type": "application/json"}
    conn = http.client.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
    urn = "/wmstatsserver/data/request/%s" % workflow
    conn.request("GET", urn, headers=headers)
    r2 = conn.getresponse()
    request = json.loads(r2.read())["result"][0]

    return request


def main():
    if len(sys.argv) != 2:
        print("A workflow name must be provided!")
        sys.exit(1)

    wf = sys.argv[1]
    data = getWMStatsData(wf)
    if 'AgentJobInfo' not in data[wf]:
        print("Request doesn't have a `AgentJobInfo` key, nothing to be done")
        sys.exit(2)
    data = data[wf]['AgentJobInfo']

    summary = {}
    for agent in data:
        print("\nChecking AgentJobInfo for: %s" % agent)
        print("  Skipped files: %s" % pformat(data[agent]['skipped']))
        print("  Overall agent status:\t\t %s" % data[agent]['status'])

        for task, values in viewitems(data[agent]['tasks']):
            if values['jobtype'] not in ['Production', 'Processing', 'Merge', 'Harvest']:
                # print("Skipping task type %s, for %s" % (values['jobtype'], task))
                continue
            taskName = task.split('/')[-1]
            if 'status' not in values:
                print("'status' key not available under task: %s" % taskName)
                continue
            if 'failure' in values['status']:
                taskName = task.split('/')[-1]
                summary.setdefault(taskName, 0)
                summary[taskName] += sum(values['status']['failure'].values())

    print("\nSummary of failures for workflow: %s" % wf)
    print(pformat(summary))

    sys.exit(0)


if __name__ == '__main__':
    sys.exit(main())
