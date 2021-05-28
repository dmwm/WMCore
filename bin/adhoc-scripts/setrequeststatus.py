#!/usr/bin/env python
"""
This script can be used to update the status of a request in ReqMgr2.
"""
from __future__ import print_function, division

from future import standard_library
standard_library.install_aliases()
import http.client
import json
import os
import sys


def setStatus(url, workflow, newstatus):
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}

    print("Setting %s to %s" % (workflow, newstatus))
    if newstatus in ['closed-out', 'announced', 'aborted', 'rejected']:
        encodedParams = json.dumps({"RequestStatus": newstatus, "cascade": True})
    else:
        encodedParams = json.dumps({"RequestStatus": newstatus})

    conn = http.client.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
    conn.request("PUT", "/reqmgr2/data/request/%s" % workflow, encodedParams, headers)
    resp = conn.getresponse()
    if resp.status != 200:
        print("Response status: %s\tResponse reason: %s" % (resp.status, resp.reason))
        if hasattr(resp.msg, "x-error-detail"):
            print("Error message: %s" % resp.msg["x-error-detail"])
            sys.exit(2)
    else:
        print("  OK!")
    conn.close()


def getStatus(url, workflow):
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}

    conn = http.client.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
    urn = "/reqmgr2/data/request/%s" % workflow
    conn.request("GET", urn, headers=headers)
    res = conn.getresponse()
    request = json.loads(res.read())["result"][0]
    return request[workflow]['RequestStatus']


def main():
    url = 'cmsweb.cern.ch'

    args = sys.argv[1:]
    if not len(args) == 2:
        print("usage: python setrequeststatus.py <text_file_with_the_workflow_names> <newStatus>")
        sys.exit(0)
    inputFile = args[0]
    newstatus = args[1]
    with open(inputFile, 'r') as fOjb:
        workflows = fOjb.readlines()

    for wflowName in workflows:
        wflowName = wflowName.rstrip('\n')
        print("%s" % wflowName)
        print("Set %s from %s to %s" % (wflowName, getStatus(url, wflowName), newstatus))
        setStatus(url, wflowName, newstatus)


if __name__ == "__main__":
    main()
