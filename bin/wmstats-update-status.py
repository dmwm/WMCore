#!/usr/bin/env python
from __future__ import print_function
from argparse import ArgumentParser
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter

def updateRequestStatus(couchURL, requestList, status):
    ww = WMStatsWriter(couchURL)
    for request in requestList:
        ww.updateRequestStatus(request, status)
        print("%s is udated to %s" % (request, status))

if __name__ == "__main__":
    parser =  ArgumentParser()
    parser.add_argument("--url", dest = "url",
                     help = "type couch db url")
    parser.add_argument("--status", dest = "status",
                     help = "type purge or delete url")
    parser.add_argument("--requests", dest = "requests",
                     help = "type last seq")
    options = parser.parse_args()
    if options.url:
        updateRequestStatus(options.url, options.requests, options.status)
