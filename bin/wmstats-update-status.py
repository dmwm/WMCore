#!/usr/bin/env python
from __future__ import print_function
from optparse import OptionParser
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter

def updateRequestStatus(couchURL, requestList, status):
    ww = WMStatsWriter(couchURL)
    for request in requestList:
        ww.updateRequestStatus(request, status)
        print("%s is udated to %s" % (request, status))
    
if __name__ == "__main__":
    parser =  OptionParser()
    parser.add_option("--url", dest = "url",
                     help = "type couch db url")
    parser.add_option("--status", dest = "status",
                     help = "type purge or delete url")
    parser.add_option("--requests", dest = "requests",
                     help = "type last seq")
    (options, args) = parser.parse_args()
    if options.url:
        updateRequestStatus(options.url, options.requests, options.status)
