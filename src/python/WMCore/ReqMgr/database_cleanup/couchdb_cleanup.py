#!/usr/bin/env python

# delete requests in CouchDB specified by names (CouchDB IDs) in the input
# file
# needs to have credentials for accessing CMS web ready in 
#   $X509_USER_CERT $X509_USER_KEY

# CMSCouch.Database only sets _deleted=True flag (all fields remain in 
# the databse), using DELETE HTTP verb, the document stays in the database
# too, however, only id, rev, and _deleted flag, everything else is wiped.

couch_host = "https://cmsweb.cern.ch"
couch_uri = "couchdb/reqmgr_workload_cache"

import sys
import os
import httplib
import json


def main():
    global couch_host, couch_uri

    if len(sys.argv) < 2:
        print ("Requires 1 input argument: file with a list of requests to "
               "delete.")
        sys.exit(1)
    
    if couch_host.startswith("https://"):
        couch_host = couch_host.replace("https://", '')
    conn = httplib.HTTPSConnection(couch_host,
                                   key_file=os.getenv("X509_USER_KEY"),
                                   cert_file=os.getenv("X509_USER_CERT"))
    input_file = sys.argv[1]
    f = open(input_file, 'r')
    # have to specify the documents revision, otherwise getting:
    # {"error":"conflict","reason":"Document update conflict."} (409 code)
    for request_name in f:
        request_name = request_name.strip()
        print "Deleting request: '%s' ... " % request_name
        uri="/%s/%s" % (couch_uri, request_name)
        print "Getting document revision _rev ..."
        # getting _rev
        conn.request("GET", uri, None)
        resp = conn.getresponse()
        print "Response: %s" % resp.status
        data = json.loads(resp.read())
        rev = data["_rev"]
        print "Delete request itself ..."
        uri += "?rev=%s" % rev
        conn.request("DELETE", uri, None)
        resp = conn.getresponse()
        # have to read the data, otherwise getting httplib.ResponseNotReady
        data = resp.read() 
        print "Response: %s\n" % resp.status
    f.close()


if __name__ == "__main__":
    main()
