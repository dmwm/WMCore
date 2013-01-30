#!/usr/bin/env python

# usage:
# python oracle_couchdb_comparison.py oracle_dump_request_table.py > \
#       comparison.log

# needs to have credentials for accessing CMS web ready in
# $X509_USER_CERT $X509_USER_KEY 

couch_url = "https://cmsweb.cern.ch/couchdb"
couch_db_name = "reqmgr_workload_cache"

import os
import sys

from WMCore.Database.CMSCouch import CouchServer, Database, Document



def main():
    if len(sys.argv) < 2:
        print ("Takes 1 input argument - dump of Oracle reqmgr_request "
               "table in a Python dictionary.")
        sys.exit(1)

    print "Creating database connection ..."
    # couch_server = CouchServer(couch_url)
    db = Database(couch_db_name, couch_url)
    
    module = __import__(sys.argv[1].replace(".py", ''),
        fromlist=["reqmgr_request"])
    oracle_requests = getattr(module, "reqmgr_request")
    print "Oracle requests: %s" % len(oracle_requests)

    print "Retrieving data from CouchDB ..."
    couch_requests = db.allDocs()
    couch_request_names = []
    for row in couch_requests["rows"]:
        if row["id"].startswith("_design"): continue
        couch_request_names.append(row["id"])
    print "CouchDB requests: %s" % len(couch_request_names)

    print "Comparing Oracle and CouchDB requests ..."
    not_present_in_couch = []
    for request in oracle_requests:
        oracle_request_name = request["REQUEST_NAME"]
        # remove first occurrence of value. Raises ValueError if not present
        try:
            couch_request_names.remove(oracle_request_name)
        except ValueError:
            not_present_in_couch.append(oracle_request_name)


    print "CouchDB requests not present in Oracle:"
    print "%s requests" % len(couch_request_names)
    for name in couch_request_names:
        request = db.document(name)
        assert name == request["RequestName"]
        assert name == request["_id"]
        print "%s  %s  %s" % (request["RequestName"], request["RequestType"],
                request["RequestStatus"])
    print "\n\n"
    print "Oracle requests not present in CouchDB:"
    print "%s requests" % len(not_present_in_couch)
    for name in not_present_in_couch:
        print name


if __name__ == "__main__":
    main()
