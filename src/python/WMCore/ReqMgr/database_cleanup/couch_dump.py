#!/usr/bin/env python

"""
Couch dumper.

Dump one or more, comma separated fields from CouchDB datatabase.
Dumps entire database ids when run without arguments.

"""

couch_url = "https://cmsweb.cern.ch/couchdb"
couch_db_name = "reqmgr_workload_cache"

import os
import sys

from WMCore.Database.CMSCouch import CouchServer, Database, Document


def dump(fields=None):
    print "Querying fields: %s\n\n" % fields
    db = Database(couch_db_name, couch_url)
    couch_requests = db.allDocs()
    doc_counter = 0
    for row in couch_requests["rows"]:
        if row["id"].startswith("_design"): continue
        doc = db.document(row["id"])
        if fields:
            s = ''
            for f in fields:
                try:
                    s += "%s:%s  " % (f, doc[f])
                except KeyError:
                    s += "%s:n/a  " % f 
            print "%s  %s\n" % (s, doc["RequestName"])
        else:
            print row["id"]
        doc_counter += 1
    print "Total documents: %s" % doc_counter 
        

def main():
    if len(sys.argv) < 2:
        dump()
    elif len(sys.argv) == 2:
        l = sys.argv[1].split(',')
        dump(l)
            

if __name__ == "__main__":
    main()