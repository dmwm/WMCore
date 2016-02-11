#!/usr/bin/env python

"""
Couch dumper.

Dump one or more, comma separated fields from CouchDB datatabase.
Dumps entire database documents IDs when run without arguments.
Dumps entire database, entire documents when run just with -f (full).

"""
from __future__ import print_function

couch_url = "https://cmsweb.cern.ch/couchdb"
couch_db_name = "reqmgr_workload_cache"

import os
import sys

from WMCore.Database.CMSCouch import CouchServer, Database, Document


def dump(full_dump=False, fields=None):
    print("Querying fields: %s\n\n" % fields)
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
            print("%s  %s\n" % (s, doc["RequestName"]))
        elif full_dump:
            print("%s\n%s\n%s\n" % (row["id"], doc, 70*'-'))
        else:
            print(row["id"])
        doc_counter += 1
        #if doc_counter > 100:
        #    break
    print("Total documents: %s" % doc_counter) 
        

def main():
    if sys.argv[-1] == "-f":
        dump(full_dump=True)
        return
    elif len(sys.argv) < 2:
        dump()
    elif len(sys.argv) == 2:
        l = sys.argv[1].split(',')
        dump(fields=l)
            

if __name__ == "__main__":
    main()