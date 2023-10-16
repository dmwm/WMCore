#!/usr/bin/env python
"""
File       : adjustMongoDocs.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: adjust documents in MongoDB with given schema
The script loops over given db documents (from given db name
and collection) and add to each document a new schema provided
via input file name, e.g.

# new schema addition:
cat > /tmp/schema.json << EOF
{"foo": 1}
EOF

# run script to update all documents in test.test database and add to them
# a json structure presented in /tmp/schema.json file
adjustMongoDocs.py --fin=/tmp/schema.json --dburi=localhost:8230 --dbname=test --dbcoll=test --verbose 1

NOTE: the script can run from any location assuming it has proper db uri with
credentials and availability of mongo db outside of its own cluster, but most
likely it will be run on mongo db k8s cluster
"""

import argparse
import json
from pymongo import MongoClient


class OptionParser(object):
    "Helper class to handle script options"

    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--fin", action="store",
                                 dest="fin", default="", help="Input file with update fields")
        self.parser.add_argument("--dburi", action="store",
                                 dest="dburi", default="", help="MongoDB URI")
        self.parser.add_argument("--dbname", action="store",
                                 dest="dbname", default="", help="MongoDB database")
        self.parser.add_argument("--dbcoll", action="store",
                                 dest="dbcoll", default="", help="MongoDB collection")
        self.parser.add_argument("--verbose", action="store",
                                 dest="verbose", default=0, help="verbosity level")


def adjust():
    """
    helper script to update documents in MongoDB with given db name/collection
    and schema file (which should contain new addition, in form of JSON, to the
    db documents)
    """
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    conn = MongoClient(host=opts.dburi)
    dbname = opts.dbname
    dbcoll = opts.dbcoll
    verbose = opts.verbose
    schema = {}
    with open(opts.fin, 'r', encoding='utf-8') as istream:
        schema = json.load(istream)
    if schema:
        if verbose:
            print("read data from '%s', %s/%s" % (opts.dburi, dbname, dbcoll))
        records = [r for r in conn[dbname][dbcoll].find()]
        for doc in records:
            if verbose:
                print("update doc=%s with schema=%s" % (doc, schema))
            conn[dbname][dbcoll].update_one(doc, {'$set': schema}, upsert=True)


if __name__ == '__main__':
    adjust()
