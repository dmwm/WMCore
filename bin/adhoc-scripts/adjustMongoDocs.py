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

# run script to add new transition record to all MSPileup records if they do not have it
adjustMongoDocs.py --add-tran-record --dburi=localhost:8230 --dbname=test --dbcoll=test --verbose 1

NOTE: the script can run from any location assuming it has proper db uri with
credentials and availability of mongo db outside of its own cluster, but most
likely it will be run on mongo db k8s cluster
"""

import argparse
import json
from pymongo import MongoClient

# WMCore services
from Utils.Timers import gmtimeSeconds


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
        self.parser.add_argument("--userdn", action="store",
                                 dest="userDN", default='', help="user DN")
        self.parser.add_argument("--add-tran-record", action="store_true",
                                 dest="transition", help="add transaction record")
        self.parser.add_argument("--verbose", action="store",
                                 dest="verbose", default=0, help="verbosity level")


def addTransitionRecord(dburi, dbname, dbcoll, userDN, verbose):
    """
    Helper function to add and update in place transition record for existing MSPileup records
    :param dburi: database URI (string)
    :param dbname: database name (string)
    :param dbcoll: database collection name (string)
    :param userDN: string representing user DN
    :param verbose: verbose flag (boolean)
    :return: nothing
    """
    if not userDN:
        raise Exception("No userDN provided")
    conn = MongoClient(host=dburi)
    records = [r for r in conn[dbname][dbcoll].find()]
    for rec in records:
        if not rec.get("transition"):
            tranRecord = {'containerFraction': 1.0,
                          'customDID': rec['pileupName'],
                          'updateTime': gmtimeSeconds(),
                          'DN': userDN}
            rec['transition'] = [tranRecord]
            if verbose:
                print(f"update rec={rec} with new transition record={tranRecord}")
            spec = {'pileupName': rec['pileupName'], 'customName': rec.get('customName', '')}
            # perform update of the record
            conn[dbname][dbcoll].update_one(spec, {'$set': {'transition': [tranRecord]}}, upsert=True)


def adjust(dburi, dbname, dbcoll, fin, verbose):
    """
    helper script to update documents in MongoDB with given db name/collection
    and schema file (which should contain new addition, in form of JSON, to the
    db documents)
    :param dburi: database URI (string)
    :param dbname: database name (string)
    :param dbcoll: database collection name (string)
    :param fin: input file with schema content
    :param verbose: verbose flag (boolean)
    :return: nothing
    """
    conn = MongoClient(host=dburi)
    schema = {}
    with open(fin, 'r', encoding='utf-8') as istream:
        schema = json.load(istream)
    if schema:
        if verbose:
            print(f"read data from '{dburi}', {dbname}/{dbcoll}")
        records = [r for r in conn[dbname][dbcoll].find()]
        for doc in records:
            if verbose:
                print(f"update doc={doc} with schema={schema}")
            conn[dbname][dbcoll].update_one(doc, {'$set': schema}, upsert=True)


def main():
    """
    Main function
    """
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    if opts.transition:
        addTransitionRecord(opts.dburi, opts.dbname, opts.dbcoll, opts.userDN, opts.verbose)
    else:
        adjust(opts.dburi, opts.dbname, opts.dbcoll, opts.fin, opts.verbose)


if __name__ == '__main__':
    main()
