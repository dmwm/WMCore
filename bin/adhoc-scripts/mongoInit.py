#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script to connect to a MongoDB instance based on a set of parameters given at runtime
Usage:
      source /data/srv/current/apps/reqmgr2ms/etc/profile.d/init.sh
      python3 -i -- mongoInit.py -c $WMCORE_SERVICE_CONFIG/reqmgr2ms-output/config-output.py
"""

import sys
import logging
import argparse
from pprint import pformat

from WMCore.Database.MongoDB import MongoDB

from WMCore.Configuration import loadConfigurationFile

if __name__ == '__main__':

    FORMAT = "%(asctime)s:%(levelname)s:%(module)s:%(funcName)s(): %(message)s"
    logging.basicConfig(stream=sys.stdout, format=FORMAT, level=logging.INFO)
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(
        prog='MongoInit',
        formatter_class=argparse.RawTextHelpFormatter,
        description=__doc__)

    parser.add_argument('-c', '--config', required=True,
                        help="""\
                        The path to MSCONFIG to be used, e.g.
                        for production: /data/srv/current/config/reqmgr2ms/config-output.py
                        """)
    args = parser.parse_args()

    logger.info("Loading configFile: %s", args.config)
    config = loadConfigurationFile(args.config)

    msConfig = config.section_('views').section_('data').dictionary_()

    msConfig.setdefault("mongoDBUrl", 'mongodb://localhost')
    msConfig.setdefault("mongoDBPort", None)
    msConfig.setdefault("mongoDB", 'mongoDBNull')
    msConfig.setdefault("mongoDBRetryCount", 3)
    msConfig.setdefault("mongoDBReplicaSet", None)
    msConfig.setdefault("mockMongoDB", False)
    msConfig.setdefault("collection", None)

    # NOTE: A full set of valid database connection parameters can be found at:
    #       https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html
    mongoDBConfig = {
        'database': msConfig['mongoDB'],
        'server': msConfig['mongoDBServer'],
        'replicaSet': msConfig['mongoDBReplicaSet'],
        'port': msConfig['mongoDBPort'],
        'username': msConfig['mongoDBUser'],
        'password': msConfig['mongoDBPassword'],
        'connect': True,
        'directConnection': False,
        'logger': logger,
        'create': False,
        'mockMongoDB': msConfig['mockMongoDB']}

    # NOTE: We need to blur `username' and `password' keys before printing the configuration:
    msg = "Connecting to MongoDB using the following mongoDBConfig:\n%s"
    logger.info(msg, pformat({**mongoDBConfig, **{'username': '****', 'password': '****'}}))

    mongoDB = MongoDB(**mongoDBConfig)
    currDB = getattr(mongoDB, msConfig['mongoDB'])
    mongoClt = mongoDB.client
    mongoColl = currDB[msConfig['collection']] if msConfig['collection'] else None
