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
    msConfig.setdefault("mongoDBPort", 27017)
    msConfig.setdefault("mongoDB", 'mongoDBNull')
    msConfig.setdefault("mongoDBRetryCount", 3)
    msConfig.setdefault("mongoDBReplicaset", None)
    msConfig.setdefault("mockMongoDB", False)
    msConfig.setdefault("collection", None)

    mongoDBConfig = {
        'database': msConfig['mongoDB'],
        'server': msConfig['mongoDBUrl'],
        'port': msConfig['mongoDBPort'],
        'replicaset': msConfig['mongoDBReplicaset'],
        'logger': logger,
        'create': False,
        'mockMongoDB': msConfig['mockMongoDB']}

    mongoClt = MongoDB(**mongoDBConfig)
    mongoDB = getattr(mongoClt, msConfig['mongoDB'])
    mongoColl = mongoDB[msConfig['collection']] if msConfig['collection'] else None
