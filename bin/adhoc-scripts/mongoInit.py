#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script to connect to a MongoDB instance based on a set of parameters given at runtime
Usage:
      source /data/srv/current/apps/reqmgr2ms/etc/profile.d/init.sh
      python3 -i mongoInit.py [--options]
"""

import sys
import os
import logging
import argparse

from pprint import pformat, pprint

from pymongo import IndexModel

from WMCore.Database.MongoDB import MongoDB

from WMCore.MicroService.MSOutput.MSOutputTemplate import MSOutputTemplate
from WMCore.Configuration import Configuration, loadConfigurationFile
from Utils.TwPrint import twFormat

if __name__ == '__main__':

    FORMAT = "%(asctime)s:%(levelname)s:%(module)s:%(funcName)s(): %(message)s"
    logging.basicConfig(stream=sys.stdout, format=FORMAT, level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(
        prog='MSOutputDocAlter',
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

    # NOTE: Add the database name here if needed
    #       (in the case of MSOutput it is hardcoded in the service code and is
    #       missing from the configuration file) :
    msConfig['mongoDB'] = 'msOutDB'

    msOutIndex = IndexModel('RequestName', unique=True)
    msOutDBConfig = {
        'database': msConfig['mongoDB'],
        'server': msConfig['mongoDBUrl'],
        'port': msConfig['mongoDBPort'],
        'logger': logger,
        'create': False,
        'collections': [
            ('msOutRelValColl', msOutIndex),
            ('msOutNonRelValColl', msOutIndex)]}

    mongoClt = MongoDB(**msOutDBConfig)
    msOutDB = getattr(mongoClt, msConfig['mongoDB'])
    msOutColl = msOutDB['msOutNonRelValColl']
