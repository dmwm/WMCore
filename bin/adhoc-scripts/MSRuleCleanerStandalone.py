#!/usr/bin/python

import sys
import os
import time
import logging
import argparse

from pprint import pformat
from itertools import izip

from WMCore.MicroService.Unified.MSRuleCleaner import MSRuleCleaner
from WMCore.Database.CMSCouch import Database as CouchDB

FORMAT = "%(asctime)s:%(levelname)s:%(module)s:%(funcName)s(): %(message)s"
logging.basicConfig(stream=sys.stdout, format=FORMAT, level=logging.DEBUG)
logger = logging.getLogger()

# Default values:
defaults = {'StartTimeStr': "Jun-9-00:00:00-2020",
            'EndTimeStr': "Nov-25-00:00:00-2020",
            'CentServices': "https://cmsweb-testbed.cern.ch",
            'RucioMStrAccount': "wmcore_transferor",
            'RucioWmaAccount': "wma_test",
            'RucioUrl': 'http://cmsrucio-int.cern.ch',
            'RucioAuthUrl': 'https://cmsrucio-auth-int.cern.ch',
            'ReqStatus': ['aborted-archived',
                          'rejected-archived',
                          'normal-archived'],
            'EnableRealMode': False}

# Parse command line arguments:
argParser = argparse.ArgumentParser()
argParser.add_argument('-s', '--startTime',
                       type=str,
                       required=False,
                       help="Start time for building the CouchDB view.",
                       dest='startTimeStr',
                       default=defaults['StartTimeStr'])

argParser.add_argument('-e', '--endTime',
                       type=str,
                       required=False,
                       help="End time for building the CouchDB view.",
                       dest='endTimeStr',
                       default=defaults['EndTimeStr'])

argParser.add_argument('-c', '--centServices',
                       type=str,
                       required=False,
                       help="Url to central services instance.",
                       dest='centServices',
                       default=defaults['CentServices'])

argParser.add_argument('-rt', '--rucioMSTrAccount',
                       type=str,
                       required=False,
                       help="Rucio MSTransferror account.",
                       dest='rucioMStrAccount',
                       default=defaults['RucioMStrAccount'])

argParser.add_argument('-rw', '--rucioWmaAccount',
                       type=str,
                       required=False,
                       help="Rucio WMAgent account.",
                       dest='rucioWmaAccount',
                       default=defaults['RucioWmaAccount'])

argParser.add_argument('-ru', '--rucioUrl',
                       type=str,
                       required=False,
                       help="Rucio URL.",
                       dest='rucioUrl',
                       default=defaults['RucioUrl'])

argParser.add_argument('-ra', '--rucioAuthUrl',
                       type=str,
                       required=False,
                       help="Rucio Authentication URL.",
                       dest='rucioAuthUrl',
                       default=defaults['RucioAuthUrl'])

argParser.add_argument('-st', '--reqStatus',
                       type=str,
                       required=False,
                       action='append',
                       help="Request statuses to be worked on. (Needs to be called multiple times in order to build a list of statuses to be worked on).",
                       dest='reqStatus',
                       default=[])

argParser.add_argument('-erm', '--enableRealMode',
                       type=bool,
                       required=False,
                       help="Enable RealRun mode",
                       dest='enableRealMode',
                       default=defaults['EnableRealMode'])

args = argParser.parse_args()

# Fix forced empty list for reqStatus default value
if not args.reqStatus:
    for status in defaults['ReqStatus']:
        argParser.parse_args(args=["--reqStatus", status], namespace=args)

logger.info("args: %s", pformat(args))

# Service config
msConfig = {"enableRealMode": args.enableRealMode,
            "verbose": True,
            "interval": 1 *60,
            "services": ['ruleCleaner'],
            "rucioWmaAccount": args.rucioWmaAccount,
            "rucioMStrAccount": args.rucioMStrAccount,
            "useRucio": True,
            "rucioAccount": args.rucioMStrAccount,
            'reqmgr2Url': args.centServices + '/reqmgr2',
            'MSOutputUrl': args.centServices + '/ms-output',
            'reqmgrCacheUrl': args.centServices + '/couchdb/reqmgr_workload_cache',
            'couchDBUrl': args.centServices + '/couchdb',
            'rucioUrl': args.rucioUrl,
            'rucioAuthUrl': args.rucioAuthUrl}


def main():
    # https://cmsweb-testbed.cern.ch/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatusandtime?descending=false&startkey=%5B%22aborted-archived%22,1588338001%5D&endkey=%5B%22aborted-archived%22,1606224631%5D

    couchDB = CouchDB(dbname='reqmgr_workload_cache',
                      url=msConfig['couchDBUrl'],
                      size=100000,
                      ckey=os.getenv("X509_USER_KEY", "Unknown"),
                      cert=os.getenv("X509_USER_CERT", "Unknown"))
    timeFormat = "%b-%d-%H:%M:%S-%Y"
    startTime = time.strptime(args.startTimeStr, timeFormat)
    endTime = time.strptime(args.endTimeStr, timeFormat)
    startTimeSec = int(time.mktime(startTime))
    endTimeSec = int(time.mktime(endTime))
    logger.info("\nstartTimeStr: %s\nstartTime: %s\nstartTimeSec: %s",
                args.startTimeStr, startTime, startTimeSec)
    logger.info("\nendTimeStr: %s\nendTime: %s\nendTimeSec: %s",
                args.endTimeStr, endTime, endTimeSec)
    reqStatus = args.reqStatus
    try:
        reqNames = []
        reqList = []
        reqRecords = {}
        for status in reqStatus:
            logger.info("Fetching requests in status: %s", status)
            design = 'ReqMgr'
            view = 'bystatusandtime?descending=false'
            view += '&startkey=["%s",%s]' % (status, startTimeSec)
            view += '&endkey=["%s",%s]' % (status, endTimeSec)
            view += '&include_docs=true'
            viewList = couchDB.loadView(design, view)['rows']
            reqList.extend([req['doc'] for req in viewList])
            reqNames.extend([req['id'] for req in viewList])
            logger.info('  retrieved %s requests in status: %s', len(viewList), status)
        logger.info("Building the final list of request records.")
        logger.info("Requests retrieved in total: %s", len(reqNames))
        reqRecords.update(dict(izip(reqNames, reqList)))
        # logger.debug("reqRecords: %s", pformat(reqRecords))
    except Exception as err:  # general error
        msg = "Unknown exception while fetching requests from ReqMgr2. Error: %s", str(err)
        logger.exception(msg)

    logger.info("################################################################")
    msRuleCleaner = MSRuleCleaner(msConfig)
    msRuleCleaner.resetCounters()
    result = msRuleCleaner._execute(reqRecords)
    logger.info('Execute result: %s', pformat(result))


if __name__ == "__main__":
    main()
