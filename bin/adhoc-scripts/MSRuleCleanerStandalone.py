import sys
import os
import time
import logging

from pprint import pformat
from itertools import izip

from WMCore.MicroService.Unified.MSRuleCleaner import MSRuleCleaner
from WMCore.Database.CMSCouch import Database as CouchDB

FORMAT = "%(asctime)s:%(levelname)s:%(module)s:%(funcName)s(): %(message)s"
logging.basicConfig(stream=sys.stdout, format=FORMAT, level=logging.DEBUG)
logger = logging.getLogger()

# Service config
msConfig = {"enableRealMode": False,
            "verbose": True,
            "interval": 1 *60,
            "services": ['ruleCleaner'],
            "rucioWmaAcct": "wma_test",
            "rucioMStrAccount": "wmcore_transferor",
            "useRucio": True,
            "rucioAccount": "wma_test",
            'reqmgr2Url': 'https://cmsweb-testbed.cern.ch/reqmgr2',
            'MSOutputUrl': 'https://cmsweb-testbed.cern.ch/ms-output',
            'reqmgrCacheUrl': 'https://cmsweb-testbed.cern.ch/couchdb/reqmgr_workload_cache',
            'phedexUrl': 'https://cmsweb-testbed.cern.ch/phedex/datasvc/json/prod',
            'dbsUrl': 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader',
            'couchDBUrl': 'https://cmsweb-testbed.cern.ch/couchdb',
            'rucioUrl': 'http://cmsrucio-int.cern.ch',
            'rucioAuthUrl': 'https://cmsrucio-auth-int.cern.ch'}

# https://cmsweb-testbed.cern.ch/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatusandtime?descending=false&startkey=%5B%22aborted-archived%22,1588338001%5D&endkey=%5B%22aborted-archived%22,1606224631%5D

couchDB = CouchDB(dbname='reqmgr_workload_cache',
                  url=msConfig['couchDBUrl'],
                  size=100000,
                  ckey=os.getenv("X509_USER_KEY", "Unknown"),
                  cert=os.getenv("X509_USER_CERT", "Unknown"))
timeFormat = "%b %d %H:%M:%S %Y"
startTimeStr = "Jun 9 00:00:00 2020"
endTimeStr = "Nov 25 00:00:00 2020"
startTime = time.strptime(startTimeStr, timeFormat)
endTime = time.strptime(endTimeStr, timeFormat)
startTimeSec = int(time.mktime(startTime))
endTimeSec = int(time.mktime(endTime))
logger.info("\nstartTimeStr: %s\nstartTime: %s\nstartTimeSec: %s",
            startTimeStr, startTime, startTimeSec)
logger.info("\nendTimeStr: %s\nendTime: %s\nendTimeSec: %s",
            endTimeStr, endTime, endTimeSec)
reqStatus = ['aborted-archived', 'rejected-archived', 'normal-archived']
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
