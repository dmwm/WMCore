import json
import time, datetime
import random, string
import os

from uuid import uuid1
from optparse import OptionParser

from WMCore.Database.CMSCouch import CouchServer
from WMCore.Lexicon import splitCouchServiceURL
from WMCore.WMBase import getWMBASE
from couchapp.commands import push as couchapppush
from couchapp.config import Config

def couchAppRoot():
    """Return path to couchapp dir"""
    wmBase = getWMBASE()
    develPath = "%s/src/couchapps" % wmBase
    if not os.path.exists(develPath):
        basePath = "%s/couchapps" % os.environ['WMCORE_ROOT']
    else:
        basePath = develPath
    return basePath

def installCouchApp(couchUrl, couchDBName, couchAppName, basePath = None):
    """
    _installCouchApp_

    Install the given couch app on the given server in the given database.  If
    the database already exists it will be deleted.
    """
    if not basePath:
        basePath = couchAppRoot()
    print "Installing %s into %s" % (couchAppName, couchDBName)

    couchServer = CouchServer(couchUrl)
    couchappConfig = Config()

    couchapppush(couchappConfig, "%s/%s" % (basePath, couchAppName),
                 "%s/%s" % (couchUrl, couchDBName))
    return

def parse_opts():
    parser = OptionParser()
    parser.add_option("-d", "--dburl",
                    dest="dburl",
                    help="CouchDB URL which data will be populated")
    parser.add_option("-c", "--couchapp-base",
                    dest="couchapp_base",
                    help="Couch sapp base path")
    parser.add_option("--no-couchapp",
                    action="store_false",
                    default=True,
                    dest="add_couchapp",
                    help="Don't update couchapp")
    parser.add_option("--no-reqmgr-data",
                    action="store_false",
                    default=True,
                    dest="add_reqmgr_data",
                    help="Don't update reqmgr data")
    parser.add_option("--no-agent-data",
                    action="store_false",
                    default=True,
                    dest="add_agent_data",
                    help="Don't update reqmgr data")
    parser.add_option("-u", "--users",
                    dest="users",
                    default=10,
                    type="int",
                    help="The number of users, default=10")
    parser.add_option("-s", "--sites",
                    dest="sites",
                    default=5,
                    type="int",
                    help="The number of sites, default=5")
    parser.add_option("-a", "--agents",
                    dest="agents",
                    default=2,
                    type="int",
                    help="The number of agents, default=2")
    parser.add_option("-i", "--iterations",
                    dest="iterations",
                    default=5,
                    type="int",
                    help="The number of iterations to make, default=5")
    parser.add_option("-r", "--requests",
                    dest="requests",
                    default=5,
                    type="int",
                    help="The number of requests to simulate, default=5")
    parser.add_option("-w", "--wait",
                    dest="wait",
                    default=0,
                    type="int",
                    help="Wait W seconds between iterations, default=0")
    
    
    return parser.parse_args()[0]

def generate_agent_requests(number=5, iterations=5):
    """ 
    generate the request with following structure
      {
       "_id": "af27057919546ff8f3fc8d7f18233355",
       "_rev": "1-181021c38a5444676d7718f42ffa9a89",
       "status": {
           "inWMBS": 1,
           "queued": {
               "retry": 1
           }
       },
       "workflow": "cmsdataops_sryu_test4_120111_114950_128",
       "timestamp": 1326306397,
       "sites": {
           "T1_DE_KIT": {
               "queued": {
                   "retry": 1
               }
           }
       },
       "agent": "WMAgentCommissioning",
       "team": "team1,team2,cmsdataops",
       "agent_url": "cms-xen39.fnal.gov",
       "type": "agent_request"
       }
    """
    current_time = int(time.time())
    docs = []
    for cycle in xrange(iterations): 
        for i in xrange(number):
            doc = {"status": {"inWMBS": 12,
                              "submitted": {"retry": 1, "running": 1, "pending": 1, "first": 1},
                              "failure": {"exception": 1, "create": 1, "submit": 1, "cancel": 1},
                              "queued": {"retry": 1, "first": 1},
                              "running": {"retry": 1, "first": 1},
                              "cooloff": 1
                             },
                    
                "workflow": "test_workflow_%s" % i,
                "timestamp": current_time + (cycle * 10),
                "sites": {"T1_DE_KIT": 
                             {
                              "submitted": {"retry": 1, "running": 1, "pending": 1, "first": 1},
                              "failure": {"exception": 1, "create": 1, "submit": 1, "cancel": 1},
                              "queued": {"retry": 1, "first": 1},
                              "running": {"retry": 1, "first": 1},
                              "cooloff": 1
                             }
                          },
                "agent": "WMAgentCommissioning",
                "agent_teams": "team1,team2,cmsdataops",
                "agent_url": "cms-xen39.fnal.gov",
                "type": "agent_request"
            }
            docs.append(doc)
    return docs
   
def generate_reqmgr_requests(number=5):
    """ 
    generate the request with following structure
      {
       "_id": "cmsdataops_sryu_test4_120111_114950_128",
       "_rev": "6-02b17b4eabcf333e7499c0fa0ae5055b",
       "inputdataset": "/Photon/Run2011A-v1/RAW",
       "group": "cmsdataops",
       "request_date": [2012, 1, 11, 17, 49, 50],
       "campaign": "SryuTest2",
       "workflow": "cmsdataops_sryu_test4_120111_114950_128",
       "priority": "1",
       "requestor": "cmsdataops",
       "request_type": "ReReco",
       "type": "reqmgr_request",
       "request_status": [
           {
               "status": "new",
               "update_time": 1326304190
           },
           {
               "status": "assignment-approved",
               "update_time": 1326304216
           },
           {
               "status": "assigned",
               "update_time": 1326304227
           }
       ],
       "site_white_list": [
           "T1_DE_KIT"
       ],
       "teams": [
           "cmsdataops"
       ]
    }
    """   
    docs = [] 
    for i in xrange(number):
        doc = {"_id": "test_workflow_%s" % i,
               "inputdataset": "/Photon/Run2011A-v1/RAW",
               "group": "cmsdataops",
               "request_date": [2012, 1, 11, 17, 49, 50],
               "campaign": "SryuTest2",
               "workflow": "test_workflow_%s" % i,
               "priority": "1",
               "requestor": "cmsdataops",
               "request_type": "ReReco",
               "type": "reqmgr_request",
               "request_status": [
                                  {"status": "new", "update_time": 1326304190},
                                  {"status": "assignment-approved", "update_time": 1326304216},
                                  {"status": "assigned", "update_time": 1326304227}
                                 ],
                "site_white_list": ["T1_DE_KIT"], 
                "teams": ["cmsdataops"]
                }
        docs.append(doc)
    return docs
        
def generate_sites(request):

    sites = [ 'T2_AT_Vienna', 'T2_BE_IIHE', 'T2_BE_UCL', 'T2_BR_SPRACE',
              'T2_BR_UERJ', 'T2_CH_CAF', 'T2_CH_CSCS', 'T2_CN_Beijing', 'T2_DE_DESY',
              'T2_DE_RWTH', 'T2_EE_Estonia', 'T2_ES_CIEMAT', 'T2_ES_IFCA',
              'T2_FI_HIP', 'T2_FR_CCIN2P3', 'T2_FR_GRIF_IRFU', 'T2_FR_GRIF_LLR',
              'T2_FR_IPHC', 'T2_HU_Budapest', 'T2_IN_TIFR', 'T2_IT_Bari',
              'T2_IT_Legnaro', 'T2_IT_Pisa', 'T2_IT_Rome', 'T2_KR_KNU', 'T2_PK_NCP',
              'T2_PL_Cracow', 'T2_PL_Warsaw', 'T2_PT_LIP_Lisbon', 'T2_PT_NCG_Lisbon',
              'T2_RU_IHEP', 'T2_RU_INR', 'T2_RU_ITEP', 'T2_RU_JINR', 'T2_RU_PNPI',
              'T2_RU_RRC_KI', 'T2_RU_SINP', 'T2_TR_METU', 'T2_TW_Taiwan',
              'T2_UA_KIPT', 'T2_UK_London_Brunel', 'T2_UK_London_IC',
              'T2_UK_SGrid_Bristol', 'T2_UK_SGrid_RALPP', 'T2_US_Caltech',
              'T2_US_Florida', 'T2_US_MIT', 'T2_US_Nebraska', 'T2_US_Purdue',
              'T2_US_UCSD', 'T2_US_Wisconsin']
    if sites not in request.keys():
      request["sites"] = {}
      # jobs run at 1-10 sites
      req_sites = random.sample(sites, random.randint(1, 10))
      # can't use a defaultdict because it doesn't thunk
      for site in req_sites:
        request["sites"][site] = {}
    
    status = {}
    status.update(request['status'])
    
    for site in request["sites"]:
      for k, v in status.items():
        j = random.randint(0, v)
        request["sites"][site][k] = j
        status[k] -= j
    
    # Mop up - must be a better way to do this...
    site = request["sites"].keys()[-1]
    for k, v in status.items():
      request["sites"][site][k] += v

def start_clock(iterations):
    difference = iterations * datetime.timedelta(minutes=15)
    weeks, days = divmod(difference.days, 7)
    minutes, seconds = divmod(difference.seconds, 60)
    hours, minutes = divmod(minutes, 60)
    
    print "Running %s iterations " % iterations
    print "Equivalent to running for %s weeks, %s days, %s hours, %s minutes" % (weeks, days, hours, minutes)
    
    now = datetime.datetime.now()
    dt = datetime.timedelta(minutes=15)

    return now, dt

def main(options):
    url, dbName = splitCouchServiceURL(options.dburl)
    db = CouchServer(url).connectDatabase(dbName)
    reqmgr_requests = generate_reqmgr_requests(options.requests)
    agent_requests = generate_agent_requests(options.requests, options.iterations)
    
    if options.add_couchapp:
        installCouchApp(url, dbName, "WMStats", options.couchapp_base)
    
    if options.add_reqmgr_data:
        for req in reqmgr_requests:
            db.queue(req)
        db.commit()
        print "Added %s reqmgr requests" % len(reqmgr_requests)
    
    
    if options.add_agent_data:
        for req in agent_requests:
            db.queue(req)
        db.commit()
        print "Added %s agent requests" % len(agent_requests)
  
if __name__ == "__main__":
    main(parse_opts())