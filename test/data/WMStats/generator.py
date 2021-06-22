from __future__ import print_function

from builtins import range
import os
import random
import time
from argparse import ArgumentParser

from couchapp.commands import push as couchapppush

from WMCore.Database.CMSCouch import CouchServer
from WMCore.Lexicon import splitCouchServiceURL
from WMCore.WMBase import getWMBASE

NUM_OF_REQUEST = 20
ITERATIONS = 100
NUM_OF_JOBS_PER_REQUEST = 10


def couchAppRoot():
    """Return path to couchapp dir"""
    wmBase = getWMBASE()
    develPath = "%s/src/couchapps" % wmBase
    if not os.path.exists(develPath):
        basePath = "%s/couchapps" % os.environ['WMCORE_ROOT']
    else:
        basePath = develPath
    return basePath


def installCouchApp(couchUrl, couchDBName, couchAppName, basePath=None):
    """
    _installCouchApp_

    Install the given couch app on the given server in the given database.  If
    the database already exists it will be deleted.
    """
    if not basePath:
        basePath = couchAppRoot()
    print("Installing %s into %s" % (couchAppName, couchDBName))

    couchapppush("%s/%s" % (basePath, couchAppName), "%s/%s" % (couchUrl, couchDBName))
    return


def parse_opts():
    parser = ArgumentParser()
    parser.add_argument("-d", "--dburl",
                        dest="dburl",
                        help="CouchDB URL which data will be populated")
    parser.add_argument("-c", "--couchapp-base",
                        dest="couchapp_base",
                        help="Couch sapp base path")
    parser.add_argument("--no-couchapp",
                        action="store_false",
                        default=True,
                        dest="add_couchapp",
                        help="Don't update couchapp")
    parser.add_argument("--no-reqmgr-data",
                        action="store_false",
                        default=True,
                        dest="add_reqmgr_data",
                        help="Don't update reqmgr data")
    parser.add_argument("--no-agent-data",
                        action="store_false",
                        default=True,
                        dest="add_agent_data",
                        help="Don't update reqmgr data")
    parser.add_argument("-u", "--users",
                        dest="users",
                        default=10,
                        type=int,
                        help="The number of users, default=10")
    parser.add_argument("-s", "--sites",
                        dest="sites",
                        default=5,
                        type=int,
                        help="The number of sites, default=5")
    parser.add_argument("-a", "--agents",
                        dest="agents",
                        default=2,
                        type=int,
                        help="The number of agents, default=2")
    parser.add_argument("-i", "--iterations",
                        dest="iterations",
                        default=ITERATIONS,
                        type=int,
                        help="The number of iterations to make, default=%s" % ITERATIONS)
    parser.add_argument("-r", "--requests",
                        dest="requests",
                        default=NUM_OF_REQUEST,
                        type=int,
                        help="The number of requests to simulate, default=%s" % NUM_OF_REQUEST)
    parser.add_argument("-w", "--wait",
                        dest="wait",
                        default=0,
                        type=int,
                        help="Wait W seconds between iterations, default=0")

    return parser.parse_args()


def generate_reqmgr_requests(number=NUM_OF_REQUEST):
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
       "team": "cmsdataops"
    }
    """
    docs = []
    for i in range(number):
        doc = {"_id": "test_workflow_%s" % i,
               "inputdataset": "/Photon/Run2011A-v1/RAW",
               "group": "cmsdataops",
               "request_date": [2012, 1, 11, 17, 49, 50],
               "campaign": "SryuTest-%s" % (i % 5),
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
               "team": "cmsdataops"
               }
        docs.append(doc)
    return docs


def generate_agent_requests(number=NUM_OF_REQUEST, iterations=ITERATIONS):
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
       "agent": "WMAgent",
       "team": "team1,team2,cmsdataops",
       "agent_url": "cms-xen39.fnal.gov",
       "type": "agent_request"
       }
    """
    current_time = int(time.time())
    docs = []
    for cycle in range(iterations):
        for i in range(number):
            doc = {"status": {"inWMBS": 12,
                              "submitted": {"retry": 2, "running": 2, "pending": 2, "first": 2},
                              "failure": {"exception": 2, "create": 2, "submit": 2},
                              "queued": {"retry": 2, "first": 2},
                              "canceled": 2,
                              "cooloff": 2,
                              "success": 2
                              },

                   "workflow": "test_workflow_%s" % i,
                   "timestamp": current_time + (cycle * 10),
                   "sites": {"T1_DE_KIT":
                       {
                           "submitted": {"retry": 1, "running": 1, "pending": 1, "first": 1},
                           "failure": {"exception": 1, "create": 1, "submit": 1},
                           "queued": {"retry": 1, "first": 1},
                           "canceled": 1,
                           "cooloff": 1,
                           "success": 1
                       },
                       "T1_US_FNAL":
                           {
                               "submitted": {"retry": 1, "running": 1, "pending": 1, "first": 1},
                               "failure": {"exception": 1, "create": 1, "submit": 1},
                               "queued": {"retry": 1, "first": 1},
                               "canceled": 1,
                               "cooloff": 1,
                               "success": 1
                           }
                   },
                   "agent": "WMAgent",
                   "agent_teams": "cmsdataops",
                   "agent_url": "cms-xen39.fnal.gov",
                   "type": "agent_request"
                   }
            docs.append(doc)
    return docs


def generate_jobsummary(request, number=NUM_OF_JOBS_PER_REQUEST):
    """
    jobSummary = {"_id": "jobid_1",  //jobid
                  "type": "jobsummary", // setvalue
                  "retrycount": job["retry_count"],
                  "workflow": workflow1, //request name
                  "task": job["task"],
                  "state": success,
                  "site": T1_US_FNAL,
                  "exitcode": 123,
                  "errors": errmsgs,
                  "lumis": inputs,
                  "output": outputs }

    errmsgs = {}
    inputs = []
    for step in fwjrDocument["fwjr"]["steps"]:
        if "errors" in fwjrDocument["fwjr"]["steps"][step]:
            errmsgs[step] = [error for error in fwjrDocument["fwjr"]["steps"][step]["errors"]]
        if "input" in fwjrDocument["fwjr"]["steps"][step] and "source" in fwjrDocument["fwjr"]["steps"][step]["input"]:
            inputs.extend( [source["runs"] for source in fwjrDocument["fwjr"]['steps'][step]["input"]["source"] if "runs" in source] )
    outputs = [ {'type': singlefile.get('module_label', None),
                 'lfn': singlefile.get('lfn', None),
                 'location': singlefile.get('locations', None),
                 'checksums': singlefile.get('checksums', {}),
                     'size': singlefile.get('size', None) } for singlefile in job["fwjr"].getAllFiles() if singlefile ]


    job status
    ['new', 'created', 'executing', 'complete', 'createfailed', 'submitfailed',
     'jobfailed', 'createcooloff',  'submitcooloff', 'jobcooloff', 'success',
     'exhausted', 'killed']
    """

    # TODO: Make more realistic
    docs = []
    statusList = ['new', 'created', 'executing', 'complete', 'createfailed', 'submitfailed',
                  'jobfailed', 'createcooloff', 'submitcooloff', 'jobcooloff', 'success',
                  'exhausted', 'killed']

    for i in range(number):
        status = statusList[random.randint(0, len(statusList) - 1)]
        errmsgs = {}
        if status.find("failed"):
            exitCode = 666
            errmsgs["step1"] = {}
            errmsgs["step1"]["out"] = {}
            errmsgs["step1"]["out"]["type"] = "test error"
        else:
            exitCode = 0

        jobSummary = {"_id": "jobid_%s_%s" % (request, i),
                      "type": "jobsummary",
                      "retrycount": random.randint(0, 5),
                      "workflow": request,
                      "task": "/%s/task_%s" % (request, i),
                      "state": status,
                      "site": "T1_US_FNAL",
                      "exitcode": exitCode,
                      "errors": errmsgs,
                      "lumis": [[123, 124], [567, 879]],
                      "output": [{'type': "test-type",
                                  'lfn': "/somewhere/file.root",
                                  'location': ['T1_US_FNAL'],
                                  'checksums': {'adler32': 'abc123', 'cksum': 'cdf123'},
                                  'size': "1000"}]
                      }
        docs.append(jobSummary)
    return docs


# def generate_sites(request):
#
#    sites = [ 'T2_AT_Vienna', 'T2_BE_IIHE', 'T2_BE_UCL', 'T2_BR_SPRACE',
#              'T2_BR_UERJ', 'T2_CH_CAF', 'T2_CH_CSCS', 'T2_CN_Beijing', 'T2_DE_DESY',
#              'T2_DE_RWTH', 'T2_EE_Estonia', 'T2_ES_CIEMAT', 'T2_ES_IFCA',
#              'T2_FI_HIP', 'T2_FR_CCIN2P3', 'T2_FR_GRIF_IRFU', 'T2_FR_GRIF_LLR',
#              'T2_FR_IPHC', 'T2_HU_Budapest', 'T2_IN_TIFR', 'T2_IT_Bari',
#              'T2_IT_Legnaro', 'T2_IT_Pisa', 'T2_IT_Rome', 'T2_KR_KNU', 'T2_PK_NCP',
#              'T2_PL_Cracow', 'T2_PL_Warsaw', 'T2_PT_LIP_Lisbon', 'T2_PT_NCG_Lisbon',
#              'T2_RU_IHEP', 'T2_RU_INR', 'T2_RU_ITEP', 'T2_RU_JINR', 'T2_RU_PNPI',
#              'T2_RU_RRC_KI', 'T2_RU_SINP', 'T2_TR_METU', 'T2_TW_Taiwan',
#              'T2_UA_KIPT', 'T2_UK_London_Brunel', 'T2_UK_London_IC',
#              'T2_UK_SGrid_Bristol', 'T2_UK_SGrid_RALPP', 'T2_US_Caltech',
#              'T2_US_Florida', 'T2_US_MIT', 'T2_US_Nebraska', 'T2_US_Purdue',
#              'T2_US_UCSD', 'T2_US_Wisconsin']
#    if sites not in request.keys():
#      request["sites"] = {}
#      # jobs run at 1-10 sites
#      req_sites = random.sample(sites, random.randint(1, 10))
#      # can't use a defaultdict because it doesn't thunk
#      for site in req_sites:
#        request["sites"][site] = {}
#
#    status = {}
#    status.update(request['status'])
#
#    for site in request["sites"]:
#      for k, v in status.items():
#        j = random.randint(0, v)
#        request["sites"][site][k] = j
#        status[k] -= j
#
#    # Mop up - must be a better way to do this...
#    site = request["sites"].keys()[-1]
#    for k, v in status.items():
#      request["sites"][site][k] += v
#
# def start_clock(iterations):
#    difference = iterations * datetime.timedelta(minutes=15)
#    weeks, days = divmod(difference.days, 7)
#    minutes, seconds = divmod(difference.seconds, 60)
#    hours, minutes = divmod(minutes, 60)
#
#    print "Running %s iterations " % iterations
#    print "Equivalent to running for %s weeks, %s days, %s hours, %s minutes" % (weeks, days, hours, minutes)
#
#    now = datetime.datetime.now()
#    dt = datetime.timedelta(minutes=15)
#
#    return now, dt

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
        print("Added %s reqmgr requests" % len(reqmgr_requests))

    if options.add_agent_data:
        for req in agent_requests:
            db.queue(req)
            jobDocs = generate_jobsummary(req['workflow'])
            for job in jobDocs:
                db.queue(job)
        db.commit()
        print("Added %s agent requests" % len(agent_requests))
        print("Added %s job Docs" % (len(agent_requests) * len(jobDocs)))


if __name__ == "__main__":
    main(parse_opts())
