from builtins import range
import time
import random

NUM_OF_REQUEST = 1
ITERATIONS =1
NUM_OF_JOBS_PER_REQUEST = 11

def generate_reqmgr_schema(number=NUM_OF_REQUEST):
    """
    generate the request with following structure
    doc["_id"] = schema['RequestName']
    doc["workflow"] = schema['RequestName']
    doc["requestor"] = schema['Requestor']
    doc["campaign"] = schema['Campaign']
    doc["request_type"] = schema['RequestType']
    doc["priority"] = schema['RequestPriority']
    doc["group"] = schema['Group']
    doc["request_date"] = schema['RequestDate']
    doc["type"] = "reqmgr_request"
    # additional field
    doc["inputdataset"] = schema.get('InputDataset', "")
    # additional field for Analysis work
    doc["vo_group"] = schema.get('VoGroup', "")
    doc["vo_role"] = schema.get('VoRole', "")
    doc["user_dn"] = schema.get('RequestorDN', "")
    doc["async_dest"] = schema.get('asyncDest', "")
    doc["dbs_url"] = schema.get("DbsUrl", "")
    doc["publish_dbs_url"] = schema.get("PublishDbsUrl", "")
    # team name is not yet available need to be updated in assign status
    #doc['team'] = schema['team']
    """
    docs = []
    for i in range(number):
        doc = {"RequestName": "test_workflow_%s" % i,
               "InputDataset": "/Photon/Run2011A-v1/RAW",
               "Group": "cmsdataops",
               "RequestDate": [2012, 1, 11, 17, 49, 50],
               "Campaign": "SryuTest-%s" % (i % 5),
               "RequestPriority": "1",
               "Requestor": "cmsdataops",
               "RequestType": "ReReco"
               #"site_white_list": ["T1_DE_KIT"],
               #"team": "cmsdataops"
                }
        docs.append(doc)
    return docs


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

    #TODO: Make more realistic
    docs = []
    statusList = ['new', 'created', 'executing', 'complete', 'createfailed', 'submitfailed',
     'jobfailed', 'createcooloff',  'submitcooloff', 'jobcooloff', 'success',
     'exhausted', 'killed']

    for i in range(number):
        status = statusList[random.randint(0, len(statusList)-1)]
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
                  "retrycount": random.randint(0,5),
                  "workflow": request,
                  "task": "/%s/task_%s" % (request, i),
                  "state": status,
                  "site": "T1_US_FNAL",
                  "exitcode": exitCode,
                  "errors": errmsgs,
                  "lumis": [[123, 124], [567, 879]],
                  "output": [ {'type': "test-type",
                               'lfn': "/somewhere/file.root",
                               'location': ['T1_US_FNAL'],
                               'checksums': {'adler32': 'abc123', 'cksum': 'cdf123'},
                               'size': "1000" }  ]
            }
        docs.append(jobSummary)
    return docs

sample_request_info = {'AcquisitionEra': 'Integ_Test',
 'AgentJobInfo': {'vocms008.cern.ch:9999': {'_id': '1444db7834fdedaf68fbf7498330adab',
                                            '_rev': '1-82d7cef10f3df98b481609152b048dd5',
                                            'agent': 'WMAgent',
                                            'agent_team': 'testbed-dev',
                                            'agent_url': 'vocms008.cern.ch:9999',
                                            'sites': {'T1_US_FNAL': {'submitted': {'first': 35,
                                                                                   'running': 43}},
                                                      'T2_CH_CERN': {'submitted': {'first': 19,
                                                                                   'running': 19}},
                                                      'T2_CH_CERN_HLT': {'submitted': {'first': 46,
                                                                                       'pending': 34,
                                                                                       'running': 4}}},
                                            'status': {'inWMBS': 100,
                                                       'submitted': {'first': 100,
                                                                     'pending': 34,
                                                                     'running': 66}},
                                            'tasks': {'/sryu_MonteCarloFromGEN_wq_testt_160220_214518_7489/MonteCarloFromGEN': {'jobtype': 'Production',
                                                                                                                                'sites': {'T1_US_FNAL': {'submitted': {'first': 35,
                                                                                                                                                                       'running': 43}},
                                                                                                                                          'T2_CH_CERN': {'submitted': {'first': 19,
                                                                                                                                                                       'running': 19}},
                                                                                                                                          'T2_CH_CERN_HLT': {'submitted': {'first': 46,
                                                                                                                                                                           'pending': 34,
                                                                                                                                                                           'running': 4}}},
                                                                                                                                'status': {'submitted': {'first': 100,
                                                                                                                                                         'pending': 34,
                                                                                                                                                         'running': 66}},
                                                                                                                                'subscription_status': {'finished': 0,
                                                                                                                                                        'open': 1,
                                                                                                                                                        'total': 1,
                                                                                                                                                        'updated': 1456032349}},
                                                      '/sryu_MonteCarloFromGEN_wq_testt_160220_214518_7489/MonteCarloFromGEN/LogCollect': {'jobtype': 'LogCollect',
                                                                                                                                           'subscription_status': {'finished': 0,
                                                                                                                                                                   'open': 1,
                                                                                                                                                                   'total': 1,
                                                                                                                                                                   'updated': 1456032349}},
                                                      '/sryu_MonteCarloFromGEN_wq_testt_160220_214518_7489/MonteCarloFromGEN/MonteCarloFromGENCleanupUnmergedRAWSIMoutput': {'jobtype': 'Cleanup',
                                                                                                                                                                             'subscription_status': {'finished': 0,
                                                                                                                                                                                                     'open': 1,
                                                                                                                                                                                                     'total': 1,
                                                                                                                                                                                                     'updated': 1456032349}},
                                                      '/sryu_MonteCarloFromGEN_wq_testt_160220_214518_7489/MonteCarloFromGEN/MonteCarloFromGENMergeRAWSIMoutput': {'jobtype': 'Merge',
                                                                                                                                                                   'subscription_status': {'finished': 0,
                                                                                                                                                                                           'open': 1,
                                                                                                                                                                                           'total': 1,
                                                                                                                                                                                           'updated': 1456032349}},
                                                      '/sryu_MonteCarloFromGEN_wq_testt_160220_214518_7489/MonteCarloFromGEN/MonteCarloFromGENMergeRAWSIMoutput/MonteCarloFromGENRAWSIMoutputMergeLogCollect': {'jobtype': 'LogCollect',
                                                                                                                                                                                                                'subscription_status': {'finished': 0,
                                                                                                                                                                                                                                        'open': 1,
                                                                                                                                                                                                                                        'total': 1,
                                                                                                                                                                                                                                        'updated': 1456032349}}},
                                            'timestamp': 1456033921,
                                            'type': 'agent_request',
                                            'workflow': 'sryu_MonteCarloFromGEN_wq_testt_160220_214518_7489'}},
 'AutoApproveSubscriptionSites': [],
 'BlockBlacklist': [],
 'BlockCloseMaxEvents': 20000000,
 'BlockCloseMaxFiles': 500,
 'BlockCloseMaxSize': 5000000000000,
 'BlockCloseMaxWaitTime': 14400,
 'BlockWhitelist': [],
 'CMSSWVersion': 'CMSSW_5_3_19',
 'Campaign': 'reqmgr2-validation',
 'Comments': 'MCFromGEN LumiBased splitting with 1l per job. Half an hour opened',
 'ConfigCacheID': '1ad063a0d73c1d81143b4182cbf84793',
 'ConfigCacheUrl': 'https://cmsweb.cern.ch/couchdb',
 'CouchDBName': 'reqmgr_config_cache',
 'CouchURL': 'https://reqmgr2-dev.cern.ch/couchdb',
 'CouchWorkloadDBName': 'reqmgr_workload_cache',
 'CustodialSites': [],
 'CustodialSubType': 'Move',
 'DN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sryu/CN=676041/CN=Seang Chan Ryu',
 'Dashboard': 'integration',
 'DbsUrl': 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader',
 'FilterEfficiency': 1,
 'GlobalTag': 'START53_V7C::All',
 'GracePeriod': 300,
 'Group': 'DATAOPS',
 'HardTimeout': 129900,
 'InputDataset': '/QDTojWinc_NC_M-1200_TuneZ2star_8TeV-madgraph/Summer12pLHE-DMWM_Validation_DONOTDELETE_Alan_TEST-v1/GEN',
 'LumisPerJob': 1,
 'MaxMergeEvents': 50000,
 'MaxMergeSize': 4294967296,
 'Memory': 2300,
 'MergedLFNBase': '/store/backfill/1',
 'MinMergeSize': 2147483648,
 'NonCustodialSites': [],
 'OpenRunningTimeout': 1800,
 'OutputDatasets': ['/QDTojWinc_NC_M-1200_TuneZ2star_8TeV-madgraph/Summer12-START53_V7C-v2/GEN-SIM'],
 'PrepID': 'B2G-Summer12-00736',
 'PrimaryDataset': 'QDTojWinc_NC_M-1200_TuneZ2star_8TeV-madgraph',
 'ProcessingString': 'MonteCarloFromGEN_reqmgr2-test',
 'ProcessingVersion': 1,
 'RequestDate': [2016, 2, 20, 20, 45, 18],
 'RequestName': 'sryu_MonteCarloFromGEN_wq_testt_160220_214518_7489',
 'RequestNumEvents': 50000,
 'RequestPriority': 90000,
 'RequestStatus': 'running-closed',
 'RequestString': 'MonteCarloFromGEN_wq_testt',
 'RequestTransition': [{'DN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sryu/CN=676041/CN=Seang Chan Ryu',
                        'Status': 'new',
                        'UpdateTime': 1456001118},
                       {'DN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sryu/CN=676041/CN=Seang Chan Ryu',
                        'Status': 'assignment-approved',
                        'UpdateTime': 1456001121},
                       {'DN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sryu/CN=676041/CN=Seang Chan Ryu',
                        'Status': 'assigned',
                        'UpdateTime': 1456001121},
                       {'DN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sryu/CN=676041/CN=Seang Chan Ryu',
                        'Status': 'acquired',
                        'UpdateTime': 1456002499},
                       {'DN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sryu/CN=676041/CN=Seang Chan Ryu',
                        'Status': 'running-open',
                        'UpdateTime': 1456033504},
                       {'DN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sryu/CN=676041/CN=Seang Chan Ryu',
                        'Status': 'running-closed',
                        'UpdateTime': 1456033517}],
 'RequestType': 'MonteCarloFromGEN',
 'RequestWorkflow': 'https://reqmgr2-dev.cern.ch/couchdb/reqmgr_workload_cache/sryu_MonteCarloFromGEN_wq_testt_160220_214518_7489/spec',
 'Requestor': 'sryu',
 'RequestorDN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sryu/CN=676041/CN=Seang Chan Ryu',
 'RunBlacklist': [],
 'RunWhitelist': [],
 'ScramArch': 'slc6_amd64_gcc472',
 'SiteBlacklist': [],
 'SiteWhitelist': ['T1_US_FNAL', 'T2_CH_CERN'],
 'SizePerEvent': 1154,
 'SoftTimeout': 129600,
 'SplittingAlgo': 'LumiBased',
 'SubscriptionPriority': 'Low',
 'Teams': ['testbed-dev'],
 'TimePerEvent': 16.87,
 'TotalEstimatedJobs': 100,
 'TotalInputEvents': 2500,
 'TotalInputFiles': 1,
 'TotalInputLumis': 100,
 'TotalTime': 28800,
 'UnmergedLFNBase': '/store/unmerged',
 '_id': 'sryu_MonteCarloFromGEN_wq_testt_160220_214518_7489'}

sample_complete = {'AcquisitionEra': 'Integ_Test',
 'AgentJobInfo': {'vocms008.cern.ch:9999': {'_id': '1444db7834fdedaf68fbf7498394376c',
                                            '_rev': '1-0250fbe480c56e57b2c939ff491aab8b',
                                            'agent': 'WMAgent',
                                            'agent_team': 'testbed-dev',
                                            'agent_url': 'vocms008.cern.ch:9999',
                                            'sites': {'T2_CH_CERN_HLT': {'failure': {'exception': 1},
                                                                         'success': 1}},
                                            'status': {'failure': {'exception': 1},
                                                       'success': 1},
                                            'tasks': {'/sryu_DQMHarvesting_wq_testt_160220_214501_5235/EndOfRunDQMHarvest': {'sites': {'T2_CH_CERN_HLT': {'failure': {'exception': 1}}},
                                                                                                                             'status': {'failure': {'exception': 1}}},
                                                      '/sryu_DQMHarvesting_wq_testt_160220_214501_5235/EndOfRunDQMHarvest/EndOfRunDQMHarvestLogCollect': {'sites': {'T2_CH_CERN': {'cmsRunCPUPerformance': {'totalEventCPU': 0,
                                                                                                                                                                                                            'totalJobCPU': 0,
                                                                                                                                                                                                            'totalJobTime': 0},
                                                                                                                                                                                   'dataset': {},
                                                                                                                                                                                   'inputEvents': 0,
                                                                                                                                                                                   'wrappedTotalJobTime': 18},
                                                                                                                                                                    'T2_CH_CERN_HLT': {'success': 1}},
                                                                                                                                                          'status': {'success': 1}}},
                                            'timestamp': 1456037523,
                                            'type': 'agent_request',
                                            'workflow': 'sryu_DQMHarvesting_wq_testt_160220_214501_5235'}},
 'AutoApproveSubscriptionSites': [],
 'BlockBlacklist': [],
 'BlockCloseMaxEvents': 250000000,
 'BlockCloseMaxFiles': 500,
 'BlockCloseMaxSize': 5000000000000,
 'BlockCloseMaxWaitTime': 72000,
 'CMSSWVersion': 'CMSSW_7_3_1_patch1',
 'Campaign': 'reqmgr2-validation',
 'Comments': 'DQMHarvest spec with multiRun DQMHarvestUnit',
 'ConfigCacheUrl': 'https://cmsweb.cern.ch/couchdb',
 'CouchDBName': 'reqmgr_config_cache',
 'CouchURL': 'https://reqmgr2-dev.cern.ch/couchdb',
 'CouchWorkloadDBName': 'reqmgr_workload_cache',
 'CustodialSites': [],
 'CustodialSubType': 'Move',
 'DN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sryu/CN=676041/CN=Seang Chan Ryu',
 'DQMConfigCacheID': 'e554f238a2ee8bb969248a343f6195d1',
 'DQMHarvestUnit': 'multiRun',
 'DQMUploadUrl': 'https://cmsweb-testbed.cern.ch/dqm/dev',
 'Dashboard': 'integration',
 'DbsUrl': 'https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader',
 'GlobalTag': 'GR_R_73_V0A',
 'GracePeriod': 1000,
 'Group': 'DATAOPS',
 'HardTimeout': 130600,
 'InputDataset': '/MinimumBias/CMSSW_7_3_1_patch1-GR_R_73_V0A_RelVal_run2010A-v1/DQMIO',
 'MaxMergeEvents': 100000,
 'MaxMergeSize': 4294967296,
 'Memory': 2200,
 'MergedLFNBase': '/store/relval',
 'MinMergeSize': 2147483648,
 'NonCustodialSites': [],
 'OutputDatasets': [],
 'ProcessingString': 'DQMHarvesting_reqmgr2-test',
 'ProcessingVersion': 1,
 'RequestDate': [2016, 2, 20, 20, 45, 1],
 'RequestName': 'sryu_DQMHarvesting_wq_testt_160220_214501_5235',
 'RequestPriority': 140000,
 'RequestStatus': 'completed',
 'RequestString': 'DQMHarvesting_wq_testt',
 'RequestTransition': [{'DN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sryu/CN=676041/CN=Seang Chan Ryu',
                        'Status': 'new',
                        'UpdateTime': 1456001101},
                       {'DN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sryu/CN=676041/CN=Seang Chan Ryu',
                        'Status': 'assignment-approved',
                        'UpdateTime': 1456001106},
                       {'DN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sryu/CN=676041/CN=Seang Chan Ryu',
                        'Status': 'assigned',
                        'UpdateTime': 1456001106},
                       {'DN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sryu/CN=676041/CN=Seang Chan Ryu',
                        'Status': 'acquired',
                        'UpdateTime': 1456002496},
                       {'DN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sryu/CN=676041/CN=Seang Chan Ryu',
                        'Status': 'running-open',
                        'UpdateTime': 1456033516},
                       {'DN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sryu/CN=676041/CN=Seang Chan Ryu',
                        'Status': 'running-closed',
                        'UpdateTime': 1456033519},
                       {'DN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sryu/CN=676041/CN=Seang Chan Ryu',
                        'Status': 'completed',
                        'UpdateTime': 1456035903}],
 'RequestType': 'DQMHarvest',
 'RequestWorkflow': 'https://reqmgr2-dev.cern.ch/couchdb/reqmgr_workload_cache/sryu_DQMHarvesting_wq_testt_160220_214501_5235/spec',
 'Requestor': 'sryu',
 'RequestorDN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sryu/CN=676041/CN=Seang Chan Ryu',
 'RunWhitelist': [138937,
                  138934,
                  138924,
                  138923,
                  139790,
                  139789,
                  139788,
                  139787,
                  144086,
                  144085,
                  144084,
                  144083,
                  144011],
 'ScramArch': 'slc6_amd64_gcc491',
 'SiteBlacklist': [],
 'SiteWhitelist': ['T1_US_FNAL', 'T2_CH_CERN'],
 'SizePerEvent': 1234,
 'SoftTimeout': 129600,
 'SubscriptionPriority': 'Low',
 'Teams': ['testbed-dev'],
 'TimePerEvent': 2,
 'TotalEstimatedJobs': 1,
 'TotalInputEvents': 0,
 'TotalInputFiles': 2,
 'TotalInputLumis': 21,
 'UnmergedLFNBase': '/store/unmerged',
 '_id': 'sryu_DQMHarvesting_wq_testt_160220_214501_5235',
 'mergedLFNBase': '/store/relval',
 'unmergedLFNBase': '/store/unmerged'}
