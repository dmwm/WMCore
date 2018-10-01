#!/usr/bin/env python
from __future__ import print_function

import argparse
import httplib
import json
import os
import pwd
import sys
import urllib
import urllib2
from pprint import pprint
from urllib2 import HTTPError, URLError

# table parameters
SEPARATELINE = "|" + "-" * 51 + "|"
SPLITLINE = "|" + "*" * 51 + "|"

# ID for the User-Agent
CLIENT_ID = 'validate-test-wfs/1.2::python/%s.%s' % sys.version_info[:2]

# Cached DQMGui data
cachedDqmgui = None


class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    """
    Basic HTTPS class
    """

    def __init__(self, key, cert):
        urllib2.HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        # Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.getConnection, req)

    def getConnection(self, host, timeout=290):
        return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)


def getX509():
    "Helper function to get x509 from env or tmp file"
    proxy = os.environ.get('X509_USER_PROXY', '')
    if not proxy:
        proxy = '/tmp/x509up_u%s' % pwd.getpwuid(os.getuid()).pw_uid
        if not os.path.isfile(proxy):
            return ''
    return proxy


def getContent(url, params=None):
    cert = getX509()
    client = '%s (%s)' % (CLIENT_ID, os.environ.get('USER', ''))
    handler = HTTPSClientAuthHandler(cert, cert)
    opener = urllib2.build_opener(handler)
    opener.addheaders = [("User-Agent", client),
                         ("Accept", "application/json")]
    try:
        response = opener.open(url, params)
        output = response.read()
    except HTTPError as e:
        print("The server couldn't fulfill the request at %s" % url)
        print("Error code: ", e.code)
        output = '{}'
        # sys.exit(1)
    except URLError as e:
        print('Failed to reach server at %s' % url)
        print('Reason: ', e.reason)
        sys.exit(2)
    return output


def getCouchSummary(reqName, baseUrl):
    """
    Queries couchdb wmstats database for the workloadSummary
    """
    couchOutput = {}
    couchWorkl = baseUrl + "/couchdb/workloadsummary/" + reqName
    couchOutput["workflow_summary"] = json.loads(getContent(couchWorkl))
    if not couchOutput["workflow_summary"]:
        # then just add the output key to get it going downstream
        couchOutput["workflow_summary"]["output"] = None
    return couchOutput


def getReqMgrCache(reqName, baseUrl):
    """
    Queries Request Manager database for the spec file and a few other stuff
    """
    urn = baseUrl + "/reqmgr2/data/request/" + reqName
    # reqmgr2 returns a different format
    reqmgrOutput = json.loads(getContent(urn))['result'][0][reqName]
    return reqmgrOutput


def getPhedexInfo(dataset, baseUrl):
    """
    Queries blockreplicas PhEDEx API to retrieve general information needed
    """
    phedexOutput = {}
    queryParams = urllib.urlencode({'dataset': dataset})
    phedexUrl = baseUrl + "/phedex/datasvc/json/prod/" + "blockreplicas"
    phedexOutput = json.loads(getContent(phedexUrl, queryParams))
    return phedexOutput["phedex"]["block"]


def getDbsInfo(dataset, baseUrl):
    """
    Queries 3 DBS APIs to get all general information needed
    """
    if 'testbed' in baseUrl:
        dbsUrl = baseUrl + "/dbs/int/global/DBSReader/"
    else:
        dbsUrl = baseUrl + "/dbs/prod/global/DBSReader/"

    dbsOutput = {}
    dbsOutput[dataset] = {"blockorigin": [], "filesummaries": [], "outputconfigs": []}
    for api in dbsOutput[dataset]:
        fullUrl = dbsUrl + api + "?" + urllib.urlencode({'dataset': dataset})
        data = json.loads(getContent(fullUrl))
        dbsOutput[dataset][api] = data

    # Separate query for prep_id, since we want any access_type
    fullUrl = dbsUrl + "datasets?" + urllib.urlencode({'dataset': dataset})
    fullUrl += "&dataset_access_type=*&detail=True"
    data = json.loads(getContent(fullUrl))
    dbsOutput[dataset]["prep_id"] = data[0]["prep_id"] if data else ''
    return dbsOutput


def getDqmInfo(url):
    """
    Fetch all the data from the DQMGui
    """
    global cachedDqmgui

    dqmguiUrl = url + "/data/json/samples"
    # check whether we have this data in cache
    if cachedDqmgui is None:
        cachedDqmgui = {}
    if url not in cachedDqmgui:
        try:
            cachedDqmgui[url] = json.loads(getContent(dqmguiUrl))
        except Exception:
            print("Failed to fetch data from DQM GUI")
            cachedDqmgui[url]['samples'] = []
    return cachedDqmgui[url]['samples']


def harvesting(workload, outDsets):
    """
    Parse the request spec and query DQMGui in case harvesting is enabled
    """
    if workload['RequestType'] == 'DQMHarvest':
        wantedOutput = [workload['InputDataset']]
    elif str(workload.get('EnableHarvesting', 'False')) == 'True':
        wantedOutput = [dset for dset in outDsets if dset.endswith('/DQMIO') or dset.endswith('/DQM')]
    else:
        return

    urls = workload['DQMUploadUrl'].split(';')
    if not wantedOutput:
        print("Well, it's embarassing! Harvesting is enabled but there is nothing to harvest")
        return

    for url in urls:
        print("Harvesting enabled. Querying DQMGui at: %s" % url)
        allFiles = getDqmInfo(url)
        for outDsets in wantedOutput:
            # dirty hack for multirun harvesting, that changes the dataset name up here
            outDsets = outDsets.rsplit('/', 1)[0]
            for sample in allFiles:
                for item in sample['items']:
                    if item['dataset'].startswith(outDsets):
                        print(item)


def compareLists(d1, d2, d3=None, key=None):
    """
    Receives list of data from different sources and compare them.

    In case a key is passed in, then the input is a dictionary and we validate
    the value of that key from different sources.
    """
    outcome = 'NOPE'
    # just to make comparison easier when the third list is not provided
    if not d3:
        d3 = d2

    try:
        if isinstance(d1, list):
            if len(d1) != len(d2) or len(d1) != len(d3):
                return outcome
            if set(d1) ^ set(d2) or set(d1) ^ set(d3):
                return outcome
        elif isinstance(d1, dict):
            if len(d1) != len(d2) or len(d1) != len(d3):
                return outcome
            for dset in d1:
                if d1[dset][key] != d2[dset][key] or d1[dset][key] != d3[dset][key]:
                    return outcome
        else:
            print("Data type is neither a list nor dict: %s" % type(d1))
            return outcome
    except KeyError:
        pass

    return 'ok'


def compareSpecial(d1, d2, key=None):
    """
    Make special comparisons that requires iterating over all blocks and etc
    """
    if not isinstance(d1, dict):
        print("You must provide a dict data type!")

    outcome = 'NOPE'
    try:
        if key == 'lumis':
            for dset in d2:
                if d1[key] != d2[dset][key]:
                    return outcome

        if key == 'PNN':
            for dset in d2:
                for block, value in d2[dset].iteritems():
                    if isinstance(value, dict):
                        if d1[dset][block][key] != d2[dset][block][key]:
                            return outcome
    except KeyError:
        pass

    return 'ok'


def handleReqMgr(reqName, reqmgrUrl):
    """
    Query ReqMgr and performs all the processing and dirty keys
    manipulation. In summary:
      1. It gathers the input dataset information
      2. It gathers the list of output datasets according to reqmgr API
      3. If harvesting is enabled, it looks up for the files in the DQM server
      4. It also calculates the workflow runtime (completed - assigned time)

    Returns two dictionaries:
      - (dict) reqmgrInputDset: contains information about the input data
      - (list) reqmgrOutDsets:  contains a list of the output datasets
    """
    reqmgrOut = getReqMgrCache(reqName, reqmgrUrl)

    if reqmgrOut['RequestStatus'] not in ['completed', 'closed-out', 'announced']:
        print("We cannot validate wfs in this state: %s\n" % reqmgrOut['RequestStatus'])
        return (None, None)

    reqmgrInputDset = {}
    try:
        reqmgrInputDset = {'TotalEstimatedJobs': reqmgrOut['TotalEstimatedJobs'],
                           'TotalInputEvents': reqmgrOut['TotalInputEvents'],
                           'TotalInputLumis': reqmgrOut['TotalInputLumis'],
                           'TotalInputFiles': reqmgrOut['TotalInputFiles'],
                           'lumis': reqmgrOut['TotalInputLumis']}  # this lumis is needed for comparison
    except KeyError:
        raise AttributeError("Total* parameters not found in reqmgr_workload_cache database")

    # calculates workflow runtime
    startTime, endTime = 0, 0
    for entry in reqmgrOut['RequestTransition']:
        if entry['Status'] == 'assigned':
            startTime = entry['UpdateTime']
        elif entry['Status'] == 'completed':
            endTime = entry['UpdateTime']
    reqmgrInputDset['Runtime'] = (endTime - startTime) / 3600.  # result in hours

    reqmgrInputDset['InputDataset'] = reqmgrOut['InputDataset'] if 'InputDataset' in reqmgrOut else ''
    if 'Task1' in reqmgrOut and 'InputDataset' in reqmgrOut['Task1']:
        reqmgrInputDset['InputDataset'] = reqmgrOut['Task1']['InputDataset']
    elif 'Step1' in reqmgrOut and 'InputDataset' in reqmgrOut['Step1']:
        reqmgrInputDset['InputDataset'] = reqmgrOut['Step1']['InputDataset']

    reqmgrInputDset['RequestNumEvents'] = reqmgrOut['RequestNumEvents'] if 'RequestNumEvents' in reqmgrOut else ''
    if 'Task1' in reqmgrOut and 'RequestNumEvents' in reqmgrOut['Task1']:
        reqmgrInputDset['RequestNumEvents'] = reqmgrOut['Task1']['RequestNumEvents']
    elif 'Step1' in reqmgrOut and 'RequestNumEvents' in reqmgrOut['Step1']:
        reqmgrInputDset['RequestNumEvents'] = reqmgrOut['Step1']['RequestNumEvents']

    reqmgrOutDsets = reqmgrOut['OutputDatasets']

    ### Handle new StepChain/TaskChain output parentage map
    if reqmgrOut['RequestType'] in ('StepChain', 'TaskChain'):
        chainMap = reqmgrOut.get('ChainParentageMap', {})
        if chainMap:
            chainMap = [dset for k, v in chainMap.items() for dset in v['ChildDsets']]
            if set(chainMap) != set(reqmgrOut['OutputDatasets']):
                print("ERROR: list of output datasets doesn't match the chain parentage map")
        else:
            print("WARNING: StepChain/TaskChain workflow without a 'ChainParentageMap' argument!")

    ### Handle harvesting case
    print(" - Comments: %s" % reqmgrOut.get('Comments', ''))
    harvesting(reqmgrOut, reqmgrOutDsets)
    if reqmgrOut['RequestType'] == 'DQMHarvest':
        print("There is nothing else that we can validate here...\n")
        return (None, None)
    return reqmgrInputDset, reqmgrOutDsets


def handleCouch(reqName, reqmgrUrl, reqmgrOutDsets):
    """
    Fetch the workloadSummary directly from couch and perform all
    the processing and dirty keys manipulation.

    Returns a dictionary key by dataset name with their information
    """
    couchOutDsets = {}

    # Get workload summary,  output samples, num of files and events
    couchOutput = getCouchSummary(reqName, reqmgrUrl)
    if couchOutput["workflow_summary"]["output"]:
        for dset in reqmgrOutDsets:
            if dset in couchOutput["workflow_summary"]["output"]:
                couchOutDsets.setdefault(dset, {})
                couchOutDsets[dset]['numFiles'] = couchOutput["workflow_summary"]["output"][dset]['nFiles']
                couchOutDsets[dset]['events'] = couchOutput["workflow_summary"]["output"][dset]['events']
                couchOutDsets[dset]['dsetSize'] = couchOutput["workflow_summary"]["output"][dset]["size"]
            else:
                couchOutDsets[dset] = {'events': '', 'numFiles': '', 'dsetSize': ''}
    return couchOutDsets


def handlePhedex(reqmgrOutDsets, cmswebUrl):
    """
    Query PhEDEx and performs all the processing and dirty keys
    manipulation.
    """
    phedexInfo = {}

    for dset in reqmgrOutDsets:
        # Get general phedex info, number of files and size of dataset
        phedexOutput = getPhedexInfo(dset, cmswebUrl)
        phedexInfo.setdefault(dset, {})
        phedexInfo[dset].setdefault('numFiles', 0)
        phedexInfo[dset].setdefault('dsetSize', 0)
        phedexInfo[dset].setdefault('numBlocks', 0)
        for item in phedexOutput:
            phedexInfo[dset]['numFiles'] += item['files']
            phedexInfo[dset]['dsetSize'] += item['bytes']
            phedexInfo[dset]['numBlocks'] += 1
            phedexInfo[dset].setdefault(item['name'], {})
            phedexInfo[dset][item['name']] = {'numFiles': item['files'],
                                              'isOpen': item['is_open'],
                                              'PNN': item['replica'][0]['node'],
                                              'SE': item['replica'][0]['se'],
                                              'custodial': item['replica'][0]['custodial'],
                                              'complete': item['replica'][0]['complete'],
                                              'subscribed': item['replica'][0]['subscribed']}
    return phedexInfo


def handleDBS(reqmgrOutDsets, cmswebUrl):
    """
    Query DBS and performs all the processing and dirty keys
    manipulation.
    """
    dbsInfo = {}

    for dset in reqmgrOutDsets:
        # Get information from 3 DBS Apis
        dbsOutput = getDbsInfo(dset, cmswebUrl)
        dbsInfo.setdefault(dset, {})
        dbsInfo[dset].setdefault('prep_id', dbsOutput[dset]['prep_id'])
        for item in dbsOutput[dset]['outputconfigs']:
            dbsInfo[dset].setdefault('release', item['release_version'])
        for item in dbsOutput[dset]['filesummaries']:
            dbsInfo[dset].setdefault('dsetSize', item['file_size'])
            dbsInfo[dset].setdefault('numBlocks', item['num_block'])
            dbsInfo[dset].setdefault('numFiles', item['num_file'])
            dbsInfo[dset].setdefault('events', item['num_event'])
            dbsInfo[dset].setdefault('lumis', item['num_lumi'])
        for item in dbsOutput[dset]['blockorigin']:
            dbsInfo[dset].setdefault(item['block_name'], {})
            dbsInfo[dset][item['block_name']] = {'PNN': item['origin_site_name'],
                                                 'numFiles': item['file_count'],
                                                 'isOpen': item['open_for_writing']}
    return dbsInfo


def validateAll(reqmgrInputDset, couchInfo, phedexInfo, dbsInfo):
    """
    Now that we have all the necessary information, we can start performing some
    x-checks. Test cases are:
      1. output dataset name in workload_cache, workloadSummary, phedex and dbs
      2. output dataset size in workloadSummary, phedex and dbs
      3. output number of blocks in phedex and dbs
      4. output number of files in workloadSummary, phedex and dbs
      5. output number of events in workloadSummary and dbs
      6. output number of lumis in the workload_cache and dbs
      7. output PNN in phedex and dbs
      8. whether blocks are closed
    """
    print('' + SPLITLINE)
    print('|' + ' ' * 25 + '| CouchDB | PhEDEx | DBS  |')
    print(SEPARATELINE)

    compRes = compareLists(couchInfo.keys(), phedexInfo.keys(), dbsInfo.keys())
    print('| Same dataset name       | {compRes:7s} | {compRes:6s} | {compRes:4s} |'.format(compRes=compRes))

    compRes = compareLists(couchInfo, phedexInfo, dbsInfo, key='dsetSize')
    print('| Same dataset size       | {compRes:7s} | {compRes:6s} | {compRes:4s} | '.format(compRes=compRes))

    compRes = compareLists(phedexInfo, dbsInfo, key='numBlocks')
    print('| Same number of blocks   | %-7s | %-6s | %-4s |' % ('--', compRes, compRes))

    compRes = compareLists(couchInfo, phedexInfo, dbsInfo, key='numFiles')
    print('| Same number of files    | {compRes:7s} | {compRes:6s} | {compRes:4s} | '.format(compRes=compRes))

    compRes = compareLists(couchInfo, dbsInfo, key='events')
    print('| Same number of events   | %-7s | %-6s | %-4s |' % (compRes, '--', compRes))

    compRes = compareSpecial(reqmgrInputDset, dbsInfo, key='lumis')
    print('| Same number of lumis    | %-7s | %-6s | %-4s |' % (compRes, '--', compRes))

    compRes = compareSpecial(phedexInfo, dbsInfo, key='PNN')
    print('| Same PhEDEx Node Name   | %-7s | %-6s | %-4s |' % ('--', compRes, compRes))

    print(SPLITLINE)


def main():
    """
    Requirements: you need to have your proxy and proper x509 environment
     variables set.

    Receive a workflow name in order to fetch the following information:
     - from couchdb: gets the output datasets and spec file
     - from phedex: gets dataset, block, files information
     - from dbs: gets dataset, block and files information
    """
    parser = argparse.ArgumentParser(description="Validate workflow input, output and config")
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-w', '--workflow', help='A single workflow name')
    group.add_argument('-i', '--inputFile', help='Plain text file containing request names (one per line)')
    parser.add_argument('-c', '--cms', help='CMSWEB url to talk to DBS/PhEDEx. E.g: cmsweb.cern.ch')
    parser.add_argument('-r', '--reqmgr', help='Request Manager URL. Example: couch-dev1.cern.ch')
    parser.add_argument('-v', '--verbose', help='Increase output verbosity', action="store_true")
    args = parser.parse_args()

    if args.workflow:
        listRequests = [args.workflow]
    elif args.inputFile:
        with open(args.inputFile, 'r') as f:
            listRequests = [req.rstrip('\n') for req in f.readlines()]
    else:
        parser.error("You must provide either a workflow name or an input file name.")
        sys.exit(3)

    verbose = True if args.verbose else False
    cmswebUrl = "https://" + args.cms if args.cms else "https://cmsweb-testbed.cern.ch"
    reqmgrUrl = "https://" + args.reqmgr if args.reqmgr else "https://cmsweb-testbed.cern.ch"

    for reqName in listRequests:
        print("==> %s" % reqName)
        ### Retrieve and process ReqMgr information
        reqmgrInputDset, reqmgrOutDsets = handleReqMgr(reqName, reqmgrUrl)
        if reqmgrInputDset is None:
            continue

        ### Retrieve and process CouchDB information
        couchInfo = handleCouch(reqName, reqmgrUrl, reqmgrOutDsets)

        ### Retrieve and process PhEDEx information
        phedexInfo = handlePhedex(reqmgrOutDsets, cmswebUrl)

        ### Retrieve and process DBS information
        dbsInfo = handleDBS(reqmgrOutDsets, cmswebUrl)

        # Perform all the possible common validations
        validateAll(reqmgrInputDset, couchInfo, phedexInfo, dbsInfo)

        ### Starts VERBOSE mode for the information retrieved so far
        if verbose:
            print("\n======> Request information from reqmgr2 db: ")
            pprint(reqmgrInputDset)
            print("\n======> ReqMgr2 output dataset info: ")
            pprint(reqmgrOutDsets)
            print("\n======> Couch output dataset info: ")
            pprint(couchInfo)
            print("\n======> DBS info: ")
            pprint(dbsInfo)
            print("\n======> PhEDEx info: ")
            pprint(phedexInfo)
        print("\n")

    sys.exit(0)


if __name__ == "__main__":
    sys.exit(main())
