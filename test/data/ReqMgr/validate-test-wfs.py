#!/usr/bin/env python
from __future__ import print_function
from future.utils import viewitems

import argparse
import getpass
import json
import os
import pwd
import sys
from collections import OrderedDict
from textwrap import TextWrapper

try:
    # python2
    import urllib2
    HTTPError = urllib2.HTTPError
    URLError = urllib2.URLError
    build_opener = urllib2.build_opener
    HTTPSHandler = urllib2.HTTPSHandler
    import urllib
    urlencode = urllib.urlencode
    quote_plus = urllib.quote_plus
    import httplib
    HTTPSConnection = httplib.HTTPSConnection
except:
    # python3
    import urllib.error
    HTTPError = urllib.error.HTTPError, 
    URLError = urllib.error.URLError
    import urllib.request
    build_opener = urllib.request.build_opener
    HTTPSHandler = urllib.request.HTTPSHandler
    import urllib.parse
    urlencode = urllib.parse.urlencode
    quote_plus = urllib.parse.quote_plus
    import http.client
    HTTPSConnection = http.client.HTTPSConnection

# table parameters
SEPARATELINE = "|" + "-" * 51 + "|"
SPLITLINE = "|" + "*" * 51 + "|"

# ID for the User-Agent
CLIENT_ID = 'validate-test-wfs/1.2::python/%s.%s' % sys.version_info[:2]

# Cached DQMGui data
cachedDqmgui = None


class HTTPSClientAuthHandler(HTTPSHandler):
    """
    Basic HTTPS class
    """

    def __init__(self, key, cert):
        HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        # Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.getConnection, req)

    def getConnection(self, host, timeout=290):
        return HTTPSConnection(host, key_file=self.key, cert_file=self.cert)


def getX509():
    "Helper function to get x509 from env or tmp file"
    certFile = os.environ.get('X509_USER_CERT', '')
    keyFile = os.environ.get('X509_USER_KEY', '')
    if certFile and keyFile:
        return certFile, keyFile

    proxy = os.environ.get('X509_USER_PROXY', '')
    if not proxy:
        proxy = '/tmp/x509up_u%s' % pwd.getpwuid(os.getuid()).pw_uid
        if not os.path.isfile(proxy):
            return '', ''
    return proxy, proxy


def getContent(url, params=None, headers=None):
    certFile, keyFile = getX509()
    client = '%s (%s)' % (CLIENT_ID, os.environ.get('USER', ''))
    handler = HTTPSClientAuthHandler(keyFile, certFile)
    opener = build_opener(handler)
    if headers:
        opener.addheaders = headers
    else:
        opener.addheaders = [("User-Agent", client),
                             ("Accept", "application/json")]

    try:
        response = opener.open(url, params)
        if "auth/x509" in url:
            output = response.headers.get('X-Rucio-Auth-Token', '')  # option 1
            # output = response.getheader('X-Rucio-Auth-Token')  # option 2
        else:
            output = response.read()
    except HTTPError as e:
        print("The server couldn't fulfill the request at %s" % url)
        print("Error: {}".format(e))
        output = '{}'
        # sys.exit(1)
    except URLError as e:
        print('Failed to reach server at %s' % url)
        print('Reason: ', e.reason)
        sys.exit(2)
    return output


def getRucioToken(rucioUrl):
    """
    Get a Rucio token for this account
    """
    mapHosts = {"http://cms-rucio-int.cern.ch": "https://cms-rucio-auth-int.cern.ch",
                "http://cms-rucio.cern.ch": "https://cms-rucio-auth.cern.ch"}

    rucioAuth = None
    for hostUrl, authUrl in viewitems(mapHosts):
        if hostUrl == rucioUrl:
            rucioAuth = authUrl
            rucioAcct = getpass.getuser()
    if not rucioAuth:
        print("Failed to parse Rucio URL.")
        sys.exit(10)

    rucioAuth = '%s/auth/x509' % rucioAuth
    token = getContent(rucioAuth, headers=[("X-Rucio-Account", rucioAcct)])
    print("Retrieved token for account: {}, token is: {}".format(rucioAcct, token))
    return token


def getDID(rucioUrl, token, dataId):
    """
    Retrieve basic information for a given data identifier (likely a container)
    """
    headers = [("X-Rucio-Auth-Token", token),
               ("Content-type", "application/json"),
               ("Accept", "application/json")]

    rucioUrl = '{}/dids/cms/{}?dynamic=anything'.format(rucioUrl, dataId)
    data = json.loads(getContent(rucioUrl, headers=headers))
    # DEBUG print("Container data retrieved:\n%s" % data)
    return data


def getBlocks(rucioUrl, token, dataId):
    """
    Retrieve information for a CMS block identifier
    """
    headers = [("X-Rucio-Auth-Token", token)]
    # Server doesn't accept these headers below ..
    #           ("Content-type", "application/json"),
    #           ("Accept", "application/json")]
    rucioUrl = '{}/dids/cms/{}/dids'.format(rucioUrl, dataId)
    data = getContent(rucioUrl, headers=headers)
    # DEBUG print("Block data retrieved:\n%s" % data)
    return data


def getBlockMeta(rucioUrl, token, dataId):
    """
    Retrieve the CMS block metadata (like whether the block is opened or not, etc).
    """
    headers = [("X-Rucio-Auth-Token", token),
               ("Content-type", "application/json"),
               ("Accept", "application/json")]

    rucioUrl = '{}/dids/cms/{}/meta'.format(rucioUrl, quote_plus(dataId))
    data = json.loads(getContent(rucioUrl, headers=headers))
    # DEBUG print("Block meta data retrieved:\n%s" % data)
    return data


def getReplicas(rucioUrl, token, dataId):
    """
    Retrieve all the replicas in a given CMS block identifier
    """
    headers = [("X-Rucio-Auth-Token", token)]
    # Here it accepts, but then it returns the wrong response!!!
    #           ("Content-type", "application/json"),
    #           ("Accept", "application/json")]

    rucioUrl = '{}/replicas/cms/{}/datasets'.format(rucioUrl, quote_plus(dataId))
    data = getContent(rucioUrl, headers=headers)
    # DEBUG print("Replicas data retrieved:\n%s" % data)
    return data


def getDIDRules(rucioUrl, token, dataId):
    """
    Retrieve all the rules for a given data identifier
    """
    headers = [("X-Rucio-Auth-Token", token)]
    # Server doesn't accept these headers below ..
    #           ("Content-type", "application/json"),
    #           ("Accept", "application/json")]

    rucioUrl = '{}/dids/cms/{}/rules'.format(rucioUrl, dataId)
    data = getContent(rucioUrl, headers=headers)
    # DEBUG print("getDIDRules data retrieved:\n%s" % data)
    return data


def reader(stream):
    """
    Home-made function to consume newline delimited json streaming data
    """
    for line in stream.split(b"\n"):
        if line:
            yield json.loads(line)


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
        fullUrl = dbsUrl + api + "?" + urlencode({'dataset': dataset})
        data = json.loads(getContent(fullUrl))
        dbsOutput[dataset][api] = data

    # Separate query for prep_id, since we want any access_type
    fullUrl = dbsUrl + "datasets?" + urlencode({'dataset': dataset})
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
                for block, value in viewitems(d2[dset]):
                    if isinstance(value, dict):
                        if d1[dset][block][key] != d2[dset][block][key]:
                            return outcome
    except KeyError:
        pass

    return 'ok'


def twClosure(replace_whitespace=False,
              break_long_words=False,
              width=120,
              initial_indent=''):
    """
    Deals with indentation of dictionaries with very long key, value pairs.
    replace_whitespace: Replace each whitespace character with a single space.
    break_long_words: If True words longer than width will be broken.
    width: The maximum length of wrapped lines.
    initial_indent: String that will be prepended to the first line of the output

    Wraps all strings for both keys and values to 120 chars.
    Uses 4 spaces indentation for both keys and values.
    Nested dictionaries and lists go to next line.
    """
    twr = TextWrapper(replace_whitespace=replace_whitespace,
                      break_long_words=break_long_words,
                      width=width,
                      initial_indent=initial_indent)

    def twEnclosed(obj, ind='', reCall=False):
        """
        The inner function of the closure
        ind: Initial indentation for the single output string
        reCall: Flag to indicate a recursive call (should not be used outside)
        """
        output = ''
        if isinstance(obj, dict):
            obj = OrderedDict(sorted(obj.items(),
                                     key=lambda t: t[0],
                                     reverse=False))
            if reCall:
                output += '\n'
            ind += '    '
            for key, value in viewitems(obj):
                output += "%s%s: %s" % (ind,
                                        ''.join(twr.wrap(key)),
                                        twEnclosed(value, ind, reCall=True))
        elif isinstance(obj, list):
            if reCall:
                output += '\n'
            ind += '    '
            for value in obj:
                output += "%s%s" % (ind, twEnclosed(value, ind, reCall=True))
        else:
            output += "%s\n" % str(obj)  # join(twr.wrap(str(obj)))
        return output

    return twEnclosed


def twPrint(obj):
    """
    A simple caller of twClosure (see docstring for twClosure)
    """
    twPrinter = twClosure()
    print(twPrinter(obj))


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

    # Handle new StepChain/TaskChain output parentage map
    if reqmgrOut['RequestType'] in ('StepChain', 'TaskChain'):
        chainMap = reqmgrOut.get('ChainParentageMap', {})
        if chainMap:
            chainMap = [dset for k, v in chainMap.items() for dset in v['ChildDsets']]
            if set(chainMap) != set(reqmgrOut['OutputDatasets']):
                print("ERROR: list of output datasets doesn't match the chain parentage map")
        else:
            print("WARNING: StepChain/TaskChain workflow without a 'ChainParentageMap' argument!")

    # Handle harvesting case

    print("----------------------------------------------------\nComments:")
    twPrint(reqmgrOut.get('Comments', ''))
    print("----------------------------------------------------\n")

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


def handleRucio(rucioToken, reqmgrOutDsets, rucioUrl):
    """
    Query Rucio and performs all the processing and dirty keys
    manipulation.
    """
    rucioInfo = {}
    if not rucioToken:
        return rucioInfo

    for dset in reqmgrOutDsets:
        rucioInfo.setdefault(dset, {})
        rucioInfo[dset].setdefault('dsetSize', 0)
        rucioInfo[dset].setdefault('numBlocks', 0)
        rucioInfo[dset].setdefault('numFiles', 0)
        containerData = getDID(rucioUrl, rucioToken, dset)
        if not containerData:
            continue
        rucioInfo[dset]['dsetSize'] = containerData['bytes']
        # FIXME should length be the number of blocks?!?
        # rucioInfo[dset]['numBlocks'] = containerData['length']

        rucioInfo[dset].setdefault('rules', [])
        for rule in reader(getDIDRules(rucioUrl, rucioToken, dset)):
            rucioInfo[dset]['rules'].append(rule['id'])

        for block in reader(getBlocks(rucioUrl, rucioToken, containerData['name'])):
            rucioInfo[dset]['numBlocks'] += 1
            blockMeta = getBlockMeta(rucioUrl, rucioToken, block['name'])
            rucioInfo[dset]['numFiles'] += blockMeta['length']

            rucioInfo[dset].setdefault(block['name'], {})
            rucioInfo[dset][block['name']]['numFiles'] = blockMeta['length']
            rucioInfo[dset][block['name']]['is_open'] = blockMeta['is_open']
            rucioInfo[dset][block['name']]['project'] = blockMeta['project']
            for replica in reader(getReplicas(rucioUrl, rucioToken, block['name'])):
                # Look only at the first replica
                rucioInfo[dset][block['name']]['state'] = replica['state']
                rucioInfo[dset][block['name']]['PNN'] = replica['rse'].replace("_Test", "")

    return rucioInfo


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


def validateAll(reqmgrInputDset, couchInfo, rucioInfo, dbsInfo):
    """
    Now that we have all the necessary information, we can start performing some
    x-checks. Test cases are:
      1. output dataset name in workload_cache, workloadSummary, rucio and dbs
      2. output dataset size in workloadSummary, rucio and dbs
      3. output number of blocks in rucio and dbs
      4. output number of files in workloadSummary, rucio and dbs
      5. output number of events in workloadSummary and dbs
      6. output number of lumis in the workload_cache and dbs
      7. output PNN in rucio and dbs
      8. whether blocks are closed
    """
    print('' + SPLITLINE)
    print('|' + ' ' * 25 + '| CouchDB | Rucio  | DBS  |')
    print(SEPARATELINE)

    compRes = compareLists(list(couchInfo.keys()), list(rucioInfo.keys()), list(dbsInfo.keys()))
    print('| Same dataset name       | {compRes:7s} | {compRes:6s} | {compRes:4s} |'.format(compRes=compRes))

    compRes = compareLists(couchInfo, rucioInfo, dbsInfo, key='dsetSize')
    print('| Same dataset size       | {compRes:7s} | {compRes:6s} | {compRes:4s} | '.format(compRes=compRes))

    compRes = compareLists(rucioInfo, dbsInfo, key='numBlocks')
    print('| Same number of blocks   | %-7s | %-6s | %-4s |' % ('--', compRes, compRes))

    compRes = compareLists(couchInfo, rucioInfo, dbsInfo, key='numFiles')
    print('| Same number of files    | {compRes:7s} | {compRes:6s} | {compRes:4s} | '.format(compRes=compRes))

    compRes = compareLists(couchInfo, dbsInfo, key='events')
    print('| Same number of events   | %-7s | %-6s | %-4s |' % (compRes, '--', compRes))

    compRes = compareSpecial(reqmgrInputDset, dbsInfo, key='lumis')
    print('| Same number of lumis    | %-7s | %-6s | %-4s |' % (compRes, '--', compRes))

    compRes = compareSpecial(rucioInfo, dbsInfo, key='PNN')
    print('| Same RSE                | %-7s | %-6s | %-4s |' % ('--', compRes, compRes))

    print(SPLITLINE)


def main():
    """
    Requirements: you need to have your proxy and proper x509 environment
     variables set.

    Receive a workflow name in order to fetch the following information:
     - from couchdb: gets the output datasets and spec file
     - from rucio: gets dataset, block, files information
     - from dbs: gets dataset, block and files information
    """
    parser = argparse.ArgumentParser(description="Validate workflow input, output and config")
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-w', '--workflow', help='A single workflow name')
    group.add_argument('-i', '--inputFile', help='Plain text file containing request names (one per line)')
    parser.add_argument('-c', '--cms', help='CMSWEB url to talk to DBS. E.g: cmsweb.cern.ch')
    parser.add_argument('-r', '--reqmgr', help='Request Manager URL. Example: couch-dev1.cern.ch')
    parser.add_argument('-v', '--verbose', help='Increase output verbosity', action="store_true")
    parser.add_argument('-x', '--rucio', help='Rucio url', default='cms-rucio-int.cern.ch')
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
    rucioUrl = "http://" + args.rucio if args.rucio else "http://cms-rucio-int.cern.ch"

    rucioToken = getRucioToken(rucioUrl)

    for reqName in listRequests:
        print("\n----------------------------------------------------")
        print("==> %s" % reqName)
        # Retrieve and process ReqMgr information
        reqmgrInputDset, reqmgrOutDsets = handleReqMgr(reqName, reqmgrUrl)
        if reqmgrInputDset is None:
            continue

        # Retrieve and process CouchDB information
        couchInfo = handleCouch(reqName, reqmgrUrl, reqmgrOutDsets)

        # Retrieve and process Rucio information
        rucioInfo = handleRucio(rucioToken, reqmgrOutDsets, rucioUrl)

        # Retrieve and process DBS information
        dbsInfo = handleDBS(reqmgrOutDsets, cmswebUrl)

        # Perform all the possible common validations
        validateAll(reqmgrInputDset, couchInfo, rucioInfo, dbsInfo)

        # Starts VERBOSE mode for the information retrieved so far
        if verbose:
            print("\n======> Request information from reqmgr2 db: ")
            twPrint(reqmgrInputDset)
            print("\n======> ReqMgr2 output dataset info: ")
            twPrint(reqmgrOutDsets)
            print("\n======> Couch output dataset info: ")
            twPrint(couchInfo)
            print("\n======> DBS info: ")
            twPrint(dbsInfo)
            print("\n======> Rucio info: ")
            twPrint(rucioInfo)
        print("\n----------------------------------------------------")

    sys.exit(0)


if __name__ == "__main__":
    sys.exit(main())
