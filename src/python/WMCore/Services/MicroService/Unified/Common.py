"""
Set of common utilities for Unified service.

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
Original code: https://github.com/CMSCompOps/WmAgentScripts/Unified
"""

# futures
from __future__ import print_function, division

# system modules
import os
import re
import json
import time
import math
import urllib

# py2/py3 modules
# from future import standard_library
# standard_library.install_aliases()

# WMCore modules
from WMCore.Services.pycurl_manager import RequestHandler
from WMCore.Services.pycurl_manager import getdata as multi_getdata

# static variables
STEP_PAT = re.compile(r'Step[0-9]')
TASK_PAT = re.compile(r'Task[0-9]')


class UnifiedConfiguration(object):
    "UnifiedConfiguration class provides access to Unified configuration parameters"
    def __init__(self):
        fname = os.getenv('UNIFIED_CONFIG_JSON', 'unifiedConfiguration.json')
        self.configs = json.loads(open(fname).read())

    def get(self, parameter, default=None):
        "Return parameter from unified configuration"
        if parameter in self.configs:
            return self.configs[parameter]['value']
        return default

# static variables used in Unified modules
uConfig = UnifiedConfiguration()

def dbsInfo(datasets):
    "Provides DBS info about dataset blocks"
    urls = ['%s/blocks?detail=True&dataset=%s' % (dbsUrl(), d) for d in datasets]
    data = multi_getdata(urls, ckey(), cert())
    datasetBlocks = {}
    datasetSizes = {}
    nblocks = 0
    for row in data:
        dataset = row['url'].split('=')[-1]
        rows = json.loads(row['data'])
        blocks = []
        size = 0
        for item in rows:
            blocks.append(item['block_name'])
            size += item['block_size']
        datasetBlocks[dataset] = blocks
        datasetSizes[dataset] = size
        nblocks += len(blocks)
    tot_size = 0
    for dataset, blocks in datasetBlocks.iteritems():
        tot_size += datasetSizes[dataset]
    return datasetBlocks, datasetSizes

def phedexInfo(datasets):
    "Fetch PhEDEx info about nodes for all datasets"
    urls = ['%s/blockreplicasummary?dataset=%s' % (phedexUrl(), d) for d in datasets]
    data = multi_getdata(urls, ckey(), cert())
    blockNodes = {}
    for row in data:
        rows = json.loads(row['data'])
        for item in rows['phedex']['block']:
            nodes = [r['node'] for r in item['replica'] if r['complete'] == 'y']
            blockNodes[item['name']] = nodes
    return blockNodes

def getWorkflows(url, state='assignment-approved'):
    "Get list of workflows from ReqMgr2 data-service"
    headers = {'Accept': 'application/json'}
    params = {'status': state}
    mgr = RequestHandler()
    res = mgr.getdata(url, params=params, headers=headers, ckey=ckey(), cert=cert())
    data = json.loads(res)
    return data.get('result', [])

def workflowsInfo(workflows):
    "Return minimum info about workflows in flat format"
    winfo = {}
    for wflow in workflows:
        for key, val in wflow.iteritems():
            datasets = set()
            pileups = set()
            selist = []
            priority = 0
            campaign = ''
            for kkk, vvv in val.iteritems():
                if STEP_PAT.match(kkk) or TASK_PAT.match(kkk):
                    dataset = vvv.get('InputDataset', '')
                    pileup = vvv.get('MCPileup', '')
                    if dataset:
                        datasets.add(dataset)
                    if pileup:
                        pileups.add(pileup)
                if kkk == 'SiteWhiteList':
                    selist = vvv
                if kkk == 'RequestPriority':
                    priority = vvv
                if kkk == 'Campaign':
                    campaign = vvv
                if kkk == 'InputDataset':
                    datasets.add(vvv)
                if kkk == 'MCPileup':
                    pileups.add(vvv)
            winfo[key] = \
                    dict(datasets=list(datasets), pileups=list(pileups),\
                         priority=priority, selist=selist, campaign=campaign)
    return winfo

def eventsLumisInfo(inputs, validFileOnly=0, sumOverLumi=0):
    "Get information about events and lumis for given set of inputs: blocks or datasets"
    what = 'dataset'
    if '#' in inputs[0]: # inputs are list of blocks
        what = 'block_name'
    urls = ['%s/filesummaries?validFileOnly=%s&sumOverLumi=%s&%s=%s' \
            % (dbsUrl(), validFileOnly, sumOverLumi, what, urllib.quote(i)) \
            for i in inputs]
    data = multi_getdata(urls, ckey(), cert())
    eventsLumis = {}
    for row in data:
        key = row['url'].split('=')[-1]
        if what == 'block_name':
            key = urllib.unquote(key)
        rows = json.loads(row['data'])
        for item in rows:
            eventsLumis[key] = item
    return eventsLumis

def getEventsLumis(dataset, blocks=None, eventsLumis=None):
    "Helper function to return number of events/lumis for given dataset or blocks"
    nevts = nlumis = 0
    if blocks:
        missing_blocks = [b for b in blocks if b not in eventsLumis]
        if missing_blocks:
            eLumis = eventsLumisInfo(missing_blocks)
            eventsLumis.update(eLumis)
        for block in blocks:
            data = eventsLumis[block]
            nevts += data['num_event']
            nlumis += data['num_lumi']
        return nevts, nlumis
    if eventsLumis and dataset in eventsLumis:
        data = eventsLumis[dataset]
        return data['num_event'], data['num_lumi']
    eLumis = eventsLumisInfo([dataset])
    data = eLumis[dataset]
    return data['num_event'], data['num_lumi']

def getComputingTime(workflow, eventsLumis=None, unit='h'):
    "Return computing time per give workflow"
    cput = None

    if 'InputDataset' in workflow:
        dataset = workflow['InputDataset']
        if 'BlockWhitelist' in workflow and workflow['BlockWhitelist']:
            nevts, _ = getEventsLumis(dataset, workflow['BlockWhitelist'], eventsLumis)
        else:
            nevts, _ = getEventsLumis(dataset, eventsLumis=eventsLumis)
        tpe = workflow['TimePerEvent']
        cput = nevts * tpe
    elif 'Chain' in workflow['RequestType']:
        base = workflow['RequestType'].replace('Chain', '')
        itask = 1
        cput = 0
        carry_on = {}
        while True:
            t = '%s%d' % (base, itask)
            itask += 1
            if t in workflow:
                task = workflow[t]
                if 'InputDataset' in task:
                    dataset = task['InputDataset']
                    if 'BlockWhitelist' in task and task['BlockWhitelist']:
                        nevts, _ = getEventsLumis(dataset, task['BlockWhitelist'], eventsLumis)
                    else:
                        nevts, _ = getEventsLumis(dataset, eventsLumis=eventsLumis)
                elif 'Input%s' % base in task:
                    nevts = carry_on[task['Input%s' % base]]
                elif 'RequestNumEvents' in task:
                    nevts = float(task['RequestNumEvents'])
                else:
                    print("this is not supported, making it zero cput")
                    nevts = 0
                tpe = task.get('TimePerEvent', 1)
                carry_on[task['%sName' % base]] = nevts
                if 'FilterEfficiency' in task:
                    carry_on[task['%sName' % base]] *= task['FilterEfficiency']
                cput += tpe * nevts
            else:
                break
    else:
        nevts = float(workflow.get('RequestNumEvents', 0))
        feff = float(workflow.get('FilterEfficiency', 1))
        tpe = workflow.get('TimePerEvent', 1)
        cput = nevts/feff * tpe

    if cput is None:
        return 0

    if unit == 'm':
        cput = cput / (60.)
    if unit == 'h':
        cput = cput / (60.*60.)
    if unit == 'd':
        cput = cput / (60.*60.*24.)
    return cput

def sigmoid(x):
    "Sigmoid function"
    return 1./(1 + math.exp(-x))

def getNCopies(cpuHours, minN=2, maxN=3, weight=50000, constant=100000):
    "Calculate number of copies for given workflow"
    func = sigmoid(-constant/weight)
    fact = (maxN - minN) / (1-func)
    base = (func*maxN - minN)/(func-1)
    return int(base + fact * sigmoid((cpuHours - constant)/weight))

def teraBytes(size):
    "Return size in TB"
    return float(size)/float(1024**4)

def ckey():
    "Return user CA key either from proxy or userkey.pem"
    return os.environ.get('X509_USER_PROXY', \
        os.path.join(os.environ['HOME'], '.globus/userkey.pem'))

def cert():
    "Return user CA cert either from proxy or usercert.pem"
    return os.environ.get('X509_USER_PROXY', \
        os.path.join(os.environ['HOME'], '.globus/usercert.pem'))

def stucktransferUrl():
    "Return stucktransfer url"
    return 'https://cms-stucktransfers.web.cern.ch/cms-stucktransfers'

def dashboardUrl():
    "Return dashboard url"
    return 'http://dashb-ssb.cern.ch/dashboard/request.py'

def monitoringUrl():
    "Return monitoring url"
    return 'http://cmsmonitoring.web.cern.ch/cmsmonitoring'

def dbsUrl():
    "Return DBS URL"
    return 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'

def reqmgrUrl():
    "Return ReqMgr2 url"
    return 'https://cmsweb.cern.ch/reqmgr2'

def reqmgrCacheUrl():
    "Return ReqMgr cache url"
    return 'https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache'

def phedexUrl():
    "Return PhEDEx url"
    return "https://cmsweb.cern.ch/phedex/datasvc/json/prod"

def ssbUrl():
    "Return Dashboard SSB url"
    return "http://dashb-ssb.cern.ch/dashboard/request.py"

def agentInfoUrl():
    "Return agent info url"
    return 'https://cmsweb.cern.ch/couchdb/wmstats/_design/WMStats/_view/agentInfo?stale=update_after'

def mcoreUrl():
    "Return mcore url"
    return "http://cmsgwms-frontend-global.cern.ch/vofrontend/stage/mcore_siteinfo.json"

def elapsedTime(time0, msg='Elapsed time', ndigits=1):
    "Helper function to print elapsed time"
    print("%s: %s sec" % (msg, round(time.time()-time0, ndigits)))

