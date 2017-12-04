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
from Utils.CertTools import getKeyCertFromEnv
from WMCore.Services.pycurl_manager import RequestHandler
from WMCore.Services.pycurl_manager import getdata as multi_getdata

# static variables
STEP_PAT = re.compile(r'Step[0-9]')
TASK_PAT = re.compile(r'Task[0-9]')


class UnifiedConfiguration(object):
    "UnifiedConfiguration class provides access to Unified configuration parameters"
    def __init__(self):
        fname = '/'.join(__file__.split('/')[:-1] + ['config.json'])
        fname = os.getenv('UNIFIED_CONFIG_JSON', fname)
        self.configs = json.loads(open(fname).read())

    def get(self, parameter, default=None):
        "Return parameter from unified configuration"
        if parameter in self.configs:
            return self.configs[parameter]
        return default

# static variables used in Unified modules
uConfig = UnifiedConfiguration()

def dbsInfo(datasets):
    "Provides DBS info about dataset blocks"
    urls = ['%s/blocks?detail=True&dataset=%s' % (dbsUrl(), d) for d in datasets]
    data = multi_getdata(urls, ckey(), cert())
    datasetBlocks = {}
    datasetSizes = {}
#     nblocks = 0
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
#         nblocks += len(blocks)
#     tot_size = 0
#     for dataset, blocks in datasetBlocks.iteritems():
#         tot_size += datasetSizes[dataset]
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

def getWorkflow(requestName):
    "Get list of workflow info from ReqMgr2 data-service for given request name"
    headers = {'Accept': 'application/json'}
    params = {}
    url = '%s/data/request/%s' % (reqmgrUrl(), requestName)
    mgr = RequestHandler()
    res = mgr.getdata(url, params=params, headers=headers, ckey=ckey(), cert=cert())
    data = json.loads(res)
    return data.get('result', [])

def getWorkflows(state):
    "Get list of workflows from ReqMgr2 data-service"
    url = '%s/data/request' % reqmgrUrl()
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
    eventsLumis = {}
    if not inputs:
        return eventsLumis
    if '#' in inputs[0]: # inputs are list of blocks
        what = 'block_name'
    urls = ['%s/filesummaries?validFileOnly=%s&sumOverLumi=%s&%s=%s' \
            % (dbsUrl(), validFileOnly, sumOverLumi, what, urllib.quote(i)) \
            for i in inputs]
    data = multi_getdata(urls, ckey(), cert())
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
        missingBlocks = [b for b in blocks if b not in eventsLumis]
        if missingBlocks:
            eLumis = eventsLumisInfo(missingBlocks)
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
    data = eLumis.get(dataset, {'num_event':0, 'num_lumi':0})
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
        carryOn = {}
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
                    nevts = carryOn[task['Input%s' % base]]
                elif 'RequestNumEvents' in task:
                    nevts = float(task['RequestNumEvents'])
                else:
                    print("this is not supported, making it zero cput")
                    nevts = 0
                tpe = task.get('TimePerEvent', 1)
                carryOn[task['%sName' % base]] = nevts
                if 'FilterEfficiency' in task:
                    carryOn[task['%sName' % base]] *= task['FilterEfficiency']
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
    pair = getKeyCertFromEnv()
    return pair[0]

def cert():
    "Return user CA cert either from proxy or usercert.pem"
    pair = getKeyCertFromEnv()
    return pair[1]

def stucktransferUrl():
    "Return stucktransfer url"
    default = 'https://cms-stucktransfers.web.cern.ch/cms-stucktransfers'
    return uConfig.get('stucktransferUrl', default)

def dashboardUrl():
    "Return dashboard url"
    default = 'http://dashb-ssb.cern.ch/dashboard/request.py'
    return uConfig.get('dashboardUrl', default)

def monitoringUrl():
    "Return monitoring url"
    default = 'http://cmsmonitoring.web.cern.ch/cmsmonitoring'
    return uConfig.get('monitoringUrl', default)

def dbsUrl():
    "Return DBS URL"
    default = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
    return uConfig.get('dbsUrl', default)

def reqmgrUrl():
    "Return ReqMgr2 url"
    default = 'https://cmsweb.cern.ch/reqmgr2'
    return uConfig.get('reqmgrUrl', default)

def reqmgrCacheUrl():
    "Return ReqMgr cache url"
    default = 'https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache'
    return uConfig.get('reqmgrCacheUrl', default)

def phedexUrl():
    "Return PhEDEx url"
    default = "https://cmsweb.cern.ch/phedex/datasvc/json/prod"
    return uConfig.get('phedexUrl', default)

def ssbUrl():
    "Return Dashboard SSB url"
    default = "http://dashb-ssb.cern.ch/dashboard/request.py"
    return uConfig.get('ssbUrl', default)

def agentInfoUrl():
    "Return agent info url"
    default = 'https://cmsweb.cern.ch/couchdb/wmstats/_design/WMStats/_view/agentInfo?stale=update_after'
    return uConfig.get('agentInfoUrl', default)

def mcoreUrl():
    "Return mcore url"
    default = "http://cmsgwms-frontend-global.cern.ch/vofrontend/stage/mcore_siteinfo.json"
    return uConfig.get('mcoreUrl', default)

def workqueueUrl():
    "Return WorkQueue url "
    default = 'https://cmsweb.cern.ch/couchdb/workqueue'
    return uConfig.get('workqueueUrl', default)

def workqueueView(view, kwds=None):
    "Return WorkQueue view url"
    if not kwds:
        kwds = {'group': True, 'reduce': True}
    keys = sorted(kwds.keys())
    args = '&'.join(['%s=%s' % (k, json.dumps(kwds[k])) for k in keys])
    url = '%s/_design/WorkQueue/_view/%s?%s' % (workqueueUrl(), view, args)
    return url

def elapsedTime(time0, msg='Elapsed time', ndigits=1):
    "Helper function to print elapsed time"
    print("%s: %s sec" % (msg, round(time.time()-time0, ndigits)))

def getNodesForId(phedexid):
    "Helper function to get nodes for given phedex id"
    url = '%s/requestlist' % phedexUrl()
    params = {'request': str(phedexid)}
    headers = {'Accept': 'application/json'}
    mgr = RequestHandler()
    data = mgr.getdata(url, params, headers, ckey=ckey(), cert=cert())
    items = json.loads(data)['phedex']['request']
    nodes = [n['name'] for i in items for n in i['node']]
    return list(set(nodes))

def alterSubscription(phedexid, decision, comments, nodes=None):
    "Helper function to alter subscriptions for given phedex id and nodes"
    mgr = RequestHandler()
    headers = {'Accept': 'application/json'}
    nodes = nodes if nodes else getNodesForId(phedexid)
    params = {
        'decision': decision,
        'request': phedexid,
        'node': ','.join(nodes),
        'comments': comments
        }
    url = '%s/updaterequest'
    data = mgr.getdata(url, params, headers, ckey=ckey(), cert=cert(), verb='POST')
    result = json.loads(data)
    if not result:
        return False
    if 'already' in result:
        return True
    return result

def approveSubscription(phedexid, nodes=None, comments=''):
    "Helper function to alter subscriptions for given phedex id and nodes"
    if not comments:
        comments = 'auto-approve subscription phedexid=%s' % phedexid
    return alterSubscription(phedexid, 'approve', comments, nodes)

def disapproveSubscription(phedexid, nodes=None, comments=''):
    "Helper function to disapprove subscription for given phedex id and nodes"
    if not comments:
        comments = 'auto-disapprove subscription phedexid=%s' % phedexid
    return alterSubscription(phedexid, 'disapprove', comments, nodes)

def getRequest(url, params):
    "Helper function to GET data from given URL"
    mgr = RequestHandler()
    headers = {'Accept': 'application/json'}
    verbose = 0
    if 'verbose' in params:
        verbose = params['verbose']
        del params['verbose']
    data = mgr.getdata(url, params, headers, ckey=ckey(), cert=cert(), verbose=verbose)
    return data

def postRequest(url, params):
    "Helper function to POST request to given URL"
    mgr = RequestHandler()
    headers = {'Accept': 'application/json'}
    verbose = 0
    if 'verbose' in params:
        verbose = params['verbose']
        del params['verbose']
    data = mgr.getdata(url, params, headers, ckey=ckey(), cert=cert(), \
            verb='POST', verbose=verbose)
    return data
