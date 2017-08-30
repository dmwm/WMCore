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
from xml.dom.minidom import getDOMImplementation

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
        fname = '/'.join(__file__.split('/')[:-1] + ['config.json'])
        fname = os.getenv('UNIFIED_CONFIG_JSON', fname)
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
    return os.environ.get('X509_USER_PROXY', \
        os.path.join(os.environ['HOME'], '.globus/userkey.pem'))

def cert():
    "Return user CA cert either from proxy or usercert.pem"
    return os.environ.get('X509_USER_PROXY', \
        os.path.join(os.environ['HOME'], '.globus/usercert.pem'))

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

def elapsedTime(time0, msg='Elapsed time', ndigits=1):
    "Helper function to print elapsed time"
    print("%s: %s sec" % (msg, round(time.time()-time0, ndigits)))

def getSubscriptions(dataset):
    "Helper fuction to get subscriptions for a given dataset"
    mgr = RequestHandler()
    url = '%s/subscriptions' % phedexUrl()
    params = {'dataset': dataset}
    headers = {'Accept': 'application/json'}
    data = mgr.getdata(url, params, headers, ckey=ckey(), cert=cert())
    return json.loads(data)['phedex']

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

def createXML(datasets):
    """
    From a list of datasets return an XML of the datasets in the format required by Phedex
    """
    # Create the minidom document
    impl = getDOMImplementation()
    doc = impl.createDocument(None, "data", None)
    result = doc.createElement("data")
    result.setAttribute('version', '2')
    # Create the <dbs> base element
    dbs = doc.createElement("dbs")
    dbs.setAttribute("name", dbsUrl())
    result.appendChild(dbs)
    #Create each of the <dataset> element
    for datasetname in datasets:
        dataset = doc.createElement("dataset")
        dataset.setAttribute("is-open", "y")
        dataset.setAttribute("is-transient", "y")
        dataset.setAttribute("name", datasetname)
        dbs.appendChild(dataset)
    return result.toprettyxml(indent="  ")

def makeDeleteRequest(site, datasets, comments):
    "Helper function to delete given datasets in Phedex subscription"
    dataXML = createXML(datasets)
    params = {
        "node": site,
        "data": dataXML,
        "level": "dataset",
        "rm_subscriptions": "y",
        #"group": "DataOps",
        #"priority": priority,
        #"request_only":"y" ,
        #"delete":"y",
        "comments": comments
    }
    mgr = RequestHandler()
    url = '%s/delete' % phedexUrl()
    headers = {'Accept': 'application/json'}
    data = mgr.getdata(url, params, headers, ckey=ckey(), cert=cert(), verb='POST')
    return data

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

def makeReplicaRequest(site, datasets, comments, priority='normal', custodial='n',\
        approve=False, mail=True, group="DataOps"):
    "Helper function to make replica request to PhEDEx"
    url = '%s/subscribe' % phedexUrl()
    dataXML = createXML(datasets)
    rOnly = "n" if approve else "y"
    notice = "n" if mail else "y"
    params = {"node": site, "data": dataXML, "group": group, "priority": priority,
              "custodial": custodial, "request_only": rOnly, "move":"n",
              "no_mail": notice, "comments": comments}
    return postRequest(url, params)

def makeMoveRequest(site, datasets, comments, priority='normal', custodial='n', group="DataOps"):
    "Helper function to make move request in PhEDEx"
    url = '%s/subscribe' % phedexUrl()
    dataXML = createXML(datasets)
    params = {"node": site, "data": dataXML, "group": group, "priority": priority,
              "custodial": custodial, "request_only": "y", "move":"y",
              "no_mail": "n", "comments": comments}
    return postRequest(url, params)

def updateSubscription(site, item, priority=None, userGroup=None, suspend=None):
    "Helper function to update subscription for given set of parameters"
    params = {"node": site}
    if '#' in item:
        params['block'] = item.replace('#', '%23')
    else:
        params['dataset'] = item
    if priority:
        params['priority'] = priority
    if userGroup:
        params['user_group'] = userGroup
    if suspend:
        params['suspend_until'] = suspend
    url = '%s/updatesubscription' % phedexUrl()
    return postRequest(url, params)

def subscriptions(**params):
    "Helper function to get list of subscriptions from PhEDEx"
    url = '%s/subscriptions' % phedexUrl()
    resp = getRequest(url, params)
    data = json.loads(resp)
    return data['phedex']['dataset']

def subsDetails4dataset(dataset):
    "Get details of specific subscriptions"
    return subscriptions(dataset=dataset)

def subsDetails4block(block):
    "Get details of specific subscriptions"
    data = subscriptions(block=block)
    for row in data:
        return row['block']
