"""
Set of common utilities for Unified service.

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
Original code: https://github.com/CMSCompOps/WmAgentScripts/Unified
"""

# futures
from __future__ import division

import json
import logging
import math
# system modules
import re
import time

try:
    from urllib import quote, unquote
except ImportError:
    # PY3
    from urllib.parse import quote, unquote

# WMCore modules
from Utils.CertTools import getKeyCertFromEnv
from WMCore.Services.pycurl_manager import RequestHandler
from WMCore.Services.pycurl_manager import getdata as multi_getdata

# py2/py3 modules
# from future import standard_library
# standard_library.install_aliases()

# static variables
STEP_PAT = re.compile(r'Step[0-9]')
TASK_PAT = re.compile(r'Task[0-9]')


def getMSLogger(verbose, logger=None):
    """
    _getMSLogger_

    Return a logger object using the standard WMCore formatter
    :param verbose: boolean setting debug or not
    :return: a logger object
    """
    if logger:
        return logger

    verbose = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger()
    logging.basicConfig(format="%(asctime)s:%(levelname)s:%(module)s: %(message)s",
                        level=verbose)
    return logger


def dbsInfo(datasets, dbsUrl):
    "Provides DBS info about dataset blocks"
    datasetBlocks = {}
    datasetSizes = {}
    datasetTransfers = {}
    if not datasets:
        return datasetBlocks, datasetSizes, datasetTransfers

    urls = ['%s/blocks?detail=True&dataset=%s' % (dbsUrl, d) for d in datasets]
    data = multi_getdata(urls, ckey(), cert())

    for row in data:
        dataset = row['url'].split('=')[-1]
        rows = json.loads(row['data'])
        blocks = []
        size = 0
        datasetTransfers.setdefault(dataset, {})  # flat dict in the format of blockName: blockSize
        for item in rows:
            blocks.append(item['block_name'])
            size += item['block_size']
            datasetTransfers[dataset].update({item['block_name']: item['block_size']})
        datasetBlocks[dataset] = blocks
        datasetSizes[dataset] = size

    return datasetBlocks, datasetSizes, datasetTransfers


def findBlockParents(blocks, dbsUrl):
    """
    Helper function to find block parents given a block name.
    Return a dictionary in the format of:
    {"child dataset name": {"child block": ["parent blocks"],
                            "child block": ["parent blocks"], ...}}
    """
    parentsByBlock = {}
    urls = ['%s/blockparents?block_name=%s' % (dbsUrl, quote(b)) for b in blocks]
    data = multi_getdata(urls, ckey(), cert())
    for row in data:
        rows = json.loads(row['data'])
        for item in rows:
            dataset = item['this_block_name'].split("#")[0]
            parentsByBlock.setdefault(dataset, {})
            parentsByBlock.setdefault(item['this_block_name'], [])
            parentsByBlock[item['this_block_name']].append(item['parent_block_name'])
    return parentsByBlock


def phedexInfo(datasets, phedexUrl):
    "Fetch PhEDEx info about nodes for all datasets"
    urls = ['%s/blockreplicasummary?dataset=%s' % (phedexUrl, d) for d in datasets]
    data = multi_getdata(urls, ckey(), cert())
    blockNodes = {}
    for row in data:
        rows = json.loads(row['data'])
        for item in rows['phedex']['block']:
            nodes = [r['node'] for r in item['replica'] if r['complete'] == 'y']
            blockNodes[item['name']] = nodes
    return blockNodes


def getWorkflow(requestName, reqMgrUrl):
    "Get list of workflow info from ReqMgr2 data-service for given request name"
    headers = {'Accept': 'application/json'}
    params = {}
    url = '%s/data/request/%s' % (reqMgrUrl, requestName)
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
                dict(datasets=list(datasets), pileups=list(pileups),
                     priority=priority, selist=selist, campaign=campaign)
    return winfo


def eventsLumisInfo(inputs, dbsUrl, validFileOnly=0, sumOverLumi=0):
    "Get information about events and lumis for given set of inputs: blocks or datasets"
    what = 'dataset'
    eventsLumis = {}
    if not inputs:
        return eventsLumis
    if '#' in inputs[0]:  # inputs are list of blocks
        what = 'block_name'
    urls = ['%s/filesummaries?validFileOnly=%s&sumOverLumi=%s&%s=%s'
            % (dbsUrl, validFileOnly, sumOverLumi, what, quote(i)) for i in inputs]
    data = multi_getdata(urls, ckey(), cert())
    for row in data:
        key = row['url'].split('=')[-1]
        if what == 'block_name':
            key = unquote(key)
        rows = json.loads(row['data'])
        for item in rows:
            eventsLumis[key] = item
    return eventsLumis


def getEventsLumis(dataset, dbsUrl, blocks=None, eventsLumis=None):
    "Helper function to return number of events/lumis for given dataset or blocks"
    nevts = nlumis = 0
    if blocks:
        missingBlocks = [b for b in blocks if b not in eventsLumis]
        if missingBlocks:
            eLumis = eventsLumisInfo(missingBlocks, dbsUrl)
            eventsLumis.update(eLumis)
        for block in blocks:
            data = eventsLumis[block]
            nevts += data['num_event']
            nlumis += data['num_lumi']
        return nevts, nlumis
    if eventsLumis and dataset in eventsLumis:
        data = eventsLumis[dataset]
        return data['num_event'], data['num_lumi']
    eLumis = eventsLumisInfo([dataset], dbsUrl)
    data = eLumis.get(dataset, {'num_event': 0, 'num_lumi': 0})
    return data['num_event'], data['num_lumi']


def getComputingTime(workflow, eventsLumis=None, unit='h', dbsUrl=None, logger=None):
    "Return computing time per give workflow"
    logger = getMSLogger(verbose=True, logger=logger)
    cput = None

    if 'InputDataset' in workflow:
        dataset = workflow['InputDataset']
        if 'BlockWhitelist' in workflow and workflow['BlockWhitelist']:
            nevts, _ = getEventsLumis(dataset, dbsUrl, workflow['BlockWhitelist'], eventsLumis)
        else:
            nevts, _ = getEventsLumis(dataset, dbsUrl, eventsLumis=eventsLumis)
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
                        nevts, _ = getEventsLumis(dataset, dbsUrl, task['BlockWhitelist'], eventsLumis)
                    else:
                        nevts, _ = getEventsLumis(dataset, dbsUrl, eventsLumis=eventsLumis)
                elif 'Input%s' % base in task:
                    nevts = carryOn[task['Input%s' % base]]
                elif 'RequestNumEvents' in task:
                    nevts = float(task['RequestNumEvents'])
                else:
                    logger.debug("this is not supported, making it zero cput")
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
        cput = nevts / feff * tpe

    if cput is None:
        return 0

    if unit == 'm':
        cput = cput / (60.)
    if unit == 'h':
        cput = cput / (60. * 60.)
    if unit == 'd':
        cput = cput / (60. * 60. * 24.)
    return cput


def sigmoid(x):
    "Sigmoid function"
    return 1. / (1 + math.exp(-x))


def getNCopies(cpuHours, minN=2, maxN=3, weight=50000, constant=100000):
    "Calculate number of copies for given workflow"
    func = sigmoid(-constant / weight)
    fact = (maxN - minN) / (1 - func)
    base = (func * maxN - minN) / (func - 1)
    return int(base + fact * sigmoid((cpuHours - constant) / weight))


def teraBytes(size):
    "Return size in TB"
    return float(size) / float(1024 ** 4)


def ckey():
    "Return user CA key either from proxy or userkey.pem"
    pair = getKeyCertFromEnv()
    return pair[0]


def cert():
    "Return user CA cert either from proxy or usercert.pem"
    pair = getKeyCertFromEnv()
    return pair[1]


def elapsedTime(time0, msg='Elapsed time', ndigits=1):
    "Helper function to return elapsed time message"
    msg = "%s: %s sec" % (msg, round(time.time() - time0, ndigits))
    return msg


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
    data = mgr.getdata(url, params, headers, ckey=ckey(), cert=cert(),
                       verb='POST', verbose=verbose)
    return data


def getIO(request, dbsUrl):
    "Get input/output info about given request"
    lhe = False
    primary = set()
    parent = set()
    secondary = set()
    if 'Chain' in request['RequestType']:
        base = request['RequestType'].replace('Chain', '')
        item = 1
        while '%s%d' % (base, item) in request:
            alhe, aprimary, aparent, asecondary = \
                ioForTask(request['%s%d' % (base, item)], dbsUrl)
            if alhe:
                lhe = True
            primary.update(aprimary)
            parent.update(aparent)
            secondary.update(asecondary)
            item += 1
    else:
        lhe, primary, parent, secondary = ioForTask(request, dbsUrl)
    return lhe, primary, parent, secondary


def ioForTask(request, dbsUrl):
    "Return lfn, primary, parent and secondary datasets for given request"
    lhe = False
    primary = set()
    parent = set()
    secondary = set()
    if 'InputDataset' in request:
        datasets = request['InputDataset']
        datasets = datasets if isinstance(datasets, list) else [datasets]
        primary = set([r for r in datasets if r])
    if primary and 'IncludeParent' in request and request['IncludeParent']:
        parent = findParent(primary, dbsUrl)
    if 'MCPileup' in request:
        pileups = request['MCPileup']
        pileups = pileups if isinstance(pileups, list) else [pileups]
        secondary = set([r for r in pileups if r])
    if 'LheInputFiles' in request and request['LheInputFiles'] in ['True', True]:
        lhe = True
    return lhe, primary, parent, secondary


def findParent(datasets, dbsUrl):
    """
    Helper function to find the parent dataset.
    It returns a dictionary key'ed by the child dataset
    """
    parentByDset = {}
    if not datasets:
        return parentByDset

    urls = ['%s/datasetparents?dataset=%s' % (dbsUrl, d) for d in datasets]
    data = multi_getdata(urls, ckey(), cert())

    for row in data:
        rows = json.loads(row['data'])
        for item in rows:
            parentByDset[item['this_dataset']] = item['parent_dataset']
    return parentByDset
