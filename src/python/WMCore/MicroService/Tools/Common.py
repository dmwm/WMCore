"""
Set of common utilities for Unified service.

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
Original code: https://github.com/CMSCompOps/WmAgentScripts/Unified
"""

# futures
from __future__ import division, print_function, absolute_import

from future import standard_library
standard_library.install_aliases()

# system modules
import json
import logging
import math
import re
import time
from urllib.parse import quote, unquote

# WMCore modules
from Utils.IteratorTools import grouper
from Utils.CertTools import ckey, cert
from WMCore.Services.pycurl_manager import RequestHandler
from WMCore.Services.pycurl_manager import getdata as multi_getdata

# static variables
STEP_PAT = re.compile(r'Step[0-9]')
TASK_PAT = re.compile(r'Task[0-9]')


def hasHTTPFailed(row):
    """
    Evaluates whether the HTTP request through PyCurl failed or not.

    :param row: dictionary data returned from pycurl_manager module
    :return: a boolean confirming failure or not
    """
    if 'data' not in row:
        return True
    if int(row.get('code', 200)) == 200:
        return False
    return True


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


def isRelVal(reqDict):
    """
    Helper function to evaluate whether the workflow is RelVal or not.
    :param reqDict: dictionary with the workflow description
    :return: True if it's a RelVal workflow, otherwise False
    """
    return reqDict.get("SubRequestType", "") in ['RelVal', 'HIRelVal']


def dbsInfo(datasets, dbsUrl):
    "Provides DBS info about dataset blocks"
    datasetBlocks = {}
    datasetSizes = {}
    datasetTransfers = {}
    if not datasets:
        return datasetBlocks, datasetSizes, datasetTransfers

    urls = ['%s/blocks?detail=True&dataset=%s' % (dbsUrl, d) for d in datasets]
    logging.info("Executing %d requests against DBS 'blocks' API, with details", len(urls))
    data = multi_getdata(urls, ckey(), cert())

    for row in data:
        dataset = row['url'].split('=')[-1]
        if hasHTTPFailed(row):
            print("FAILURE: dbsInfo for %s. Error: %s %s" % (dataset, row.get('code'), row.get('error')))
            continue
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


def getPileupDatasetSizes(datasets, phedexUrl):
    """
    Given a list of datasets, find all their blocks with replicas
    available, i.e., blocks that have valid files to be processed,
    and calculate the total dataset size
    :param datasets: list of dataset names
    :param phedexUrl: a string with the PhEDEx URL
    :return: a dictionary of datasets and their respective sizes
    NOTE: Value `None` is returned in case the data-service failed to serve a given request.
    """
    sizeByDset = {}
    if not datasets:
        return sizeByDset

    urls = ['%s/blockreplicas?dataset=%s' % (phedexUrl, dset) for dset in datasets]
    logging.info("Executing %d requests against PhEDEx 'blockreplicas' API", len(urls))
    data = multi_getdata(urls, ckey(), cert())

    for row in data:
        dataset = row['url'].split('=')[-1]
        if row['data'] is None:
            print("Failure in getPileupDatasetSizes for dataset %s. Error: %s %s" % (dataset,
                                                                                     row.get('code'),
                                                                                     row.get('error')))
            sizeByDset.setdefault(dataset, None)
            continue
        rows = json.loads(row['data'])
        sizeByDset.setdefault(dataset, 0)
        try:
            for item in rows['phedex']['block']:
                sizeByDset[dataset] += item['bytes']
        except Exception as exc:
            print("Failure in getPileupDatasetSizes for dataset %s. Error: %s" % (dataset, str(exc)))
            sizeByDset[dataset] = None
    return sizeByDset


def getBlockReplicasAndSize(datasets, phedexUrl, group=None):
    """
    Given a list of datasets, find all their blocks with replicas
    available (thus blocks with at least 1 valid file), completed
    and subscribed.
    If PhEDEx group is provided, make sure it's subscribed under that
    same group.
    :param datasets: list of dataset names
    :param phedexUrl: a string with the PhEDEx URL
    :param group: optional PhEDEx group name
    :return: a dictionary in the form of:
    {"dataset":
        {"block":
            {"blockSize": 111, "locations": ["x", "y"]}
        }
    }
    NOTE: Value `None` is returned in case the data-service failed to serve a given request.
    """
    dsetBlockSize = {}
    if not datasets:
        return dsetBlockSize

    urls = ['%s/blockreplicas?dataset=%s' % (phedexUrl, dset) for dset in datasets]
    logging.info("Executing %d requests against PhEDEx 'blockreplicas' API", len(urls))
    data = multi_getdata(urls, ckey(), cert())

    for row in data:
        dataset = row['url'].split('=')[-1]
        if row['data'] is None:
            print("Failure in getBlockReplicasAndSize for dataset %s. Error: %s %s" % (dataset,
                                                                                       row.get('code'),
                                                                                       row.get('error')))
            dsetBlockSize.setdefault(dataset, None)
            continue
        rows = json.loads(row['data'])
        dsetBlockSize.setdefault(dataset, {})
        try:
            for item in rows['phedex']['block']:
                block = {item['name']: {'blockSize': item['bytes'], 'locations': []}}
                for repli in item['replica']:
                    if repli['complete'] == 'y' and repli['subscribed'] == 'y':
                        if not group:
                            block[item['name']]['locations'].append(repli['node'])
                        elif repli['group'] == group:
                            block[item['name']]['locations'].append(repli['node'])
                dsetBlockSize[dataset].update(block)
        except Exception as exc:
            print("Failure in getBlockReplicasAndSize for dataset %s. Error: %s" % (dataset, str(exc)))
            dsetBlockSize[dataset] = None
    return dsetBlockSize


def getPileupSubscriptions(datasets, phedexUrl, group=None, percentMin=99):
    """
    Provided a list of datasets, find dataset level subscriptions where it's
    as complete as `percent_min`.
    :param datasets: list of dataset names
    :param phedexUrl: a string with the PhEDEx URL
    :param group: optional string with the PhEDEx group
    :param percent_min: only return subscriptions that are this complete
    :return: a dictionary of datasets and a list of their location.
    NOTE: Value `None` is returned in case the data-service failed to serve a given request.
    """
    locationByDset = {}
    if not datasets:
        return locationByDset

    if group:
        url = "%s/subscriptions?group=%s" % (phedexUrl, group)
        url += "&percent_min=%s&dataset=%s"
    else:
        url = "%s/subscriptions?" % phedexUrl
        url += "percent_min=%s&dataset=%s"
    urls = [url % (percentMin, dset) for dset in datasets]

    logging.info("Executing %d requests against PhEDEx 'subscriptions' API", len(urls))
    data = multi_getdata(urls, ckey(), cert())

    for row in data:
        dataset = row['url'].rsplit('=')[-1]
        if row['data'] is None:
            print("Failure in getPileupSubscriptions for dataset %s. Error: %s %s" % (dataset,
                                                                                      row.get('code'),
                                                                                      row.get('error')))
            locationByDset.setdefault(dataset, None)
            continue
        rows = json.loads(row['data'])
        locationByDset.setdefault(dataset, [])
        try:
            for item in rows['phedex']['dataset']:
                for subs in item['subscription']:
                    locationByDset[dataset].append(subs['node'])
        except Exception as exc:
            print("Failure in getPileupSubscriptions for dataset %s. Error: %s" % (dataset, str(exc)))
            locationByDset[dataset] = None
    return locationByDset


def getBlocksByDsetAndRun(datasetName, runList, dbsUrl):
    """
    Given a dataset name and a list of runs, find all the blocks
    :return: flat list of blocks
    """
    blocks = set()
    if isinstance(runList, set):
        runList = list(runList)

    urls = []
    for runSlice in grouper(runList, 50):
        urls.append('%s/blocks?run_num=%s&dataset=%s' % (dbsUrl, str(runSlice).replace(" ", ""), datasetName))
    logging.info("Executing %d requests against DBS 'blocks' API, with run_num list", len(urls))
    data = multi_getdata(urls, ckey(), cert())

    for row in data:
        dataset = row['url'].rsplit('=')[-1]
        if hasHTTPFailed(row):
            msg = "Failure in getBlocksByDsetAndRun for %s. Error: %s %s" % (dataset,
                                                                             row.get('code'),
                                                                             row.get('error'))
            raise RuntimeError(msg)
        rows = json.loads(row['data'])
        for item in rows:
            blocks.add(item['block_name'])

    return list(blocks)


def getFileLumisInBlock(blocks, dbsUrl, validFileOnly=1):
    """
    Given a list of blocks, find their file run lumi information
    in DBS for up to 10 blocks concurrently
    :param blocks: list of block names
    :param dbsUrl: string with the DBS URL
    :param validFileOnly: integer flag for valid files only or not
    :return: a dict of blocks with list of file/run/lumi info
    """
    # importing dbs3-client only in the functions where it is used so that 
    # we do not need to add it to the docker images of microservices that do
    # not use it.
    from dbs.apis.dbsClient import aggFileLumis

    runLumisByBlock = {}
    urls = ['%s/filelumis?validFileOnly=%d&block_name=%s' % (dbsUrl, validFileOnly, quote(b)) for b in blocks]
    # limit it to 10 concurrent calls not to overload DBS
    logging.info("Executing %d requests against DBS 'filelumis' API, concurrency limited to 10", len(urls))
    data = multi_getdata(urls, ckey(), cert(), num_conn=10)

    for row in data:
        blockName = unquote(row['url'].rsplit('=')[-1])
        if hasHTTPFailed(row):
            msg = "Failure in getFileLumisInBlock for block %s. Error: %s %s" % (blockName,
                                                                                 row.get('code'),
                                                                                 row.get('error'))
            raise RuntimeError(msg)
        rows = json.loads(row['data'])
        rows = aggFileLumis(rows)  # adjust to DBS Go server output
        runLumisByBlock.setdefault(blockName, [])
        for item in rows:
            runLumisByBlock[blockName].append(item)
    return runLumisByBlock


def findBlockParents(blocks, dbsUrl):
    """
    Helper function to find block parents given a list of block names.
    Return a dictionary in the format of:
    {"child dataset name": {"child block": ["parent blocks"],
                            "child block": ["parent blocks"], ...}}
    NOTE: Value `None` is returned in case the data-service failed to serve a given request.
    """
    parentsByBlock = {}
    urls = ['%s/blockparents?block_name=%s' % (dbsUrl, quote(b)) for b in blocks]
    logging.info("Executing %d requests against DBS 'blockparents' API", len(urls))
    data = multi_getdata(urls, ckey(), cert())
    for row in data:
        blockName = unquote(row['url'].rsplit('=')[-1])
        dataset = blockName.split("#")[0]
        if hasHTTPFailed(row):
            print("Failure in findBlockParents for block %s. Error: %s %s" % (blockName,
                                                                              row.get('code'),
                                                                              row.get('error')))
            parentsByBlock.setdefault(dataset, None)
            continue
        rows = json.loads(row['data'])
        try:
            if dataset in parentsByBlock and parentsByBlock[dataset] is None:
                # then one of the block calls has failed, keep it failed!
                continue
            parentsByBlock.setdefault(dataset, {})
            for item in rows:
                parentsByBlock[dataset].setdefault(item['this_block_name'], set())
                parentsByBlock[dataset][item['this_block_name']].add(item['parent_block_name'])
        except Exception as exc:
            print("Failure in findBlockParents for block %s. Error: %s" % (blockName, str(exc)))
            parentsByBlock[dataset] = None
    return parentsByBlock


def getRunsInBlock(blocks, dbsUrl):
    """
    Provided a list of block names, find their run numbers
    :param blocks: list of block names
    :param dbsUrl: string with the DBS URL
    :return: a dictionary of block names and a list of run numbers
    """
    # importing dbs3-client only in the functions where it is used so that 
    # we do not need to add it to the docker images of microservices that do
    # not use it.
    from dbs.apis.dbsClient import aggRuns

    runsByBlock = {}
    urls = ['%s/runs?block_name=%s' % (dbsUrl, quote(b)) for b in blocks]
    logging.info("Executing %d requests against DBS 'runs' API", len(urls))
    data = multi_getdata(urls, ckey(), cert())
    for row in data:
        blockName = unquote(row['url'].rsplit('=')[-1])
        if hasHTTPFailed(row):
            msg = "Failure in getRunsInBlock for block %s. Error: %s %s" % (blockName,
                                                                            row.get('code'),
                                                                            row.get('error'))
            raise RuntimeError(msg)
        rows = json.loads(row['data'])
        rows = aggRuns(rows) # adjust to DBS Go server output
        runsByBlock[blockName] = rows[0]['run_num']
    return runsByBlock


def getWorkflow(requestName, reqMgrUrl):
    "Get list of workflow info from ReqMgr2 data-service for given request name"
    headers = {'Accept': 'application/json'}
    params = {}
    url = '%s/data/request/%s' % (reqMgrUrl, requestName)
    mgr = RequestHandler()
    res = mgr.getdata(url, params=params, headers=headers, ckey=ckey(), cert=cert())
    data = json.loads(res)
    return data.get('result', [])


def getDetoxQuota(url):
    "Get list of workflow info from ReqMgr2 data-service for given request name"
    headers = {}
    params = {}
    mgr = RequestHandler()
    res = mgr.getdata(url, params=params, headers=headers, ckey=ckey(), cert=cert())
    res = res.split('\n')
    return res


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
        data = unquote(row['url'].split('=')[-1])
        if hasHTTPFailed(row):
            print("FAILURE: eventsLumisInfo for %s. Error: %s %s" % (data,
                                                                     row.get('code'),
                                                                     row.get('error')))
            continue
        rows = json.loads(row['data'])
        for item in rows:
            eventsLumis[data] = item
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
    "Return size in TB (Terabytes)"
    return size / (1000 ** 4)


def gigaBytes(size):
    "Return size in GB (Gigabytes), rounded to 2 digits"
    return round(size / (1000 ** 3), 2)


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


def findParent(datasets, dbsUrl):
    """
    Helper function to find the parent dataset.
    It returns a dictionary key'ed by the child dataset
    NOTE: Value `None` is returned in case the data-service failed to serve a given request.
    """
    parentByDset = {}
    if not datasets:
        return parentByDset

    urls = ['%s/datasetparents?dataset=%s' % (dbsUrl, d) for d in datasets]
    logging.info("Executing %d requests against DBS 'datasetparents' API", len(urls))
    data = multi_getdata(urls, ckey(), cert())

    for row in data:
        dataset = row['url'].split('=')[-1]
        if hasHTTPFailed(row):
            print("Failure in findParent for dataset %s. Error: %s %s" % (dataset,
                                                                          row.get('code'),
                                                                          row.get('error')))
            parentByDset.setdefault(dataset, None)
            continue
        rows = json.loads(row['data'])
        try:
            for item in rows:
                parentByDset[item['this_dataset']] = item['parent_dataset']
        except Exception as exc:
            print("Failure in findParent for dataset %s. Error: %s" % (dataset, str(exc)))
            parentByDset[dataset] = None
    return parentByDset
