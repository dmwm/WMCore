"""
UnifiedrequestInfo module provides set of tools to handle given worflow request.

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
Original code: https://github.com/CMSCompOps/WmAgentScripts/Unified
"""
# futures
from __future__ import print_function, division

# system modules
import os
import json
import time
import copy
import pickle

# WMCore modules
from WMCore.Services.pycurl_manager import RequestHandler
from WMCore.Services.pycurl_manager import getdata as multi_getdata
from WMCore.Services.MicroService.Unified.Common import uConfig, cert, ckey,\
        reqmgrCacheUrl, dbsUrl, eventsLumisInfo, getWorkflows, workflowsInfo,\
        dbsInfo, phedexInfo, reqmgrUrl, elapsedTime, getComputingTime,\
        getNCopies, teraBytes
from WMCore.Services.MicroService.Unified.SiteInfo import SiteInfo


class CampaignInfo(object):
    """
    CampaignInfo class provides information about campains.
    Class should be initialized with appropriate JSON files for campaigns, see
    WmAgentScripts/campaigns.json and WmAgentScripts/campaigns.relval.json
    """
    def __init__(self):
        fname = '/'.join(__file__.split('/')[:-1] + ['campaigns.json'])
        campaigns = os.getenv('UNIFIED_CAMPAIGNS', fname)
        fname = '/'.join(__file__.split('/')[:-1] + ['campaigns.relval.json'])
        campaignsRelVal = os.getenv('UNIFIED_CAMPAIGNS_RELVAL', fname)
        self.campaigns = json.loads(open(campaigns).read())
        self.campaigns.update(json.loads(open(campaignsRelVal).read()))
        siteInfo = SiteInfo()
        for camp in self.campaigns:
            if 'parameters' in self.campaigns[camp]:
                if 'SiteBlacklist' in self.campaigns[camp]['parameters']:
                    siteBlackList = \
                        copy.deepcopy(self.campaigns[camp]['parameters']['SiteBlacklist'])
                    for black in siteBlackList:
                        if black.endswith('*'):
                            self.campaigns[camp]['parameters']['SiteBlacklist'].remove(black)
                            reg = black[0:-1]
                            self.campaigns[camp]['parameters']['SiteBlacklist'].extend(\
                                [s for s in siteInfo.all_sites if s.startswith(reg)])

    def get(self, camp, key, default):
        "Get certain campaign from campaign list"
        if camp in self.campaigns:
            if key in self.campaigns[camp]:
                return copy.deepcopy(self.campaigns[camp][key])
        return copy.deepcopy(default)

    def parameters(self, camp):
        "Return parameters dict of given campaign"
        if camp in self.campaigns and 'parameters' in self.campaigns[camp]:
            return self.campaigns[camp]['parameters']
        return {}

def findParent(dataset):
    "Helper function to find a parent of the dataset"
    url = '%s/datasetparents' % dbsUrl()
    params = {'dataset': dataset}
    headers = {'Accept': 'application/json'}
    mgr = RequestHandler()
    data = mgr.getdata(url, params=params, headers=headers, cert=cert(), ckey=ckey())
    return [str(i['parent_dataset']) for i in json.loads(data)]

def ioForTask(request):
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
        parent = findParent(primary)
    if 'MCPileup' in request:
        pileups = request['MCPileup']
        pileups = pileups if isinstance(pileups, list) else [pileups]
        secondary = set([r for r in pileups if r])
    if 'LheInputFiles' in request and request['LheInputFiles'] in ['True', True]:
        lhe = True
    return lhe, primary, parent, secondary

def getIO(request):
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
                    ioForTask(request['%s%d' % (base, item)])
            if alhe:
                lhe = True
            primary.update(aprimary)
            parent.update(aparent)
            secondary.update(asecondary)
            item += 1
    else:
        lhe, primary, parent, secondary = ioForTask(request)
    return lhe, primary, parent, secondary

def isRelval(request):
    "Return if given request is RelVal sample"
    if 'SubRequestType' in request and 'RelVal' in request['SubRequestType']:
        return True
    return False

def collectionHelper(request, member, func=None, default=None, base=None):
    "Helper function to return uhm chain as a dictionary"
    coll = {}
    item = 1
    while '%s%d' % (base, item) in request:
        if member in request['%s%d'%(base, item)]:
            if func:
                coll[request['%s%d' % (base, item)]['%sName' % base]] = \
                        func(request['%s%d' % (base, item)][member])
            else:
                coll[request['%s%d' % (base, item)]['%sName' % base]] = \
                        request['%s%d' % (base, item)].get(member, default)
        item += 1
    return coll

def collectinchain(request, member, func=None, default=None):
    "Helper function to return dictionary of collection chain"
    if request['RequestType'] == 'StepChain':
        return collectionHelper(request, member, func, default, base='Step')
    elif request['RequestType'] == 'TaskChain':
        return collectionHelper(request, member, func, default, base='Task')
    else:
        raise Exception("should not call collectinchain on non-chain request")

def getCampaigns(request):
    "Return campaigns of given request"
    if 'Chain' in request['RequestType'] and not isRelval(request):
        return list(set(collectinchain(request, 'AcquisitionEra').values()))
    return [request['Campaign']]

def heavyRead(request):
    """
    Return True by default. False if 'premix' appears in the
    output datasets or in the campaigns
    """
    response = True
    if any(['premix' in c.lower() for c in getCampaigns(request)]):
        response = False
    if any(['premix' in o.lower() for o in request['OutputDatasets']]):
        response = False
    return response

def taskDescending(node, select=None):
    "Helper function to walk through task nodes in descending order"
    allTasks = []
    if not select:
        allTasks.append(node)
    else:
        for key, value in select.items():
            if (isinstance(value, list) and getattr(node, key) in value) or \
                (not isinstance(value, list) and getattr(node, key) == value):
                allTasks.append(node)
                break

    for child in node.tree.childNames:
        chItem = getattr(node.tree.children, child)
        allTasks.extend(taskDescending(chItem, select))
    return allTasks

def getRequestSpecs(requestNames):
    "Helper function to get all specs for given set of request names"
    urls = [str('%s/%s/spec' % (reqmgrCacheUrl(), r)) for r in requestNames]
    data = multi_getdata(urls, ckey(), cert())
    rdict = {}
    for row in data:
        req = row['url'].split('/')[-2]
        rdict[req] = pickle.loads(row['data'])
    return rdict

def getSpec(request, reqSpecs=None):
    "Get request from workload cache"
    if reqSpecs and request['RequestName'] in reqSpecs:
        return reqSpecs[request['RequestName']]
    url = str('%s/%s/spec' % (reqmgrCacheUrl(), request['RequestName']))
    mgr = RequestHandler()
    data = mgr.getdata(url, params={}, cert=cert(), ckey=ckey())
    return pickle.loads(data)

def getAllTasks(request, select=None, reqSpecs=None):
    "Return all task for given request"
    allTasks = []
    tasks = getSpec(request, reqSpecs).tasks
    for task in tasks.tasklist:
        node = getattr(tasks, task)
        allTasks.extend(taskDescending(node, select))
    return allTasks

def getWorkTasks(request, reqSpecs=None):
    "Return work tasks for given request"
    return getAllTasks(request, select={'taskType': ['Production', 'Processing', 'Skim']},\
            reqSpecs=reqSpecs)

def getSplittings(request, reqSpecs=None):
    "Return splittings for given request"
    spl = []
    for task in getWorkTasks(request, reqSpecs=reqSpecs):
        tsplit = task.input.splitting
        spl.append({"splittingAlgo": tsplit.algorithm, "splittingTask": task.pathName})
        get_those = ['events_per_lumi', 'events_per_job', 'lumis_per_job',\
                     'halt_job_on_file_boundaries', 'max_events_per_lumi',\
                     'halt_job_on_file_boundaries_event_aware']
        translate = {'EventAwareLumiBased': [('events_per_job', 'avg_events_per_job')]}
        include = {'EventAwareLumiBased': {'halt_job_on_file_boundaries_event_aware': 'True'},\
            'LumiBased': {'halt_job_on_file_boundaries': 'True'}}
        if tsplit.algorithm in include:
            for key, val in include[tsplit.algorithm].items():
                spl[-1][key] = val
        for get in get_those:
            if hasattr(tsplit, get):
                setTo = get
                if tsplit.algorithm in translate:
                    for src, des in translate[tsplit.algorithm]:
                        if src == get:
                            setTo = des
                            break
                spl[-1][setTo] = getattr(tsplit, get)
    return spl

def getBlowupFactors(request, reqSpecs=None):
    "Return blowup factors for given request"
    if request['RequestType'] != 'TaskChain':
        return 1., 1., 1.
    minChildJobPerEvent = None
    rootJobPerEvent = None
    maxBlowUp = 0
    splits = getSplittings(request, reqSpecs=reqSpecs)
    for item in splits:
        cSize = None
        pSize = None
        task = item['splittingTask']
        for key in ['events_per_job', 'avg_events_per_job']:
            if key in item:
                cSize = item[key]
        parents = [s for s in splits \
                if task.startswith(s['splittingTask']) and task != s['splittingTask']]
        if parents:
            for parent in parents:
                for key in ['events_per_job', 'avg_events_per_job']:
                    if key in parent:
                        pSize = parent[key]
                if not minChildJobPerEvent or minChildJobPerEvent > cSize:
                    minChildJobPerEvent = cSize
        else:
            rootJobPerEvent = cSize
        if cSize and pSize:
            blowUp = float(pSize)/cSize
            if blowUp > maxBlowUp:
                maxBlowUp = blowUp
    return minChildJobPerEvent, rootJobPerEvent, maxBlowUp

def getMulticore(request):
    "Return max number of cores for a given request"
    mcores = [int(request.get('Multicore', 1))]
    if 'Chain' in request['RequestType']:
        mcoresCol = collectinchain(request, 'Multicore', default=1)
        mcores.extend([int(v) for v in mcoresCol.values()])
    return max(mcores)

def getSiteWhiteList(request, siteInfo, reqSpecs=None, pickone=False, verbose=True):
    "Return site list for given request"
    lheinput, primary, parent, secondary = getIO(request)
    allowedSites = []
    if lheinput:
        allowedSites = sorted(siteInfo.sites_eos)
    elif secondary:
        if heavyRead(request):
            allowedSites = sorted(set(siteInfo.sites_T1s + siteInfo.sites_with_goodIO))
        else:
            allowedSites = sorted(set(siteInfo.sites_T1s + siteInfo.sites_with_goodAAA))
    elif primary:
        allowedSites = sorted(set(siteInfo.sites_T1s + siteInfo.sites_T2s + siteInfo.sites_T3s))
    else:
        # no input at all all site should contribute
        allowedSites = sorted(set(siteInfo.sites_T2s + siteInfo.sites_T1s + siteInfo.sites_T3s))
    if pickone:
        allowedSites = sorted([siteInfo.pick_CE(allowedSites)])

    # do further restrictions based on memory
    # do further restrictions based on blow-up factor
    minChildJobPerEvent, rootJobPerEvent, blowUp = \
            getBlowupFactors(request, reqSpecs=reqSpecs)
    maxBlowUp, neededCores = uConfig.get('blow_up_limits', (0, 0))
    if blowUp > maxBlowUp:
        ## then restrict to only sites with >4k slots
        newAllowedSites = list(set(allowedSites) &\
                                 set([site for site in allowedSites \
                                        if siteInfo.cpu_pledges[site] > neededCores]))
        if newAllowedSites:
            allowedSites = newAllowedSites
            print("swaping", verbose)
            if verbose:
                print("restricting site white list because of blow-up factor",\
                        minChildJobPerEvent, rootJobPerEvent, maxBlowUp)

    campaignInfo = CampaignInfo()
    for campaign in getCampaigns(request):
        campSites = campaignInfo.get(campaign, 'SiteWhitelist', [])
        campSites.extend(campaignInfo.parameters(campaign).get('SiteWhitelist', []))
        if campSites:
            if verbose:
                print("Using site whitelist restriction by campaign,",\
                        campaign, "configuration", sorted(campSites))
            allowedSites = list(set(allowedSites) & set(campSites))
            if not allowedSites:
                allowedSites = list(campSites)

        campBlackList = campaignInfo.get(campaign, 'SiteBlacklist', [])
        campBlackList.extend(campaignInfo.parameters(campaign).get('SiteBlacklist', []))
        if campBlackList:
            if verbose:
                print("Reducing the whitelist due to black list in campaign configuration")
                print("Removing", campBlackList)
            allowedSites = list(set(allowedSites) - set(campBlackList))

    ncores = getMulticore(request)
    memAllowed = siteInfo.sitesByMemory(float(request['Memory']), maxCore=ncores)
    if memAllowed != None:
        if verbose:
            print("sites allowing", request['Memory'], "MB and", ncores, \
                    "core are", sorted(memAllowed))
        ## mask to sites ready for mcore
        if  ncores > 1:
            memAllowed = list(set(memAllowed) & set(siteInfo.sites_mcore_ready))
        allowedSites = list(set(allowedSites) & set(memAllowed))
    return lheinput, primary, parent, secondary, sorted(allowedSites)

def requestsInfo(state='assignment-approved'):
    """
    Helper function to get information about all requests
    in assignment-approved state in ReqMgr
    """
    url = '%s/data/request' % reqmgrUrl()
    time0 = orig = time.time()
    workflows = getWorkflows(url, state=state)
    elapsedTime(time0, "### getWorkflows")

    winfo = workflowsInfo(workflows)
    datasets = [d for row in winfo.values() for d in row['datasets']]

    time0 = time.time()
    datasetBlocks, datasetSizes = dbsInfo(datasets)
    elapsedTime(time0, "### dbsInfo")

    time0 = time.time()
    blockNodes = phedexInfo(datasets)
    elapsedTime(time0, "### phedexInfo")

    time0 = time.time()
    eventsLumis = eventsLumisInfo(datasets)
    elapsedTime(time0, "### eventsLumisInfo")

    # get specs for all requests and re-use them later in getSiteWhiteList as cache
    requests = [v['RequestName'] for w in workflows for v in w.values()]
    reqSpecs = getRequestSpecs(requests)

    # get siteInfo instance once and re-use it later, it is time-consumed object
    siteInfo = SiteInfo()

    requests = {}
    totBlocks = totEvents = totSize = totCpuT = 0
    tst0 = time.time()
    for wflow in workflows:
        for wname, wspec in wflow.items():
            time0 = time.time()
            cput = getComputingTime(wspec, eventsLumis=eventsLumis)
            ncopies = getNCopies(cput)

            attrs = winfo[wname]
            ndatasets = len(attrs['datasets'])
            npileups = len(attrs['pileups'])
            nblocks = nevts = nlumis = size = 0
            nodes = set()
            for dataset in attrs['datasets']:
                blocks = datasetBlocks[dataset]
                for blk in blocks:
                    for node in blockNodes[blk]:
                        nodes.add(node)
                nblocks += len(blocks)
                size += datasetSizes[dataset]
                edata = eventsLumis[dataset]
                nevts += edata['num_event']
                nlumis += edata['num_lumi']
            totBlocks += nblocks
            totEvents += nevts
            totSize += size
            totCpuT += cput
            sites = json.dumps(sorted(list(nodes)))
            print("\n### %s" % wname)
            print("%s datasets, %s blocks, %s bytes (%s TB), %s nevts, %s nlumis, cput %s, copies %s, %s" \
                    % (ndatasets, nblocks, size, teraBytes(size), nevts, nlumis, cput, ncopies, sites))
            # find out which site can serve given workflow request
            t0 = time.time()
            lheInput, primary, parent, secondary, allowedSites \
                    = getSiteWhiteList(wspec, siteInfo, reqSpecs)
            rdict = dict(datasets=datasets, blocks=datasetBlocks,\
                    npileups=npileups, size=size,\
                    nevents=nevts, nlumis=nlumis, cput=cput, ncopies=ncopies,\
                    sites=sites, allowedSites=allowedSites, parent=parent,\
                    lheInput=lheInput, primary=primary, secondary=secondary)
            requests[wname] = rdict
            print("sites", allowedSites)
            elapsedTime(t0, "getSiteWhiteList")
    print("\ntotal # of workflows %s, datasets %s, blocks %s, evts %s, size %s (%s TB), cput %s (hours)" \
            % (len(winfo.keys()), len(datasets), totBlocks, totEvents, totSize, teraBytes(totSize), totCpuT))
    elapsedTime(tst0, 'workflows info')
    elapsedTime(orig)
    return requests

if __name__ == '__main__':
    import cProfile # python profiler
    import pstats   # profiler statistics
    cmd = 'requestsInfo()'
    cProfile.runctx(cmd, globals(), locals(), 'profile.dat')
    info = pstats.Stats('profile.dat')
    info.sort_stats('cumulative')
    info.print_stats()
#     requestsInfo()
