"""
UnifiedrequestInfo module provides set of tools to handle given worflow request.
"""
# futures
from __future__ import print_function, division

# system modules
import os
import json
import copy
import pickle

# WMCore modules
from WMCore.Services.pycurl_manager import RequestHandler
from WMCore.Services.MicroService.Unified.Common import uConfig, cert, ckey,\
        reqmgr_cache_url, dbs_url
from WMCore.Services.MicroService.Unified.SiteInfo import SiteInfo

class CampaignInfo(object):
    """
    CampaignInfo class provides information about campains.
    Class should be initialized with appropriate JSON files for campaigns, see
    WmAgentScripts/campaigns.json and WmAgentScripts/campaigns.relval.json
    """
    def __init__(self):
        campaigns = os.environ.get('UNIFIED_CAMPAIGNS')
        campaignsRelVal = os.environ.get('UNIFIED_CAMPAIGNS_RELVAL')
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
    url = '%s/datasetparents' % dbs_url()
    params = {'dataset': dataset}
    headers = {'Accept': 'application/json'}
    mgr = RequestHandler()
    data = mgr.getdata(url, params=params, headers=headers, cert=cert(), ckey=ckey())
    return [str(i['parent_dataset']) for i in json.loads(data)]

def IOforTask(request):
    "Return lfn, primary, parent and secondary datasets for given request"
    lhe = False
    primary = set()
    parent = set()
    secondary = set()
    if 'InputDataset' in request:
        primary = set(filter(None, [request['InputDataset']]))
    if primary and 'IncludeParent' in request and request['IncludeParent']:
        parent = findParent(primary)
    if 'MCPileup' in request:
        secondary = set(filter(None, [request['MCPileup']]))
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
            alhe, aprimary, aparent, asecondary = IOforTask(request['%s%d' % (base, item)])
            if alhe:
                lhe = True
            primary.update(aprimary)
            parent.update(aparent)
            secondary.update(asecondary)
            item += 1
    else:
        lhe, primary, parent, secondary = IOforTask(request)
    return lhe, primary, parent, secondary

def isRelval(request):
    "Return if given request is RelVal sample"
    if 'SubRequestType' in request and 'RelVal' in request['SubRequestType']:
        return True
    return False

def collectin_uhm_chain(request, member, func=None, default=None, base=None):
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
        return collectin_uhm_chain(request, member, func, default, base='Step')
    elif request['RequestType'] == 'TaskChain':
        return collectin_uhm_chain(request, member, func, default, base='Task')
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
    all_tasks = []
    if not select:
        all_tasks.append(node)
    else:
        for key, value in select.items():
            if (isinstance(value, list) and getattr(node, key) in value) or \
                (not isinstance(value, list) and getattr(node, key) == value):
                all_tasks.append(node)
                break

    for child in node.tree.childNames:
        ch = getattr(node.tree.children, child)
        all_tasks.extend(taskDescending(ch, select))
    return all_tasks

def get_spec(request):
    "Get request from workload cache"
    url = str('%s/%s/spec' % (reqmgr_cache_url(), request['RequestName']))
    mgr = RequestHandler()
    data = mgr.getdata(url, params={}, cert=cert(), ckey=ckey())
    return pickle.loads(data)

def getAllTasks(request, select=None):
    "Return all task for given request"
    all_tasks = []
    tasklist = get_spec(request).tasks.tasklist
    for task in tasklist:
        node = getattr(get_spec(request).tasks, task)
        all_tasks.extend(taskDescending(node, select))
    return all_tasks

def getWorkTasks(request):
    "Return work tasks for given request"
    return getAllTasks(request, select={'taskType': ['Production', 'Processing', 'Skim']})

def getSplittings(request):
    "Return splittings for given request"
    spl = []
    for task in getWorkTasks(request):
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
                set_to = get
                if tsplit.algorithm in translate:
                    for src, des in translate[tsplit.algorithm]:
                        if src == get:
                            set_to = des
                            break
                spl[-1][set_to] = getattr(tsplit, get)
    return spl

def getBlowupFactors(request):
    "Return blowup factors for given request"
    if request['RequestType'] != 'TaskChain':
        return 1., 1., 1.
    min_child_job_per_event = None
    root_job_per_event = None
    max_blow_up = 0
    splits = getSplittings(request)
    for item in splits:
        c_size = None
        p_size = None
        task = item['splittingTask']
        for key in ['events_per_job', 'avg_events_per_job']:
            if key in item:
                c_size = item[key]
        parents = [s for s in splits \
                if task.startswith(s['splittingTask']) and task != s['splittingTask']]
#         parents = filter(lambda o: \
#                 task.startswith(o['splittingTask']) and task != o['splittingTask'], splits)
        if parents:
            for parent in parents:
                for key in ['events_per_job', 'avg_events_per_job']:
                    if key in parent:
                        p_size = parent[key]
                if not min_child_job_per_event or min_child_job_per_event > c_size:
                    min_child_job_per_event = c_size
        else:
            root_job_per_event = c_size
        if c_size and p_size:
            blow_up = float(p_size)/c_size
            if blow_up > max_blow_up:
                max_blow_up = blow_up
    return min_child_job_per_event, root_job_per_event, max_blow_up

def getMulticore(request):
    "Return max number of cores for a given request"
    mcores = [int(request.get('Multicore', 1))]
    if 'Chain' in request['RequestType']:
        mcores_d = collectinchain(request, 'Multicore', default=1)
        mcores.extend(map(int, mcores_d.values()))
    return max(mcores)

def getSiteWhiteList(request, siteInfo, pickone=False, verbose=True):
    "Return site list for given request"
    lheinput, primary, parent, secondary = getIO(request)
    sites_allowed = []
    if lheinput:
        sites_allowed = sorted(siteInfo.sites_eos)
    elif secondary:
        if heavyRead(request):
            sites_allowed = sorted(set(siteInfo.sites_T1s + siteInfo.sites_with_goodIO))
        else:
            sites_allowed = sorted(set(siteInfo.sites_T1s + siteInfo.sites_with_goodAAA))
    elif primary:
        sites_allowed = sorted(set(siteInfo.sites_T1s + siteInfo.sites_T2s + siteInfo.sites_T3s))
    else:
        # no input at all all site should contribute
        sites_allowed = sorted(set(siteInfo.sites_T2s + siteInfo.sites_T1s + siteInfo.sites_T3s))
    if pickone:
        sites_allowed = sorted([siteInfo.pick_CE(sites_allowed)])

    # do further restrictions based on memory
    # do further restrictions based on blow-up factor
    min_child_job_per_event, root_job_per_event, blow_up = getBlowupFactors(request)
    max_blow_up, needed_cores = uConfig.get('blow_up_limits', (0, 0))
    if blow_up > max_blow_up:
        ## then restrict to only sites with >4k slots
        new_sites_allowed = list(set(sites_allowed) &\
                                 set([site for site in sites_allowed \
                                        if siteInfo.cpu_pledges[site] > needed_cores]))
        if new_sites_allowed:
            sites_allowed = new_sites_allowed
            print("swaping", verbose)
            if verbose:
                print("restricting site white list because of blow-up factor",\
                        min_child_job_per_event, root_job_per_event, max_blow_up)

    campaignInfo = CampaignInfo()
    for campaign in getCampaigns(request):
        c_sites_allowed = campaignInfo.get(campaign, 'SiteWhitelist', [])
        c_sites_allowed.extend(campaignInfo.parameters(campaign).get('SiteWhitelist', []))
        if c_sites_allowed:
            if verbose:
                print("Using site whitelist restriction by campaign,",\
                        campaign, "configuration", sorted(c_sites_allowed))
            sites_allowed = list(set(sites_allowed) & set(c_sites_allowed))
            if not sites_allowed:
                sites_allowed = list(c_sites_allowed)

        c_black_list = campaignInfo.get(campaign, 'SiteBlacklist', [])
        c_black_list.extend(campaignInfo.parameters(campaign).get('SiteBlacklist', []))
        if c_black_list:
            if verbose:
                print("Reducing the whitelist due to black list in campaign configuration")
                print("Removing", c_black_list)
            sites_allowed = list(set(sites_allowed) - set(c_black_list))

    ncores = getMulticore(request)
    memory_allowed = siteInfo.sitesByMemory(float(request['Memory']), maxCore=ncores)
    if memory_allowed != None:
        if verbose:
            print("sites allowing", request['Memory'], "MB and", ncores, \
                    "core are", sorted(memory_allowed))
        ## mask to sites ready for mcore
        if  ncores > 1:
            memory_allowed = list(set(memory_allowed) & set(siteInfo.sites_mcore_ready))
        sites_allowed = list(set(sites_allowed) & set(memory_allowed))
    return lheinput, primary, parent, secondary, sorted(sites_allowed)

def test():
    "Helper test function"
    import time
    from WMCore.Services.MicroService.Unified.Common import eventsLumisInfo, \
            getWorkflows, workflowsInfo, dbsInfo, phedexInfo, reqmgr_url, elapsedTime, \
            getComputingTime, getNCopies, teraBytes

    url = '%s/data/request' % reqmgr_url()
    time0 = orig = time.time()
    workflows = getWorkflows(url)
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

    tot_blks = tot_evts = tot_size = tot_cput = 0
    for wflow in workflows:
        for wname, wspec in wflow.items():
            time0 = time.time()
            cput = getComputingTime(wspec, eventsLumis=eventsLumis)
            ncopies = getNCopies(wspec, cput)

            attrs = winfo[wname]
            ndatasets = len(attrs['datasets'])
#             npileups = len(attrs['pileups'])
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
            tot_blks += nblocks
            tot_evts += nevts
            tot_size += size
            tot_cput += cput
            sites = json.dumps(sorted(list(nodes)))
            print("\n### %s" % wname)
            print("%s datasets, %s blocks, %s bytes (%s TB), %s nevts, %s nlumis, cput %s, copies %s, %s" \
                    % (ndatasets, nblocks, size, teraBytes(size), nevts, nlumis, cput, ncopies, sites))
            lheinput, primary, parent, secondary = getIO(wspec)
            print("### LHE %s, Primary %s, Parent %s, Secondary %s" \
                    % (lheinput, primary, parent, secondary))
            # find out which site can serve given workflow request
            t0 = time.time()
            siteInfo = SiteInfo()
            data = getSiteWhiteList(wspec, siteInfo)
            # data = (lheinput, primary, parent, secondary, sites_allowed)
            print("sites", data)
            elapsedTime(t0, "getSiteWhiteList")
    print("\ntotal # of workflows %s, datasets %s, blocks %s, evts %s, size %s (%s TB), cput %s (hours)" \
            % (len(winfo.keys()), len(datasets), tot_blks, tot_evts, tot_size, teraBytes(tot_size), tot_cput))
    elapsedTime(orig)


if __name__ == '__main__':
    test()
