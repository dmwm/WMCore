#!/usr/bin/env python
# encoding: utf-8

"""
File       : cms.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: CMS modules
"""

from __future__ import (division, print_function)

from WMCore.Cache.GenericDataCache import MemoryCacheStruct
from WMCore.ReqMgr.DataStructs.RequestStatus import REQUEST_STATE_LIST, REQUEST_STATE_TRANSITION
from WMCore.Services.CRIC.CRIC import CRIC
from WMCore.Services.TagCollector.TagCollector import TagCollector

# initialize TagCollector instance to be used in this module
TC = TagCollector()


def next_status(status=None):
    "Return next ReqMgr status for given status"
    if status:
        if status in REQUEST_STATE_TRANSITION:
            return REQUEST_STATE_TRANSITION[status]
        else:
            return 'N/A'
    return REQUEST_STATE_LIST


def sites():
    "Return known CMS site list from CRIC"
    try:
        # Download a list of all the sites from CRIC
        cric = CRIC()
        site_list = sorted(cric.getAllPSNs())
    except Exception as exc:
        msg = "ERROR: Could not retrieve sites from CRIC, reason: %s" % str(exc)
        raise Exception(msg)
    return site_list


# create a site cache and pnn cache 2 hour duration
SITE_CACHE = MemoryCacheStruct(5200, sites)


def pnns():
    """
    Returns all PhEDEx node names, excluding Buffer endpoints
    """
    cric = CRIC()

    try:
        pnn_list = sorted(cric.getAllPhEDExNodeNames(excludeBuffer=True))
    except Exception as exc:
        msg = "ERROR: Could not retrieve PNNs from CRIC, reason: %s" % str(exc)
        raise Exception(msg)
    return pnn_list


# create a site cache and pnn cache 2 hour duration
PNN_CACHE = MemoryCacheStruct(5200, pnns)


def site_white_list():
    "site white list, default all T1"
    t1_sites = [s for s in SITE_CACHE.getData() if s.startswith('T1_')]
    return t1_sites


def site_black_list():
    "site black list, default all T3"
    t3_sites = [s for s in SITE_CACHE.getData() if s.startswith('T3_')]
    return t3_sites


def lfn_bases():
    "Return LFN Base list"
    storeResultLFNBase = [
        "/store/backfill/1",
        "/store/backfill/2",
        "/store/data",
        "/store/mc",
        "/store/generator",
        "/store/relval",
        "/store/hidata",
        "/store/himc",
        "/store/results/analysisops",
        "/store/results/b_physics",
        "/store/results/b_tagging",
        "/store/results/b2g",
        "/store/results/e_gamma_ecal",
        "/store/results/ewk",
        "/store/results/exotica",
        "/store/results/forward",
        "/store/results/heavy_ions",
        "/store/results/higgs",
        "/store/results/jets_met_hcal",
        "/store/results/muon",
        "/store/results/qcd",
        "/store/results/susy",
        "/store/results/tau_pflow",
        "/store/results/top",
        "/store/results/tracker_dpg",
        "/store/results/tracker_pog",
        "/store/results/trigger"]
    return storeResultLFNBase


def lfn_unmerged_bases():
    "Return list of LFN unmerged bases"
    out = ["/store/unmerged", "/store/data", "/store/temp"]
    return out


def web_ui_names():
    "Return dict of web UI JSON naming conventions"
    maps = {
        "TimePerEvent": "TimePerEvent (seconds)",
        "OpenRunningTimeout": "OpenRunningTimeout (seconds)",
        "SizePerEvent": "SizePerEvent (KBytes)",
        "Memory": "Memory (MBytes)",
        "BlockCloseMaxSize": "BlockCloseMaxSize (Bytes)",
        "SoftTimeout": "SoftTimeout (seconds)",
    }
    #     maps = {"InputDataset": "Input dataset",
    #             "IncludeParents": "Include parents",
    #             "PrepID": "Optional Prep ID String",
    #             "BlockBlacklist": "Block black list",
    #             "RequestPriority": "Request priority",
    #             "TimePerEvent": "Time per event (seconds)",
    #             "RunWhitelist": "Run white list",
    #             "BlockWhitelist": "Block white list",
    #             "OpenRunningTimeout": "Open Running Timeout",
    #             "DQLUploadURL": "DQM URL",
    #             "DqmSequences": "DQM Sequences",
    #             "SizePerEvent": "Size per event (KBytes)",
    #             "ScramArch": "Architecture",
    #             "EnableHarvesting": "Enable DQM Harvesting",
    #             "DQMConfigCacheID": "DQM Config CacheID",
    #             "Memory": "Memory (MBytes)",
    #             "RunBlacklist": "Run black list",
    #             "RequestString": "Optional request ID string",
    #             "CMSSWVersion": "Software releases",
    #             "DQMUploadURL":"DQM URL",
    #             "DQMSequences": "DQM sequences",
    #             "DataPileup": "Data pileup",
    #             "FilterEfficiency": "Filter efficiency",
    #             "GlobalTag": "Global tag",
    #             "MCPileup": "MonteCarlo pileup",
    #             "PrimaryDataset": "Parimary dataset",
    #             "Acquisitionera": "Acquisition era",
    #             "CmsPath": "CMS path",
    #             "DBS": "DBS urls",
    #             "ProcessingVersion": "Processing version",
    #             "ProcessingString": "Processing string",
    #             "RequestType": "Request type",
    #             "ACDCDatabase": "ACDC database",
    #             "ACDCServer": "ACDC server",
    #             "CollectionName": "Collection name",
    #             "IgnoredOutputModules": "Ignored output modules",
    #             "InitialTaskPath": "Initial task path",
    #             "KeepStepOneOutput": "Keep step one output",
    #             "KeepStepTwoOutput": "Keep step two output",
    #             "StepOneConfigCacheID": "Step one config cache id",
    #             "StepOneOutputModuleName": "Step one output module name",
    #             "StepThreeConfigCacheID": "Step three config cache id",
    #             "StepTwoConfigCacheID": "Step two config cache id",
    #             "StepTwoOutputModuleName": "Step two output module name",
    #             "Sitewhitelist": "Site white list",
    #             "Siteblacklist": "Site black list",
    #             "Mergedlfnbase": "Merged LFN base",
    #             "Maxrss": "Max RSS",
    #             "Trustsitelists": "Trust site lists",
    #             "Unmergedlfnbase": "Unmerged LFN base",
    #             "Minmergesize": "Min merge size",
    #             "Maxmergesize": "Max merge size",
    #             "Maxmergeevents": "Max merge events",
    #             "Blockclosemaxwaittime": "Block close max wait time",
    #             "Blockclosemaxfiles": "Block close max files",
    #             "Blockclosemaxevents": "Block close max events",
    #             "Blockclosemaxsize": "Block close max size",
    #             "Softtimeout": "Soft time out",
    #             "Graceperiod": "Grace period"
    #             }
    return maps


def dqm_urls():
    "Return list of DQM urls"
    urls = [
        "https://cmsweb.cern.ch/dqm/dev",
        "https://cmsweb.cern.ch/dqm/offline",
        "https://cmsweb.cern.ch/dqm/relval",
        "https://cmsweb-testbed.cern.ch/dqm/dev",
        "https://cmsweb-testbed.cern.ch/dqm/offline",
        "https://cmsweb-testbed.cern.ch/dqm/relval",
    ]
    return urls


def dbs_urls():
    "Return list of DBS urls"
    urls = []
    base = "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader"
    for inst in ["prod", "phys01", "phys02", "phys03"]:
        urls.append(base.replace("prod", inst))
    return urls


def couch_url():
    "Return central couch url"
    url = "https://cmsweb.cern.ch/couchdb"
    return url


def releases(arch=None):
    "Return list of CMSSW releases"
    return TC.releases(arch)


def architectures():
    "Return list of CMSSW architectures"
    return TC.architectures()


def scenarios():
    "Return list of scenarios"
    slist = ["pp", "cosmics", "hcalnzs", "preprodmc", "prodmc"]
    return slist


def cms_groups():
    "Return list of CMS data-ops groups"
    groups = ["DATAOPS"]
    return groups


def dashboardActivities():
    "Return list of dashboard activities"
    activity = ['reprocessing', 'production', 'relval', 'harvesting',
                'storeresults', 'integration', 'test']
    return activity
