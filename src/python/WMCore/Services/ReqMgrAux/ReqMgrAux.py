from __future__ import division, print_function

import json
import logging

from Utils.Utilities import diskUse
from WMCore.Cache.GenericDataCache import MemoryCacheStruct
from WMCore.Services.Service import Service


class ReqMgrAux(Service):
    """
    API for dealing with retrieving information from RequestManager dataservice

    """

    def __init__(self, url, header=None, logger=None):
        """
        responseType will be either xml or json
        """

        httpDict = {}
        header = header or {}
        # url is end point
        httpDict['endpoint'] = "%s/data" % url
        httpDict['logger'] = logger if logger else logging.getLogger()

        # cherrypy converts request.body to params when content type is set
        # application/x-www-form-urlencodeds
        httpDict.setdefault("content_type", 'application/json')
        httpDict.setdefault('cacheduration', 0)
        httpDict.setdefault("accept_type", "application/json")
        httpDict.update(header)
        self.encoder = json.dumps
        Service.__init__(self, httpDict)
        # This is only for the unittest: never set it true unless it is unittest
        self._noStale = False

    def _getResult(self, callname, clearCache=True, args=None, verb="GET",
                   encoder=json.loads, decoder=json.loads, contentType=None):
        """
        _getResult_
        """
        cfile = callname.replace("/", "_")
        if clearCache:
            self.clearCache(cfile, args, verb)

        f = self.refreshCache(cfile, callname, args, encoder=encoder,
                              verb=verb, contentType=contentType)
        result = f.read()
        f.close()

        if result and decoder:
            result = decoder(result)

        return result['result']

    def _getDataFromMemoryCache(self, callname):
        cache = MemoryCacheStruct(expire=0, func=self._getResult, initCacheValue={},
                                  kwargs={'callname': callname, "verb": "GET"})
        return cache.getData()

    def getCMSSWVersion(self):
        """
        get dictionary format of architecture and cmssw versions
        i.e.
        {"slc5_amd64_gcc462": [
          "CMSSW_5_3_4_patch1",
          "CMSSW_5_2_9",
          "CMSSW_5_3_4_patch2",
          "CMSSW_5_3_3_cand1_patch1"],
          ...
          }
        """
        return self._getDataFromMemoryCache('cmsswversions')

    def updateRecords(self, callname, kwparams):

        return self["requests"].put(callname, kwparams)[0]['result']

    def populateCMSSWVersion(self, tc_url, **kwargs):
        """
        query TagCollector and populate cmsswversions
        """
        from WMCore.Services.TagCollector.TagCollector import TagCollector
        cmsswVersions = TagCollector(tc_url, **kwargs).releases_by_architecture()

        return self["requests"].post('cmsswversions', cmsswVersions)[0]['result']

    def getWMAgentConfig(self, agentName):
        """
        retrieve agent configuration reqmgr aux db.
        """
        agentConfig = self._getDataFromMemoryCache('wmagentconfig/%s' % agentName)

        if len(agentConfig) != 1:
            # something wrong with database record. returns default value for both case.
            self["logger"].warning("agent config is not correct: %s" % agentConfig)
            return {}

        return agentConfig[0]

    def postWMAgentConfig(self, agentName, agentConfig):
        """
        Create a new WMAgent configuration file in ReqMgrAux.
        If document already exists, nothing happens.
        """
        return self["requests"].post('wmagentconfig/%s' % agentName, agentConfig)[0]['result']

    def deleteWMAgentConfig(self, agentName):
        """Mind your own business. Delete the agent config from ReqMgrAux"""
        self["requests"].delete('wmagentconfig/%s' % agentName)

    def updateAgentConfig(self, agentName, key, value):
        # update config DB
        resp = self.updateRecords('wmagentconfig/%s' % agentName, {key: value})

        if len(resp) == 1 and resp[0].get("ok", False):
            self["logger"].info("update agent key %s to %s successful." % (key, value))
            return True
        else:
            self["logger"].warning("update agent config failed: %s should be %s, response: %s" %
                                   (key, value, resp))
            return False

    def getCampaignConfig(self, campaignName):
        """
        get campaign config for transferor function in unified ReqMgr2MS.
        """
        return self._getDataFromMemoryCache('campaignconfig/%s' % campaignName)

    def postCampaignConfig(self, campaignName, campaignConfig):
        """

        :param campaignName
        :type basestringg
        :param campaignConfig:
        :type dict - only can replace whole campaign  no partial parameters

        campaignName: "HIRun2015":
        campaignConfig: {
            "go": true,
            "labels" : ["02May2016","25Aug2016"],
            "overflow" : {"PRIM" : {}},
            "DDMcopies": {
                         "all" : { "N" : 2 }
            },
            "custodial_override" : ["DQMIO"],
            "fractionpass": 1.0,
            "lumisize" : -1,
            "maxcopies" : 1,
            "custodial": "T1_FR_CCIN2P3_MSS",
            "parameters" :{
              "NonCustodialSites" : ["T2_US_Vanderbilt"],
              "SiteBlacklist": [
              "T1_US_FNAL",
              "T2_US_Purdue",
              "T2_US_Caltech",
              "T2_US_Florida",
              "T2_US_Nebraska",
              "T2_US_UCSD",
              "T2_US_Wisconsin"
             ]
           }
          }

        :return: None
        """
        return self["requests"].post('campaignconfig/%s' % campaignName, campaignConfig)[0]['result']

    def updateCampaignConfig(self, campaignName, propDict):
        # update config DB
        resp = self.updateRecords('campaignconfig/%s' % campaignName, propDict)

        return bool(len(resp) == 1 and resp[0].get("ok", False))


AUXDB_AGENT_CONFIG_CACHE = {}


# function to check whether agent is should be in draining status.
def isDrainMode(config):
    """
    config is loaded from WMAgentConfig in local config
    agentDrainMode is boolean value. (if it is passed update the WMAgentConfig in couchdb)
    """
    if hasattr(config, "Tier0Feeder"):
        return False

    global AUXDB_AGENT_CONFIG_CACHE

    reqMgrAux = ReqMgrAux(config.General.ReqMgr2ServiceURL)
    agentConfig = reqMgrAux.getWMAgentConfig(config.Agent.hostName)
    if "UserDrainMode" in agentConfig and "AgentDrainMode" in agentConfig:
        AUXDB_AGENT_CONFIG_CACHE = agentConfig
        return agentConfig["UserDrainMode"] or agentConfig["AgentDrainMode"]
    else:
        # if the cache is empty this will raise Key not exist exception.
        return AUXDB_AGENT_CONFIG_CACHE["UserDrainMode"] or AUXDB_AGENT_CONFIG_CACHE["AgentDrainMode"]

def listDiskUsageOverThreshold(config, updateDB):
    """
    check whether disk usage is over threshold,
    return list of disk paths
    if updateDB is True update the aux couch db value.
    This function contains both check an update to avoide multiple db calls.
    """
    defaultDiskThreshold = 85
    defaultIgnoredDisks = []
    if hasattr(config, "Tier0Feeder"):
        # get the value from config.
        ignoredDisks = getattr(config.AgentStatusWatcher, "ignoreDisks", defaultIgnoredDisks)
        diskUseThreshold = getattr(config.AgentStatusWatcher, "diskUseThreshold", defaultDiskThreshold)
        t0Flag = True
    else:
        reqMgrAux = ReqMgrAux(config.General.ReqMgr2ServiceURL)
        agentConfig = reqMgrAux.getWMAgentConfig(config.Agent.hostName)
        diskUseThreshold = agentConfig.get("DiskUseThreshold", defaultDiskThreshold)
        ignoredDisks = agentConfig.get("IgnoreDisks", defaultIgnoredDisks)
        t0Flag = False

    # Disk space warning
    diskUseList = diskUse()
    overThresholdDisks = []
    for disk in diskUseList:
        if (float(disk['percent'].strip('%')) >= diskUseThreshold and
                    disk['mounted'] not in ignoredDisks):
            overThresholdDisks.append(disk)

    if updateDB and not t0Flag:
        agentDrainMode = bool(len(overThresholdDisks))
        if agentDrainMode != agentConfig["AgentDrainMode"]:
            reqMgrAux.updateAgentConfig(config.Agent.hostName, "AgentDrainMode", agentDrainMode)

    return overThresholdDisks
