from __future__ import division, print_function

import json
import logging

from Utils.Utilities import diskUse
from WMCore.Services.Service import Service
from WMCore.Cache.GenericDataCache import MemoryCacheStruct

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

        return self["requests"].post('wmagentconfig/%s' % agentName, agentConfig)[0]['result']
    
    def updateAgentDrainingMode(self, agentName, drainFlag):
        # update config DB
        resp = self.updateRecords('wmagentconfig/%s' % agentName, {"AgentDrainMode": drainFlag})

        if len(resp) == 1 and resp[0].get("ok", False):
            self["logger"].info("update drain mode suceeded: %s" % drainFlag)
            return True
        else:
            self["logger"].warning("update agent drain mode failed: it should be %s, response: %s" %
                                (drainFlag, resp))
            return False


# function to check whether agent is should be in draining status.
def isDrainMode(config):
    """
    config is loaded from WMAgentConfig in local config
    agentDrainMode is boolean value. (if it is passed update the WMAgentConfig in couchdb)
    """
    if hasattr(config, "Tier0Feeder"):
        return False

    reqMgrAux = ReqMgrAux(config.TaskArchiver.ReqMgr2ServiceURL)
    agentConfig = reqMgrAux.getWMAgentConfig(config.Agent.hostName)

    return agentConfig["UserDrainMode"] or agentConfig["AgentDrainMode"]


def listDiskUsageOverThreshold(config, updateDB):
    """
    check whether disk usage is over threshold,
    return list of disk paths
    if updateDB is True update the aux couch db value.
    This function contains both check an update to avoide multiple db calls.
    """
    reqMgrAux = ReqMgrAux(config.TaskArchiver.ReqMgr2ServiceURL)
    agentConfig = reqMgrAux.getWMAgentConfig(config.Agent.hostName)
    diskUseThreshold = agentConfig["DiskUseThreshold"]
    ignoredDisks = agentConfig["IgnoreDisks"]
    # Disk space warning
    diskUseList = diskUse()
    overThresholdDisks = []
    for disk in diskUseList:
        if (float(disk['percent'].strip('%')) >= diskUseThreshold and
                        disk['mounted'] not in ignoredDisks):
            overThresholdDisks.append(disk)

    if updateDB:
        agentDrainMode = bool(len(overThresholdDisks))
        if agentDrainMode != agentConfig["AgentDrainMode"]:
            reqMgrAux.updateAgentDrainingMode(config.Agent.hostName, agentDrainMode)

    return overThresholdDisks
