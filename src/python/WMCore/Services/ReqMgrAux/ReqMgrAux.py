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

    def __init__(self, url, httpDict=None, logger=None):
        """
        responseType will be either xml or json
        """

        httpDict = httpDict or {}
        # url is end point
        httpDict['endpoint'] = "%s/data" % url
        httpDict['logger'] = logger if logger else logging.getLogger()

        # cherrypy converts request.body to params when content type is set
        # application/x-www-form-urlencodeds
        httpDict.setdefault("content_type", 'application/json')
        httpDict.setdefault('cacheduration', 0)
        self.cacheExpire = httpDict['cacheduration']
        httpDict.setdefault("accept_type", "application/json")
        self.encoder = json.dumps
        Service.__init__(self, httpDict)
        # This is only for the unittest: never set it true unless it is unittest
        self._noStale = False

    def _getResult(self, callname, clearCache=False, args=None, verb="GET",
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
        cache = MemoryCacheStruct(expire=self.cacheExpire, func=self._getResult, initCacheValue={},
                                  logger=self['logger'], kwargs={'callname': callname, "verb": "GET"})
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

    def _updateRecords(self, callName, resource, kwparams):
        """
        Fetches the original document, locally update it according to
        the key/value pairs provided and update the document.
        :param callName: resource to be requested
        :param resource: name of the resource/document to be updated
        :param kwparams: a dictionary with the content to be updated
        :return: a dictionary with the CouchDB response
        """
        apiMap = {'wmagentconfig': self.getWMAgentConfig,
                  'campaignconfig': self.getCampaignConfig,
                  'transferinfo': self.getTransferInfo}

        thisDoc = apiMap[callName](resource)
        # getWMAgentConfig method returns directly the document, while the others
        # return a list of document(s)
        if isinstance(thisDoc, (list, set)):
            thisDoc = thisDoc[0]
        thisDoc.update(kwparams)
        return self["requests"].put("%s/%s" % (callName, resource), thisDoc)[0]['result']

    def populateCMSSWVersion(self, tcUrl, **kwargs):
        """
        Query TagCollector and update the CMSSW versions document in Couch
        :return: a boolean with the result of the operation
        """
        from WMCore.Services.TagCollector.TagCollector import TagCollector
        cmsswVersions = TagCollector(tcUrl, **kwargs).releases_by_architecture()
        resp = self["requests"].put('cmsswversions', cmsswVersions)[0]['result']

        if resp and resp[0].get("ok", False):
            self["logger"].info("CMSSW document successfuly updated.")
            return True

        msg = "Failed to update CMSSW document. Response: %s" % resp
        self["logger"].warning(msg)
        return False

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

    def updateWMAgentConfig(self, agentName, content, inPlace=False):
        """
        Update the corresponding agent configuration with the content
        provided, replacing the old document.
        If inPlace is set to True, then a modification of the current
        document is performed, according to the key/value pairs provided.
        :param agentName: name of the agent/document in couch
        :param content: a dictionary with the data to be updated
        :param inPlace: a boolean defining whether to perform a replace
        or a update modification
        :return: a boolean with the result of the operation
        """
        api = 'wmagentconfig'
        if inPlace:
            resp = self._updateRecords(api, agentName, content)
        else:
            resp = self["requests"].put("%s/%s" % (api, agentName), content)[0]['result']

        if resp and resp[0].get("ok", False):
            self["logger"].info("Update in-place: %s for agent: %s was successful.", inPlace, agentName)
            return True

        msg = "Failed to update agent: %s in-place: %s. Response: %s" % (agentName, inPlace, resp)
        self["logger"].warning(msg)
        return False

    def getCampaignConfig(self, campaignName):
        """
        get campaign config for transferor function in unified ReqMgr2MS.
        """
        return self._getDataFromMemoryCache('campaignconfig/%s' % campaignName)

    def postCampaignConfig(self, campaignName, campaignConfig):
        """
        Create a new campaign configuration document

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

        :return: CouchDB response dictionary
        """
        return self["requests"].post('campaignconfig/%s' % campaignName, campaignConfig)[0]['result']

    def updateCampaignConfig(self, campaignName, content, inPlace=False):
        """
        Update the corresponding campaign configuration with the content
        provided, replacing the old document.
        If inPlace is set to True, then a modification of the current
        document is performed, according to the key/value pairs provided.
        :param campaignName: name of the campaign document in couch
        :param content: a dictionary with the data to be updated
        :param inPlace: a boolean defining whether to perform a replace
            or a update modification
        :return: a boolean with the result of the operation
        """
        api = 'campaignconfig'
        if inPlace:
            resp = self._updateRecords(api, campaignName, content)
        else:
            resp = self["requests"].put("%s/%s" % (api, campaignName), content)[0]['result']

        if resp and resp[0].get("ok", False):
            self["logger"].info("Update in-place: %s for campaign: %s was successful.", inPlace, campaignName)
            return True
        msg = "Failed to update campaign: %s in-place: %s. Response: %s" % (campaignName, inPlace, resp)
        self["logger"].warning(msg)
        return False

    def getUnifiedConfig(self, docName=None):
        """
        Retrieve a Unified document type from the auxiliary db.
        If docName is not provided, then fetch the default UNIFIED_CONFIG doc
        """
        if docName:
            unifiedConfig = self._getDataFromMemoryCache('unifiedconfig/%s' % docName)
        else:
            unifiedConfig = self._getDataFromMemoryCache('unifiedconfig')

        if not unifiedConfig:
            self["logger"].warning("Unified configuration document not found. Result: %s", unifiedConfig)

        return unifiedConfig

    def postUnifiedConfig(self, unifiedConfig, docName=None):
        """
        Create a new unified configuration document
        """
        if docName:
            return self["requests"].post('unifiedconfig/%s' % docName, unifiedConfig)[0]['result']
        return self["requests"].post('unifiedconfig', unifiedConfig)[0]['result']

    def updateUnifiedConfig(self, content, docName=None):
        """
        Update the unified configuration with the content provided, replacing
        the old document.
        :param content: a dictionary with the data to be updated
        :return: a boolean with the result of the operation
        """
        api = 'unifiedconfig'
        if docName:
            resp = self["requests"].put("%s/%s" % (api, docName), content)[0]['result']
        else:
            resp = self["requests"].put("%s" % api, content)[0]['result']

        if resp and resp[0].get("ok", False):
            self["logger"].info("Unified configuration successfully updated.")
            return True

        self["logger"].warning("Failed to update the unified configuration. Response: %s", resp)
        return False

    def getTransferInfo(self, docName):
        """
        get a workflow transfer document, to be used by unified ReqMgr2MS.
        """
        return self._getDataFromMemoryCache('transferinfo/%s' % docName)

    def postTransferInfo(self, docName, transferInfo):
        """
        Create a new workflow transfer document

        :param docName: the name of the document to be created
        :param transferInfo: a dictionary with the document content
        :return: CouchDB response dictionary
        """
        return self["requests"].post('transferinfo/%s' % docName, transferInfo)[0]['result']

    def updateTransferInfo(self, docName, content, inPlace=False):
        """
        Update the corresponding workflow transfer document with the content
        provided, replacing the old one.
        If inPlace is set to True, then a modification of the current
        document is performed, according to the key/value pairs provided.
        :param docName: name of the transfer document in couch
        :param content: a dictionary with the data to be updated
        :param inPlace: a boolean defining whether to perform a replace
            or a update modification
        :return: a boolean with the result of the operation
        """
        api = 'transferinfo'
        if inPlace:
            resp = self._updateRecords(api, docName, content)
        else:
            resp = self["requests"].put("%s/%s" % (api, docName), content)[0]['result']

        if resp and resp[0].get("ok", False):
            self["logger"].info("Update in-place: %s for transfer doc: %s was successful.", inPlace, docName)
            return True
        msg = "Failed to update transfer doc: %s in-place: %s. Response: %s" % (docName, inPlace, resp)
        self["logger"].warning(msg)
        return False

    def updateParentLocks(self, content, docName=None):
        """
        Update the list of locked parent datasets with the content provided, replacing
        the old document.
        :param content: a dictionary with the data to be updated
        :return: a boolean with the result of the operation
        """
        api = 'parentlocks'
        if docName:
            resp = self["requests"].put("%s/%s" % (api, docName), content)[0]['result']
        else:
            resp = self["requests"].put("%s" % api, content)[0]['result']

        if resp and resp[0].get("ok", False):
            self["logger"].info("Parent dataset locks successfully updated.")
            return True

        self["logger"].warning("Failed to update parent dataset locks. Response: %s", resp)
        return False

    def getParentLocks(self):
        """
        get the list of parent locks
        """
        return self._getDataFromMemoryCache('parentlocks')

    def deleteConfigDoc(self, docType, docName):
        """
        Given a document type and a document name, delete it from the
        Couch auxiliary DB.
        :param docType: a string with the document type to be deleted, which
            corresponds to the same API name
        :param docName: the name of the document to be deleted
        :return: response dictionary from the couch operation
        """
        allowedValues = ('wmagentconfig', 'campaignconfig', 'unifiedconfig', 'transferinfo')
        if docType not in allowedValues:
            msg = "Document type: '%s' not allowed for deletion." % docType
            msg += " Supported documents are: %s" % allowedValues
            self["logger"].warning(msg)
        else:
            return self["requests"].delete('%s/%s' % (docType, docName))[0]['result']


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
        if agentConfig and (agentDrainMode != agentConfig["AgentDrainMode"]):
            reqMgrAux.updateWMAgentConfig(config.Agent.hostName, {"AgentDrainMode": agentDrainMode},
                                          inPlace=True)

    return overThresholdDisks
