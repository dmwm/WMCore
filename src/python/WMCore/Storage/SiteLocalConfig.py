#!/usr/bin/env python
"""
_SiteLocalConfig_

Utility for reading a site local config XML file and converting it
into an object with an API for getting info from it.

"""


from builtins import next, str, object

import os
import logging

from WMCore.Algorithms.ParseXMLFile import xmlFileToNode
from WMCore.Storage.TrivialFileCatalog import getCatalogString, tfcFilename, tfcProtocol, readTFC, rseName, lfnPrefix


def loadSiteLocalConfig(useTFC=False):
    """
    _loadSiteLocalConfig_

    Runtime Accessor for the site local config.

    Requires that SITECONFIG_PATH is defined as an environment variable

    """
    overVarName = "WMAGENT_SITE_CONFIG_OVERRIDE"
    if os.getenv(overVarName, None):
        overridePath = os.getenv(overVarName)
        if os.path.exists(os.getenv(overVarName, None)):
            m = ("Using site-local-config.xml override due to set %s env. variable, "
                 "loading: '%s'" % (overVarName, overridePath))
            logging.log(logging.DEBUG, m)
            config = SiteLocalConfig(overridePath,useTFC)
            return config
        else:
            msg = "%s env. var. provided but not pointing to an existing file, ignoring." % overVarName
            logging.log(logging.ERROR, msg)

    defaultPath = "$SITECONFIG_PATH/JobConfig/site-local-config.xml"
    actualPath = os.path.expandvars(defaultPath)
    if os.environ.get("SITECONFIG_PATH", None) is None:
        msg = "Unable to find site local config file:\n"
        msg += "SITECONFIG_PATH variable is not defined."
        raise SiteConfigError(msg)

    if not os.path.exists(actualPath):
        msg = "Unable to find site local config file:\n"
        msg += actualPath
        raise SiteConfigError(msg)

    config = SiteLocalConfig(actualPath,useTFC)
    return config

def makeStorageAttribute(siteName,subSiteName,storageSiteName,volume,protocol):
    return {'site':siteName,'subSite':subSiteName,'storageSite':storageSiteName,'volume':volume,'protocol':protocol}

class SiteConfigError(Exception):
    """
    Exception class placeholder

    """
    pass


class SiteLocalConfig(object):
    """
    _SiteLocalConfig_

    Readonly API object for getting info out of the SiteLocalConfig file

    """
    def __init__(self, siteConfigXML, useTFC):
        self.useTFC = useTFC #switch to use old TFC or new Rucio
        self.siteConfigFile = siteConfigXML
        self.siteName = None
        self.subSiteName = None
        self.eventData = {}

        self.frontierProxies = []
        self.frontierServers = []

        self.localStageOut = {}
        self.fallbackStageOut = []

        self.read()
        return

    def trivialFileCatalog(self):
        """
        _trivialFileCatalog_

        Return an instance of FwkJobRep.TrivialFileCatalog

        """
        tfcUrl = self.localStageOut.get('catalog', None)
        if tfcUrl is None:
            return None
        try:
            tfcFile = tfcFilename(tfcUrl)
            tfcProto = tfcProtocol(tfcUrl,self.useTFC)
            storageAtt = None
            if not self.useTFC:
                aVolume = tfcUrl.split('?')[1].split('&')[1].replace('volume=','') 
                storageAtt = makeStorageAttribute(self.siteName,self.subSiteName,self.localStageOut.get('storageSite',None),aVolume,tfcProto)
            tfcInstance = readTFC(tfcFile,storageAtt,self.useTFC)
            tfcInstance.preferredProtocol = tfcProto
        except Exception as ex:
            msg = "Unable to load TrivialFileCatalog:\n"
            msg += "URL = %s\n" % tfcUrl
            msg += str(ex)
            raise SiteConfigError(msg)
        return tfcInstance

    def localStageOutCommand(self):
        """
        _localStageOutCommand_

        Return the stage out command setting from local-stage-out

        """
        return self.localStageOut['command']

    def localStageOutOption(self):
        """
        _localStageOutOption_

        Return the stage out option setting from local-stage-out
        """
        return self.localStageOut['option']

    def localStageOutPNN(self):
        """
        _localStageOutPNN_

        return the local PhEDExNodeName used for stage out

        """
        return self.localStageOut['phedex-node']


    def read(self):
        """
        _read_

        Load data from SiteLocal Config file and populate this object

        """
        try:
            node = xmlFileToNode(self.siteConfigFile)
        except Exception as ex:
            msg = "Unable to read SiteConfigFile: %s\n" % self.siteConfigFile
            msg += str(ex)
            raise SiteConfigError(msg)

        nodeResult = nodeReader(node,self.useTFC)

        if 'siteName' not in nodeResult:
            msg = "Unable to find site name in SiteConfigFile:\n"
            msg += self.siteConfigFile
            raise SiteConfigError(msg)
        if 'catalog' not in nodeResult:
            msg = "Unable to find catalog entry for event data in SiteConfigFile:\n"
            msg += self.siteConfigFile
            raise SiteConfigError(msg)
        if 'localStageOut' not in nodeResult:
            if self.useTFC:
                msg = "Error:Unable to find any local-stage-out"
            else:
                msg = "Error:Unable to find any stage-out"
            msg += " information in:\n"
            msg += self.siteConfigFile
            raise SiteConfigError(msg)

        self.siteName             = nodeResult.get('siteName', None)
        self.subsiteName          = nodeResult.get('subSiteName', None)
        self.eventData['catalog'] = nodeResult.get('catalog', None)
        self.localStageOut        = nodeResult.get('localStageOut', [])
        self.fallbackStageOut     = nodeResult.get('fallbackStageOut', [])
        self.frontierServers      = nodeResult.get('frontierServers', [])
        self.frontierProxies      = nodeResult.get('frontierProxies', [])
        return

def coroutine(func):
    """
    _coroutine_

    Decorator method used to prime coroutines

    """
    def start(*args, **kwargs):
        cr = func(*args, **kwargs)
        next(cr)
        return cr
    return start



def nodeReader(node,useTFC):
    """
    _nodeReader_

    Given a node, see if we can find what we're looking for
    """
    processSiteInfo = {
        'event-data': processEventData(),
        'local-stage-out': processLocalStageOut(),
        'calib-data': processCalibData(),
        'fallback-stage-out': processFallbackStageOut()
        }

    report = {}
    sProcess = processSite(processSiteInfo)
    processor = processNode(sProcess)
    processor.send((report, node, useTFC))

    return report

@coroutine
def processNode(target):
    """
    Starts at the top of the tree and finds the site
    """
    while True:
        report, node, useTFC = (yield)
        for subnode in node.children:
            if subnode.name == 'site-local-config':
                for child in subnode.children:
                    if child.name == 'site':
                        target.send((report, child, useTFC))


@coroutine
def processSite(targets):
    """
    Process the site tree in a config.

    """

    while True:
        report, node, useTFC = (yield)
        #Get the name first
        report['siteName'] = node.attrs.get('name', None)
        for subnode in node.children:
            if subnode.name == 'subsite':
                report['subSiteName'] = subnode.attrs.get('name',None)
            if subnode.name == 'event-data':
                targets['event-data'].send((report, subnode))
            elif subnode.name == 'calib-data':
                targets['calib-data'].send((report, subnode))
            elif useTFC and subnode.name == 'local-stage-out':
                targets['local-stage-out'].send((report,subnode,useTFC))
            elif not useTFC and subnode.name == 'stage-out':
                targets['local-stage-out'].send((report,subnode,useTFC))
                targets['fallback-stage-out'].send((report,subnode,useTFC))
            elif useTFC and subnode.name == 'fallback-stage-out':
                targets['fallback-stage-out'].send((report,subnode,useTFC))


@coroutine
def processEventData():
    """
    Process eventData in a site

    """

    while True:
        report, node = (yield)
        for subnode in node.children:
            if subnode.name == 'catalog':
                report['catalog'] = str(subnode.attrs.get('url', None))

@coroutine
def processLocalStageOut():
    """
    Find the local-stage-out directory

    """

    while True:
        report, node, useTFC = (yield)
        localReport = {}
        if useTFC:
            for subnode in node.children:
                if subnode.name in ['phedex-node', 'command', 'option']:
                    localReport[subnode.name] = subnode.attrs.get('value', None)
                elif subnode.name == 'catalog':
                    localReport[subnode.name] = subnode.attrs.get('url', None)
        else:
            for subnode in node.children:
                #now construct an url as used in trivial file catalog
                subSiteName = None
                if 'subSiteName' in report.keys():
                    subSiteName = report['subSiteName']
                aStorageSite = subnode.attrs.get('site', None)
                if aStorageSite is None: aStorageSite = report['siteName']
                aProtocol = subnode.attrs.get('protocol', None)
                aVolume = subnode.attrs.get('volume', None)
                storageAtt = makeStorageAttribute(report['siteName'],subSiteName,aStorageSite,aVolume,aProtocol) 
                #localReport['catalog'] = 'trivialcatalog_file:'+tfcFilename(None,storageAtt,False)+'?protocol='+aProtocol+'&volume='+aVolume
                localReport['catalog'] = getCatalogString(storageAtt)
                localReport['command'] = subnode.attrs.get('command', None)
                localReport['option'] = subnode.attrs.get('option', None)
                localReport['phedex-node'] = rseName(storageAtt)
                localReport['storageSite'] = aStorageSite
                break #only take the first stageOut, others are in fallbacks
        
        report['localStageOut'] = localReport

#This method is not used currently, for future development
#<stage-out>
#     <method volume="CERN_EOS_T0" protocol="XRootD" command="xrdcp" option="--wma-diablewriterecovery"/>
#</stage-out>
@coroutine
def processStageOut():
    """
    Find the stage-out directory

    """
    while True:
        report, node = (yield)
        #store multiple stage-out instances in <stage-out> of site-local-config.xml
        localReport = []
        for subnode in node.children:
            tmp = {}
            tmp['site'] = subnode.attrs.get('site', None)
            tmp['volume'] = subnode.attrs.get('volume', None)
            tmp['protocol'] = subnode.attrs.get('protocol', None)
            tmp['command'] = subnode.attrs.get('command', None)
            tmp['option'] = subnode.attrs.get('option', None)
            localReport.append(tmp)
        report['stageOut'] = localReport


@coroutine
def processFallbackStageOut():
    """
    Find the fallback stage out directory

    """

    while True:
        report, node, useTFC = (yield)
        if useTFC:
            localReport = {}
            for subnode in node.children:
                if subnode.name in ['phedex-node', 'command', 'option', 'lfn-prefix']:
                    localReport[subnode.name] = subnode.attrs.get('value', None)
            report['fallbackStageOut'] = [localReport]
        else:
            #fallback stageOut starts from the second method
            report['fallbackStageOut'] = []
            for subnode in node.children[1:]:
                subSiteName = report['subSiteName'] if 'subSiteName' in report.keys() else None
                aStorageSite = subnode.attrs.get('site', None)
                if aStorageSite is None: aStorageSite = report['siteName']
                aProtocol = subnode.attrs.get('protocol', None)
                aVolume = subnode.attrs.get('volume', None)
                storageAtt = makeStorageAttribute(report['siteName'],subSiteName,aStorageSite,aVolume,aProtocol)
                lfnPrefixes = lfnPrefix(storageAtt)
                for pre in lfnPrefixes:
                    localReport = {}
                    localReport['command'] = subnode.attrs.get('command', None)
                    localReport['option'] = subnode.attrs.get('option', None)
                    localReport['phedex-node'] = rseName(storageAtt)
                    localReport['lfn-prefix'] = pre
                    report['fallbackStageOut'].append(localReport)

@coroutine
def processCalibData():
    """
    Process calib-data

    """

    while True:
        report, node = (yield)

        frontierProxies = []
        frontierServers = []
        for subnode in node.children:
            if subnode.name == "frontier-connect":
                for frontierSubnode in subnode.children:
                    subNodeUrl = frontierSubnode.attrs.get("url", None)

                    if frontierSubnode.name == "proxy":
                        frontierProxies.append(subNodeUrl)
                    elif frontierSubnode.name == "server":
                        frontierServers.append(subNodeUrl)

        report["frontierProxies"] = frontierProxies
        report["frontierServers"] = frontierServers
