#!/usr/bin/env python
"""
_SiteLocalConfig_

Utility for reading a site local config XML file and converting it
into an object with an API for getting info from it.

"""

import logging
import os

from builtins import next, str, object

from WMCore.Algorithms.ParseXMLFile import xmlFileToNode
from WMCore.Storage.RucioFileCatalog import rseName


def loadSiteLocalConfig():
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
            config = SiteLocalConfig(overridePath)
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

    config = SiteLocalConfig(actualPath)
    return config


def makeStorageAttribute(siteName, subSiteName, storageSiteName, volume, protocol):
    return {'site': siteName, 'subSite': subSiteName, 'storageSite': storageSiteName, 'volume': volume,
            'protocol': protocol}


# return a string of a stage out
def stageOutStr(stageOut):
    msg = ""
    for sTmp in ['storageSite', 'volume', 'protocol', 'command', 'options']:
        msg += sTmp + ': ' + str(stageOut.get(sTmp)) + ', '
    msg += 'phedex-node: ' + str(stageOut.get('phedex-node'))
    return msg


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

    def __init__(self, siteConfigXML):
        self.siteConfigFile = siteConfigXML
        self.siteName = None
        self.subSiteName = None
        self.eventData = {}

        self.frontierProxies = []
        self.frontierServers = []

        self.localStageOut = {}
        self.stageOuts = []
        self.fallbackStageOut = []

        self.read()
        return

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

        nodeResult = nodeReader(node)

        if 'siteName' not in nodeResult:
            msg = "Unable to find site name in SiteConfigFile:\n"
            msg += self.siteConfigFile
            raise SiteConfigError(msg)
        if 'catalog' not in nodeResult:
            msg = "Unable to find catalog entry for event data in SiteConfigFile:\n"
            msg += self.siteConfigFile
            raise SiteConfigError(msg)
        if 'localStageOut' not in nodeResult:
            msg = "Error:Unable to find any local-stage-out"
            msg += " information in:\n"
            msg += self.siteConfigFile
            raise SiteConfigError(msg)
        if 'stageOuts' not in nodeResult:
            msg = "Error:Unable to find any stage-out"
            msg += " information in:\n"
            msg += self.siteConfigFile
            raise SiteConfigError(msg)

        self.siteName = nodeResult.get('siteName', None)
        self.subSiteName = nodeResult.get('subSiteName', None)
        self.eventData['catalog'] = nodeResult.get('catalog', None)
        self.localStageOut = nodeResult.get('localStageOut', [])
        self.stageOuts = nodeResult.get('stageOuts', [])
        self.fallbackStageOut = nodeResult.get('fallbackStageOut', [])
        self.frontierServers = nodeResult.get('frontierServers', [])
        self.frontierProxies = nodeResult.get('frontierProxies', [])
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


def nodeReader(node):
    """
    _nodeReader_

    Given a node, see if we can find what we're looking for
    """
    processSiteInfo = {
        'event-data': processEventData(),
        'local-stage-out': processLocalStageOut(),
        'stage-out': processStageOut(),
        'calib-data': processCalibData(),
        'fallback-stage-out': processFallbackStageOut()
        }

    report = {}
    sProcess = processSite(processSiteInfo)
    processor = processNode(sProcess)
    processor.send((report, node))

    return report


@coroutine
def processNode(target):
    """
    Starts at the top of the tree and finds the site
    """
    while True:
        report, node = (yield)
        for subnode in node.children:
            if subnode.name == 'site-local-config':
                for child in subnode.children:
                    if child.name == 'site':
                        target.send((report, child))


@coroutine
def processSite(targets):
    """
    Process the site tree in a config.

    """

    while True:
        report, node = (yield)
        # Get the name first
        report['siteName'] = node.attrs.get('name', None)
        for subnode in node.children:
            if subnode.name == 'subsite':
                report['subSiteName'] = subnode.attrs.get('name', None)
            if subnode.name == 'event-data':
                targets['event-data'].send((report, subnode))
            elif subnode.name == 'calib-data':
                targets['calib-data'].send((report, subnode))
            elif subnode.name == 'local-stage-out':
                targets['local-stage-out'].send((report, subnode))
            elif subnode.name == 'stage-out':
                targets['stage-out'].send((report, subnode))
            elif subnode.name == 'fallback-stage-out':
                targets['fallback-stage-out'].send((report, subnode))


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
        report, node = (yield)
        localReport = {}
        for subnode in node.children:
            if subnode.name in ['phedex-node', 'command', 'option']:
                localReport[subnode.name] = subnode.attrs.get('value', None)
            elif subnode.name == 'catalog':
                localReport[subnode.name] = subnode.attrs.get('url', None)
        report['localStageOut'] = localReport


@coroutine
def processStageOut():
    """
    Find the stage-out directory

    """
    while True:
        report, node = (yield)
        report['stageOuts'] = []
        for subnode in node.children:
            subSiteName = report['subSiteName'] if 'subSiteName' in report.keys() else None
            aStorageSite = subnode.attrs.get('site', None)
            if aStorageSite is None:
                aStorageSite = report['siteName']
            aProtocol = subnode.attrs.get('protocol', None)
            aVolume = subnode.attrs.get('volume', None)

            localReport = {}
            localReport['storageSite'] = aStorageSite
            localReport['command'] = subnode.attrs.get('command', None)
            # use default command='gfal2' when 'command' is not specified
            if localReport['command'] is None:
                localReport['command'] = 'gfal2'
            localReport['option'] = subnode.attrs.get('option', None)
            localReport['volume'] = aVolume
            localReport['protocol'] = aProtocol
            localReport['phedex-node'] = rseName(report["siteName"], subSiteName, aStorageSite, aVolume)
            report['stageOuts'].append(localReport)


@coroutine
def processFallbackStageOut():
    """
    Find the fallback stage out directory

    """

    while True:
        report, node = (yield)
        localReport = {}
        for subnode in node.children:
            if subnode.name in ['phedex-node', 'command', 'option', 'lfn-prefix']:
                localReport[subnode.name] = subnode.attrs.get('value', None)
        report['fallbackStageOut'] = [localReport]


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
