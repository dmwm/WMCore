#!/usr/bin/env python
"""
_SiteLocalConfig_

Utility for reading a site local config XML file and converting it
into an object with an API for getting info from it

"""

__version__ = "$Revision: 1.2 $"
__revision__ = "$Id: SiteLocalConfig.py,v 1.2 2009/11/19 21:21:21 mnorman Exp $"

import os

from IMProv.IMProvLoader import loadIMProvFile
from IMProv.IMProvQuery import IMProvQuery

from WMCore.Algorithms.ParseXMLFile import Node, xmlFileToNode

from WMCore.Storage.TrivialFileCatalog import tfcFilename, tfcProtocol, readTFC

class SiteConfigError(StandardError):
    """
    Exception class placeholder
    """
    pass


def loadSiteLocalConfig():
    """
    _loadSiteLocalConfig_

    Runtime Accessor for the site local config.

    Requires that CMS_PATH is defined as an environment variable

    """
    defaultPath = "$CMS_PATH/SITECONF/local/JobConfig/site-local-config.xml"
    actualPath = os.path.expandvars(defaultPath)
    if os.environ.get("CMS_PATH", None) == None:
        msg = "Unable to find site local config file:\n"
        msg += "CMS_PATH variable is not defined."
        raise SiteConfigError, msg
    
    if not os.path.exists(actualPath):
        msg = "Unable to find site local config file:\n"
        msg += actualPath
        raise SiteConfigError, msg

    config = SiteLocalConfig(actualPath)
    return config
    

class SiteLocalConfig:
    """
    _SiteLocalConfig_

    Readonly API object for getting info out of the SiteLocalConfig file

    """
    def __init__(self, siteConfigXML):
        self.siteConfigFile = siteConfigXML
        self.siteName = None
        self.eventData = {}
        self.calibData = {}
        self.localStageOut = {}
        self.fallbackStageOut = []
        self.read()


    def trivialFileCatalog(self):
        """
        _trivialFileCatalog_

        Return an instance of FwkJobRep.TrivialFileCatalog

        """
        tfcUrl = self.localStageOut.get('catalog', None)
        if tfcUrl == None:
            return None
        try:
            tfcFile = tfcFilename(tfcUrl)
            tfcProto = tfcProtocol(tfcUrl)
            tfcInstance = readTFC(tfcFile)
            tfcInstance.preferredProtocol = tfcProto
        except StandardError, ex:
            msg = "Unable to load TrivialFileCatalog:\n"
            msg += "URL = %s\n" % tfcUrl
            raise SiteConfigError, msg
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

    def localStageOutSEName(self):
        """
        _localStageOutSEName_

        return the local SE Name used for stage out

        """
        return self.localStageOut['se-name']
    

    def read(self):
        """
        _read_

        Load data from SiteLocal Config file and populate this object

        """
        try:
            node = loadIMProvFile(self.siteConfigFile)
        except StandardError, ex:
            msg = "Unable to read SiteConfigFile: %s\n" % self.siteConfigFile
            msg += str(ex)
            raise SiteConfigError, msg

        node2 = xmlFileToNode(self.siteConfigFile)
        nodeResult =  nodeReader(node2)

        if not nodeResult.has_key('siteName'):
            msg = "Unable to find site name in SiteConfigFile:\n"
            msg += self.siteConfigFile
            raise SiteConfigError, msg
        if not nodeResult.has_key('catalog'):
            msg = "Unable to find catalog entry for event data in SiteConfigFile:\n"
            msg += self.siteConfigFile
            raise SiteConfigError, msg
        if not nodeResult.has_key('localStageOut'):
            msg = "Error:Unable to find any local-stage-out"
            msg += "information in:\n"
            msg += self.siteConfigFile
            raise SiteConfigError, msg
        if not nodeResult.has_key('calib-data'):
            msg = "Unable to find calib data entry in:\n"
            msg += self.siteConfigFile
            raise SiteConfigError, msg

        self.siteName             = nodeResult.get('siteName', None)
        self.eventData['catalog'] = nodeResult.get('catalog', None)
        self.localStageOut        = nodeResult.get('localStageOut', [])
        self.fallbackStageOut     = nodeResult.get('fallbackStageOut', [])
        for entry in nodeResult.get('calib-data', None):
            for event in entry.keys():
                self.calibData[str(event)] = str(entry.get(event, None))


        return




def coroutine(func):
    """
    _coroutine_

    Decorator method used to prime coroutines

    """
    def start(*args,**kwargs):
        cr = func(*args,**kwargs)
        cr.next()
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
        #Get the name first
        report['siteName'] = node.attrs.get('name', None)
        for subnode in node.children:
            if subnode.name == 'event-data':
                targets['event-data'].send((report, subnode))
            elif subnode.name == 'calib-data':
                targets['calib-data'].send((report, subnode))
            elif subnode.name == 'local-stage-out':
                targets['local-stage-out'].send((report, subnode))
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
            if subnode.name == 'se-name':
                localReport['se-name'] = subnode.attrs.get('value', None)
            elif subnode.name == 'command':
                localReport['command'] = subnode.attrs.get('value', None)
            elif subnode.name == 'option':
                localReport['option'] = subnode.attrs.get('value', None)
            elif subnode.name == 'catalog':
                localReport['catalog'] = subnode.attrs.get('url', None)
        report['localStageOut'] = localReport

@coroutine
def processFallbackStageOut():
    """
    Find the processed stage out directory

    """

    while True:
        report, node = (yield)
        localReport = {}
        for subnode in node.children:
            if subnode.name == 'se-name':
                localReport['se-name'] = subnode.attrs.get('value', None)
            elif subnode.name == 'command':
                localReport['command'] = subnode.attrs.get('value', None)
            elif subnode.name == 'option':
                localReport['option'] = subnode.attrs.get('value', None)
            elif subnode.name == 'lfn-prefix':
                localReport['lfn-prefix'] = subnode.attrs.get('value', None)
        report['fallbackStageOut'] = localReport


@coroutine
def processCalibData():
    """
    Process calib-data

    """

    while True:
        report, node = (yield)
        tmpReport = []
        for subnode in node.children:
            tmpReport.append({subnode.name: subnode.attrs.get('url', None)})
        report['calib-data'] = tmpReport

