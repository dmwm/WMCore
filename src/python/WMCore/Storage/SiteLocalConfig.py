#!/usr/bin/env python
"""
_SiteLocalConfig_

Utility for reading a site local config XML file and converting it
into an object with an API for getting info from it

"""

import os

from IMProv.IMProvLoader import loadIMProvFile
from IMProv.IMProvQuery import IMProvQuery

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

        #  //
        # // site name
        #//
        nameQ = IMProvQuery("/site-local-config/site")
        nameNodes = nameQ(node)
        if len(nameNodes) == 0:
            msg = "Unable to find site name in SiteConfigFile:\n"
            msg += self.siteConfigFile
            raise SiteConfigError, msg
        self.siteName = str(nameNodes[0].attrs.get("name"))

        #  //
        # // event data (Read Trivial Catalog location)
        #//
        
        catalogQ = IMProvQuery("/site-local-config/site/event-data/catalog")
        catNodes = catalogQ(node)
        if len(catNodes) == 0:
            msg = "Unable to find catalog entry for event data in:\n"
            msg += self.siteConfigFile
            raise SiteConfigError, msg

        self.eventData['catalog'] = str(catNodes[0].attrs.get("url"))

        #  //
        # // local stage out information
        #//
        stageOutQ = IMProvQuery(
            "/site-local-config/site/local-stage-out"
            )
        stageOutNodes = stageOutQ(node)
        if len(stageOutNodes) == 0:
            msg = "Error:Unable to find any local-stage-out"
            msg += "information in:\n"
            msg += self.siteConfigFile
            raise SiteConfigError, msg
        else:
            #  //
            # // Assume single local-stage-out node.
            #//  Extract details from it:
            localSO = stageOutNodes[0]
            self.localStageOut = self.readLocalStageOut(localSO)
            
        #  //
        # // remote stage out information
        #//  Assume that there are N of them, in order of preference
        fallbackQ = IMProvQuery(
            "/site-local-config/site/fallback-stage-out"
            )
        fallbackNodes = fallbackQ(node)
        for fallbackNode in fallbackNodes:
            nodeContent = self.readFallbackStageOut(fallbackNode)
            self.fallbackStageOut.append(nodeContent)
            
        #  //
        # // calib data
        #//
        
        calibQ = IMProvQuery("/site-local-config/site/calib-data/*")
        
        calibNodes = calibQ(node)
        if len(calibNodes) == 0:
            msg = "Unable to find calib data entry in:\n"
            msg += self.siteConfigFile
            raise SiteConfigError, msg
        for calibNode in calibNodes:
            self.calibData[str(calibNode.name)] = \
                      str(calibNode.attrs.get("url"))

        return




    def readLocalStageOut(self, node):
        """
        _readLocalStageOut_

        Extract data from local stage out node, return it as a dictionary

        """
        result = {}
        result.setdefault("catalog", None)
        result.setdefault("se-name", None)
        result.setdefault("command", None)
        result.setdefault("option", None)
        for child in node.children:
            if child.name == "catalog":
                result['catalog'] = str(child.attrs['url'])
                continue
            if child.name == "se-name":
                result['se-name'] = str(child.attrs['value'])
                continue
            if child.name == "command":
                result['command'] = str(child.attrs['value'])
                continue
            if child.name == "option":
                result['option'] = str(child.attrs['value'])
                continue
            
        return result

    def readFallbackStageOut(self, node):
        """
        _readFallbackStageOut_

        Extract data from fallback stage out node, return it as a dictionary

        """
        result = {}
        result.setdefault("lfn-prefix", None)
        result.setdefault("se-name", None)
        result.setdefault("command", None)
        result.setdefault("option", None)
        for child in node.children:
            if child.name == "lfn-prefix":
                result['lfn-prefix'] = str(child.attrs['value'])
                continue
            if child.name == "se-name":
                result['se-name'] = str(child.attrs['value'])
                continue
            if child.name == "command":
                result['command'] = str(child.attrs['value'])
                continue
            if child.name == "option":
                result['option'] = str(child.attrs['value'])
                continue
            
        return result
