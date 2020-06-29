#!/usr/bin/env python
"""
DeleteMgr

Util class to provide delete functionality as an interface object.

Based on StageOutMgr class

"""
from __future__ import print_function

import logging

from WMCore.Storage.Registry import retrieveStageOutImpl
# do we want seperate exceptions - for the moment no
from WMCore.Storage.StageOutError import StageOutFailure
from WMCore.Storage.StageOutError import StageOutInitError
from WMCore.WMException import WMException


# from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig


class DeleteMgrError(WMException):
    """
    _DeleteMgrError_

    Specific exception class to work out file deletion exception details
    """

    def __init__(self, message, **data):
        WMException.__init__(self, message, **data)
        self.data.setdefault("ErrorCode", 60313)
        self.data.setdefault("ErrorType", self.__class__.__name__)


class DeleteMgr:
    """
    _DeleteMgr_

    Object that can be used to delete a set of files
    using TFC or an override.

    """

    def __init__(self, **overrideParams):
        self.override = False
        self.logger = overrideParams.pop("logger", logging.getLogger())
        self.overrideConf = overrideParams
        if overrideParams != {}:
            self.override = True

        #  //
        # // Try an get the TFC for the site
        # //
        self.tfc = None

        from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig

        self.numberOfRetries = 3
        self.retryPauseTime = 600
        self.pnn = None
        self.fallbacks = []

        #  //
        # // If override isnt None, we dont need SiteCfg, if it is
        # //  then we need siteCfg otherwise we are dead.

        if self.override == False:
            self.siteCfg = loadSiteLocalConfig()

        if self.override:
            self.initialiseOverride()
        else:
            self.initialiseSiteConf()

    def initialiseSiteConf(self):
        """
        _initialiseSiteConf_

        Extract required information from site conf and TFC

        """

        implName = self.siteCfg.localStageOut.get("command", None)
        if implName == None:
            msg = "Unable to retrieve local stage out command\n"
            msg += "From site config file.\n"
            msg += "Unable to perform StageOut operation"
            raise StageOutInitError(msg)
        msg = "Local Stage Out Implementation to be used is:"
        msg += "%s\n" % implName

        pnn = self.siteCfg.localStageOut.get("phedex-node", None)
        if pnn == None:
            msg = "Unable to retrieve local stage out phedex-node\n"
            msg += "From site config file.\n"
            msg += "Unable to perform StageOut operation"
            raise StageOutInitError(msg)
        msg += "Local Stage Out PNN to be used is %s\n" % pnn
        catalog = self.siteCfg.localStageOut.get("catalog", None)
        if catalog == None:
            msg = "Unable to retrieve local stage out catalog\n"
            msg += "From site config file.\n"
            msg += "Unable to perform StageOut operation"
            raise StageOutInitError(msg)
        msg += "Local Stage Out Catalog to be used is %s\n" % catalog

        try:
            self.tfc = self.siteCfg.trivialFileCatalog()
            msg += "Trivial File Catalog has been loaded:\n"
            msg += str(self.tfc)
        except Exception as ex:
            msg = "Unable to load Trivial File Catalog:\n"
            msg += "Local stage out will not be attempted\n"
            msg += str(ex)
            raise StageOutInitError(msg)

        self.logger.info(msg)
        self.pnn = pnn
        return

    def initialiseOverride(self):
        """
        _initialiseOverride_

        Extract and verify that the Override parameters are all present

        """
        overrideConf = self.overrideConf
        overrideParams = {
            "command": None,
            "option": None,
            "phedex-node": None,
            "lfn-prefix": None,
        }

        try:
            overrideParams['command'] = overrideConf['command']
            overrideParams['phedex-node'] = overrideConf['phedex-node']
            overrideParams['lfn-prefix'] = overrideConf['lfn-prefix']
        except Exception as ex:
            msg = "Unable to extract Override parameters from config:\n"
            msg += str(overrideConf)
            raise StageOutInitError(msg)
        if 'option' in overrideConf:
            if len(overrideConf['option']) > 0:
                overrideParams['option'] = overrideConf['option']
            else:
                overrideParams['option'] = ""

        msg = "=======Delete Override Initialised:================\n"
        for key, val in overrideParams.items():
            msg += " %s : %s\n" % (key, val)
        msg += "=====================================================\n"
        self.logger.info(msg)
        self.fallbacks.append(overrideParams)
        self.pnn = overrideParams['phedex-node']
        return

    def __call__(self, fileToDelete):
        """
        _operator()_

        Use call to delete a file

        """
        self.logger.info("==>Working on file: %s" % fileToDelete['LFN'])

        lfn = fileToDelete['LFN']
        fileToDelete['PNN'] = self.pnn

        deleteSuccess = False

        #  //
        # // No override => use local-stage-out from site conf
        # //  invoke for all files and check failures/successes
        if not self.override:
            self.logger.info("===> Attempting To Delete.")
            try:
                fileToDelete['PFN'] = self.deleteLFN(lfn)
                deleteSuccess = True
            except Exception as ex:
                self.logger.error("===> Local file deletion failure. Exception:\n%s", str(ex))

        if not deleteSuccess and len(self.fallbacks) > 0:
            #  //
            # // Still here => override start using the fallback stage outs
            # //  If override is set, then that will be the only fallback available
            self.logger.info("===> Attempting To Delete files with fallback.")
            for fallback in self.fallbacks:
                if not deleteSuccess:
                    try:
                        fileToDelete['PFN'] = self.deleteLFN(lfn, fallback)
                        deleteSuccess = True
                    except Exception as ex:
                        continue

        if deleteSuccess:
            msg = "===> Delete Successful:\n"
            msg += "====> LFN: %s\n" % fileToDelete['LFN']
            msg += "====> PFN: %s\n" % fileToDelete['PFN']
            msg += "====> PNN:  %s\n" % fileToDelete['PNN']
            self.logger.info(msg)
            return fileToDelete
        else:
            msg = "Unable to delete file:\n"
            msg += fileToDelete['LFN']
            raise StageOutFailure(msg, **fileToDelete)

    def deleteLFN(self, lfn, override=None):
        """
        deleteLFN

        Given the lfn and local stage out params, invoke the delete

        if override is used the follwoing params should be defined:
        command - the stage out impl plugin name to be used
        option - the option values to be passed to that command (None is allowed)
        lfn-prefix - the LFN prefix to generate the PFN
        phedex-node - the Name of the PNN to which the file is being xferred
        """

        if override:
            command = override['command']
            pfn = "%s%s" % (override['lfn-prefix'], lfn)
        else:
            command = self.siteCfg.localStageOut['command']
            pfn = self.searchTFC(lfn)

        if pfn == None:
            msg = "Unable to match lfn to pfn: \n  %s" % lfn
            raise StageOutFailure(msg, LFN=lfn, TFC=str(self.tfc))

        return self.deletePFN(pfn, lfn, command)

    def deletePFN(self, pfn, lfn, command):
        """
        Delete the given PFN
        """
        try:
            impl = retrieveStageOutImpl(command)
        except Exception as ex:
            msg = "Unable to retrieve impl for file deletion in:\n"
            msg += "Error retrieving StageOutImpl for command named: %s\n" % (
                command,)
            raise StageOutFailure(msg, Command=command,
                                  LFN=lfn, ExceptionDetail=str(ex))
        impl.numRetries = self.numberOfRetries
        impl.retryPause = self.retryPauseTime

        try:
            impl.removeFile(pfn)
        except Exception as ex:
            self.logger.error("Failed to delete file: %s", pfn)
            ex.addInfo(Protocol=command, LFN=lfn, TargetPFN=pfn)
            raise ex

        return pfn

    #    def reportStageOutFailure(self, stageOutExcep):
    #        """
    #        _reportStageOutFailure_
    #
    #        When a stage out failure occurs, report it to the input
    #        framework job report.
    #
    #        - *stageOutExcep* : Instance of on of the StageOutError derived classes
    #
    #        """
    #        errStatus = stageOutExcep.data["ErrorCode"]
    #        errType = stageOutExcep.data["ErrorType"]
    #        desc = stageOutExcep.message
    #
    #        errReport = self.inputReport.addError(errStatus, errType)
    #        errReport['Description'] = desc
    #        return

    def searchTFC(self, lfn):
        """
        _searchTFC_

        Search the Trivial File Catalog for the lfn provided,
        if a match is made, return the matched PFN

        """
        if self.tfc == None:
            msg = "Trivial File Catalog not available to match LFN:\n"
            msg += lfn
            self.logger.error(msg)
            return None
        if self.tfc.preferredProtocol == None:
            msg = "Trivial File Catalog does not have a preferred protocol\n"
            msg += "which prevents local stage out for:\n"
            msg += lfn
            self.logger.error(msg)
            return None

        pfn = self.tfc.matchLFN(self.tfc.preferredProtocol, lfn)
        if pfn == None:
            msg = "Unable to map LFN to PFN:\n"
            msg += "LFN: %s\n" % lfn
            return None

        msg = "LFN to PFN match made:\n"
        msg += "LFN: %s\nPFN: %s\n" % (lfn, pfn)
        self.logger.info(msg)
        return pfn

##if __name__ == '__main__':
##    import StageOut.Impl
##    mgr = StageOutMgr()
##    pfn = "/home/evansde/work/PRODAGENT/work/JobCreator/RelValMinBias-170pre12/Processing/RelValMinBias-170pre12-Processing.tar.gz"
##    lfn = "/store/unmerged/mc/2007/11/13/RelVal-RelValMinBias-1194987281/GEN-SIM-DIGI-RECO/0201/DCCP-FNAL-TEST.dat"

##    mgr.searchTFC(lfn)
##    mgr(LFN = lfn, PFN = pfn, GUID=None)
