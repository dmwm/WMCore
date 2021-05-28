#!/usr/bin/env python
"""
_StageOutMgr_

Util class to provide stage out functionality as an interface object.

Based of RuntimeStageOut.StageOutManager, that should probably eventually
use this class as a basic API
"""
from __future__ import print_function
from builtins import object
from future.utils import viewitems

import logging
# If we don't import them, they cannot be ever used (bad PyCharm!)
import WMCore.Storage.Backends
import WMCore.Storage.Plugins

from WMCore.Storage.DeleteMgr import DeleteMgr
from WMCore.Storage.Registry import retrieveStageOutImpl
from WMCore.Storage.StageOutError import StageOutFailure
from WMCore.Storage.StageOutError import StageOutInitError
from WMCore.WMException import WMException


def stageoutPolicyReport(fileToStage, pnn, command, stageOutType, stageOutExit):
    """
    Prepare some extra information regarding the stage out step (for both prod/analysis jobs).

    NOTE: this information used to be shipped to the old SSB dashboard. I'm unsure
    whether it's provided to any other monitoring system at the moment.
    """
    tempDict = {}
    tempDict['LFN'] = fileToStage['LFN'] if 'LFN' in fileToStage else None
    tempDict['PNN'] = fileToStage['PNN'] if 'PNN' in fileToStage else None
    tempDict['PNN'] = pnn if pnn else tempDict['PNN']
    tempDict['StageOutCommand'] = fileToStage['command'] if 'command' in fileToStage else None
    tempDict['StageOutCommand'] = command if command else tempDict['StageOutCommand']
    tempDict['StageOutType'] = stageOutType
    tempDict['StageOutExit'] = stageOutExit
    fileToStage['StageOutReport'].append(tempDict)
    return fileToStage


class StageOutMgr(object):
    """
    _StageOutMgr_

    Object that can be used to stage out a set of files
    using TFC or an override.

    """

    def __init__(self, **overrideParams):
        logging.info("StageOutMgr::__init__()")
        self.overrideConf = overrideParams

        # Figure out if any of the override parameters apply to stage-out
        self.override = False
        if overrideParams != {}:
            logging.info("StageOutMgr::__init__(): Override: %s", overrideParams)
            checkParams = ["command", "option", "phedex-node", "lfn-prefix"]
            for param in checkParams:
                if param in self.overrideConf:
                    self.override = True
            if not self.override:
                logging.info("=======StageOut Override: These are not the parameters you are looking for")

        self.substituteGUID = True
        self.fallbacks = []

        #  //
        # // Try an get the TFC for the site
        # //
        self.tfc = None

        self.numberOfRetries = 3
        self.retryPauseTime = 600

        from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig

        #  //
        # // If override isnt None, we dont need SiteCfg, if it is
        # //  then we need siteCfg otherwise we are dead.

        if self.override == False:
            self.siteCfg = loadSiteLocalConfig()

        if self.override:
            self.initialiseOverride()
        else:
            self.initialiseSiteConf()

        self.failed = {}
        self.completedFiles = {}
        return

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
        msg = "Local Stage Out Implementation to be used is: "
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

        self.fallbacks = self.siteCfg.fallbackStageOut

        msg += "There are %s fallback stage out definitions.\n" % len(self.fallbacks)
        for item in self.fallbacks:
            pnn = item.get("phedex-node")
            command = item.get("command")
            if pnn is None:
                msg = "Unable to retrieve fallback phedex-node\n"
                msg += "From site config file.\n"
                msg += "Unable to perform StageOut operation"
                raise StageOutInitError(msg)
            if command is None:
                msg = "Unable to retrieve fallback command\n"
                msg += "From site config file.\n"
                msg += "Unable to perform StageOut operation"
                raise StageOutInitError(msg)
            msg += "\tFallback to : %s using: %s \n" % (pnn, command)

        logging.info("==== Stageout configuration start ====")
        logging.info(msg)
        logging.info("==== Stageout configuration finish ====")
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
            msg += str(ex)
            raise StageOutInitError(msg)
        if 'option' in overrideConf:
            if len(overrideConf['option']) > 0:
                overrideParams['option'] = overrideConf['option']
            else:
                overrideParams['option'] = ""

        msg = "=======StageOut Override Initialised:================\n"
        for key, val in viewitems(overrideParams):
            msg += " %s : %s\n" % (key, val)
        msg += "=====================================================\n"
        logging.info(msg)
        self.fallbacks = []
        self.fallbacks.append(overrideParams)
        return

    def __call__(self, fileToStage):
        """
        _operator()_

        Use call to invoke transfers

        """
        lastException = Exception("empty exception")

        logging.info("==>Working on file: %s", fileToStage['LFN'])
        lfn = fileToStage['LFN']

        fileToStage['StageOutReport'] = []
        #  //
        # // No override => use local-stage-out from site conf
        # //  invoke for all files and check failures/successes
        if not self.override:
            logging.info("===> Attempting Local Stage Out.")
            try:
                pfn = self.localStageOut(lfn, fileToStage['PFN'], fileToStage.get('Checksums'))
                fileToStage['PFN'] = pfn
                fileToStage['PNN'] = self.siteCfg.localStageOut['phedex-node']
                fileToStage['StageOutCommand'] = self.siteCfg.localStageOut['command']
                self.completedFiles[fileToStage['LFN']] = fileToStage

                logging.info("===> Stage Out Successful: %s", fileToStage)
                fileToStage = stageoutPolicyReport(fileToStage, None, None, 'LOCAL', 0)
                return fileToStage
            except WMException as ex:
                lastException = ex
                logging.info("===> Local Stage Out Failure for file:")
                logging.info("======>  %s\n", fileToStage['LFN'])
                fileToStage = stageoutPolicyReport(fileToStage, self.siteCfg.localStageOut.get('phedex-node', None),
                                                   self.siteCfg.localStageOut['command'], 'LOCAL', 60311)
            except Exception as ex:
                lastException = StageOutFailure("Error during local stage out",
                                                error=str(ex))
                logging.info("===> Local Stage Out Failure for file:\n")
                logging.info("======>  %s\n", fileToStage['LFN'])
                fileToStage = stageoutPolicyReport(fileToStage, self.siteCfg.localStageOut.get('phedex-node', None),
                                                   self.siteCfg.localStageOut['command'], 'LOCAL', 60311)

        # //
        # // Still here => failure, start using the fallback stage outs
        # //  If override is set, then that will be the only fallback available
        logging.info("===> Attempting %s Fallback Stage Outs", len(self.fallbacks))
        for fallback in self.fallbacks:
            try:
                pfn = self.fallbackStageOut(lfn, fileToStage['PFN'],
                                            fallback, fileToStage.get('Checksums'))
                fileToStage['PFN'] = pfn
                fileToStage['PNN'] = fallback['phedex-node']
                fileToStage['StageOutCommand'] = fallback['command']
                logging.info("attempting fallback")
                self.completedFiles[fileToStage['LFN']] = fileToStage
                if lfn in self.failed:
                    del self.failed[lfn]

                logging.info("===> Stage Out Successful: %s", fileToStage)
                fileToStage = stageoutPolicyReport(fileToStage, None, None, 'FALLBACK', 0)
                return fileToStage
            except Exception as ex:
                fileToStage = stageoutPolicyReport(fileToStage, fallback.get('phedex-node', None),
                                                   fallback['command'], 'FALLBACK', 60310)
                lastException = ex
                continue

        raise lastException

    def fallbackStageOut(self, lfn, localPfn, fbParams, checksums):
        """
        _fallbackStageOut_

        Given the lfn and parameters for a fallback stage out, invoke it

        parameters should contain:

        command - the stage out impl plugin name to be used
        option - the option values to be passed to that command (None is allowed)
        lfn-prefix - the LFN prefix to generate the PFN
        phedex-node - the Name of the PNN to which the file is being xferred

        """
        pfn = "%s%s" % (fbParams['lfn-prefix'], lfn)

        try:
            impl = retrieveStageOutImpl(fbParams['command'])
        except Exception as ex:
            msg = "Unable to retrieve impl for fallback stage out:\n"
            msg += "Error retrieving StageOutImpl for command named: "
            msg += "%s\n" % fbParams['command']
            raise StageOutFailure(msg, Command=fbParams['command'],
                                  LFN=lfn, ExceptionDetail=str(ex))

        impl.numRetries = self.numberOfRetries
        impl.retryPause = self.retryPauseTime

        try:
            impl(fbParams['command'], localPfn, pfn, fbParams.get("option", None), checksums)
        except Exception as ex:
            msg = "Failure for fallback stage out:\n"
            msg += str(ex)
            raise StageOutFailure(msg, Command=fbParams['command'],
                                  LFN=lfn, InputPFN=localPfn, TargetPFN=pfn)

        return pfn

    def localStageOut(self, lfn, localPfn, checksums):
        """
        _localStageOut_

        Given the lfn and local stage out params, invoke the local stage out

        """
        command = self.siteCfg.localStageOut['command']
        options = self.siteCfg.localStageOut.get('option', None)
        pfn = self.searchTFC(lfn)
        protocol = self.tfc.preferredProtocol
        if pfn == None:
            msg = "Unable to match lfn to pfn: \n  %s" % lfn
            raise StageOutFailure(msg, LFN=lfn, TFC=str(self.tfc))

        try:
            impl = retrieveStageOutImpl(command)
        except Exception as ex:
            msg = "Unable to retrieve impl for local stage out:\n"
            msg += "Error retrieving StageOutImpl for command named: %s\n" % (
                command,)
            raise StageOutFailure(msg, Command=command,
                                  LFN=lfn, ExceptionDetail=str(ex))
        impl.numRetries = self.numberOfRetries
        impl.retryPause = self.retryPauseTime

        try:
            impl(protocol, localPfn, pfn, options, checksums)
        except Exception as ex:
            msg = "Failure for local stage out:\n"
            msg += str(ex)
            try:
                import traceback
                msg += traceback.format_exc()
            except AttributeError as ex:
                msg += "Traceback unavailable\n"
            raise StageOutFailure(msg, Command=command, Protocol=protocol,
                                  LFN=lfn, InputPFN=localPfn, TargetPFN=pfn)

        return pfn

    def cleanSuccessfulStageOuts(self):
        """
        _cleanSucessfulStageOuts_

        In the event of a failed stage out, this method can be called to cleanup the
        files that may have previously been staged out so that the job ends in a clear state
        of failure, rather than a partial success


        """
        for lfn, fileInfo in viewitems(self.completedFiles):
            pfn = fileInfo['PFN']
            command = fileInfo['StageOutCommand']
            msg = "Cleaning out file: %s\n" % lfn
            msg += "Removing PFN: %s" % pfn
            msg += "Using command implementation: %s\n" % command
            logging.info(msg)
            delManager = DeleteMgr(**self.overrideConf)
            try:
                delManager.deletePFN(pfn, lfn, command)
            except StageOutFailure as ex:
                msg = "Failed to cleanup staged out file after error:"
                msg += " %s\n%s" % (lfn, str(ex))
                logging.error(msg)

    def searchTFC(self, lfn):
        """
        _searchTFC_

        Search the Trivial File Catalog for the lfn provided,
        if a match is made, return the matched PFN

        """
        if self.tfc == None:
            msg = "Trivial File Catalog not available to match LFN:\n"
            msg += lfn
            logging.error(msg)
            return None
        if self.tfc.preferredProtocol == None:
            msg = "Trivial File Catalog does not have a preferred protocol\n"
            msg += "which prevents local stage out for:\n"
            msg += lfn
            logging.error(msg)
            return None

        pfn = self.tfc.matchLFN(self.tfc.preferredProtocol, lfn)
        if pfn == None:
            msg = "Unable to map LFN to PFN:\n"
            msg += "LFN: %s\n" % lfn
            return None

        msg = "LFN to PFN match made:\n"
        msg += "LFN: %s\nPFN: %s\n" % (lfn, pfn)
        logging.info(msg)
        return pfn
