#!/usr/bin/env python
"""
_StageOutMgr_

Util class to provide stage out functionality as an interface object.

Based of RuntimeStageOut.StageOutManager, that should probably eventually
use this class as a basic API
"""
import logging

from builtins import object
from future.utils import viewitems
from WMCore.Storage.DeleteMgr import DeleteMgr
from WMCore.Storage.Registry import retrieveStageOutImpl
from WMCore.Storage.RucioFileCatalog import storageJsonPath, readRFC
from WMCore.Storage.SiteLocalConfig import stageOutStr
from WMCore.Storage.StageOutError import StageOutFailure
from WMCore.Storage.StageOutError import StageOutInitError
from WMCore.WMException import WMException
# If we don't import them, they cannot be ever used (bad PyCharm!)
import WMCore.Storage.Backends
import WMCore.Storage.Plugins


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


def searchRFC(rfc, lfn):
    """
    _searchRFC_

    Search the Rucio File Catalog for the lfn provided,
    if a match is made, return the matched PFN

    """
    if rfc == None:
        msg = "Rucio File Catalog not available to match LFN:\n"
        msg += lfn
        logging.error(msg)
        return None
    if rfc.preferredProtocol == None:
        msg = "Rucio File Catalog: " + str(rfc) + "does not have a preferred protocol\n"
        msg += "which prevents stage out for:\n"
        msg += lfn
        logging.error(msg)
        return None

    pfn = rfc.matchLFN(rfc.preferredProtocol, lfn)
    if pfn == None:
        msg = "Unable to map LFN to PFN:\n"
        msg += "LFN: %s\n" % lfn
        msg += "using this Rucio File Catalog\n"
        msg += str(rfc)
        logging.error(msg)
        return None

    msg = "LFN to PFN match made:\n"
    msg += "LFN: %s\nPFN: %s\n" % (lfn, pfn)
    logging.info(msg)

    return pfn


class StageOutMgr(object):
    """
    _StageOutMgr_

    Object that can be used to stage out a set of files
    using RFC or an override.

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

        self.stageOuts_rfcs = []  # pairs of stageOut and Rucio file catalog

        self.numberOfRetries = 3
        self.retryPauseTime = 600

        from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig

        #  //
        # // If override isnt None, we dont need SiteCfg, if it is
        # //  then we need siteCfg otherwise we are dead.

        if self.override == False:
            self.siteCfg = loadSiteLocalConfig()
            self.initialiseSiteConf()
        else:
            self.initialiseOverride()

        self.failed = {}
        self.completedFiles = {}
        return

    def initialiseSiteConf(self):
        """
        _initialiseSiteConf_

        Extract required information from site conf and RFC

        """
        logging.info("==== Stageout configuration start ====")

        self.stageOuts = self.siteCfg.stageOuts

        logging.info("\nThere are %s stage out definitions." % len(self.stageOuts))
        for stageOut in self.stageOuts:
            foundNoneAttr = False
            for k in ['phedex-node', 'command', 'storageSite', 'volume', 'protocol']:
                v = stageOut.get(k)
                if v is None:
                    amsg = ""
                    amsg += "Unable to retrieve " + k + " of this stageOut: \n"
                    amsg += stageOutStr(stageOut) + "\n"
                    amsg += "From site config file.\n"
                    amsg += "Continue to the next stageOut.\n"
                    logging.error(amsg)
                    foundNoneAttr = True
                    break
            if foundNoneAttr: continue
            storageSite = stageOut.get("storageSite")
            volume = stageOut.get("volume")
            protocol = stageOut.get("protocol")
            command = stageOut.get("command")
            pnn = stageOut.get("phedex-node")

            logging.info("\nStage out to : %s using: %s" % (pnn, command))

            try:
                aPath = storageJsonPath(self.siteCfg.siteName, self.siteCfg.subSiteName, storageSite)
                rfc = readRFC(aPath, storageSite, volume, protocol)
                self.stageOuts_rfcs.append((stageOut, rfc))
                msg = "\nRucio File Catalog has been loaded:"
                msg += "\n" + str(rfc)
                logging.info(msg)
            except Exception as ex:
                amsg = "\nUnable to load Rucio File Catalog. This stage out will not be attempted:\n"
                amsg += '\t' + stageOutStr(stageOut) + '\n'
                amsg += str(ex)
                logging.exception(amsg)
                continue

        # no Rucio file catalog is initialized
        if not self.stageOuts_rfcs:
            raise StageOutInitError("===>Can not initialize Rucio file catalog")

        logging.info("==== Stageout configuration finish ====")

        return

    def initialiseOverride(self):
        """
        _initialiseOverride_

        Extract and verify that the Override parameters are all present

        """
        overrideConf = {
            "command": None,
            "option": None,
            "phedex-node": None,
            "lfn-prefix": None,
            }
        try:
            overrideConf['command'] = self.overrideConf['command']
            overrideConf['phedex-node'] = self.overrideConf['phedex-node']
            overrideConf['lfn-prefix'] = self.overrideConf['lfn-prefix']
        except Exception as ex:
            msg = "Unable to extract override parameters from config:\n"
            msg += str(ex)
            raise StageOutInitError(msg)
        if self.overrideConf.get('option') is not None:
            if len(self.overrideConf['option']) > 0:
                overrideConf['option'] = self.overrideConf['option']
            else:
                overrideConf['option'] = ""

        self.overrideConf = overrideConf

        msg = "=======StageOut Override Initialised:================\n"
        for key, val in viewitems(self.overrideConf):
            msg += " %s : %s\n" % (key, val)
        msg += "=====================================================\n"
        logging.info(msg)
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

        # // No override => use stage-out from site conf
        if not self.override:
            logging.info("===> Attempting %s Stage Outs", len(self.stageOuts))
            for stageOut_rfc in self.stageOuts_rfcs:
                try:
                    pfn = self.stageOut(lfn, fileToStage['PFN'], fileToStage.get('Checksums'), stageOut_rfc)
                    fileToStage['PFN'] = pfn
                    fileToStage['PNN'] = stageOut_rfc[0]['phedex-node']
                    fileToStage['StageOutCommand'] = stageOut_rfc[0]['command']
                    logging.info("attempting stageOut")
                    self.completedFiles[fileToStage['LFN']] = fileToStage
                    if lfn in self.failed:
                        del self.failed[lfn]

                    logging.info("===> Stage Out Successful: %s", fileToStage)
                    fileToStage = stageoutPolicyReport(fileToStage, None, None, 'LOCAL', 0)
                    return fileToStage
                except WMException as ex:
                    lastException = ex
                    logging.info("===> Stage Out Failure for file:")
                    logging.info("======>  %s\n", fileToStage['LFN'])
                    logging.info("======>  %s\n using this stage out", stageOutStr(stageOut_rfc[0]))
                    fileToStage = stageoutPolicyReport(fileToStage, stageOut_rfc[0]['phedex-node'],
                                                       stageOut_rfc[0]['command'], 'LOCAL', 60311)
                    continue
                except Exception as ex:
                    lastException = StageOutFailure("Error during local stage out",
                                                    error=str(ex))
                    logging.info("===> Stage Out Failure for file:\n")
                    logging.info("======>  %s\n", fileToStage['LFN'])
                    logging.info("======>  %s\n using this stage out", stageOutStr(stageOut_rfc[0]))
                    fileToStage = stageoutPolicyReport(fileToStage, stageOut_rfc[0]['phedex-node'],
                                                       stageOut_rfc[0]['command'], 'LOCAL', 60311)
                    continue

        else:
            logging.info("===> Attempting stage outs from override")
            try:
                pfn = self.stageOut(lfn, fileToStage['PFN'], fileToStage.get('Checksums'))
                fileToStage['PFN'] = pfn
                fileToStage['PNN'] = self.overrideConf['phedex-node']
                fileToStage['StageOutCommand'] = self.overrideConf['command']
                logging.info("attempting override stage out")
                self.completedFiles[fileToStage['LFN']] = fileToStage
                if lfn in self.failed:
                    del self.failed[lfn]

                logging.info("===> Stage Out Successful: %s", fileToStage)
                fileToStage = stageoutPolicyReport(fileToStage, None, None, 'OVERRIDE', 0)
                return fileToStage
            except Exception as ex:
                fileToStage = stageoutPolicyReport(fileToStage, self.overrideConf['phedex-node'], \
                                                   self.overrideConf['command'], 'OVERRIDE', 60310)
                lastException = ex

        raise lastException

    def stageOut(self, lfn, localPfn, checksums, stageOut_rfc=None):
        """
        Given the lfn and a pair of stage out and corresponding Rucio file catalog, stageOut_rfc, or override configuration invoke the stage out
        If use override configuration self.overrideConf should contain:
        command - the stage out impl plugin name to be used
        option - the option values to be passed to that command (None is allowed)
        lfn-prefix - the LFN prefix to generate the PFN
        phedex-node - the Name of the PNN to which the file is being xferred
        :param lfn: logical file name 
        :param localPfn: physical file name of file at local location (source) that will be staged out to another location (destination)
        :param checksums: check sum of file
        :param stageOut_rfc: a pair of stage out and corresponding Rucio file catalog
        """

        if not self.override:
            if not stageOut_rfc:
                msg = "Can not perform stage out for this lfn because of missing stage out information (stageOut_rfc is None or empty): \n %s" % lfn
                raise StageOutFailure(msg, LFN=lfn)
            command = stageOut_rfc[0]['command']
            options = stageOut_rfc[0]['option']
            pfn = searchRFC(stageOut_rfc[1], lfn)
            protocol = stageOut_rfc[1].preferredProtocol
            if pfn == None:
                msg = "Unable to match lfn to pfn: \n  %s" % lfn
                raise StageOutFailure(msg, LFN=lfn, StageOut=stageOutStr(stageOut_rfc[0]))
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
                msg = "Failure for stage out:\n"
                msg += str(ex)
                try:
                    import traceback
                    msg += traceback.format_exc()
                except AttributeError:
                    msg += "Traceback unavailable\n"
                raise StageOutFailure(msg, Command=command, Protocol=protocol,
                                      LFN=lfn, InputPFN=localPfn, TargetPFN=pfn)
            return pfn

        else:
            if self.overrideConf['lfn-prefix'] is None:
                msg = "Unable to match lfn to pfn during stage out because override lfn-prefix is None: \n %s" % lfn
                raise StageOutFailure(msg, LFN=lfn)

            pfn = "%s%s" % (self.overrideConf['lfn-prefix'], lfn)

            try:
                impl = retrieveStageOutImpl(self.overrideConf['command'])
            except Exception as ex:
                msg = "Unable to retrieve impl for override stage out:\n"
                msg += "Error retrieving StageOutImpl for command named: "
                msg += "%s\n" % self.overrideConf['command']
                raise StageOutFailure(msg, Command=self.overrideConf['command'],
                                      LFN=lfn, ExceptionDetail=str(ex))

            impl.numRetries = self.numberOfRetries
            impl.retryPause = self.retryPauseTime

            try:
                impl(self.overrideConf['command'], localPfn, pfn, self.overrideConf["option"], checksums)
            except Exception as ex:
                msg = "Failure for override stage out:\n"
                msg += str(ex)
                raise StageOutFailure(msg, Command=self.overrideConf['command'],
                                      LFN=lfn, InputPFN=localPfn, TargetPFN=pfn)

            return pfn

    def cleanSuccessfulStageOuts(self):
        """
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
