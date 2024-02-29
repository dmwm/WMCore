#!/usr/bin/env python
"""
_StageInMgr_

Util class to provide stage in functionality as an interface object.

Based on StageOutMgr class

"""
import logging
import os

from builtins import object
from future.utils import viewitems

from WMCore.Storage.Registry import retrieveStageOutImpl
from WMCore.Storage.RucioFileCatalog import storageJsonPath, readRFC
from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig, stageOutStr
from WMCore.Storage.StageOutError import StageOutFailure, StageOutInitError
from WMCore.Storage.StageOutMgr import searchRFC


class StageInSuccess(Exception):
    """
    _StageOutSuccess_

    Exception used to escape stage out loop when stage out is successful
    """
    pass


class StageInMgr(object):
    """
    _StageInMgr_

    Object that can be used to stage out a set of files
    using TFC or an override.

    """

    def __init__(self, **overrideParams):
        self.override = False
        self.overrideConf = overrideParams
        if overrideParams != {}:
            self.override = True

        # pairs of stageOut and Rucio file catalog
        self.stageOuts_rfcs = []

        self.numberOfRetries = 3
        self.retryPauseTime = 600

        #  //
        # // If override isnt None, we dont need SiteCfg, if it is
        # //  then we need siteCfg otherwise we are dead.

        if self.override == False:
            self.siteCfg = loadSiteLocalConfig()
            self.initialiseSiteConf()
        else:
            self.initialiseOverride()

    def initialiseSiteConf(self):
        """
        _initialiseSiteConf_

        Extract required information from site conf and TFC

        """

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
                    amsg += "from site config file.\n"
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
            msg = "Unable to extract Override parameters from config:\n"
            msg += str(self.overrideConf) + "\n"
            msg += str(ex)
            raise StageOutInitError(msg)
        if self.overrideConf.get('option') is not None:
            if len(overrideConf['option']) > 0:
                overrideConf['option'] = self.overrideConf['option']
            else:
                overrideConf['option'] = ""

        self.overrideConf = overrideConf

        msg = "=======StageIn Override Initialised:================\n"
        for key, val in viewitems(self.overrideConf):
            msg += " %s : %s\n" % (key, val)
        msg += "=====================================================\n"
        logging.info(msg)

        return

    def __call__(self, **fileToStage):
        """
        _operator()_

        Use call to invoke transfers

        """
        try:
            logging.info("==>Working on file: %s", fileToStage['LFN'])
            lfn = fileToStage['LFN']
            #  //
            # // No override => use local-stage-out from site conf
            # //  invoke for all files and check failures/successes
            if not self.override:
                logging.info("===> Attempting Local Stage In")
                for stageOut_rfc in self.stageOuts_rfcs:
                    try:
                        pfn = self.stageIn(lfn, stageOut_rfc)
                        fileToStage['PFN'] = pfn
                        raise StageInSuccess
                    except StageOutFailure as ex:
                        msg = "===> Local Stage Out Failure for file:\n"
                        msg += "======>  %s\n" % fileToStage['LFN']
                        msg += "======>  using this stageOut %s\n" % str(stageOut_rfc[0])
                        msg += str(ex)
                        msg += "\n Continue to the next stage-out"
                        logging.exception(msg)
                        continue
            else:
                logging.info("===> Attempting stage outs from override")
                try:
                    pfn = self.stageIn(lfn)
                    fileToStage['PFN'] = pfn
                    raise StageInSuccess
                except StageOutFailure as ex:
                    msg = "===> Local Stage Out Failure for file:\n"
                    msg += "======>  %s using override\n" % fileToStage['LFN']
                    msg += str(ex)
                    logging.exception(msg)

        except StageInSuccess:
            msg = "===> Stage In Successful:\n"
            msg += "====> LFN: %s\n" % fileToStage['LFN']
            msg += "====> PFN: %s\n" % fileToStage['PFN']
            logging.info(msg)
            return fileToStage
        msg = "Unable to stage out file:\n"
        msg += fileToStage['LFN']
        raise StageOutFailure(msg, **fileToStage)

    def stageIn(self, lfn, stageOut_rfc=None):
        """
        Given the lfn and a pair of stage out and corresponding Rucio file catalog,
        stageOut_rfc, or override configuration invoke the stage in.
        If use override configuration self.overrideConf should contain:
        command - the stage out impl plugin name to be used
        option - the option values to be passed to that command (None is allowed)
        lfn-prefix - the LFN prefix to generate the PFN
        phedex-node - the Name of the PNN to which the file is being xferred
        :param lfn: logical file name
        :param stageOut_rfc: a pair of stage out attributes and corresponding Rucio file catalog
        """
        localPfn = os.path.join(os.getcwd(), os.path.basename(lfn))
        if self.override:
            pnn = self.overrideConf['phedex-node']
            command = self.overrideConf['command']
            options = self.overrideConf['option']
            if self.overrideConf['lfn-prefix'] is None:
                msg = "Unable to match lfn to pfn during stage in because override lfn-prefix is None: \n %s" % lfn
                raise StageOutFailure(msg, LFN=lfn)
            pfn = "%s%s" % (self.overrideConf['lfn-prefix'], lfn)
            protocol = command
        else:
            if not stageOut_rfc:
                msg = "Can not perform stage in for this lfn because of missing information (stageOut_rfc is None or empty): \n %s" % lfn
                raise StageOutFailure(msg, LFN=lfn)
            pnn = stageOut_rfc[0]['phedex-node']
            command = stageOut_rfc[0]['command']
            options = stageOut_rfc[0]['option']
            pfn = searchRFC(stageOut_rfc[1], lfn)
            protocol = stageOut_rfc[1].preferredProtocol

        if pfn == None:
            msg = "Unable to match lfn to pfn: \n  %s" % lfn
            raise StageOutFailure(msg, LFN=lfn, StageOut=stageOutStr(stageOut_rfc[0]))
        try:
            impl = retrieveStageOutImpl(command, stagein=True)
        except Exception as ex:
            msg = "Unable to retrieve impl for local stage in:\n"
            msg += "Error retrieving StageOutImpl for command named: %s\n" % (command,)
            raise StageOutFailure(msg, Command=command,
                                  LFN=lfn, ExceptionDetail=str(ex))
        impl.numRetries = self.numberOfRetries
        impl.retryPause = self.retryPauseTime

        try:
            impl(protocol, pfn, localPfn, options)
        except Exception as ex:
            msg = "Failure for stage in:\n"
            msg += str(ex)
            raise StageOutFailure(msg, Command=command, Protocol=protocol,
                                  LFN=lfn, InputPFN=localPfn, TargetPFN=pfn)
        return localPfn
