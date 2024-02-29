#!/usr/bin/env python
"""
DeleteMgr

Util class to provide delete functionality as an interface object.

Based on StageOutMgr class

"""
import logging

from builtins import object
from future.utils import viewitems

from WMCore.Storage.Registry import retrieveStageOutImpl
from WMCore.Storage.RucioFileCatalog import storageJsonPath, readRFC
from WMCore.Storage.SiteLocalConfig import stageOutStr, loadSiteLocalConfig
from WMCore.Storage.StageOutError import StageOutFailure, StageOutInitError
from WMCore.WMException import WMException


class DeleteMgrError(WMException):
    """
    _DeleteMgrError_

    Specific exception class to work out file deletion exception details
    """

    def __init__(self, message, **data):
        WMException.__init__(self, message, **data)
        self.data.setdefault("ErrorCode", 60313)
        self.data.setdefault("ErrorType", self.__class__.__name__)


class DeleteMgr(object):
    """
    _DeleteMgr_

    Object that can be used to delete a set of files
    using TFC or an override.

    """

    def __init__(self, **overrideParams):

        self.logger = overrideParams.pop("logger", logging.getLogger())
        self.overrideConf = overrideParams

        # pairs of stageOut and Rucio file catalog: [(stageOut1,rfc1),(stageOut2,rfc2), ...]
        # a "stageOut" corresponds to a entry in the <stage-out> block in the site-local-config.xml, for example <method volume="KIT_dCache" protocol="WebDAV"/>
        # a "rfc" is the correponding RucioFileCatalog instance (RucioFileCatalog.py) of this "stageOut"
        self.stageOuts_rfcs = []
        self.numberOfRetries = 3
        self.retryPauseTime = 600

        #  //
        # // If override isnt None, we dont need SiteCfg, if it is
        # //  then we need siteCfg otherwise we are dead.

        if not self.overrideConf:
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

        self.logger.info("There are %s stage out definitions.\n" % len(self.stageOuts))

        for stageOut in self.stageOuts:
            foundNoneAttr = False
            for k in ['phedex-node', 'command', 'storageSite', 'volume', 'protocol']:
                v = stageOut.get(k)
                if v is None:
                    msg = "Unable to retrieve " + k + " of this stageOut: \n"
                    msg += stageOutStr(stageOut) + "\n"
                    msg += "from site config file.\n"
                    msg += "Continue to the next stageOut.\n"
                    self.logger.error(msg)
                    foundNoneAttr = True
                    break
            if foundNoneAttr: continue
            storageSite = stageOut.get("storageSite")
            volume = stageOut.get("volume")
            protocol = stageOut.get("protocol")
            command = stageOut.get("command")
            pnn = stageOut.get("phedex-node")

            self.logger.info("\tStage out to : %s using: %s \n" % (pnn, command))

            try:
                aPath = storageJsonPath(self.siteCfg.siteName, self.siteCfg.subSiteName, storageSite)
                rfc = readRFC(aPath, storageSite, volume, protocol)
                self.stageOuts_rfcs.append((stageOut, rfc))
                msg = "Rucio File Catalog has been loaded:\n"
                msg += str(self.stageOuts_rfcs[-1][1])
                self.logger.info(msg)
            except Exception as ex:
                msg = "Unable to load Rucio File Catalog:\n"
                msg += "This stage out will not be attempted:\n"
                msg += stageOutStr(stageOut) + '\n'
                msg += str(ex)
                self.logger.exception(msg)
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
            msg = "Unable to extract override parameters from config:\n"
            msg += str(self.overrideConf)
            raise StageOutInitError(msg)
        if self.overrideConf.get('option') is not None:
            if len(self.overrideConf['option']) > 0:
                overrideConf['option'] = self.overrideConf['option']
            else:
                overrideConf['option'] = ""

        self.overrideConf = overrideConf

        msg = "=======Delete Override Initialised:================\n"
        for key, val in viewitems(overrideConf):
            msg += " %s : %s\n" % (key, val)
        msg += "=====================================================\n"
        self.logger.info(msg)

        return

    def __call__(self, fileToDelete):
        """
        _operator()_

        Use call to delete a file

        """
        self.logger.info("==>Working on file: %s" % fileToDelete['LFN'])

        lfn = fileToDelete['LFN']

        deleteSuccess = False

        if not self.overrideConf:
            self.logger.info("===> Attempting to delete with %s stage outs", len(self.stageOuts))
            for stageOut_rfc in self.stageOuts_rfcs:
                if not deleteSuccess:
                    try:
                        fileToDelete['PNN'] = stageOut_rfc[0]['phedex-node']
                        fileToDelete['PFN'] = self.deleteLFN(lfn, stageOut_rfc)
                        deleteSuccess = True
                        break
                    except Exception as ex:
                        continue
        else:
            self.logger.info("===> Attempting stage outs from override")
            try:
                fileToDelete['PNN'] = self.overrideConf['phedex-node']
                fileToDelete['PFN'] = self.deleteLFN(lfn)
                deleteSuccess = True
            except Exception as ex:
                self.logger.error("===> Local file deletion failure. Exception:\n%s", str(ex))
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

    def deleteLFN(self, lfn, stageOut_rfc=None):
        """
        deleteLFN
        Given the lfn and an stageOut Rucio file catalog pair or override config, invoke the delete
        lfn: logical file name
        stageOut_rfc: a pair fo stageOut and correponding Rucio file catalog, required when no override provided 
        self.overrideConf: the follwoing params should be defined for override
            command - the stage out impl plugin name to be used
            option - the option values to be passed to that command (None is allowed)
            lfn-prefix - the LFN prefix to generate the PFN
            phedex-node - the Name of the PNN to which the file is being xferred
        """
        if not self.overrideConf:
            if stageOut_rfc is None:
                msg = "Can not delete lfn because of missing stage out information (stageOut_rfc is None): \n %s" % lfn
                raise StageOutFailure(msg, LFN=lfn)
            # FIXME there is circular import that is why this module is imported here
            from WMCore.Storage.StageOutMgr import searchRFC
            command = stageOut_rfc[0]['command']
            pfn = searchRFC(stageOut_rfc[1], lfn)
            if pfn is None:
                msg = "Unable to match lfn to pfn: \n  %s" % lfn
                raise StageOutFailure(msg, LFN=lfn, STAGEOUT=stageOutStr(stageOut_rfc[0]))
        else:
            if self.overrideConf['lfn-prefix'] is None:
                msg = "Unable to match lfn to pfn during lfn deletion because override lfn-prefix is None: \n %s" % lfn
                raise StageOutFailure(msg, LFN=lfn)
            command = self.overrideConf['command']
            pfn = "%s%s" % (self.overrideConf['lfn-prefix'], lfn)
        return self.deletePFN(pfn, lfn, command)

    def deletePFN(self, pfn, lfn, command):
        """
        Delete the given PFN
        """
        try:
            impl = retrieveStageOutImpl(command)
        except Exception as ex:
            msg = "Unable to retrieve impl for file deletion in %s\n" % (pfn)
            msg += "Error retrieving StageOutImpl for command named: %s\n" % (
                command,)
            raise StageOutFailure(msg, Command=command,
                                  LFN=lfn, ExceptionDetail=str(ex))
        impl.numRetries = self.numberOfRetries
        impl.retryPause = self.retryPauseTime

        try:
            impl.removeFile(pfn)
        except Exception as ex:
            self.logger.exception("Failed to delete file: %s", pfn)
            raise ex

        return pfn
