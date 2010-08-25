#!/usr/bin/env python
"""
_StageOutMgr_

Util class to provide stage out functionality as an interface object.

Based of RuntimeStageOut.StageOutManager, that should probably eventually
use this class as a basic API

"""

import os

#from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig

from WMCore.Storage.StageOutError import StageOutFailure
from WMCore.Storage.StageOutError import StageOutInitError
from WMCore.Storage.DeleteMgr import DeleteMgr
from WMCore.Storage.Registry import retrieveStageOutImpl
import WMCore.Storage.Backends


class StageOutSuccess(Exception):
    """
    _StageOutSuccess_

    Exception used to escape stage out loop when stage out is successful
    """
    pass



class StageOutMgr:
    """
    _StageOutMgr_

    Object that can be used to stage out a set of files
    using TFC or an override.

    """
    def __init__(self, **overrideParams):
        self.override = False
        self.overrideConf = overrideParams
        if overrideParams != {}:
            self.override = True

        self.substituteGUID = True
        self.fallbacks = []

        #  //
        # // Try an get the TFC for the site
        #//
        self.tfc = None



        self.numberOfRetries = 3
        self.retryPauseTime = 600

        from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig

        #  //
        # // If override isnt None, we dont need SiteCfg, if it is
        #//  then we need siteCfg otherwise we are dead.

        if self.override == False:
            self.siteCfg = loadSiteLocalConfig()

        if self.override:
            self.initialiseOverride()
        else:
            self.initialiseSiteConf()

        self.failed = {}
        self.completedFiles = {}


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
            raise StageOutInitError( msg)
        msg = "Local Stage Out Implementation to be used is:"
        msg += "%s\n" % implName

        seName = self.siteCfg.localStageOut.get("se-name", None)
        if seName == None:
            msg = "Unable to retrieve local stage out se-name\n"
            msg += "From site config file.\n"
            msg += "Unable to perform StageOut operation"
            raise StageOutInitError( msg)
        msg += "Local Stage Out SE Name to be used is %s\n" % seName
        catalog = self.siteCfg.localStageOut.get("catalog", None)
        if catalog == None:
            msg = "Unable to retrieve local stage out catalog\n"
            msg += "From site config file.\n"
            msg += "Unable to perform StageOut operation"
            raise StageOutInitError( msg)
        msg += "Local Stage Out Catalog to be used is %s\n" % catalog

        try:
            self.tfc = self.siteCfg.trivialFileCatalog()
            msg += "Trivial File Catalog has been loaded:\n"
            msg += str(self.tfc)
        except StandardError, ex:
            msg = "Unable to load Trivial File Catalog:\n"
            msg += "Local stage out will not be attempted\n"
            msg += str(ex)
            raise StageOutInitError( msg )

        self.fallbacks = self.siteCfg.fallbackStageOut

        msg += "There are %s fallback stage out definitions.\n" % len(self.fallbacks)
        for item in self.fallbacks:
            msg += "Fallback to : %s using: %s \n" % (item['se-name'], item['command'])

        print msg
        return


    def initialiseOverride(self):
        """
        _initialiseOverride_

        Extract and verify that the Override parameters are all present

        """
        overrideConf = self.overrideConf
        overrideParams = {
            "command" : None,
            "option" : None,
            "se-name" : None,
            "lfn-prefix" : None,
            }

        try:
            overrideParams['command'] = overrideConf['command']
            overrideParams['se-name'] = overrideConf['se-name']
            overrideParams['lfn-prefix'] = overrideConf['lfn-prefix']
        except StandardError, ex:
            msg = "Unable to extract Override parameters from config:\n"
            msg += str(ex)
            raise StageOutInitError(msg)
        if overrideConf.has_key('option'):
            if len(overrideConf['option']) > 0:
                overrideParams['option'] = overrideConf['option']
            else:
                overrideParams['option'] = ""

        msg = "=======StageOut Override Initialised:================\n"
        for key, val in overrideParams.items():
            msg += " %s : %s\n" % (key, val)
        msg += "=====================================================\n"
        print msg
        self.fallbacks = []
        self.fallbacks.append(overrideParams)
        return


    def __call__(self, fileToStage):
        """
        _operator()_

        Use call to invoke transfers

        """


        try:
            print "==>Working on file: %s" % fileToStage['LFN']
            lfn = fileToStage['LFN']

            #  //
            # // No override => use local-stage-out from site conf
            #//  invoke for all files and check failures/successes
            if not self.override:
                print "===> Attempting Local Stage Out."
                try:
                    pfn = self.localStageOut(lfn, fileToStage['PFN'])
                    fileToStage['PFN'] = pfn
                    fileToStage['SEName'] = self.siteCfg.localStageOut['se-name']
                    fileToStage['StageOutCommand'] = self.siteCfg.localStageOut['command']
                    self.completedFiles[fileToStage['LFN']] = fileToStage
                    raise StageOutSuccess
                except StageOutFailure, ex:
                    msg = "===> Local Stage Out Failure for file:\n"
                    msg += "======>  %s\n" % fileToStage['LFN']
                    msg += str(ex)
                    # go to fallback
                    pass
            #  //
            # // Still here => failure, start using the fallback stage outs
            #//  If override is set, then that will be the only fallback available
            print "===> Attempting %s Fallback Stage Outs" % len(self.fallbacks)
            for fallback in self.fallbacks:
                try:
                    pfn = self.fallbackStageOut(lfn, fileToStage['PFN'],
                                                fallback)
                    fileToStage['PFN'] = pfn
                    fileToStage['SEName'] = fallback['se-name']
                    fileToStage['StageOutCommand'] = fallback['command']
                    print "attempting fallback"
                    self.completedFiles[fileToStage['LFN']] = fileToStage
                    if self.failed.has_key(lfn):
                        del self.failed[lfn]
                    raise StageOutSuccess
                except StageOutFailure, ex:
                    continue

        except StageOutSuccess:
            msg = "===> Stage Out Successful:\n"
            msg += "====> LFN: %s\n" % fileToStage['LFN']
            msg += "====> PFN: %s\n" % fileToStage['PFN']
            msg += "====> SE:  %s\n" % fileToStage['SEName']
            print msg
            return fileToStage
        msg = "Unable to stage out file:\n"
        msg += fileToStage['LFN']
        raise StageOutFailure(msg, **fileToStage)



    def fallbackStageOut(self, lfn, localPfn, fbParams):
        """
        _fallbackStageOut_

        Given the lfn and parameters for a fallback stage out, invoke it

        parameters should contain:

        command - the stage out impl plugin name to be used
        option - the option values to be passed to that command (None is allowed)
        lfn-prefix - the LFN prefix to generate the PFN
        se-name - the Name of the SE to which the file is being xferred

        """
        pfn = "%s%s" % (fbParams['lfn-prefix'], lfn)

        try:
            impl = retrieveStageOutImpl(fbParams['command'])
        except Exception, ex:
            msg = "Unable to retrieve impl for fallback stage out:\n"
            msg += "Error retrieving StageOutImpl for command named: "
            msg += "%s\n" % fbParams['command']
            raise StageOutFailure(msg, Command = fbParams['command'],
                                  LFN = lfn, ExceptionDetail = str(ex))

        impl.numRetries = self.numberOfRetries
        impl.retryPause = self.retryPauseTime

        try:
            impl(fbParams['command'], localPfn, pfn, fbParams['option'])
        except Exception, ex:
            msg = "Failure for fallback stage out:\n"
            msg += str(ex)
            raise StageOutFailure(msg, Command = fbParams['command'],
                                  LFN = lfn, InputPFN = localPfn,
                                  TargetPFN = pfn)

        return pfn

    def localStageOut(self, lfn, localPfn):
        """
        _localStageOut_

        Given the lfn and local stage out params, invoke the local stage out

        """
        seName = self.siteCfg.localStageOut['se-name']
        command = self.siteCfg.localStageOut['command']
        options = self.siteCfg.localStageOut.get('option', None)
        pfn = self.searchTFC(lfn)
        protocol = self.tfc.preferredProtocol
        if pfn == None:
            msg = "Unable to match lfn to pfn: \n  %s" % lfn
            raise StageOutFailure(msg, LFN = lfn, TFC = str(self.tfc))


        try:
            impl = retrieveStageOutImpl(command)
        except Exception, ex:
            msg = "Unable to retrieve impl for local stage out:\n"
            msg += "Error retrieving StageOutImpl for command named: %s\n" % (
                command,)
            raise StageOutFailure(msg, Command = command,
                                  LFN = lfn, ExceptionDetail = str(ex))
        impl.numRetries = self.numberOfRetries
        impl.retryPause = self.retryPauseTime

        try:
            impl(protocol, localPfn, pfn, options)
        except Exception, ex:
            msg = "Failure for local stage out:\n"
            msg += str(ex)
            try:
                import traceback
                msg += traceback.format_exc()
            except AttributeError, ex:
                msg += "Traceback unavailable\n"
            raise StageOutFailure(msg, Command = command, Protocol = protocol,
                                  LFN = lfn, InputPFN = localPfn,
                                  TargetPFN = pfn)

        return pfn



    def cleanSuccessfulStageOuts(self):
        """
        _cleanSucessfulStageOuts_

        In the event of a failed stage out, this method can be called to cleanup the
        files that may have previously been staged out so that the job ends in a clear state
        of failure, rather than a partial success


        """
        for lfn, fileInfo in self.completedFiles.items():
            pfn = fileInfo['PFN']
            command = fileInfo['StageOutCommand']
            msg = "Cleaning out file: %s\n" % lfn
            msg +=  "Removing PFN: %s" % pfn
            msg += "Using command implementation: %s\n" % command
            print msg
            delManager = DeleteMgr(**self.overrideConf)
            try:
                delManager.deletePFN(pfn, lfn, command)
            except StageOutFailure, ex:
                msg = "Failed to cleanup staged out file after error:"
                msg += " %s\n%s" % (lfn, str(ex))
                print msg




    def searchTFC(self, lfn):
        """
        _searchTFC_

        Search the Trivial File Catalog for the lfn provided,
        if a match is made, return the matched PFN

        """
        if self.tfc == None:
            msg = "Trivial File Catalog not available to match LFN:\n"
            msg += lfn
            print msg
            return None
        if self.tfc.preferredProtocol == None:
            msg = "Trivial File Catalog does not have a preferred protocol\n"
            msg += "which prevents local stage out for:\n"
            msg += lfn
            print msg
            return None

        pfn = self.tfc.matchLFN(self.tfc.preferredProtocol, lfn)
        if pfn == None:
            msg = "Unable to map LFN to PFN:\n"
            msg += "LFN: %s\n" % lfn
            return None

        msg = "LFN to PFN match made:\n"
        msg += "LFN: %s\nPFN: %s\n" % (lfn, pfn)
        print msg
        return pfn



if __name__ == '__main__':
    import time
    import WMCore.Storage.Backends

    overrideParams = {
        "command" : 'srmv2',
        "option" : '-streams_num=1',
        "se-name" : 'gfe02.hep.ph.ic.ac.uk',
        "lfn-prefix" : 'srm://gfe02.hep.ph.ic.ac.uk:8443/srm/managerv2?SFN=/pnfs/hep.ph.ic.ac.uk/data/cms',
        }

    mgr = StageOutMgr()
    #mgr = StageOutMgr(**overrideParams)
    pfn = "/etc/hosts"
    lfn = "/store/test/stageOutTest-%s" % int(time.time())

    mgr(LFN = lfn, PFN = pfn, GUID=None)



