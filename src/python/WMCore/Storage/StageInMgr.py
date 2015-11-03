#!/usr/bin/env python
"""
_StageInMgr_

Util class to provide stage in functionality as an interface object.

Based on StageOutMgr class

"""
from __future__ import print_function

import os

from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig

#do we want seperate exceptions - for the moment no
from WMCore.Storage.StageOutError import StageOutFailure
from WMCore.Storage.StageOutError import StageOutInitError
from WMCore.Storage.Registry import retrieveStageOutImpl


class StageInSuccess(Exception):
    """
    _StageOutSuccess_

    Exception used to escape stage out loop when stage out is successful
    """
    pass



class StageInMgr:
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

        self.fallbacks = []
        #  //
        # // Try an get the TFC for the site
        #//
        self.tfc = None



        self.numberOfRetries = 3
        self.retryPauseTime = 600

        #  //
        # // If override isnt None, we dont need SiteCfg, if it is
        #//  then we need siteCfg otherwise we are dead.

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
            raise StageOutInitError( msg)
        msg = "Local Stage Out Implementation to be used is:"
        msg += "%s\n" % implName

        pnn = self.siteCfg.localStageOut.get("phedex-node", None)
        if pnn == None:
            msg = "Unable to retrieve local stage out phedex-node\n"
            msg += "From site config file.\n"
            msg += "Unable to perform StageOut operation"
            raise StageOutInitError( msg)
        msg += "Local Stage Out PNN to be used is %s\n" % pnn
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
        except Exception as ex:
            msg = "Unable to load Trivial File Catalog:\n"
            msg += "Local stage out will not be attempted\n"
            msg += str(ex)
            raise StageOutInitError( msg )

        print(msg)
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
            "phedex-node" : None,
            "lfn-prefix" : None,
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

        msg = "=======StageIn Override Initialised:================\n"
        for key, val in overrideParams.items():
            msg += " %s : %s\n" % (key, val)
        msg += "=====================================================\n"
        print(msg)
        self.fallbacks = []
        self.fallbacks.append(overrideParams)
        return


    def __call__(self, **fileToStage):
        """
        _operator()_

        Use call to invoke transfers

        """


        try:
            print("==>Working on file: %s" % fileToStage['LFN'])

            lfn = fileToStage['LFN']

            #  //
            # // No override => use local-stage-out from site conf
            #//  invoke for all files and check failures/successes
            if not self.override:
                print("===> Attempting Local Stage In.")
                try:
                    pfn = self.localStageIn(lfn)
                    fileToStage['PFN'] = pfn
                    raise StageInSuccess
                except StageOutFailure as ex:
                    msg = "===> Local Stage Out Failure for file:\n"
                    msg += "======>  %s\n" % fileToStage['LFN']
                    msg += str(ex)
                    print(msg)
            #  //
            # // Still here => override start using the fallback stage outs
            #//  If override is set, then that will be the only fallback available
            print("===> Attempting %s Override Stage Outs" % len(self.fallbacks))
            for fallback in self.fallbacks:
                try:
                    pfn = self.localStageIn(lfn, fallback)
                    fileToStage['PFN'] = pfn
                    raise StageInSuccess
                except StageOutFailure as ex:
                    continue

        except StageInSuccess:
            msg = "===> Stage In Successful:\n"
            msg += "====> LFN: %s\n" % fileToStage['LFN']
            msg += "====> PFN: %s\n" % fileToStage['PFN']
            print(msg)
            return fileToStage
        msg = "Unable to stage out file:\n"
        msg += fileToStage['LFN']
        raise StageOutFailure(msg, **fileToStage)


    def localStageIn(self, lfn, override = None):
        """
        _localStageOut_

        Given the lfn and local stage out params, invoke the local stage in
        i.e. stage in lfn to pfn

        if override is used the follwoing params should be defined:
        command - the stage out impl plugin name to be used
        option - the option values to be passed to that command (None is allowed)
        lfn-prefix - the LFN prefix to generate the PFN
        phedex-node - the Name of the PNN to which the file is being xferred

        """
        localPfn = os.path.join(os.getcwd(), os.path.basename(lfn))

        if override:
            pnn = override['phedex-node']
            command = override['command']
            options = override['option']
            pfn = "%s%s" % (override['lfn-prefix'], lfn)
            protocol = command
        else:
            pnn = self.siteCfg.localStageOut['phedex-node']
            command = self.siteCfg.localStageOut['command']
            options = self.siteCfg.localStageOut.get('option', None)
            pfn = self.searchTFC(lfn)
            protocol = self.tfc.preferredProtocol

        if pfn == None:
            msg = "Unable to match lfn to pfn: \n  %s" % lfn
            raise StageOutFailure(msg, LFN = lfn, TFC = str(self.tfc))

        try:
            impl = retrieveStageOutImpl(command, stagein=True)
        except Exception as ex:
            msg = "Unable to retrieve impl for local stage in:\n"
            msg += "Error retrieving StageOutImpl for command named: %s\n" % (
                command,)
            raise StageOutFailure(msg, Command = command,
                                  LFN = lfn, ExceptionDetail = str(ex))
        impl.numRetries = self.numberOfRetries
        impl.retryPause = self.retryPauseTime

        try:
            impl(protocol, pfn, localPfn, options)
        except Exception as ex:
            msg = "Failure for local stage in:\n"
            msg += str(ex)
            raise StageOutFailure(msg, Command = command, Protocol = protocol,
                                  LFN = lfn, InputPFN = localPfn,
                                  TargetPFN = pfn)

        return localPfn


    def searchTFC(self, lfn):
        """
        _searchTFC_

        Search the Trivial File Catalog for the lfn provided,
        if a match is made, return the matched PFN

        """
        if self.tfc == None:
            msg = "Trivial File Catalog not available to match LFN:\n"
            msg += lfn
            print(msg)
            return None
        if self.tfc.preferredProtocol == None:
            msg = "Trivial File Catalog does not have a preferred protocol\n"
            msg += "which prevents local stage out for:\n"
            msg += lfn
            print(msg)
            return None

        pfn = self.tfc.matchLFN(self.tfc.preferredProtocol, lfn)
        if pfn == None:
            msg = "Unable to map LFN to PFN:\n"
            msg += "LFN: %s\n" % lfn
            return None

        msg = "LFN to PFN match made:\n"
        msg += "LFN: %s\nPFN: %s\n" % (lfn, pfn)
        print(msg)
        return pfn
