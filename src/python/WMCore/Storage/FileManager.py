#!/usr/bin/env python
"""
_StageOutMgrV2_

Refactoring of StageOutMgr -- for now, not accessed by default


"""

import os
import logging
log = logging
import traceback
from WMCore.WMException import WMException

from WMCore.Storage.StageOutError import StageOutError
from WMCore.Storage.StageOutError import StageOutFailure
from WMCore.Storage.StageOutError import StageOutInitError
from WMCore.Storage.DeleteMgr import DeleteMgr
from WMCore.Storage.Registry import retrieveStageOutImpl, RegistryError
from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig

import WMCore.Storage.Backends
import WMCore.Storage.Plugins
import time

class FileManager:
    """
    _FileManager_

    Object that handles modifying files in a site-specific way.
    Supercedes StageInMgr, StageOutMgr, DeleteMgr
    
    new easy to use interface:
    deleteLFN(lfn) - tries to delete a certain LFN, returning details on success. Raising on failure
    stageIn/stageOut - accepts a dict containing the details
                        PFN stores the local file name
                        LFN stores the lfn of the file, which will be mapped to a PFN
                      
    plugin implementations require:
    newPfn =  pluginImplementation.doTransfer( lfn, pfn, stageOut, seName, command, options, protocol  )
    pluginImplementation.doDelete(pfn, seName, command, options, protocol  )
    
    Which make one attempt to perform the action and raises if it doesn't succeed. It is the plugins
    responsibility to verify that things are complete.
    """
    def __init__(self, numberOfRetries = 30, retryPauseTime=60, **overrideParams):
        
        # set defaults
        self.failed = {}
        self.completedFiles = {}
        self.override = False
        self.overrideConf = overrideParams
        self.substituteGUID = True
        self.defaultMethod = {}
        self.fallbacks = []
        self.tfc = None
        self.numberOfRetries = numberOfRetries
        self.retryPauseTime = retryPauseTime
                                
        if overrideParams != {}:
            log.critical("Override: %s" % overrideParams)
            self.override = True
            self.initialiseOverride()
        else:
            self.siteCfg = loadSiteLocalConfig()
            self.initialiseSiteConf()
            
    def stageFile(self, fileToStage, stageOut = True):
        """
        _stageFile_

        Use call to invoke transfers (either in or out)
        input: 
            fileToStage: a dict containing at least:
                        LFN: the LFN for one end of the transfer, will be
                            mapped to a PFN before the transfer
                        PFN: (annoyingly) Not a PFN, but is the local filename
                            for the file when it's transferred to/from
            stageOut: boolean for if the file is staged in or out
        output:
            dict from fileToStage with PFN, SEName, StageOutCommand added
            
        I'm not entirely sure that StageOutCommand makes sense, but I don't want to break old code
        -AMM 6/30/2010

        """

        log.info("Working on file: %s" % fileToStage['LFN'])
        lfn =           fileToStage['LFN']
        localFileName = fileToStage['PFN']


        log.info("Beginning %s" % ('StageOut' if stageOut else 'StageIn'))
        
        # generate list of stageout methods we will try
        stageOutMethods = [ self.defaultMethod ]
        stageOutMethods.extend( self.fallbacks )
        
        # loop over all the different methods. This unifies regular and fallback stuff. Nice.
        methodCounter = 1
        for currentMethod in stageOutMethods:
            # the PFN that is received here is mapped from the LFN
            (seName, command, options, pfn, protocol) =\
                self.getTransferDetails(lfn, currentMethod)
            log.info("Mapped LFN:    %s" % lfn)
            log.info("    to PFN:    %s" % pfn)
            log.info("LocalFileName: %s" % localFileName)
            newPfn = self._doTransfer(currentMethod, methodCounter, localFileName, pfn, stageOut)
            if newPfn:
                log.info("Transfer succeeded: %s" % fileToStage)
                fileToStage['PFN'] = newPfn
                fileToStage['SEName'] = seName
                fileToStage['StageOutCommand'] = command
                self.completedFiles[fileToStage['LFN']] = fileToStage
                return fileToStage
            else:
                # transfer method didn't work, go to next one
                break
        # if we're here, then nothing worked. transferfail.
        log.error("Error in stageout")
        raise StageOutError, "Error in stageout, this has been logged in the logs"
    
    def deleteLFN(self, lfn):
        """
        attempts to delete a file. will raise if none of the methods work, returns details otherwise
        """
        log.info("Beginning to delete %s" % 'lfn')
        retval = {}
        # generate list of stageout methods we will try
        stageOutMethods = [ self.defaultMethod ]
        stageOutMethods.extend( self.fallbacks )
        
        # loop over all the different methods. This unifies regular and fallback stuff. Nice.
        methodCounter = 1
        for currentMethod in stageOutMethods:
    
            (seName, command, options, pfn, protocol) =\
                self.getTransferDetails(lfn, currentMethod)
            
            retval = { 'LFN' : lfn,
                      'PFN': pfn,
                      'SEName': seName}        
            
            log.info("Attempting deletion method %s" % (methodCounter, ))
            log.info("Current method information: %s" % currentMethod)
            
            deleteSlave =  retrieveStageOutImpl(command, useNewVersion=True)
            
            # do the copy. The implementation is responsible for its own verification
            try:
                deleteSlave.doDelete( pfn, seName, command, options, protocol  )
            except StageOutError, ex:
                log.info("Delete failed in an expected manner. Exception is:")
                log.info("%s" % str(ex))
                log.info(traceback.format_exc())
                continue
            # note to people who think it's cheeky to catch exception after ranting against it:
            # this makes sense because no matter what the exception, we want to keep going
            # additionally, it prints out the proper backtrace so we can diagnose issues
            # AMM - 6/30/2010
            except Exception, ex:
                log.critical("Delete failed in an unexpected manner. Exception is:")
                log.critical("%s" % str(ex))
                log.info(traceback.format_exc())
                continue
            
            # successful deletions make it here
            return retval
        
        # unseuccessful transfers make it here
        raise StageOutFailure("Could not delete", **retval)
    def initialiseSiteConf(self):
        """
        _initialiseSiteConf_

        Extract required information from site conf and TFC

        """
        implName = seName = catalog = option = None
        try:
            implName = self.siteCfg.localStageOut.get("command")
            seName   = self.siteCfg.localStageOut.get("se-name")
            catalog  = self.siteCfg.localStageOut.get("catalog")
            option   = self.siteCfg.localStageOut.get('option', None)
              
        except:
            log.critical( 'Either command, se-name or the catalog are missing from site-local-config.xml' )
            log.critical( 'File operations cannot proceed like this' )
            log.critical( 'command: %s se-name: %s catalog: %s' % (implName, seName, catalog) )
            raise
        try:
            self.tfc = self.siteCfg.trivialFileCatalog()
        except:
            log.critical( "TFC wasn't loaded, file operations cannot proceed")
            raise

        self.fallbacks = self.siteCfg.fallbackStageOut
        self.defaultMethod = { 'command' : implName,
                              'se-name' : seName,
                              'catalog' : catalog }
        if option:
            self.defaultMethod['option'] = option
        
        log.info("Local Stage Out Implementation to be used is: %s" % implName)
        log.info("Local Stage Out SE Name to be used is %s" % seName)
        log.info("Local Stage Out Catalog to be used is %s" % catalog)
        log.info("Trivial File Catalog has been loaded:\n%s" % str(self.tfc))
        log.info("There are %s fallback stage out definitions" % len(self.fallbacks))
        for item in self.fallbacks:
            log.info("Fallback to : %s using: %s " % (item['se-name'], item['command']))

    def initialiseOverride(self):
        """
        _initialiseOverride_
    
        Extract required information from override.
        
        TODO: this should be merged with the initializeSiteConf function
        but I can't think of a nice way to do it
    
        """
        implName = seName = lfn_prefix = None
        option = ""
        try:
            implName   = self.overrideConf["command"]
            seName     = self.overrideConf["se-name"]
            lfn_prefix = self.overrideConf["lfn-prefix"]
              
        except:
            log.critical( 'Either command, se-name or the lfn-prefix are missing from the override' )
            log.critical( 'File operations cannot proceed like this' )
            log.critical( 'command: %s se-name: %s lfn-prefix: %s' % (implName, seName, lfn_prefix) )
            raise
    
        self.fallbacks = []
        self.defaultMethod = { 'command' : implName,
                              'se-name' : seName,
                              'lfn-prefix' : lfn_prefix }
        if option:
            self.defaultMethod['option'] = option
        
        log.info("Note: We have been directed to use a StageOut override")
        log.info("Local Stage Out Implementation to be used is: %s" % implName)
        log.info("Local Stage Out SE Name to be used is %s" % seName)
        log.info("Local Stage Out lfn-prefix to be used is %s" % lfn_prefix)


    
    def getTransferDetails(self, lfn, currentMethod):
        """
        helper procedure to return the proper parameters to interact with the filesystem
        regardless of whether or not there's an override involved
        """
        
        if currentMethod.has_key( 'lfn-prefix' ):
            seName   = self.overrideConf['se-name']
            command  = self.overrideConf['command']
            options  = self.overrideConf['option']
            pfn      = "%s%s" % (self.overrideConf['lfn-prefix'], lfn)
            protocol = command
        else:
            seName   = self.siteCfg.localStageOut['se-name']
            command  = self.siteCfg.localStageOut['command']
            options  = self.siteCfg.localStageOut.get('option', None)
            pfn      = self.searchTFC(lfn)
            protocol = self.tfc.preferredProtocol
        return (seName, command, options, pfn, protocol)
    
    def stageIn(self,fileToStage):
        self.stageFile(fileToStage, stageOut=False)
    
    def stageOut(self,fileToStage):
        self.stageFile(fileToStage, stageOut=True)
  
    def _doTransfer(self, currentMethod, methodCounter, localFileName, pfn, stageOut):
        """
        performs a transfer using a selected method and retries.
        necessary because python doesn't have a good nested loop break syntax
        """
        
        (seName, command, options, _, protocol) =\
            self.getTransferDetails(localFileName, currentMethod)
        
        # Swap directions if we're staging in
        if not stageOut:
            tempPfn       = pfn
            pfn           = localFileName
            localFileName = tempPfn
                    
        for retryNumber in range(self.numberOfRetries + 1):
            log.info("Attempting transfer method %s, Retry number: %s" % (methodCounter, retryNumber))
            log.info("Current method information: %s" % currentMethod)
            
            try:
                stageOutSlave =  retrieveStageOutImpl(command, useNewVersion=True)
            except RegistryError:
                stageOutSlave =  retrieveStageOutImpl(command, useNewVersion=False)
                logging.error("Tried to load stageout backend %s, a new version isn't there yet" % command)
                logging.error("Will try to fall back to the oldone, but it's really best to redo it")
                logging.error("Here goes...")
                return stageOutSlave( protocol, localFileName, pfn, options )
            
            # do the copy. The implementation is responsible for its own verification
            newPfn = None
            try:
                newPfn = stageOutSlave.doTransfer( localFileName, pfn, stageOut, seName, command, options, protocol  )
            except StageOutError, ex:
                log.info("Transfer failed in an expected manner. Exception is:")
                log.info("%s" % str(ex))
                log.info("Sleeping for %s seconds" % self.retryPauseTime)
                log.info(traceback.format_exc())
                time.sleep( self.retryPauseTime )
                continue
            # note to people who think it's cheeky to catch exception after ranting against it:
            # this makes sense because no matter what the exception, we want to keep going
            # additionally, it prints out the proper backtrace so we can diagnose issues
            # AMM - 6/30/2010
            except Exception, ex:
                log.critical("Transfer failed in an unexpected manner. Exception is:")
                log.critical("%s" % str(ex))
                log.critical("Since this is an unexpected error, we are continuing to the next method")
                log.critical("and not retrying the same one")
                log.critical(traceback.format_exc())
                break
            
            # successful transfers make it here
            return newPfn
        # unseuccessful transfers make it here
        return False
    


    def cleanSuccessfulStageOuts(self):
        """
        _cleanSucessfulStageOuts_

        In the event of a failed stage out, this method can be called to cleanup the
        files that may have previously been staged out so that the job ends in a clear state
        of failure, rather than a partial success


        """
        for lfn in self.completedFiles.keys():
            self.info("Cleaning out file: %s\n" % lfn)
            try:
                self.deleteLFN(lfn)
            except StageOutFailure, ex:
                log.info("Failed to cleanup staged out file after error:")
                log.info(" %s\n%s" % (lfn, str(ex)))
                log.info(traceback.format_exc())




    def searchTFC(self, lfn):
        """
        _searchTFC_

        Search the Trivial File Catalog for the lfn provided,
        if a match is made, return the matched PFN

        """
        if self.tfc == None:
            log.info("Trivial File Catalog not available to match LFN:")
            log.info(lfn)
            return None
        if self.tfc.preferredProtocol == None:
            log.info("Trivial File Catalog does not have a preferred protocol")
            log.info("which prevents local stage out for:")
            log.info(lfn)
            return None

        pfn = self.tfc.matchLFN(self.tfc.preferredProtocol, lfn)
        if pfn == None:
            log.info("Unable to map LFN to PFN:")
            log.info("LFN: %s" % lfn)
            return None

        log.info("LFN to PFN match made:")
        log.info("LFN: %s\nPFN: %s\n" % (lfn, pfn))
        return pfn


# wrapper classes for compatibility
class StageInMgr(FileManager):
    def __init__(self, numberOfRetries = 30, retryPauseTime=60, **overrideParams):
        FileManager.__init__(self, numberOfRetries = numberOfRetries, retryPauseTime=retryPauseTime, **overrideParams)
    def __call__(self, fileToStage):
        """
        stages in a file, fileToStage is a dict with at least the LFN key
        the dict will be modified and returned, or an exception will be raised
        """
        return self.stageIn(fileToStage)
    
class StageOutMgr(FileManager):
    def __init__(self, numberOfRetries = 30, retryPauseTime=60, **overrideParams):
        FileManager.__init__(self, numberOfRetries = numberOfRetries, retryPauseTime=retryPauseTime, **overrideParams)
    def __call__(self, fileToStage):
        """
        stages out a file, fileToStage is a dict with at least the LFN key
        the dict will be modified and returned, or an exception will be raised
        """
        log.info("StageOutMgr called with file: %s" % fileToStage)
        return self.stageOut(fileToStage)

class DeleteMgr(FileManager):
    def __init__(self, numberOfRetries = 30, retryPauseTime=60, **overrideParams):
        FileManager.__init__(self, numberOfRetries = numberOfRetries, retryPauseTime=retryPauseTime, **overrideParams)
    def __call__(self, fileToDelete):
        """
        stages out a file, fileToStage is a dict with at least the LFN key
        the dict will be modified and returned, or an exception will be raised
        """
        if not 'LFN' in fileToDelete:
            raise StageOutFailure, 'LFN not provided to deleteLFN'
        return self.deleteLFN(fileToDelete['LFN'])




