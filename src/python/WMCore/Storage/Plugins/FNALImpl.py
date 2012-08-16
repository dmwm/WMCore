#!/usr/bin/env python
"""
_FNALImpl_

Implementation of StageOutImpl interface for FNAL

"""
import os
import logging
import commands
from WMCore.Storage.Plugins.LCGImpl import LCGImpl
from WMCore.Storage.Plugins.CPImpl import CPImpl


from WMCore.Storage.StageOutImplV2 import StageOutImplV2
from WMCore.Storage.StageOutError import StageOutError, StageOutFailure


_CheckExitCodeOption = True
checkPathsCount = 5
checkPaths = ['/lustre/unmerged/', '/lustre/temp/', '/store/unmerged/', '/store/temp/','/store/user/meloam/lustre']
checkPathsReplace = ['/lustre/unmerged/', '/lustre/temp/', '/lustre/unmerged/', '/lustre/temp/','/uscms_data/d3/meloam/wmagent-lustre']
srmPaths = ['/store/temp/user/', '/store/user/']
dccpPaths = ['/store/user/meloam/t4/']

# For testing
envScript = "/opt/d-cache/dcap/bin/setenv-cmsprod.sh"
#envScript = "/uscms_data/d3/meloam/wmagentstuff/site-packages/setenv-meloam.sh"

def pnfsPfn2(pfn):
    """
    _pnfsPfn2_

    Convert a dcap PFN to a PNFS PFN

    """
    # only change PFN on remote storage
    if pfn.find('/pnfs/') == -1:
        return pfn

    pfnSplit = pfn.split("WAX/11/store/", 1)[1]
    filePath = "/pnfs/cms/WAX/11/store/%s" % pfnSplit

    # Find vetoed paths first
    for path in srmPaths:
        if pfn.find(path) != -1:
            return filePath

    # handle lustre location
    for i in range(checkPathsCount):
        if pfn.find(checkPaths[i]) != -1:
          pfnSplit = pfn.split(checkPaths[i], 1)[1]
	  filePath = "%s%s" % (checkPathsReplace[i],pfnSplit)
    return filePath



class FNALImpl(StageOutImplV2):
    """
    _FNALImpl_

    Implement interface for dcache door based dccp command

    """

    def __init__(self):

        StageOutImplV2.__init__(self)

        # Create and hold onto a srm implementation in case we need it
        self.srmImpl = LCGImpl()
        self.cpImpl  = CPImpl()

    def storageMethod(self, PFN):
        """
        Figure out which paths use DCAP, lustre, or SRM for access
        """

        method = 'dccp'
        for path in checkPaths:
            if PFN.find(path) != -1:
                method = 'lustre'

        # Over ride a few paths for srm
        for path in srmPaths:
            if PFN.find(path) != -1:
                method = 'srm'

        # overrides for testing
        for path in dccpPaths:
            if PFN.find(path) != -1:
                method = 'dccp'

        if PFN.find('/store/user/meloam/lustre/') != -1:
            method = 'lustre'

        logging.debug("Using method %s for PFN %s" % (method, PFN))
        return method

    def substituteLustrePath(self, pfn):
        for i in range(checkPathsCount):
            if pfn.find(checkPaths[i]) != -1:
                pfnSplit = pfn.split(checkPaths[i], 1)[1]
                pfn = "%s%s" % (checkPathsReplace[i],pfnSplit)
        return pfn

    def dcapToPNFS(self, pfn):
        if pfn.find('/pnfs/') == -1:
            raise RuntimeError, "dcapToPNFS called on a non-dcap/pnfs URL: %s" % pfn

        pfnSplit = pfn.split("WAX/11/store/", 1)[1]
        filePath = "/pnfs/cms/WAX/11/store/%s" % pfnSplit
        return filePath
       
    def createOutputDirectoryDCAP(self, targetPFN):
        """
        _createOutputDirectory_

        Create a dir for the target pfn by translating it to
        a /pnfs name and calling mkdir

        PFN will be of the form:
        dcap://cmsdca.fnal.gov:22125/pnfs/fnal.gov/usr/cms/WAX/11/store/blah

        We need to convert that into /pnfs/cms/WAX/11/store/blah, as it
        will be seen from the worker node

        Unmerged PFN will be of the form:
        /lustre/unmerged

        we don't need to convert it, just mkdir.


        """
        logging.info("createOutputDirectoryDCAP called on %s" % targetPFN)
        # only create dir on remote storage
        if targetPFN.find('/pnfs/') == -1:
            return
        
        filePath = self.dcapToPNFS( targetPFN )
        directory = os.path.dirname(filePath)
        logging.info("Creating path: %s" % directory)
        if not os.path.exists( directory ):
            os.makedirs( directory )
        
    def createSourceName(self, protocol, pfn):
        """
        createTargetName

        generate the target PFN

        """
        #if not pfn.startswith("srm"):
        #    return pfn

        method =  self.storageMethod(pfn)

        if method == 'srm':
            return self.srmImpl.createSourceName(protocol, pfn)
        elif method == 'dccp':
            logging.info("Translating PFN for dcache: %s" % pfn)
            pfn = pfn.split("/store/")[1]
            dcacheDoor = commands.getoutput(
                ". " + envScript + "\n" +\
                "/opt/d-cache/dcap/bin/select_RdCapDoor.sh")
            pfn = "%s%s" % (dcacheDoor, pfn)
            logging.info("  Translated PFN: %s" % pfn )
        elif method == 'lustre':
            logging.info("Translating PFN for lustre: %s" % pfn)
            pfn = self.substituteLustrePath( pfn )
            logging.info("  Translated PFN: %s" % pfn)
        else:
            raise RuntimeError, "Unknown method found in createSourceName: %s" % method
    	
        return pfn

    def doDelete(self, pfnToRemove, seName, command, options, protocol ):
        """
        _removeFile_

        CleanUp pfn provided

        """

        method =  self.storageMethod(pfnToRemove)

        if method == 'srm':
            return self.srmImpl.doDelete(pfnToRemove, seName, command, options, protocol)
        elif method == 'dccp':
            pfnSplit = pfnToRemove.split("/11/store/", 1)[1]
            filePath = "/pnfs/cms/WAX/11/store/%s" % pfnSplit
            if os.path.exists( filePath ):
                logging.info( "Removing file: " + filePath )
                os.unlink( filePath )
        elif method == 'lustre':
            pfnToRemove = self.substituteLustrePath( pfnToRemove )
            return self.cpImpl.doDelete(pfnToRemove, seName, command, options, protocol)
        else:
            raise RuntimeError, "Unsupported storage method in doDelete: %s" % method

    def doTransfer( self, fromPfn, toPfn, stageOut, seName, command, options, protocol, checksum ):
        """
            performs a transfer. stageOut tells you which way to go. returns the new pfn or
            raises on failure. StageOutError (and inherited exceptions) are for expected errors
            such as temporary connection failures. Anything else will be handled as an unexpected
            error and skip retrying with this plugin
            
            if stageOut is true:
                The fromPfn is the LOCAL FILE NAME on the node, without file://
                the toPfn is the target PFN, mapped from the LFN using the TFC or overrrides
            if stageOut is false:
                The toPfn is the LOCAL FILE NAME on the node, without file://
                the fromPfn is the source PFN, mapped from the LFN using the TFC or overrrides
            
            this behavior is because most transfer commands will switch their direction
            simply by swapping the order of the arguments. the stageOut flag is provided
            however, because sometimes you want to pass different command line args
        """
        if stageOut:
            localPFN = sourcePFN
            remotePFN = targetPFN
        else:
            localPFN = targetPFN
            remotePFN = sourcePFN
        
        method = self.storageMethod( remotePFN )
        sourceMethod = self.storageMethod( localPFN )

        if method == 'srm' or sourceMethod == 'srm':
            return self.srmImpl.doTransfer( sourcePFN, targetPFN, stageOut,\
                                           seName, command, options, protocol, checksums )

        # transfer with lustre
        elif method == 'lustre':
            sourcePFN = self.substituteLustrePath( sourcePFN )
            targetPFN = self.substituteLustrePath( targetPFN )
            return self.cpImpl.doTransfer( sourcePFN, targetPFN, stageOut,\
                                           seName, command, options, protocol, checksums )

        elif method == 'dccp':
            if stageOut:
                self.createOutputDirectoryDCAP( targetPFN )
                targetPFN = self.createSourceName( protocol, targetPFN )
            else:
                self.createOutputDirectoryDCAP( sourcePFN )
                sourcePFN = self.createSourceName( protocol, sourcePFN )

            optionsStr = ""
            if options != None:
                optionsStr = str(options)
            dirname = os.path.dirname(targetPFN)
            result = "#!/bin/bash\n"
            result += ". " + envScript + "\n"
            result += "dccp -o 86400  -d 0 -X -role=cmsprod %s %s %s" % ( optionsStr, sourcePFN, targetPFN)
            try:
                self.runCommandFailOnNonZero( result )
            except:
                logging.info("dccp failed, removing failed file")
                if not stageOut and os.path.exists( pnfsPfn2(targetPFN) ):
                    logging.info("unlinking %s" % pnfsPfn2(targetPFN))
                    os.unlink( pnfsPfn2(targetPFN) )
                else:
                    self.doDelete( targetPFN, seName, command, options, protocol )
                raise

            #  //
            # //  CRC check
            #//
            try:
                if stageOut:
                    self.runCommandFailOnNonZero( "/opt/d-cache/dcap/bin/check_dCachefilecksum.sh %s %s" \
                                        % (pnfsPfn2(targetPFN), sourcePFN))
                else:
                     self.runCommandFailOnNonZero( "/opt/d-cache/dcap/bin/check_dCachefilecksum.sh %s %s" \
                                        % (pnfsPfn2(sourcePFN), targetPFN))

            except:
                logging.info("DCCP CRC Check failed, removing failed file")
                self.doDelete( targetPFN, seName, command, options, protocol )
                raise
            return

