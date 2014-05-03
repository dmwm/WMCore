#!/usr/bin/env python
"""
_FNALImpl_

Implementation of StageOutImpl interface for FNAL

"""
import os
import commands
import logging
from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl
from WMCore.Storage.Backends.LCGImpl import LCGImpl


_CheckExitCodeOption = True
srmPaths = ['/store/temp/user/', '/store/user/']


def stripPrefixTOUNIX(filePath):
    return filePath.split(".fnal.gov/", 1)[1]


def pnfsPfn2(pfn):
    """
    _pnfsPfn2_
    
    Convert a dcap PFN to a PNFS PFN
    
    """
    return pfn
    # handle lustre location



class FNALImpl(StageOutImpl):
    """
    _FNALImpl_

    Implement interface for dcache xrootd command

    """

    def __init__(self, stagein=False):

        StageOutImpl.__init__(self, stagein)
        
        # Create and hold onto a srm implementation in case we need it
        self.srmImpl = LCGImpl(stagein)


    def storageMethod(self, PFN):
        """
        Return xrdcp or srm
        """

        method = 'srm'
        if PFN.startswith("root://"):
            method = 'xrdcp'
        print "Using method:", method
        return method


    def createOutputDirectory(self, targetPFN):
        """
        _createOutputDirectory_

        Create a dir for the target pfn by translating it to
        a /dcache or /lustre name and calling mkdir
        we don't need to convert it, just mkdir.       
        """
        method = self.storageMethod(targetPFN)

        if method == 'srm':
            self.srmImpl.createOutputDirectory(targetPFN)
        else:
            pfnSplit = stripPrefixTOUNIX(targetPFN)
            targetdir= os.path.dirname(pfnSplit)
            command = "#!/bin/sh\n"
            command += "if [ ! -e \"%s\" ]; then\n" % targetdir
            command += " mkdir -p %s\n" % targetdir
            command += "fi\n"
            self.executeCommand(command)

    def createSourceName(self, protocol, pfn):
        """
        createTargetName
        
        generate the target PFN
        
        """
        method = self.storageMethod(pfn)

        if method == 'srm':
            return self.srmImpl.createSourceName(protocol, pfn)
        elif method == 'xrdcp':
            print "Translated PFN: %s\n To use xrdcp" % pfn
        return pfn



    def createStageOutCommand(self, sourcePFN, targetPFN, options = None, checksums = None):
        """
        _createStageOutCommand_
        
        Build a mkdir to generate the directory
        
        """

        method = self.storageMethod(targetPFN)
        sourceMethod = self.storageMethod(sourcePFN)

        if method == 'srm' or sourceMethod == 'srm':
            return self.srmImpl.createStageOutCommand(sourcePFN, targetPFN, options)

        if getattr(self, 'stageIn', False):
            return self.buildStageInCommand(sourcePFN, targetPFN, options)

        if method == 'xrdcp':
            original_size = os.stat(sourcePFN)[6]
            print "Local File Size is: %s" % original_size
            pfnWithoutChecksum = stripPrefixTOUNIX(targetPFN)
            
            useChecksum = (checksums != None and checksums.has_key('adler32') and not self.stageIn)
            if useChecksum:
                checksums['adler32'] = "%08x" % int(checksums['adler32'], 16)
                # non-functional in 3.3.1 xrootd clients due to bug
                #result += "-ODeos.targetsize=$LOCAL_SIZE\&eos.checksum=%s " % checksums['adler32']

                # therefor embed information into target URL
                targetPFN += "\?eos.targetsize=%s\&eos.checksum=%s" % (original_size, checksums['adler32'])
                print "Local File Checksum is: %s\"\n" % checksums['adler32']
            
            # remove the file first befor it writes. 
            # there is eos bug when disk partition is full.
            result = "/bin/rm -f %s" % pfnWithoutChecksum
            result += "; /usr/bin/xrdcp -d 0 "
            if options != None:
                result += " %s " % options
            result += " %s " % sourcePFN
            result += " %s " % targetPFN
            result += "; if [ $? -eq 0 ] ; then exit 0; else echo \"Error: xrdcp exited with $?\"; exit 60311 ; fi "
            return result



    def buildStageInCommand(self, sourcePFN, targetPFN, options = None):
        """
        _buildStageInCommand_
        
        Create normal dccp commad for staging in files.
        """

        # Even if matched above, some paths are not lustre
        for path in srmPaths:
            if sourcePFN.find(path) != -1:
                dcapLocation = 0
        
        result = "/usr/bin/xrdcp -d 0 "
        if options != None:
            result += " %s " % options
        result += " %s " % sourcePFN
        result += " %s " % targetPFN
        result += "; if [ $? -eq 0 ] ; then exit 0; else echo \"Error: xrdcp exited with $?\"; exit 60311 ; fi "
        return result


    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided
        """

        method = self.storageMethod(pfnToRemove)

        if method == 'srm':
            return self.srmImpl.removeFile(pfnToRemove)
        elif method == 'xrdcp':
            command = "/bin/rm %s" % stripPrefixTOUNIX(pfnToRemove)
            self.executeCommand(command)

registerStageOutImpl("stageout-xrdcp-fnal", FNALImpl)