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
checkPathsCount=4
checkPaths = ['/lustre/unmerged/', '/lustre/temp/', '/store/unmerged/', '/store/temp/']
checkPathsReplace = ['root://cmseos.fnal.gov//lustre/unmerged/', 'root://cmseos.fnal.gov//lustre/temp/', 
                     'root://cmseos.fnal.gov//lustre/unmerged/', 'root://cmseos.fnal.gov//lustre/temp/']
srmPaths = ['/store/temp/user/', '/store/user/']


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



class FNALImpl(StageOutImpl):
    """
    _FNALImpl_

    Implement interface for dcache door based dccp command

    """

    def __init__(self, stagein=False):

        StageOutImpl.__init__(self, stagein)

        # Create and hold onto a srm implementation in case we need it
        self.srmImpl = LCGImpl(stagein)


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
        print "Using method:", method
        return method


    def createOutputDirectory(self, targetPFN):
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
        method =  self.storageMethod(targetPFN)

        if method == 'srm':
            self.srmImpl.createOutputDirectory(targetPFN)
        elif method == 'dccp':
            # only create dir on remote storage
            if targetPFN.find('/pnfs/') == -1:
                return

            pfnSplit = targetPFN.split("WAX/11/store/", 1)[1]
            filePath = "/pnfs/cms/WAX/11/store/%s" % pfnSplit
            directory = os.path.dirname(filePath)
            command = "#!/bin/sh\n"
            command += " . /opt/d-cache/dcap/bin/setenv-cmsprod.sh\n"
            command += "if [ ! -e \"%s\" ]; then\n" % directory
            command += "  mkdir -p %s\n" % directory
            command += "fi\n"
            self.executeCommand(command)
        else:
            for i in range(checkPathsCount):
                if targetPFN.find(checkPaths[i]) != -1:
                    pfnSplit = targetPFN.split(checkPaths[i], 1)[1]
                    filePath = "%s%s" % (checkPathsReplace[i],pfnSplit)
                    targetdir= os.path.dirname(filePath)
                    # checkdircmd="/bin/ls %s > /dev/null " % targetdir
                    # print "Check dir existence : %s" %checkdircmd
                    # checkdirexitCode = 0
                    # try:
                    #     checkdirexitCode = self.executeCommand(checkdircmd)
                    # except Exception, ex:
                    #     msg = "Warning: Exception while invoking command:\n"
                    #     msg += "%s\n" % checkdircmd
                    #     msg += "Exception: %s\n" % str(ex)
                    #     msg += "Go on anyway..."
                    #     print msg
                    #     pass
                    # if checkdirexitCode != 0:
                    #     mkdircmd = "/bin/mkdir -m 775 -p %s" % targetdir
                    #     print "=> creating the dir : %s" %mkdircmd
                    #     try:
                    #         self.executeCommand(mkdircmd)
                    #     except Exception, ex:
                    #         msg = "Warning: Exception while invoking command:\n"
                    #         msg += "%s\n" % mkdircmd
                    #         msg += "Exception: %s\n" % str(ex)
                    #         msg += "Go on anyway..."
                    #         print msg
                    #         pass
                    # else:
                    #     print "=> dir already exists... do nothing."
                    command = "#!/bin/sh\n"
                    command += "if [ ! -e \"%s\" ]; then\n" % targetdir
                    command += "  mkdir -p %s\n" % targetdir
                    command += "fi\n"
                    self.executeCommand(command)



    def createSourceName(self, protocol, pfn):
        """
        createTargetName

        generate the target PFN

        """
        if not pfn.startswith("srm"):
            return pfn

        method =  self.storageMethod(pfn)

        if method == 'srm':
            return self.srmImpl.createSourceName(protocol, pfn)
        elif method == 'dccp':
            print "Translating PFN: %s\n To use dcache door" % pfn
            dcacheDoor = commands.getoutput(
                "/opt/d-cache/dcap/bin/setenv-cmsprod.sh; /opt/d-cache/dcap/bin/select_RdCapDoor.sh")
            pfn = pfn.split("/store/")[1]
            pfn = "%s%s" % (dcacheDoor, pfn)
            print "Created Target PFN with dCache Door: ", pfn
        else:
            print "Translating PFN: %s\n To use lustre" % pfn
            for i in range(checkPathsCount):
                if pfn.find(checkPaths[i]) != -1:
                    pfnSplit = pfn.split(checkPaths[i], 1)[1]
                    pfn = "%s%s" % (checkPathsReplace[i],pfnSplit)
        return pfn



    def createStageOutCommand(self, sourcePFN, targetPFN, options = None, checksums = None):
        """
        _createStageOutCommand_

        Build a dccp command with a pnfs mkdir to generate the directory

        """

        method =  self.storageMethod(targetPFN)
        sourceMethod = self.storageMethod(sourcePFN)

        if method == 'srm' or sourceMethod == 'srm':
            return self.srmImpl.createStageOutCommand(sourcePFN, targetPFN, options)

        if getattr(self, 'stageIn', False):
            return self.buildStageInCommand(sourcePFN, targetPFN, options)

        if method == 'dccp':
            optionsStr = ""
            if options != None:
                optionsStr = str(options)
            dirname = os.path.dirname(targetPFN)
            result = "#!/bin/sh\n"
            result += ". /opt/d-cache/dcap/bin/setenv-cmsprod.sh\n"
            result += "dccp -o 86400  -d 0 -X -role=cmsprod %s %s %s" % ( optionsStr, sourcePFN, targetPFN)

            result += \
"""
EXIT_STATUS=$?
echo "dccp exit status: $EXIT_STATUS"
if [[ $EXIT_STATUS != 0 ]]; then
   echo "Non-zero dccp Exit status!!!"
   echo "Cleaning up failed file:"
   /bin/rm -fv %s
   exit 60311
fi
"""  % pnfsPfn2(targetPFN)

            #  //
            # //  CRC check
            #//
            result += \
"""
/opt/d-cache/dcap/bin/check_dCachefilecksum.sh %s %s
EXIT_STATUS=$?
echo "CRC Check Exit status: $EXIT_STATUS"
if [[ $EXIT_STATUS != 0 ]]; then
   echo "Non-zero CRC Check Exit status!!!"
   echo "Cleaning up failed file:"
   /bin/rm -fv %s
   exit 60311
fi

""" % (pnfsPfn2(targetPFN), sourcePFN, pnfsPfn2(targetPFN))

            print "Executing:\n", result
            return result

        else:
                
            original_size = os.stat(sourcePFN)[6]
            print "Local File Size is: %s" % original_size
            
            useChecksum = (checksums != None and checksums.has_key('adler32') and not self.stageIn)
            if useChecksum:    
                checksums['adler32'] = "%08x" % int(checksums['adler32'], 16)

                # non-functional in 3.3.1 xrootd clients due to bug
                #result += "-ODeos.targetsize=$LOCAL_SIZE\&eos.checksum=%s " % checksums['adler32']

                # therefor embed information into target URL
                targetPFN += "\?eos.targetsize=%s\&eos.checksum=%s" % (original_size, checksums['adler32'])
                print "Local File Checksum is: %s\"\n" % checksums['adler32']
                
            result = "/usr/bin/xrdcp -d 0 "
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

        dcapLocation = 0
        for i in range(checkPathsCount):
            if sourcePFN.find(checkPaths[i]) != -1:
                dcapLocation = 1

        # Even if matched above, some paths are not lustre
        for path in srmPaths:
            if sourcePFN.find(path) != -1:
                dcapLocation = 0

        if dcapLocation == 0:
            optionsStr = ""
            if options != None:
                optionsStr = str(options)
            dirname = os.path.dirname(targetPFN)
            result = "#!/bin/sh\n"
            result += "dccp %s %s %s" % (optionsStr, pnfsPfn2(sourcePFN), targetPFN)
            result += \
"""
EXIT_STATUS=$?
echo "dccp exit status: $EXIT_STATUS"
if [[ $EXIT_STATUS != 0 ]]; then
   echo "Non-zero dccp Exit status!!!"
   echo "Cleaning up failed file:"
   /bin/rm -fv %s
   exit 60311
fi
"""  % targetPFN

            #  //
            # //  Size Check
            #//
            result += \
"""
DEST_SIZE=`dcsize %s | cut -d" " -f1`
FILE_SIZE=`dcsize %s | cut -d" " -f1`
if [[ $DEST_SIZE == "" || $FILE_SIZE == "" ]]; then
    echo "dcsize command is not available or produced an invalid result."
    echo "Trying stat command:"
    DEST_SIZE=`stat -c %s %s`
    FILE_SIZE=`stat -c %s %s`
fi
if [[ $DEST_SIZE == "" || $FILE_SIZE == "" ]]; then
    echo "stat command is not available or produced an invalid result."
    echo "Trying ls command:"
    DEST_SIZE=`/bin/ls -l %s | awk '{ print $5 }'`
    FILE_SIZE=`/bin/ls -l %s | awk '{ print $5 }'`
fi
if [ $FILE_SIZE != $DEST_SIZE ]; then
    echo "Source and destination files do not have same file size."
    echo "Cleaning up failed file:"
    /bin/rm -fv %s
    exit 60311
fi
""" % (pnfsPfn2(targetPFN), pnfsPfn2(sourcePFN),
       '%s', pnfsPfn2(targetPFN), '%s', pnfsPfn2(sourcePFN),
       pnfsPfn2(targetPFN), pnfsPfn2(sourcePFN),
       pnfsPfn2(targetPFN))

            print "Executing:\n", result
            return result

        else:
            for i in range(checkPathsCount):
                if sourcePFN.find(checkPaths[i]) != -1:
                    pfnSplit = sourcePFN.split(checkPaths[i], 1)[1]
                    filePath = "%s%s" % (checkPathsReplace[i],pfnSplit)
            original_size = os.stat(filePath)[6]
            print "Local File Size is: %s" % original_size
            result = "/usr/bin/xrdcp -d 0 "
            if options != None:
                result += " %s " % options
            result += " %s " % filePath
            result += " %s " % targetPFN
            result += "; if [ $? -eq 0 ] ; then exit 0; else echo \"Error: xrdcp exited with $?\"; exit 60311 ; fi "
            return result


    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided

        """

        method =  self.storageMethod(pfnToRemove)

        if method == 'srm':
            return self.srmImpl.removeFile(pfnToRemove)
        elif method == 'dccp':
            pfnSplit = pfnToRemove.split("/11/store/", 1)[1]
            filePath = "/pnfs/cms/WAX/11/store/%s" % pfnSplit
            command = "rm -fv %s" %filePath
            self.executeCommand(command)
        else:
            for i in range(checkPathsCount):
                if pfnToRemove.find(checkPaths[i]) != -1:
                    pfnSplit = pfnToRemove.split(checkPaths[i], 1)[1]
                    pfnToRemove = "%s%s" % (checkPathsReplace[i],pfnSplit)
            command = "/bin/rm %s" % pfnToRemove
            self.executeCommand(command)



registerStageOutImpl("stageout-fnal", FNALImpl)
