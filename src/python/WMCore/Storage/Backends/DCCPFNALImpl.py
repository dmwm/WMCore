#!/usr/bin/env python
"""
_DCCPFNALImpl_

Implementation of StageOutImpl interface for DCCPFNAL
"""

import os
import commands

from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl

from WMCore.WMException import WMException

_CheckExitCodeOption = True

def pnfsPfn(pfn):
    """
    _pnfsPfn_

    Convert a dcap PFN to a PNFS PFN

    """
    # only change PFN on remote storage
    if pfn.find('/pnfs/') == -1:
        return pfn

    pfnSplit = pfn.split("WAX/11/store/", 1)[1]
    filePath = "/pnfs/cms/WAX/11/store/%s" % pfnSplit
    
    # handle lustre location
    if pfn.find('/store/unmerged/lustre/') == -1:
        return filePath
    else:
        pfnSplit = pfn.split("/store/unmerged/lustre/", 1)[1]
        filePath = "/lustre/unmerged/%s" % pfnSplit
        return filePath



class DCCPFNALImpl(StageOutImpl):
    """
    _DCCPFNALImpl_

    Implement interface for dcache door based dccp command

    """


    def createOutputDirectory(self, targetPFN):
        """
        _createOutputDirectory_

        Create a dir for the target pfn by translating it to
        a /pnfs name and calling mkdir

        PFN will be of the form:
        dcap://cmsdca.fnal.gov:22125/pnfs/fnal.gov/usr/cms/WAX/11/store/blah

        We need to convert that into /pnfs/cms/WAX/11/store/blah, as it
        will be seen from the worker node

        """
        print "createOutputDirectory(): %s" % targetPFN

        # only create dir on remote storage
        if targetPFN.find('/pnfs/') == -1 and targetPFN.find('lustre') == -1:
            return

        # handle dcache or lustre location
        if targetPFN.find('lustre') == -1:
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
            targetdir= os.path.dirname(targetPFN)
            checkdircmd="/bin/ls %s > /dev/null " % targetdir
            print "Check dir existence : %s" %checkdircmd 
            try:
                (checkdirexitCode, output) = commands.getstatusoutput(checkdircmd)
            except Exception, ex:
                msg = "Warning: Exception while invoking command:\n"
                msg += "%s\n" % checkdircmd
                msg += "Exception: %s\n" % str(ex)
                msg += "Go on anyway..."
                print msg
                pass
            if checkdirexitCode:
                mkdircmd = "/bin/mkdir -m 775 -p %s" % targetdir
                print "=> creating the dir : %s" %mkdircmd
                try:
                    self.executeCommand(mkdircmd)
                except Exception, ex:
                    msg = "Warning: Exception while invoking command:\n"
                    msg += "%s\n" % mkdircmd
                    msg += "Exception: %s\n" % str(ex)
                    msg += "Go on anyway..."
                    print msg
                    pass
            else:
                print "=> dir already exists... do nothing."


    def createSourceName(self, protocol, pfn):
        """
        createTargetName

        generate the target PFN

        """
        if not pfn.startswith("srm"):
            return pfn

        if pfn.find('/store/unmerged/lustre/') == -1:
            print "Translating PFN: %s\n To use dcache door" % pfn
            dcacheDoor = commands.getoutput(
                "/opt/d-cache/dcap/bin/setenv-cmsprod.sh; /opt/d-cache/dcap/bin/select_RdCapDoor.sh")
            pfn = pfn.split("/store/")[1]
            pfn = "%s%s" % (dcacheDoor, pfn)
            print "Created Target PFN with dCache Door: ", pfn
        else: 
            pfnSplit = pfn.split("/store/unmerged/lustre/", 1)[1]
            pfn = "/lustre/unmerged/%s" % pfnSplit

        return pfn



    def createStageOutCommand(self, sourcePFN, targetPFN, options = None):
        """
        _createStageOutCommand_

        Build a dccp command with a pnfs mkdir to generate the directory

        """

        if getattr(self, 'stageIn', False):
            return self.buildStageInCommand(sourcePFN, targetPFN, options)

        
        if targetPFN.find('lustre') == -1:
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
"""  % pnfsPfn(targetPFN)

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

""" % (pnfsPfn(targetPFN), sourcePFN, pnfsPfn(targetPFN))

            print "Executing:\n", result
            return result

        else:
            original_size = os.stat(sourcePFN)[6]
            print "Local File Size is: %s" % original_size
            result = "/bin/cp "
            if options != None:
                result += " %s " % options
            result += " %s " % sourcePFN
            result += " %s " % targetPFN
            result += "; DEST_SIZE=`/bin/ls -l %s | /bin/awk '{print $5}'` ; if [ $DEST_SIZE ] && [ '%s' == $DEST_SIZE ]; then exit 0; else echo \"Error: Size Mismatch between local and SE\"; exit 60311 ; fi " % (targetPFN,original_size)
            return result



    def buildStageInCommand(self, sourcePFN, targetPFN, options = None):
        """
        _buildStageInCommand_

        Create normal dccp commad for staging in files.
        """


        if targetPFN.find('lustre') == -1:
            optionsStr = ""
            if options != None:
                optionsStr = str(options)
            dirname = os.path.dirname(targetPFN)
            result = "#!/bin/sh\n"
            result += "dccp %s %s %s" % (optionsStr, pnfsPfn(sourcePFN), targetPFN)
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
""" % (pnfsPfn(targetPFN), pnfsPfn(sourcePFN),
       '%s', pnfsPfn(targetPFN), '%s', pnfsPfn(sourcePFN),
       pnfsPfn(targetPFN), pnfsPfn(sourcePFN),
       pnfsPfn(targetPFN))

            print "Executing:\n", result
            return result

        else:
            original_size = os.stat(sourcePFN)[6]
            print "Local File Size is: %s" % original_size
            result = "/bin/cp "
            if options != None:
                result += " %s " % options
            result += " %s " % sourcePFN
            result += " %s " % targetPFN
            result += "; DEST_SIZE=`/bin/ls -l %s | /bin/awk '{print $5}'` ; if [ $DEST_SIZE ] && [ '%s' == $DEST_SIZE ]; then exit 0; else echo \"Error: Size Mismatch between local and SE\"; exit 60311 ; fi " % (targetPFN,original_size)
            return result


    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided

        """
        if pfnToRemove.find('/store/unmerged/lustre/') == -1:
            pfnSplit = pfnToRemove.split("/store/", 1)[1]
            filePath = "/pnfs/cms/WAX/11/store/%s" % pfnSplit
            command = "rm -fv %s" %filePath
            self.executeCommand(command)
        else: 
            pfnSplit = pfnToRemove.split("/store/unmerged/lustre/", 1)[1]
            pfnToRemove = "/lustre/unmerged/%s" % pfnSplit
            command = "/bin/rm %s" % pfnToRemove
            self.executeCommand(command)



registerStageOutImpl("dccp-fnal", DCCPFNALImpl)

