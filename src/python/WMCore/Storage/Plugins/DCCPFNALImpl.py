#!/usr/bin/env python
"""
_DCCPFNALImpl_

Implementation of StageOutImpl interface for DCCPFNAL
"""

import os
import commands
import logging
import subprocess
from WMCore.Storage.StageOutError import StageOutError, StageOutFailure

from WMCore.Storage.Registry import registerStageOutImplVersionTwo, retrieveStageOutImpl
from WMCore.Storage.StageOutImplV2 import StageOutImplV2
from WMCore.Storage.Execute import runCommandWithOutput as runCommand

from WMCore.WMException import WMException
from WMCore.WMBase import getWMBASE

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



class DCCPFNALImpl(StageOutImplV2):
    """
    _DCCPFNALImpl_

    Implement interface for dcache door based dccp command

    """
    
    def doWrapped(self, commandArgs):
        wrapperPath = os.path.join(getWMBASE(),'src','python','WMCore','Storage','Plugins','DDCPFNAL','wrapenv.sh')
        commandArgs.insert(0,wrapperPath)
        return runCommand(commandArgs)
    
    def doTransfer(self, sourcePFN, targetPFN, stageOut, seName, command, options, protocol  ):
        """
            performs a transfer. stageOut tells you which way to go. returns the new pfn or
            raises on failure. StageOutError (and inherited exceptions) are for expected errors
            such as temporary connection failures. Anything else will be handled as an unexpected
            error and skip retrying with this plugin
        """
        # munge filenames
        if stageOut:
            sourcePFN = self.createSourceName(protocol, sourcePFN)
            targetPFN = self.createTargetName(protocol, targetPFN)
        
        # make directories
        self.createOutputDirectory(targetPFN)
        
        if targetPFN.find('lustre') == -1:
            optionsStr = ""
            if options != None:
                optionsStr = str(options)
            dirname = os.path.dirname(targetPFN)
            if optionsStr:
                # FIXME this prob. won't work if you have more than one option. Need to do a thing like accepting options
                # as a list
                logging.info("Options used in DCCPFNAL transfer. might not work. danger.")
                if stageOut:
                    copyCommand = ["dccp", "-o", "86400",  "-d", "0", "-X", "-role=cmsprod", optionsStr, sourcePFN, targetPFN]
                else:
                    copyCommand = ["dccp", optionsStr, pnfsPfn(sourcePFN), targetPFN]          
            else:
                if stageOut:
                    copyCommand = ["dccp", "-o", "86400",  "-d", "0", "-X", "-role=cmsprod", sourcePFN, targetPFN]
                else:
                    copyCommand = ["dccp", pnfsPfn(sourcePFN), targetPFN]

            logging.info("Staging out with DCCPFNAL")
            logging.info("  commandline: %s" % copyCommand)
            (exitCode, output) = self.doWrapped(copyCommand)
            if not exitCode:
                logging.error("Transfer failed")
                raise StageOutFailure, "DCCP failed - No good"
            # riddle me this, the following line fails with:
            # not all arguments converted during string formatting
            #FIXME
            logging.info("  output from dccp: %s" % output)
            logging.info("  complete. #" )#exit code" is %s" % exitCode)
        
            logging.info("Verifying file")
            (exitCode, output) = self.doWrapped('/opt/d-cache/dcap/bin/check_dCachefilecksum.sh',
                                                    pnfsPfn(targetPFN),
                                                    sourcePFN)
            if not exitCode:
                logging.error("Checksum verify failed")
                try:
                    self.doDelete(targetPFN,None,None,None,None)
                except:
                    pass
                raise StageOutFailure, "DCCP failed - No good"
            
            return targetPFN
        else:
            # looks like lustre -- do a regular CP
            copyGuy = retrieveStageOutImpl('cp',useNewVersion = True)
            return copyGuy.doTransfer(self,sourcePFN,targetPFN,stageOut, seName, command, options, protocol)
        
    
    def doDelete(self, pfnToRemove, seName, command, options, protocol  ):
        """
            deletes a file, raises on error
            StageOutError (and inherited exceptions) are for expected errors
            such as temporary connection failures. Anything else will be handled as an unexpected
            error and skip retrying with this plugin
        """

        if pfnToRemove.find('/store/unmerged/lustre/') == -1:
            pfnSplit = pfnToRemove.split("/store/", 1)[1]
            filePath = "/pnfs/cms/WAX/11/store/%s" % pfnSplit
            command = ["rm","-fv",filePath]
            runCommand(command)
        else: 
            pfnSplit = pfnToRemove.split("/store/unmerged/lustre/", 1)[1]
            pfnToRemove = "/lustre/unmerged/%s" % pfnSplit
            command = ["/bin/rm", pfnToRemove]
            runCommand(command)


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



registerStageOutImplVersionTwo("dccp-fnal", DCCPFNALImpl)

