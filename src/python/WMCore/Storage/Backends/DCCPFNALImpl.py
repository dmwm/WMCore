#!/usr/bin/env python
"""
_DCCPFNALImpl_

Implementation of StageOutImpl interface for DCCPFNAL

"""

import os
import commands

from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl

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


    def createSourceName(self, protocol, pfn):
        """
        createTargetName

        generate the target PFN

        """
        if not pfn.startswith("srm"):
            return pfn

        print "Translating PFN: %s\n To use dcache door" % pfn
        dcacheDoor = commands.getoutput(
            "/opt/d-cache/dcap/bin/setenv-cmsprod.sh; /opt/d-cache/dcap/bin/select_RdCapDoor.sh")


        pfn = pfn.split("/store/")[1]
        pfn = "%s%s" % (dcacheDoor, pfn)


        print "Created Target PFN with dCache Door: ", pfn

        return pfn





    def createStageOutCommand(self, sourcePFN, targetPFN, options = None):
        """
        _createStageOutCommand_

        Build a dccp command with a pnfs mkdir to generate the directory

        """

        if getattr(self, 'stageIn', False):
            return self.buildStageInCommand(sourcePFN, targetPFN, options)

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


    def buildStageInCommand(self, sourcePFN, targetPFN, options = None):
        """
        _buildStageInCommand_

        Create normal dccp commad for staging in files.
        """
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
DEST_SIZE=`ls -l %s | cut -d" " -f6`
FILE_SIZE=`ls -l %s | cut -d" " -f6`
if [ $FILE_SIZE != $DEST_SIZE ]; then
    echo "Source and destination files do not have same file size."
    echo "Cleaning up failed file:"
   /bin/rm -fv %s
   exit 60311
fi
""" % (pnfsPfn(targetPFN), pnfsPfn(sourcePFN), pnfsPfn(targetPFN))

        print "Executing:\n", result
        return result



    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided

        """
        pfnSplit = pfnToRemove.split("/store/", 1)[1]
        filePath = "/pnfs/cms/WAX/11/store/%s" % pfnSplit
        command = "rm -fv %s" %filePath
        self.executeCommand(command)


registerStageOutImpl("dccp-fnal", DCCPFNALImpl)
