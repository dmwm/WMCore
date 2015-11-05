#!/usr/bin/env python
"""
_LCGImpl_

Implementation of StageOutImpl interface for lcg-cp

"""
import os, re
from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl
from WMCore.Storage.StageOutError import StageOutError

from WMCore.Storage.Execute import runCommandWithOutput as runCommand

_CheckExitCodeOption = True



class LCGImpl(StageOutImpl):
    """
    _LCGImpl_

    Implement interface for srmcp v2 command with lcg-* commands

    """

    run = staticmethod(runCommand)

    def __init__(self, stagein=False):
        StageOutImpl.__init__(self, stagein)

        self.setups = ''
        setupScripts = {
            'OSG_GRID'           : '/setup.sh',
            'GLITE_WMS_LOCATION' : '/etc/profile.d/glite-wmsui.sh',
            'GLITE_LOCATION'     : '/../etc/profile.d/grid-env.sh',
            'GRID_ENV_LOCATION'  : '/grid-env.sh',
        }

        for env, script in setupScripts.iteritems():
            if os.environ.get(env):
                fullScript = os.path.normpath(os.path.join(os.environ[env], script))
                if os.path.isfile(fullScript):
                    self.setups += 'source %s; ' % fullScript


    def createSourceName(self, protocol, pfn):
        """
        _createSourceName_

        SRM uses file:/// urls

        """
        if pfn.startswith('/'):
            return "file:%s" % pfn
        elif os.path.isfile(pfn):
            return "file:%s" % os.path.abspath(pfn)
        else:
            return pfn


    def createOutputDirectory(self, targetPFN):
        """
        _createOutputDirectory_

        lcg-cp handles this

        """
        return

    def createRemoveFileCommand(self, pfn):
        """
        handle both srm and file pfn types
        """
        if pfn.startswith("srm://"):
            return "%s lcg-del -b -l -D srmv2 --vo cms %s" % (self.setups, pfn)
        elif pfn.startswith("file:"):
            return "/bin/rm -f %s" % pfn.replace("file:", "", 1)
        else:
            return StageOutImpl.createRemoveFileCommand(self, pfn)


    def createStageOutCommand(self, sourcePFN, targetPFN, options = None, checksums = None):
        """
        _createStageOutCommand_

        Build an srmcp command

        """
        result = "#!/bin/sh\n"

        # check if we should use the grid UI from CVMFS
        useCVMFS = False
        if options == "cvmfs":
            options = None
            useCVMFS = True

        copyCommand = "lcg-cp -b -D srmv2 --vo cms --srm-timeout 2400 --sendreceive-timeout 2400 --connect-timeout 300 --verbose"
        if options != None:
            copyCommand += " %s " % options
        copyCommand += " %s " % sourcePFN
        copyCommand += " %s 2> stageout.log" % targetPFN

        # for CVMFS skip the usual environment setup and use grid.cern.ch instead
        # also open a sub-shell and remove the cms.cern.ch path elements
        if useCVMFS:
            result += "(\n"

            result += "echo Modifying PATH and LD_LIBRARY_PATH to remove /cvmfs/cms.cern.ch elements\n"
            result += "export PATH=`echo $PATH | sed -e 's+:*/cvmfs/cms.cern.ch/[^:]*++'g`\n"
            result += "export LD_LIBRARY_PATH=`echo $LD_LIBRARY_PATH | sed -e 's+:*/cvmfs/cms.cern.ch/[^:]*++'g`\n"

            result += "echo Sourcing CVMFS UI setup script\n"
            result += ". /cvmfs/grid.cern.ch/emi-ui-2.9.0-1_sl5v1/etc/profile.d/setup-cvmfs-ui.sh\n"

            result += copyCommand
        else:
            result += self.setups 
            result += copyCommand

        if _CheckExitCodeOption:
            result += """
            EXIT_STATUS=$?
            echo "lcg-cp logging:"; cat stageout.log
            echo -e "\nlcg-cp exit status: $EXIT_STATUS"
            if [[ $EXIT_STATUS != 0 ]]; then
                echo "Non-zero lcg-cp Exit status!!!"
                echo "Cleaning up failed file:"
                %s
                exit 60311
            fi

            """ % self.createRemoveFileCommand(targetPFN)

        if self.stageIn:
            remotePFN, localPFN = sourcePFN, targetPFN.replace("file:", "", 1)
        else:
            remotePFN, localPFN = targetPFN, sourcePFN.replace("file:", "", 1)

        result += "FILE_SIZE=`stat -c %s"
        result += " %s`\n" % localPFN
        result += "echo \"Local File Size is: $FILE_SIZE\"\n"

        removeCommand = self.createRemoveFileCommand(targetPFN)

        useChecksum = (checksums != None and 'adler32' in checksums and not self.stageIn)

        if useChecksum:
            localAdler32 = str(checksums['adler32']).zfill(8)
            checksumCommand = \
            """
            if [[ "X$SRM_CHECKSUM" != "X" ]]; then
                if [[ "$SRM_CHECKSUM" == "%s" ]]; then
                    exit 0
                else
                    echo "ERROR: Checksum Mismatch between local and SE"
                    echo "Cleaning up failed file"
                    %s
                    exit 60311
                fi
            else
                exit 0
            fi
            """ % (localAdler32, removeCommand)
        else:
            checksumCommand = "exit 0"

        metadataCheck = \
        """
        LCG_OUTPUT=`lcg-ls -l -b -D srmv2 --srm-timeout 1800 %s 2>/dev/null`
        SRM_SIZE=`echo "$LCG_OUTPUT" | awk 'NR==1{print $5}'`
        SRM_CHECKSUM=`echo "$LCG_OUTPUT" | sed -nr 's/^.*\s([a-f0-9]{8})\s*\([aA][dD][lL][eE][rR]32\)\s*$/\\1/p'`
        echo "Remote Size is $SRM_SIZE"
        echo "Remote Checksum is $SRM_CHECKSUM"
        if [[ $SRM_SIZE == $FILE_SIZE ]]; then
            %s
        else
            echo $LCG_OUTPUT
            echo "ERROR: Size Mismatch between local and SE. Cleaning up failed file..."
            %s
            exit 60311
        fi
        echo "Cleaning up failed file:"
        %s
        exit 60311

        """ % (remotePFN, checksumCommand, removeCommand, removeCommand)
        result += metadataCheck

        # close sub-shell for CVMFS use case
        if useCVMFS:
            result += ")\n"

        return result


    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided

        """
        command = "%s lcg-del -b -l -D srmv2 --srm-timeout 1800 --vo cms %s" % (self.setups, pfnToRemove)
        self.executeCommand(command)


registerStageOutImpl("srmv2-lcg", LCGImpl)
