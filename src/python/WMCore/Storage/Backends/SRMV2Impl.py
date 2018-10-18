#!/usr/bin/env python
"""
_SRMV2Impl_

Implementation of StageOutImpl interface for SRM Version 2

"""
from __future__ import print_function

import os
import re

from WMCore.Storage.Execute import runCommandWithOutput
from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutError import StageOutError
from WMCore.Storage.StageOutImpl import StageOutImpl

_CheckExitCodeOption = True


class SRMV2Impl(StageOutImpl):
    """
    _SRMV2Impl_

    Implement interface for srmcp v2 command

    """
    # also used in unittests
    run = staticmethod(runCommandWithOutput)

    def __init__(self, stagein=False):
        StageOutImpl.__init__(self, stagein)

    def createSourceName(self, protocol, pfn):
        """
        _createSourceName_

        SRM uses file:/// urls

        """
        if pfn.startswith('/'):
            return "file:///%s" % pfn
        elif os.path.isfile(pfn):
            return "file:///%s" % os.path.abspath(pfn)
        else:
            return pfn

    def createOutputDirectory(self, targetPFN):
        """
        _createOutputDirectory_

        SRMV2 does not create directories,
            see http://sdm.lbl.gov/srm-wg/doc/SRM.v2.2.html#_Toc199734394

        """
        targetdir = os.path.dirname(targetPFN)
        if self.stageIn:
            # stage in to local directory - should exist but you never know
            if not os.path.exists(targetdir):
                os.makedirs(targetdir)
            return

        mkdircommand = "srmmkdir -retry_num=0 "
        checkdircmd = "srmls -recursion_depth=0 -retry_num=1 "

        #  // Loop from top level checking existence stop when directory exists
        # // assume first 4 slashes are from srm://host:8443/srm/managerv2?SFN=
        dirs = ["/".join(targetdir.split("/")[0:6 + i]) \
                for i in range(targetdir.count("/") - 4)]
        dirsToCheck = dirs[:];
        dirsToCheck.reverse()
        levelToCreateFrom = len(dirs)
        for count, folder in zip(range(len(dirsToCheck), 0, -1), dirsToCheck):
            try:
                exitCode, output = self.run(checkdircmd + folder)
                levelToCreateFrom = count  # create dirs from here (at least)
                if exitCode:  # did srmls fail to execute properly?
                    raise RuntimeError("ERROR checking directory existence, %s" % str(output))
                if not output.count('SRM_FAILURE'):  # any other codes?
                    break
            except Exception as ex:
                msg = "Warning: Exception while invoking command:\n"
                msg += "%s\n" % checkdircmd + folder
                msg += "Exception: %s\n" % str(ex)
                msg += "Go on anyway..."
                print(msg)

        # // Create needed directory levels from end of previous loop
        # //  to end of directory structure
        for folder in dirs[levelToCreateFrom:]:
            print("Create directory: %s" % folder)
            try:
                exitCode, output = self.run(mkdircommand + folder)
                if exitCode:
                    raise RuntimeError("ERROR creating directory, %s" % str(output))
            except Exception as ex:
                msg = "Warning: Exception while invoking command:\n"
                msg += "%s\n" % mkdircommand + folder
                msg += "Exception: %s\n" % str(ex)
                msg += "Go on anyway..."
                print(msg)

    def createRemoveFileCommand(self, pfn):
        """
        handle both srm and file pfn types
        """
        if pfn.startswith("srm://"):
            return "srmrm -2 -retry_num=0 %s" % pfn
        elif pfn.startswith("file:"):
            return "/bin/rm -f %s" % pfn.replace("file://", "", 1)
        else:
            return StageOutImpl.createRemoveFileCommand(self, pfn)

    def createStageOutCommand(self, sourcePFN, targetPFN, options=None, checksums=None):
        """
        _createStageOutCommand_

        Build an srmcp command

        srmcp options used (so hard to find documentation for it...):
          -2                enables srm protocol version 2
          -report           path to the report file
          -retry_num        number of retries before before client gives up
          -request_lifetime request lifetime in seconds
        """
        result = "#!/bin/sh\n"
        result += "REPORT_FILE=`pwd`/srm.report.$$\n"
        result += "srmcp -2 -report=$REPORT_FILE -retry_num=0 -request_lifetime=2400"

        if options != None:
            result += " %s " % options
        result += " %s " % sourcePFN
        result += " %s" % targetPFN
        result += " 2>&1 | tee srm.output.$$ \n"

        if _CheckExitCodeOption:
            result += """
            EXIT_STATUS=`cat $REPORT_FILE | cut -f3 -d" "`
            echo "srmcp exit status: $EXIT_STATUS"
            if [[ "X$EXIT_STATUS" == "X" ]] && [[ `grep -c SRM_INVALID_PATH srm.output.$$` != 0 ]]; then
                echo "ERROR: srmcp failed with SRM_INVALID_PATH"
                exit 1   # dir does not exist
            elif [[ $EXIT_STATUS != 0 ]]; then
               echo "ERROR: srmcp exited with $EXIT_STATUS"
               echo "Cleaning up failed file:"
               %s
               exit $EXIT_STATUS
            fi

            """ % self.createRemoveFileCommand(targetPFN)

        if self.stageIn:
            remotePFN, localPFN = sourcePFN, targetPFN.replace("file://", "", 1)
        else:
            remotePFN, localPFN = targetPFN, sourcePFN.replace("file://", "", 1)
        remotePath = None
        SFN = '?SFN='
        sfn_idx = remotePFN.find(SFN)
        if sfn_idx >= 0:
            remotePath = remotePFN[sfn_idx + 5:]
        r = re.compile('srm://([A-Za-z\-\.0-9]*)(:[0-9]*)?(/.*)')
        m = r.match(remotePFN)
        if not m:
            raise StageOutError("Unable to determine path from PFN for " \
                                "target %s." % remotePFN)
        if remotePath == None:
            remotePath = m.groups()[2]
        remoteHost = m.groups()[0]

        result += "FILE_SIZE=`stat -c %s"
        result += " %s `\n" % localPFN
        result += "echo \"Local File Size is: $FILE_SIZE\"\n"

        metadataCheck = \
            """
            SRM_OUTPUT=`srmls -recursion_depth=0 -retry_num=1 %s 2>/dev/null`
            SRM_SIZE=`echo $SRM_OUTPUT | grep '%s' | grep -v '%s' | awk '{print $1;}'`
            echo "SRM Size is $SRM_SIZE"
            if [[ $SRM_SIZE == $FILE_SIZE ]]; then
               exit 0
            else
               echo $SRM_OUTPUT
               echo "ERROR: Size Mismatch between local and SE. Cleaning up failed file..."
               %s
               exit 60311
            fi
            """ % (remotePFN, remotePath, remoteHost, self.createRemoveFileCommand(targetPFN))
        result += metadataCheck

        return result

    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided

        """
        command = "srmrm -2 -retry_num=0 %s" % pfnToRemove
        self.executeCommand(command)


registerStageOutImpl("srmv2", SRMV2Impl)
