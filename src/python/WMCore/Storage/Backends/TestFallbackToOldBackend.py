#!/usr/bin/env python
"""
_TestFallbackToOldBackend_

A copy of the CP implementation that's not implemented with the V2 plugins
this will let us test that the fallback works properly

"""
from __future__ import print_function

import os

from WMCore.Storage.Execute import runCommandWithOutput
from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl


class TestFallbackToOldBackendImpl(StageOutImpl):
    """
    _TestFallbackToOldBackendImpl_

    Implement interface for plain cp command

    """

    def createSourceName(self, protocol, pfn):
        """
        _createSourceName_

        uses pfn

        """
        return "%s" % pfn

    def createOutputDirectory(self, targetPFN):
        """
        _createOutputDirectory_

        create dir with group permission
        """
        targetdir = os.path.dirname(targetPFN)

        checkdircmd = "/bin/ls %s > /dev/null " % targetdir
        print("Check dir existence : %s" % checkdircmd)
        try:
            checkdirexitCode, output = runCommandWithOutput(checkdircmd)
        except Exception as ex:
            msg = "Warning: Exception while invoking command:\n"
            msg += "%s\n" % checkdircmd
            msg += "Exception: %s\n" % str(ex)
            msg += "Go on anyway..."
            print(msg)

        if checkdirexitCode:
            mkdircmd = "/bin/mkdir -m 775 -p %s" % targetdir
            print("=> creating the dir : %s" % mkdircmd)
            try:
                exitCode, output = runCommandWithOutput(mkdircmd)
            except Exception as ex:
                msg = "Warning: Exception while invoking command:\n"
                msg += "%s\n" % mkdircmd
                msg += "Exception: %s\n" % str(ex)
                msg += "Go on anyway..."
                print(msg)
            if exitCode:
                msg = "Warning: failed to create the dir %s with the following error:\n%s" % (targetdir, output)
                print(msg)
            else:
                print("=> dir %s correctly created" % targetdir)
        else:
            print("=> dir already exists... do nothing.")

    def createStageOutCommand(self, sourcePFN, targetPFN, options=None, checksums=None, authMethod=None, forceMethod=False):
        """
        _createStageOutCommand_

        Build an cp command

        """
        original_size = os.stat(sourcePFN)[6]
        print("Local File Size is: %s" % original_size)
        result = " /bin/cp "
        if options != None:
            result += " %s " % options
        result += " %s " % sourcePFN
        result += " %s " % targetPFN
        result += """
        DEST_SIZE=`/bin/ls -l %s | awk '{print $5}'`
        if [ $DEST_SIZE ] && [ '%s' -eq $DEST_SIZE ]; then
            exit 0
        else
            echo "ERROR: Size Mismatch between local and SE"
            exit 60311
        fi
        """ % (targetPFN, original_size)
        print(result)
        return result

    def createDebuggingCommand(self, sourcePFN, targetPFN, options=None, checksums=None, authMethod=None, forceMethod=False):
        """
        Debug a failed copy command for stageOut, without re-running it,
        providing information on the environment and the certifications

        :sourcePFN: str, PFN of the source file
        :targetPFN: str, destination PFN
        :options: str, additional options for copy command
        :checksums: dict, collect checksums according to the algorithms saved as keys
        """
        # Build the command for debugging purposes
        copyCommand = " /bin/cp "
        if options != None:
            copyCommand += " %s " % options
        copyCommand += " %s " % sourcePFN
        copyCommand += " %s " % targetPFN

        result = self.debuggingTemplate.format(copy_command=copyCommand, source=sourcePFN, destination=targetPFN)
        return result

    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided

        """
        command = "/bin/rm %s" % pfnToRemove
        self.executeCommand(command)


registerStageOutImpl("testFallbackToOldBackend", TestFallbackToOldBackendImpl)
