from __future__ import (print_function, division)
import os
import unittest

from mock import mock

from WMCore.Storage.Backends.LCGImpl import LCGImpl


class LCGImplTest(unittest.TestCase):
    def setUp(self):
        self.LCGImpl = LCGImpl()
        self.LCGImpl.setups = self.setups = "test setups"
        self.timeoutOptions = self.LCGImpl.timeoutOptions = \
            '--srm-timeout 600 --sendreceive-timeout 600 --connect-timeout 300'

    @mock.patch('WMCore.Storage.Backends.LCGImpl.os')
    def testInit(self, mock_os):
        mock_os.environ.get.side_effect = lambda x: True if x == "GLITE_LOCATION" or x == "GRID_ENV_LOCATION" else False
        mock_os.path.isfile.return_value = True
        mock_os.path.normpath.side_effect = lambda x: "home" + x
        mock_os.path.join.side_effect = lambda a, b: os.path.join("", b)
        testLCGImpl = LCGImpl(True)
        self.assertTrue(testLCGImpl.stageIn)
        setups = []
        setups += ['source home/../etc/profile.d/grid-env.sh; ']
        setups += ['source home/grid-env.sh; ']
        setups += ['date "+%Y-%m-%dT%H:%M:%S"; ']
        self.assertTrue(testLCGImpl.stageIn)
        for setup in setups:
            self.assertIn(setup, testLCGImpl.setups)

    def testCreateSourceName_startswithSlash(self):
        self.assertEqual("file://name", self.LCGImpl.createSourceName("protocol", "//name"))

    @mock.patch('WMCore.Storage.Backends.LCGImpl.os.path')
    def testCreateSourceName_isfile(self, mock_path):
        mock_path.isfile.return_value = True
        mock_path.abspath.return_value = "/some/path"
        self.assertEqual("file:/some/path", self.LCGImpl.createSourceName("protocol", "name"))

    def testCreateSourceName_simple(self):
        self.assertEqual("name", self.LCGImpl.createSourceName("protocol", "name"))

    def testCreateStageOutCommand_stageInFile(self):
        self.LCGImpl.stageIn = True
        result = self.LCGImpl.createStageOutCommand("srm://sourcePFN", "file://targetPFN")
        expectedResults = self.getStageOutCommandResults("srm://sourcePFN", "file://targetPFN", "srm://sourcePFN",
                                                         "//targetPFN", "/bin/rm -f //targetPFN", False)
        self.assertEqual(expectedResults, result)

    def testCreateStageOutCommand_stageISrmCvmfs(self):
        self.LCGImpl.stageIn = True
        result = self.LCGImpl.createStageOutCommand("srm://sourcePFN", "srm://targetPFN", options="cvmfs")
        createRemoveFileCommandResult = "%s lcg-del -b -l -D srmv2 %s --vo cms srm://targetPFN" % \
                                        (self.setups, self.timeoutOptions)
        expectedResults = self.getStageOutCommandResults("srm://sourcePFN", "srm://targetPFN", "srm://sourcePFN",
                                                         "srm://targetPFN", createRemoveFileCommandResult, True)
        self.assertEqual(expectedResults, result)

    @mock.patch('WMCore.Storage.Backends.LCGImpl.StageOutImpl.createRemoveFileCommand')
    def testCreateStageOutCommand_options(self, mock_createRemoveFileCommand):
        mock_createRemoveFileCommand.return_value = "command"
        result = self.LCGImpl.createStageOutCommand("file://sourcePFN", "targetPFN", options="test")
        expectedResults = self.getStageOutCommandResults("file://sourcePFN", "targetPFN",
                                                         "targetPFN", "//sourcePFN", "command", False, options="test")
        self.assertEqual(expectedResults, result)

    @mock.patch('WMCore.Storage.Backends.LCGImpl.StageOutImpl.createRemoveFileCommand')
    def testCreateStageOutCommand_checksum(self, mock_createRemoveFileCommand):
        mock_createRemoveFileCommand.return_value = "command"
        result = self.LCGImpl.createStageOutCommand("file://sourcePFN", "targetPFN", checksums={"adler32": "32"})
        expectedResults = self.getStageOutCommandResults("file://sourcePFN", "targetPFN",
                                                         "targetPFN", "//sourcePFN", "command", False,
                                                         checksums="00000032")
        self.assertEqual(expectedResults, result)

    def getStageOutCommandResults(self, sourcePFN, targetPFN, remotePFN, localPFN,
                                  createRemoveFileCommandResult, useCVMFS, options=None, checksums=None):
        result = "#!/bin/sh\n"

        copyCommand = "lcg-cp -b -D srmv2 --vo cms --srm-timeout 2400 --sendreceive-timeout 2400 --connect-timeout 300 --verbose"
        if options != None:
            copyCommand += " %s " % options
        copyCommand += " %s " % sourcePFN
        copyCommand += " %s 2> stageout.log" % targetPFN
        if useCVMFS:
            result += "(\n"
            result += "echo Modifying PATH and LD_LIBRARY_PATH to remove /cvmfs/cms.cern.ch elements\n"
            result += "export PATH=`echo $PATH | sed -e 's+:*/cvmfs/cms.cern.ch/[^:]*++'g`\n"
            result += "export LD_LIBRARY_PATH=`echo $LD_LIBRARY_PATH | sed -e 's+:*/cvmfs/cms.cern.ch/[^:]*++'g`\n"
            result += "echo Sourcing CVMFS UI setup script\n"
            result += ". /cvmfs/grid.cern.ch/emi3ui-latest/etc/profile.d/setup-ui-example.sh\n"

            result += copyCommand
        else:
            result += self.setups
            result += copyCommand

        result += """
            EXIT_STATUS=$?
            cat stageout.log
            echo -e "\nlcg-cp exit status: $EXIT_STATUS"
            if [[ $EXIT_STATUS != 0 ]]; then
                echo "ERROR: lcg-cp exited with $EXIT_STATUS"
                echo "Cleaning up failed file:"
                %s
                exit $EXIT_STATUS
            fi

            """ % createRemoveFileCommandResult

        result += "FILE_SIZE=`stat -c %s"
        result += " %s`\n" % localPFN
        result += "echo \"Local File Size is:  $FILE_SIZE\"\n"

        if checksums:
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
                fi
                exit 0
                """ % (checksums, createRemoveFileCommandResult)
        else:
            checksumCommand = "exit 0"

        metadataCheck = \
            """
            LCG_OUTPUT=`lcg-ls -l -b -D srmv2 %s %s 2>/dev/null`
            SRM_SIZE=`echo "$LCG_OUTPUT" | awk 'NR==1{print $5}'`
            SRM_CHECKSUM=`echo "$LCG_OUTPUT" | sed -nr 's/^.*\s([a-f0-9]{8})\s*\([aA][dD][lL][eE][rR]32\)\s*$/\\1/p'`
            echo "Remote File Size is: $SRM_SIZE"
            echo "Remote Checksum is:  $SRM_CHECKSUM"
            if [[ $SRM_SIZE == $FILE_SIZE ]]; then
                %s
            else
                echo $LCG_OUTPUT
                echo "ERROR: Size Mismatch between local and SE. Cleaning up failed file..."
                %s
                exit 60311
            fi
            """ % (self.timeoutOptions, remotePFN, checksumCommand, createRemoveFileCommandResult)
        result += metadataCheck
        if useCVMFS:
            result += ")\n"
        return result

    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.executeCommand')
    def testRemoveFile(self, mock_executeCommand):
        self.LCGImpl.removeFile("file")
        mock_executeCommand.assert_called_with("%s lcg-del -b -l -D srmv2 %s --vo cms file" %
                                               (self.setups, self.timeoutOptions))
