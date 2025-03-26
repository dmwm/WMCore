from __future__ import (print_function, division)
import unittest

from mock import mock

from WMCore.Storage.Backends.GFAL2Impl import GFAL2Impl


class GFAL2ImplTest(unittest.TestCase):
    def setUp(self):
        self.GFAL2Impl = GFAL2Impl()
        self.removeCommand = self.GFAL2Impl.removeCommand = "removeCommand {}"
        self.copyCommand = self.GFAL2Impl.copyCommand = "copyCommand {checksum} {options} {source} {destination}"

    def testInit(self):
        testGFAL2Impl = GFAL2Impl()
        # The default setup without a token
        removeCommand = "env -i {set_auth} JOBSTARTDIR=$JOBSTARTDIR bash -c " \
                        "'. $JOBSTARTDIR/startup_environment.sh; {unset_auth} date; {dry_run} gfal-rm -t 600 {}'"
        copyCommand = "env -i {set_auth} JOBSTARTDIR=$JOBSTARTDIR bash -c '" \
                    ". $JOBSTARTDIR/startup_environment.sh; {unset_auth} date; {dry_run} gfal-copy -t 2400 -T 2400 -p " \
                    "-v --abort-on-failure {checksum} {options} {source} {destination}'"
        self.assertEqual(removeCommand, testGFAL2Impl.removeCommand)
        self.assertEqual(copyCommand, testGFAL2Impl.copyCommand)

    def testCreateSourceName_file(self):
        self.assertEqual("file://name", self.GFAL2Impl.createSourceName("protocol", "file://name"))

    @mock.patch('WMCore.Storage.Backends.GFAL2Impl.os.path')
    def testCreateSourceName_isfile(self, mock_path):
        mock_path.isfile.return_value = True
        mock_path.abspath.return_value = "/some/path"
        self.assertEqual("file:///some/path", self.GFAL2Impl.createSourceName("protocol", "name"))

    @mock.patch('WMCore.Storage.Backends.GFAL2Impl.os.path')
    def testCreateSourceName_startsSlash(self, mock_path):
        mock_path.isfile.return_value = False
        mock_path.abspath.return_value = "/some/path"
        self.assertEqual("file:///some/path", self.GFAL2Impl.createSourceName("protocol", "/name"))

    @mock.patch('WMCore.Storage.Backends.GFAL2Impl.os.path')
    def testCreateSourceName_none(self, mock_path):
        mock_path.isfile.return_value = False
        self.assertEqual("name", self.GFAL2Impl.createSourceName("protocol", "name"))

    def testCreateTargetName_file(self):
        self.assertEqual("file://name", self.GFAL2Impl.createTargetName("protocol", "file://name"))

    @mock.patch('WMCore.Storage.Backends.GFAL2Impl.os.path')
    def testCreateTargetName_isfile(self, mock_path):
        mock_path.isfile.return_value = True
        mock_path.abspath.return_value = "/some/path"
        self.assertEqual("file:///some/path", self.GFAL2Impl.createTargetName("protocol", "name"))

    @mock.patch('WMCore.Storage.Backends.GFAL2Impl.os.path')
    def testCreateTargetName_startsSlash(self, mock_path):
        mock_path.isfile.return_value = False
        mock_path.abspath.return_value = "/some/path"
        self.assertEqual("file:///some/path", self.GFAL2Impl.createTargetName("protocol", "/name"))

    @mock.patch('WMCore.Storage.Backends.GFAL2Impl.os.path')
    def testCreateTargetName_none(self, mock_path):
        mock_path.isfile.return_value = False
        self.assertEqual("name", self.GFAL2Impl.createTargetName("protocol", "name"))

    @mock.patch('WMCore.Storage.Backends.GFAL2Impl.os.path')
    def testCreateRemoveFileCommand_isFile(self, mock_path):
        mock_path.abspath.return_value = "/some/path"
        mock_path.isfile.return_value = True
        self.assertEqual(" /bin/rm -f /some/path", self.GFAL2Impl.createRemoveFileCommand("name"))
        mock_path.abspath.assert_called_with("name")

    @mock.patch('WMCore.Storage.Backends.GFAL2Impl.os.path')
    def testCreateRemoveFileCommand_removeCommand(self, mock_path):
        mock_path.isfile.return_value = False
        self.assertEqual("removeCommand file:name", self.GFAL2Impl.createRemoveFileCommand("file:name"))
        mock_path.isfile.assert_called_with("file:name")

    @mock.patch('WMCore.Storage.Backends.GFAL2Impl.GFAL2Impl.createRemoveFileCommand')
    def testCreateStageOutCommand_stageIn(self, mock_createRemoveFileCommand):
        self.GFAL2Impl.stageIn = True
        mock_createRemoveFileCommand.return_value = "targetPFN2"

        # Call createStageOutCommand with authMethod='TOKEN'
        result = self.GFAL2Impl.createStageOutCommand(
            "sourcePFN", "targetPFN", authMethod='TOKEN'
        )

        # Generate the expected result with authMethod='TOKEN'
        expectedResult = self.getStageOutCommandResult(
            self.getCopyCommandDict("-K adler32", "", "sourcePFN", "targetPFN"),
            "targetPFN2",
            authMethod="TOKEN"
        )

        # Assert that the removeFileCommand was called correctly
        mock_createRemoveFileCommand.assert_called_with("targetPFN", authMethod='TOKEN', forceMethod=False)

        # Compare the expected and actual result
        self.assertEqual(expectedResult, result)

    @mock.patch('WMCore.Storage.Backends.GFAL2Impl.GFAL2Impl.createRemoveFileCommand')
    def testCreateStageOutCommand_options(self, mock_createRemoveFileCommand):
        self.maxDiff = None
        mock_createRemoveFileCommand.return_value = "targetPFN2"
        result = self.GFAL2Impl.createStageOutCommand("file:sourcePFN", "file:targetPFN", "--nochecksum unknow", authMethod='TOKEN')
        expectedResult = self.getStageOutCommandResult(
            self.getCopyCommandDict("", "unknow", "file:sourcePFN", "file:targetPFN"), "targetPFN2")
        
        mock_createRemoveFileCommand.assert_called_with("file:targetPFN", authMethod='TOKEN', forceMethod=False)
        self.assertEqual(expectedResult, result)

    def getCopyCommandDict(self, checksum, options, source, destination, authMethod=None):
        """
        Generate a dictionary for the gfal-copy command, dynamically adjusting for authMethod.
        """
        copyCommandDict = {
            'checksum': checksum,
            'options': options,
            'source': source,
            'destination': destination
        }
        return copyCommandDict

    def getStageOutCommandResult(self, copyCommandDict, createRemoveFileCommandResult, authMethod=None):
        """
        Generate the expected result for the gfal-copy command, including dynamic adjustments for authMethod.
        """
        # Construct the full result
        result = "#!/bin/bash\n"
        result += self.copyCommand.format_map(copyCommandDict)
        result += """
        EXIT_STATUS=$?
        echo "gfal-copy exit status: $EXIT_STATUS"
        if [[ $EXIT_STATUS != 0 ]]; then
            echo "ERROR: gfal-copy exited with $EXIT_STATUS"
            echo "Cleaning up failed file:"
            {remove_command}
        fi
        exit $EXIT_STATUS
        """.format(remove_command=createRemoveFileCommandResult)

        return result
    
    @mock.patch('WMCore.Storage.Backends.GFAL2Impl.os.path')
    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.executeCommand')
    def testRemoveFile_isFile(self, mock_executeCommand, mock_path):
        mock_executeCommand.return_value = "nu nx"
        mock_path.isfile.return_value = True
        mock_path.abspath.return_value = "/some/file"
        self.GFAL2Impl.removeFile("file")
        mock_path.abspath.assert_called_with("file")
        mock_executeCommand.assert_called_with(" /bin/rm -f /some/file")

    @mock.patch('WMCore.Storage.Backends.GFAL2Impl.os.path')
    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.executeCommand')
    def testRemoveFile(self, mock_executeCommand, mock_path):
        mock_path.isfile.return_value = False
        self.GFAL2Impl.removeFile("file://name")
        mock_executeCommand.assert_called_with("removeCommand file://name")
