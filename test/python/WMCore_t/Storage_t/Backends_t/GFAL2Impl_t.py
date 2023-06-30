from __future__ import (print_function, division)
import unittest

from mock import mock

from WMCore.Storage.Backends.GFAL2Impl import GFAL2Impl


class GFAL2ImplTest(unittest.TestCase):
    def setUp(self):
        self.GFAL2Impl = GFAL2Impl()
        self.removeCommand = self.GFAL2Impl.removeCommand = "removeCommand %s"
        self.copyCommand = self.GFAL2Impl.copyCommand = "copyCommand %(checksum)s %(options)s %(source)s %(destination)s"

    def testInit(self):
        testGFAL2Impl = GFAL2Impl()
        removeCommand = "env -i X509_USER_PROXY=$X509_USER_PROXY JOBSTARTDIR=$JOBSTARTDIR bash -c " \
                        "'. $JOBSTARTDIR/startup_environment.sh; date; gfal-rm -t 600 %s '"
        copyCommand = "env -i X509_USER_PROXY=$X509_USER_PROXY JOBSTARTDIR=$JOBSTARTDIR bash -c '" \
                      ". $JOBSTARTDIR/startup_environment.sh; date; gfal-copy -t 2400 -T 2400 -p " \
                      "-v --abort-on-failure %(checksum)s %(options)s %(source)s %(destination)s'"
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
        self.assertEqual("/bin/rm -f /some/path", self.GFAL2Impl.createRemoveFileCommand("name"))
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
        result = self.GFAL2Impl.createStageOutCommand("sourcePFN", "targetPFN")
        expectedResult = self.getStageOutCommandResult(
            self.getCopyCommandDict("-K adler32", "", "sourcePFN", "targetPFN"), "targetPFN2")
        mock_createRemoveFileCommand.assert_called_with("targetPFN")
        self.assertEqual(expectedResult, result)

    @mock.patch('WMCore.Storage.Backends.GFAL2Impl.GFAL2Impl.createRemoveFileCommand')
    def testCreateStageOutCommand_options(self, mock_createRemoveFileCommand):
        mock_createRemoveFileCommand.return_value = "targetPFN2"
        result = self.GFAL2Impl.createStageOutCommand("file:sourcePFN", "file:targetPFN", "--nochecksum unknow")
        expectedResult = self.getStageOutCommandResult(
            self.getCopyCommandDict("", "unknow", "file:sourcePFN", "file:targetPFN"), "targetPFN2")
        mock_createRemoveFileCommand.assert_called_with("file:targetPFN")
        self.assertEqual(expectedResult, result)

    def getCopyCommandDict(self, checksum, options, source, destination):
        copyCommandDict = {'checksum': '', 'options': '', 'source': '', 'destination': ''}
        copyCommandDict['checksum'] = checksum
        copyCommandDict['options'] = options
        copyCommandDict['source'] = source
        copyCommandDict['destination'] = destination
        return copyCommandDict

    def getStageOutCommandResult(self, copyCommandDict, createRemoveFileCommandResult):
        result = "#!/bin/bash\n"

        copyCommand = self.copyCommand % copyCommandDict
        result += copyCommand

        result += """
            EXIT_STATUS=$?
            echo "gfal-copy exit status: $EXIT_STATUS"
            if [[ $EXIT_STATUS != 0 ]]; then
               echo "ERROR: gfal-copy exited with $EXIT_STATUS"
               echo "Cleaning up failed file:"
               %s
            fi
            exit $EXIT_STATUS
            """ % createRemoveFileCommandResult

        return result

    @mock.patch('WMCore.Storage.Backends.GFAL2Impl.os.path')
    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.executeCommand')
    def testRemoveFile_isFile(self, mock_executeCommand, mock_path):
        mock_executeCommand.return_value = "nu nx"
        mock_path.isfile.return_value = True
        mock_path.abspath.return_value = "/some/file"
        self.GFAL2Impl.removeFile("file")
        mock_path.abspath.assert_called_with("file")
        mock_executeCommand.assert_called_with("/bin/rm -f /some/file")

    @mock.patch('WMCore.Storage.Backends.GFAL2Impl.os.path')
    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.executeCommand')
    def testRemoveFile(self, mock_executeCommand, mock_path):
        mock_path.isfile.return_value = False
        self.GFAL2Impl.removeFile("file://name")
        mock_executeCommand.assert_called_with("removeCommand file://name")
