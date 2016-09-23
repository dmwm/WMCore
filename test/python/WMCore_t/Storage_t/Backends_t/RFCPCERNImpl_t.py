from __future__ import (print_function, division)

import unittest

from mock import mock, call

from WMCore.Storage.Backends.RFCPCERNImpl import RFCPCERNImpl
from WMCore.Storage.StageOutError import StageOutError


class RFCPCERNImplTest(unittest.TestCase):
    def setUp(self):
        self.RFCPCERNImpl = RFCPCERNImpl()

    def testCreateSourceName(self):
        self.assertEqual("name", self.RFCPCERNImpl.createSourceName("", "name"))
        self.assertEqual("file:////name", self.RFCPCERNImpl.createSourceName("", "file:////name"))

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.isEOS')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.checkDirExists')
    def testCreateOutputDirectory_isEOS(self, mock_checkDirExists, mock_isEOS):
        mock_isEOS.return_value = True
        self.RFCPCERNImpl.createOutputDirectory("name")
        mock_checkDirExists.assert_not_called()

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.isEOS')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.checkDirExists')
    def testCreateOutputDirectory_stageIn(self, mock_checkDirExists, mock_isEOS):
        mock_isEOS.return_value = False
        self.RFCPCERNImpl.stageIn = True
        self.RFCPCERNImpl.createOutputDirectory("name")
        mock_checkDirExists.assert_not_called()

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.isEOS')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.checkDirExists')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.parseCastorPath')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.os.environ')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.createDir')
    def testCreateOutputDirectory_environ(self, mock_createDir, mock_environ, mock_parseCastorPath, mock_checkDirExists,
                                          mock_isEOS):
        mock_environ.get.return_value = "t0export2"
        mock_isEOS.return_value = False
        mock_checkDirExists.return_value = False
        mock_parseCastorPath.return_value = "path/testPath/file"
        self.RFCPCERNImpl.createOutputDirectory("name")
        mock_parseCastorPath.assert_called_once_with("name")
        mock_checkDirExists.assert_called_once_with("path/testPath")
        mock_createDir.assert_called_once_with("path/testPath")

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.isEOS')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.checkDirExists')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.parseCastorPath')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.os.environ')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.createDir')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.setFileClass')
    def testCreateOutputDirectory_t0export(self, mock_setFileClass, mock_createDir, mock_environ, mock_parseCastorPath,
                                           mock_checkDirExists, mock_isEOS):
        mock_environ.get.return_value = "t0export"
        mock_isEOS.return_value = False
        mock_checkDirExists.return_value = False
        mock_parseCastorPath.return_value = "/castor/cern.ch/cms/store/aa1data/aa2/aa3/aa4/aa5/file"
        self.RFCPCERNImpl.createOutputDirectory("name")
        mock_parseCastorPath.assert_called_once_with("name")

        mock_checkDirExists.assert_has_calls([call("/castor/cern.ch/cms/store/aa1data/aa2/aa3/aa4/aa5"),
                                              call("/castor/cern.ch/cms/store/aa1data/aa2/aa3/aa4")])
        mock_createDir.assert_has_calls([call("/castor/cern.ch/cms/store/aa1data/aa2/aa3/aa4"),
                                         call("/castor/cern.ch/cms/store/aa1data/aa2/aa3/aa4/aa5")])
        mock_setFileClass.assert_called_once_with("/castor/cern.ch/cms/store/aa1data/aa2/aa3/aa4", "cms_production")

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.isEOS')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.checkDirExists')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.parseCastorPath')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.os.environ')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.createDir')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.setFileClass')
    def testCreateOutputDirectory_RAW(self, mock_setFileClass, mock_createDir, mock_environ, mock_parseCastorPath,
                                      mock_checkDirExists, mock_isEOS):
        mock_environ.get.return_value = "t0export"
        mock_isEOS.return_value = False
        mock_checkDirExists.return_value = False
        mock_parseCastorPath.return_value = "/castor/cern.ch/cms/store/aa1data/aa2/aa3/RAW/aa5/file"
        self.RFCPCERNImpl.createOutputDirectory("name")
        mock_parseCastorPath.assert_called_once_with("name")
        mock_checkDirExists.assert_has_calls([call("/castor/cern.ch/cms/store/aa1data/aa2/aa3/RAW/aa5"),
                                              call("/castor/cern.ch/cms/store/aa1data/aa2/aa3/RAW")])
        mock_createDir.assert_has_calls([call("/castor/cern.ch/cms/store/aa1data/aa2/aa3/RAW"),
                                         call("/castor/cern.ch/cms/store/aa1data/aa2/aa3/RAW/aa5")])
        mock_setFileClass.assert_called_once_with("/castor/cern.ch/cms/store/aa1data/aa2/aa3/RAW", "cms_raw")

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.isEOS')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.createRemoveFileCommand')
    def testCreateStageOutCommand_stageIn(self, mock_createRemoveFileCommand, mock_isEOS):
        mock_createRemoveFileCommand.return_value = "remove file command"
        mock_isEOS.return_value = False
        self.RFCPCERNImpl.stageIn = True
        result = self.RFCPCERNImpl.createStageOutCommand("sourcePFN", "targetPFN")
        expectedResult = self.getStageOutCommandResult("sourcePFN", "targetPFN", True, False, "remove file command",
                                                       False, False, False)
        mock_createRemoveFileCommand.assert_called_once_with("targetPFN")
        self.assertEqual(expectedResult, result)

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.splitPFN')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.isEOS')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.createRemoveFileCommand')
    def testCreateStageOutCommand_isEOS(self, mock_createRemoveFileCommand, mock_isEOS, mock_splitPFN):
        mock_createRemoveFileCommand.return_value = "remove file command"
        mock_isEOS.return_value = True
        mock_splitPFN.return_value = (None, "host", "path", None)
        result = self.RFCPCERNImpl.createStageOutCommand("sourcePFN", "targetPFN")
        expectedResult = self.getStageOutCommandResult("sourcePFN", "targetPFN", False, True, "remove file command",
                                                       "host", "path", False)
        mock_createRemoveFileCommand.assert_called_once_with("targetPFN")
        self.assertEqual(expectedResult, result)

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.splitPFN')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.isEOS')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.createRemoveFileCommand')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.parseCastorPath')
    def testCreateStageOutCommand_isEOSChecksums(self, mock_parseCastorPath, mock_createRemoveFileCommand, mock_isEOS,
                                                 mock_splitPFN):
        mock_createRemoveFileCommand.return_value = "remove file command"
        mock_isEOS.return_value = True
        mock_parseCastorPath.return_value = "targetFile"
        mock_splitPFN.return_value = (None, "host", "path", None)
        result = self.RFCPCERNImpl.createStageOutCommand("sourcePFN", "targetPFN", checksums={"adler32": "32"})
        expectedResult = self.getStageOutCommandResult("sourcePFN", "targetPFN", False, True, "remove file command",
                                                       "host", "path", "targetFile", useChecksum="00000032")
        mock_createRemoveFileCommand.assert_called_once_with("targetPFN")
        self.assertEqual(expectedResult, result)

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.splitPFN')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.isEOS')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.createRemoveFileCommand')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.parseCastorPath')
    def testCreateStageOutCommand_checksumsOptions(self, mock_parseCastorPath, mock_createRemoveFileCommand, mock_isEOS,
                                                   mock_splitPFN):
        mock_createRemoveFileCommand.return_value = "remove file command"
        mock_isEOS.return_value = False
        mock_parseCastorPath.return_value = "targetFile"
        mock_splitPFN.return_value = (None, "host", "path", None)
        result = self.RFCPCERNImpl.createStageOutCommand("sourcePFN", "targetPFN", options="options",
                                                         checksums={"adler32": "32"})
        expectedResult = self.getStageOutCommandResult("sourcePFN", "targetPFN", False, False, "remove file command",
                                                       "host", "path", "targetFile", options="options",
                                                       useChecksum="32")
        mock_createRemoveFileCommand.assert_called_once_with("targetPFN")
        self.assertEqual(expectedResult, result)

    def getStageOutCommandResult(self, sourcePFN, targetPFN, stageIn, isEOS, removeCommand, host, path, targetFile,
                                 options=None, useChecksum=None):
        result = ""
        if stageIn:
            remotePFN, localPFN = sourcePFN, targetPFN
        else:
            remotePFN, localPFN = targetPFN, sourcePFN
            result += "LOCAL_SIZE=`stat -c%%s \"%s\"`\n" % localPFN
            result += "echo \"Local File Size is: $LOCAL_SIZE\"\n"
        if isEOS:
            result += "xrdcp -f -N "
            if useChecksum:
                targetPFN += "?eos.targetsize=$LOCAL_SIZE&eos.checksum=%s" % useChecksum
        else:
            if useChecksum:
                result += "nstouch %s\n" % targetFile
                result += "nssetchecksum -n adler32 -k %s %s\n" % (useChecksum, targetFile)
            result += "rfcp "
            if options != None:
                result += " %s " % options
        result += " \"%s\" " % sourcePFN
        result += " \"%s\" \n" % targetPFN
        if stageIn:
            result += "LOCAL_SIZE=`stat -c%%s \"%s\"`\n" % localPFN
            result += "echo \"Local File Size is: $LOCAL_SIZE\"\n"
        if isEOS:
            result += "REMOTE_SIZE=`xrd '%s' stat '%s' | sed -r 's/.* Size: ([0-9]+) .*/\\1/'`\n" % (host, path)
            result += "echo \"Remote File Size is: $REMOTE_SIZE\"\n"
            if useChecksum:
                result += "echo \"Local File Checksum is: %s\"\n" % useChecksum
                result += "REMOTE_XS=`xrd '%s' getchecksum '%s' | sed -r 's/.* adler32 ([0-9a-fA-F]{8}).*/\\1/'`\n" % (
                host, path)
                result += "echo \"Remote File Checksum is: $REMOTE_XS\"\n"
                result += "if [ $REMOTE_SIZE ] && [ $REMOTE_XS ] && [ $LOCAL_SIZE == $REMOTE_SIZE ] &&" \
                          " [ '%s' == $REMOTE_XS ]; then exit 0; " % useChecksum
                result += "else echo \"Error: Size or Checksum Mismatch between local and SE\";" \
                          " %s ; exit 60311 ; fi" % removeCommand
            else:
                result += "if [ $REMOTE_SIZE ] && [ $LOCAL_SIZE == $REMOTE_SIZE ]; then exit 0; "
                result += "else echo \"Error: Size Mismatch between local and SE\"; %s ; exit 60311 ; fi" % removeCommand
        else:
            result += "REMOTE_SIZE=`rfstat '%s' | grep Size | cut -f2 -d: | tr -d ' '`\n" % remotePFN
            result += "echo \"Remote File Size is: $REMOTE_SIZE\"\n"
            result += "if [ $REMOTE_SIZE ] && [ $LOCAL_SIZE == $REMOTE_SIZE ]; then exit 0; else echo " \
                      "\"Error: Size Mismatch between local and SE\"; %s ; exit 60311 ; fi" % removeCommand
        return result

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.isEOS')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.splitPFN')
    def testCreateRemoveFileCommand_isEos(self, mock_splitPFN, mock_isEOS):
        mock_isEOS.return_value = True
        mock_splitPFN.return_value = (None, "host", "path", None)
        self.assertEqual("xrd host rm path", self.RFCPCERNImpl.createRemoveFileCommand("file/path"))
        mock_isEOS.assert_called_once_with("file/path")
        mock_splitPFN.assert_called_once_with("file/path")

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.isEOS')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.parseCastorPath')
    def testCreateRemoveFileCommand_simplePFN(self, mock_parseCastorPath, mock_isEOS):
        mock_isEOS.return_value = False
        mock_parseCastorPath.return_value = "simplePFN"
        self.assertEqual("stager_rm -a -M simplePFN ; nsrm simplePFN",
                         self.RFCPCERNImpl.createRemoveFileCommand("file/path"))
        mock_isEOS.assert_called_once_with("file/path")
        mock_parseCastorPath.assert_called_once_with("file/path")

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.isEOS')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.parseCastorPath')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.StageOutImpl.createRemoveFileCommand')
    def testCreateRemoveFileCommand_StageOutError(self, mock_createRemoveFileCommand, mock_parseCastorPath, mock_isEOS):
        mock_isEOS.return_value = False
        mock_createRemoveFileCommand.return_value = "test"
        mock_parseCastorPath.side_effect = StageOutError("Errors!")
        self.assertEqual("test", self.RFCPCERNImpl.createRemoveFileCommand("file/path"))
        mock_isEOS.assert_called_once_with("file/path")

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.isEOS')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.splitPFN')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.execute')
    def testRemoveFile_isEOS(self, mock_executeCommand, mock_splitPFN, mock_isEOS):
        mock_isEOS.return_value = True
        mock_splitPFN.return_value = (None, "host", "path", None)
        self.RFCPCERNImpl.removeFile("file/path")
        mock_executeCommand.assert_called_with("xrd host rm path")
        mock_isEOS.assert_called_with("file/path")
        mock_splitPFN.assert_called_with("file/path")

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.isEOS')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.parseCastorPath')
    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.execute')
    def testRemoveFile_simplePFN(self, mock_executeCommand, mock_parseCastorPath, mock_isEOS):
        mock_isEOS.return_value = False
        mock_parseCastorPath.return_value = "simplePFN"
        self.RFCPCERNImpl.removeFile("file/path")
        mock_executeCommand.assert_called_with("stager_rm -a -M simplePFN ; nsrm simplePFN")
        mock_isEOS.assert_called_with("file/path")
        mock_parseCastorPath.assert_called_with("file/path")

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.runCommandWithOutput')
    def testCheckDirExists_true(self, mock_runCommandWithOutput):
        mock_runCommandWithOutput.return_value = (0, "Protection : d test")
        self.assertTrue(self.RFCPCERNImpl.checkDirExists("file/path"))
        mock_runCommandWithOutput.assert_called_with("rfstat file/path 2> /dev/null | grep Protection")

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.runCommandWithOutput')
    def testCheckDirExists_false(self, mock_runCommandWithOutput):
        mock_runCommandWithOutput.return_value = (1, "Protection : d test")
        self.assertFalse(self.RFCPCERNImpl.checkDirExists("file"))
        mock_runCommandWithOutput.assert_called_with("rfstat file 2> /dev/null | grep Protection")

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.runCommandWithOutput')
    def testCheckDirExists_trueStageOutError(self, mock_runCommandWithOutput):
        mock_runCommandWithOutput.return_value = (0, "Protection:test")
        with self.assertRaises(StageOutError) as context:
            self.RFCPCERNImpl.checkDirExists("file/test")
            self.assertTrue('Output path is not a directory !' in context.exception)
        mock_runCommandWithOutput.assert_called_with("rfstat file/test 2> /dev/null | grep Protection")

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.runCommandWithOutput')
    def testCheckDirExists_stageOutError(self, mock_runCommandWithOutput):
        mock_runCommandWithOutput.return_value = StageOutError("Error")
        with self.assertRaises(StageOutError) as context:
            self.RFCPCERNImpl.checkDirExists("test2")
            self.assertTrue('Error: Exception while invoking command:' in context.exception)
        mock_runCommandWithOutput.assert_called_with("rfstat test2 2> /dev/null | grep Protection")

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.execute')
    def testCreateDir(self, mock_execute):
        self.RFCPCERNImpl.createDir("dirName")
        mock_execute.assert_called_with("nsmkdir -p \"dirName\"")

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.execute')
    def testSetFileClass(self, mock_execute):
        self.RFCPCERNImpl.setFileClass("dir", "fileClass")
        mock_execute.assert_called_with("nschclass fileClass dir")

    def testParseCastorPath_stageOutError(self):
        with self.assertRaises(StageOutError) as context:
            self.RFCPCERNImpl.parseCastorPath("test2")
            self.assertTrue("Can't parse castor path out of URL !" in context.exception)

    def testParseCastorPath_xrootd(self):
        self.assertEqual("/castor/cern.ch/testas",
                         self.RFCPCERNImpl.parseCastorPath("root:test//castor/cern.ch/testas"))
        self.assertEqual("/castor/cern.ch/testas",
                         self.RFCPCERNImpl.parseCastorPath("root:test/castor/cern.ch/testas?as"))
        self.assertEqual("/castor/cern.ch/testas/test",
                         self.RFCPCERNImpl.parseCastorPath("root://castor/cern.ch/testas//test?as"))

    def testParseCastorPath_rfio(self):
        self.assertEqual("/castor/cern.ch/a/a/",
                         self.RFCPCERNImpl.parseCastorPath("rfio:test//castor/cern.ch/a/a//?testas"))
        self.assertEqual("/castor/cern.ch/testas",
                         self.RFCPCERNImpl.parseCastorPath("rfio:test//castor/cern.ch/testas?as"))
        self.assertEqual("/castor/cern.ch/testas/test",
                         self.RFCPCERNImpl.parseCastorPath("rfio:test/castor/cern.ch/testas//test?as"))

    def testParseCastorPath_castor(self):
        self.assertEqual("/castor/cern.ch/a/a/?testas",
                         self.RFCPCERNImpl.parseCastorPath("//castor/cern.ch/a/a//?testas"))
        self.assertEqual("/castor/cern.ch/testas?as", self.RFCPCERNImpl.parseCastorPath("//castor/cern.ch/testas?as"))
        self.assertEqual("/castor/cern.ch/testas/test",
                         self.RFCPCERNImpl.parseCastorPath("/castor/cern.ch/testas//test"))

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.splitPFN')
    def testIsEOS_rootFalse(self, mock_splitPFN):
        mock_splitPFN.return_value = ("protocol", "host", None, None)
        self.assertFalse(self.RFCPCERNImpl.isEOS("test"))
        mock_splitPFN.assert_called_with("test")

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.splitPFN')
    def testIsEOS_rootTrue(self, mock_splitPFN):
        mock_splitPFN.return_value = ("root", "host", None, None)
        self.assertTrue(self.RFCPCERNImpl.isEOS("test"))
        mock_splitPFN.assert_called_with("test")

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.splitPFN')
    def testIsEOS_rootCastorFalse(self, mock_splitPFN):
        mock_splitPFN.return_value = ("root", "castorhost", None, None)
        self.assertFalse(self.RFCPCERNImpl.isEOS("test"))
        mock_splitPFN.assert_called_with("test")

    @mock.patch('WMCore.Storage.Backends.RFCPCERNImpl.RFCPCERNImpl.splitPFN')
    def testIsEOS_rootCastorTrue(self, mock_splitPFN):
        mock_splitPFN.return_value = ("root", "host:castor", None, None)
        self.assertTrue(self.RFCPCERNImpl.isEOS("test"))
        mock_splitPFN.assert_called_with("test")
