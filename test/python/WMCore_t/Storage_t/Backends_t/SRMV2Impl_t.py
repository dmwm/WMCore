from __future__ import (print_function, division)

import unittest

from mock import mock, call

from WMCore.Storage.Backends.SRMV2Impl import SRMV2Impl
from WMCore.Storage.StageOutError import StageOutError


class SRMV2ImplTest(unittest.TestCase):
    def setUp(self):
        self.SRMV2Impl = SRMV2Impl()

    def testCreateSourceName_startswithSlash(self):
        self.assertEqual("file:////name", self.SRMV2Impl.createSourceName("protocol", "/name"))

    @mock.patch('WMCore.Storage.Backends.SRMV2Impl.os.path')
    def testCreateSourceName_isfile(self, mock_path):
        mock_path.isfile.return_value = True
        mock_path.abspath.return_value = "some/path"
        self.assertEqual("file:///some/path", self.SRMV2Impl.createSourceName("protocol", "name"))

    def testCreateSourceName_simple(self):
        self.assertEqual("name", self.SRMV2Impl.createSourceName("protocol", "name"))

    @mock.patch('WMCore.Storage.Backends.SRMV2Impl.os')
    def testCreateOutputDirectory_stageIn(self, mock_os):
        self.SRMV2Impl.stageIn = True
        mock_os.path.dirname.return_value = "dirName"
        mock_os.path.exists.return_value = True
        self.SRMV2Impl.createOutputDirectory("name")
        mock_os.path.dirname.assert_called_with("name")
        mock_os.path.dirname.exists("name")

        mock_os.path.exists.return_value = False
        self.SRMV2Impl.createOutputDirectory("name")
        mock_os.path.dirname.assert_called_with("name")
        mock_os.path.dirname.exists("name")
        mock_os.makedirs.assert_called_with("dirName")

    @mock.patch('WMCore.Storage.Backends.SRMV2Impl.SRMV2Impl.run')
    def testCreateOutputDirectory_Exception(self, mock_run):
        mock_run.return_value = Exception("I'm in test")
        self.SRMV2Impl.createOutputDirectory("/folder/test/test2/test3/test4/test5")
        calls = [call("srmls -recursion_depth=0 -retry_num=1 /folder/test/test2/test3/test4")]
        mock_run.assert_has_calls(calls)

    @mock.patch('WMCore.Storage.Backends.SRMV2Impl.SRMV2Impl.run')
    def testCreateOutputDirectory_SRM_FAILURE(self, mock_run):
        mock_run.return_value = (0, "SRM_FAILURE test")
        self.SRMV2Impl.createOutputDirectory("folder/test1/test2/test3/test4/test5/test6/test7")
        calls = [call("srmls -recursion_depth=0 -retry_num=1 folder/test1/test2/test3/test4/test5/test6"),
                 call("srmls -recursion_depth=0 -retry_num=1 folder/test1/test2/test3/test4/test5"),
                 call("srmmkdir -retry_num=0 folder/test1/test2/test3/test4/test5/test6")]
        mock_run.assert_has_calls(calls)

    @mock.patch('WMCore.Storage.Backends.SRMV2Impl.SRMV2Impl.run')
    def testCreateOutputDirectory_exitCode(self, mock_run):
        mock_run.return_value = (1, "test")
        self.assertIsNone(self.SRMV2Impl.createOutputDirectory("/folder/test/test2/test3/test4/test5"))
        calls = [call("srmls -recursion_depth=0 -retry_num=1 /folder/test/test2/test3/test4")]
        mock_run.assert_has_calls(calls)

    @mock.patch('WMCore.Storage.Backends.SRMV2Impl.SRMV2Impl.run')
    def testCreateOutputDirectory_exitCode2(self, mock_run):
        mock_run.side_effect = [(0, "SRM_FAILURE test"), (0, "SRM_FAILURE test"), (1, "test")]
        self.assertIsNone(self.SRMV2Impl.createOutputDirectory("folder/test1/test2/test3/test4/test5/test6/test7"))
        calls = [call("srmls -recursion_depth=0 -retry_num=1 folder/test1/test2/test3/test4/test5/test6"),
                 call("srmls -recursion_depth=0 -retry_num=1 folder/test1/test2/test3/test4/test5"),
                 call("srmmkdir -retry_num=0 folder/test1/test2/test3/test4/test5/test6")]
        mock_run.assert_has_calls(calls)

    def testCreateStageOutCommand_stageIn(self):
        self.SRMV2Impl.stageIn = True
        result = self.SRMV2Impl.createStageOutCommand("srm://sourcePFN/test", "file://targetPFN")
        expectedResult = self.geCreateStageOutCommandResults("srm://sourcePFN/test", "file://targetPFN",
                                                             "srm://sourcePFN/test", "targetPFN"
                                                             , "/test", "sourcePFN", "/bin/rm -f targetPFN")
        self.assertEqual(expectedResult, result)

    def testCreateStageOutCommand_options(self):
        result = self.SRMV2Impl.createStageOutCommand("file://sourcePFN/test", "srm://targetPFN/test", options="test")
        expectedResult = self.geCreateStageOutCommandResults("file://sourcePFN/test", "srm://targetPFN/test",
                                                             "srm://targetPFN/test", "sourcePFN/test"
                                                             , "/test", "targetPFN",
                                                             "srmrm -2 -retry_num=0 srm://targetPFN/test",
                                                             options="test")
        self.assertEqual(expectedResult, result)

    def testCreateStageOutCommand_SPF(self):
        result = self.SRMV2Impl.createStageOutCommand("file://sourcePFN/test", "srm://targetPFN/test?SFN=testSPF")
        expectedResult = self.geCreateStageOutCommandResults("file://sourcePFN/test",
                                                             "srm://targetPFN/test?SFN=testSPF",
                                                             "srm://targetPFN/test?SFN=testSPF", "sourcePFN/test"
                                                             , "testSPF", "targetPFN",
                                                             "srmrm -2 -retry_num=0 srm://targetPFN/test?SFN=testSPF")
        self.assertEqual(expectedResult, result)

    @mock.patch('WMCore.Storage.Backends.SRMV2Impl.SRMV2Impl.createRemoveFileCommand')
    def testCreateStageOutCommand_Exception(self, mock_createRemoveFileCommand):
        mock_createRemoveFileCommand.return_value = "test"
        with self.assertRaises(StageOutError):
            self.SRMV2Impl.createStageOutCommand("sourcePFN", "targetPFN")
        mock_createRemoveFileCommand.assert_called_with("targetPFN")

    def geCreateStageOutCommandResults(self, sourcePFN, targetPFN, remotePFN, localPFN, remotePath, remoteHost,
                                       createRemoveFileCommandResult, options=None):
        result = "#!/bin/sh\n"
        result += "REPORT_FILE=`pwd`/srm.report.$$\n"
        result += "srmcp -2 -report=$REPORT_FILE -retry_num=0 -request_lifetime=2400"

        if options != None:
            result += " %s " % options
        result += " %s " % sourcePFN
        result += " %s" % targetPFN
        result += " 2>&1 | tee srm.output.$$ \n"

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

            """ % createRemoveFileCommandResult
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
            """ % (remotePFN, remotePath, remoteHost, createRemoveFileCommandResult)
        result += metadataCheck

        return result

    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.executeCommand')
    def testRemoveFile(self, mock_executeCommand):
        self.SRMV2Impl.removeFile("file")
        mock_executeCommand.assert_called_with("srmrm -2 -retry_num=0 file")
