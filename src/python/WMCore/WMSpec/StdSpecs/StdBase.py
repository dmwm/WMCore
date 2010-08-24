#!/usr/bin/env python
"""
_StdBase_

Base class with helper functions for standard WMSpec files.
"""




import tempfile
import subprocess
import os
import shutil

from WMCore.Wrappers.JsonWrapper import JSONEncoder, JSONDecoder

class StdBase(object):
    """
    _StdBase_

    """
    def __init__(self):
        return
    
    def getOutputModuleInfo(self, configUrl, scenarioName, scenarioFunc,
                            scenarioArgs):
        """
        _getOutputModuleInfo_

        For a given config try to determine the output module configuration.
        Create a temporary directory and setup a CMSSW project using scramv1 in
        it.  Use a subshell and scramv1 to setup the CMSSW environment and then
        execute the outputmodules-from-config script.  Looks for the JSON file
        that the script leaves behind and pass that back to the caller.
        """
        stderr = None
        stdout = None
        
        try:
            self.tempDir = tempfile.mkdtemp()

            scramProcess = subprocess.Popen(["/bin/bash"], shell = True,
                                            cwd = self.tempDir,
                                            stdout = subprocess.PIPE,
                                            stderr = subprocess.PIPE,
                                            stdin = subprocess.PIPE)

            scramProcess.stdin.write("export SCRAM_ARCH=%s\n" % self.scramArch)
            scramProcess.stdin.write(". %s/cmsset_default.sh\n" % self.cmsPath)
            scramProcess.stdin.write("scramv1 project CMSSW %s\n" % self.frameworkVersion)

            scramProcess.stdin.write("cd %s\n" % self.frameworkVersion)
            scramProcess.stdin.write("eval `scramv1 ru -sh`\n")

            encoder = JSONEncoder()
            config = {"configUrl": configUrl, "scenarioName": scenarioName,
                      "scenarioFunc": scenarioFunc, "scenarioArgs": scenarioArgs}

            scramProcess.stdin.write("outputmodules-from-config\n")
            scramProcess.stdin.write(encoder.encode(config))

            (stdout, stderr) = scramProcess.communicate()

            outputHandle = open(os.path.join(self.tempDir, self.frameworkVersion,
                                             "outputModules.json"))
            outputJSON = outputHandle.read()
            outputHandle.close()

            shutil.rmtree(self.tempDir)

            decoder = JSONDecoder()        
            return decoder.decode(outputJSON)
        except Exception, ex:
            error = "Error determining output modules: %s.  " % str(ex)
            error += "STDOUT from process: %s\n" % stdout
            error += "STDERR from process: %s\n" % stderr
            shutil.rmtree(self.tempDir)
            raise Exception, error
            
