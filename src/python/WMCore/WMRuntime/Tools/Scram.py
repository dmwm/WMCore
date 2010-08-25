#!/usr/bin/env python
"""
_Scram_

Command line scram environment wrapper that can be used to run small commands
requiring the scram environment setup

sample usage:

    scram = Scram(
        version = "CMSSW_X_Y_Z",             # CMSSW Version
        directory = "path to working area"   # defaults to cwd
        initialise = " . /uscmst1/prod/sw/cms/setup/shrc prod"  # boot strap env command
    )



    scram.project() # creates project area
    scram.runtime() # creates runtime environment

    # run something requireing scram env
    retval = scram("edmFileUtil")
    print retval, scram.stdout, scram.stderr


"""

__revision__ = "$Id: Scram.py,v 1.4 2010/04/26 20:05:43 mnorman Exp $"
__version__ = "$Revision: 1.4 $"

import os
import os.path
import sys
import subprocess

class Scram:
    """
    _Scram_

    Object to encapsulate a scram "session" that can be used to create
    a project area, bootstrap the environment and then use that to
    execute commands

    Simple enumeration of scram errors is performed, to allow to be mapped to standard
    exception/error conditions

    """
    def __init__(self, **options):
        self.command = options.get("command", "scramv1")
        self.initialise = options.get("initialise", None)
        self.version = options.get("version", None)
        self.directory = options.get("directory", os.getcwd())
        self.architecture = options.get("architecture", None)

        # state checks
        self.projectArea = None
        self.runtimeEnv = {}

        #buffers for debug/error reporting
        self.stdout = None
        self.stderr = None
        self.code = None
        self.lastExecuted = None

    def preCommand(self):
        """
        _preCommand_

        build the pre execution command for scram environment

        """
        result = ""
        if self.architecture != None:
            result += "export SCRAM_ARCH=%s\n" % self.architecture

        if self.initialise != None:
            result += "%s\n" % self.initialise
            result += """if [ "$?" -ne "0" ]; then exit 2; fi\n"""
        return result




    def project(self):
        """
        _project_

        Do a scram project command in the directory provided
        Sets the projectArea attribute checked by runtime and call

        """

        proc = subprocess.Popen(
            ["/bin/bash"], shell=True, cwd=self.directory,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
            )

        # BADPYTHON
        proc.stdin.write("export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/uscmst1/prod/sw/cms/slc5_amd64_gcc434/external/openssl/0.9.7m/lib:/uscmst1/prod/sw/cms/slc5_amd64_gcc434/external/bz2lib/1.0.5/lib\n")
        proc.stdin.write(self.preCommand())
        proc.stdin.write("%s project CMSSW %s\n" % (self.command, self.version))
        proc.stdin.write("""if [ "$?" -ne "0" ]; then exit 3; fi\n""")
        proc.stdin.write("exit 0")
        self.projectArea = "%s/%s" % (self.directory, self.version)
        self.stdout, self.stderr =  proc.communicate()
        self.code = proc.returncode
        self.lastExecuted = "%s project CMSSW %s" % (
            self.command, self.version)
        return proc.returncode




    def runtime(self):
        """
        _runtime_

        Scram runtime command

        """
        if self.projectArea == None:
            msg = "Scram Runtime called with Project Area not set"
            self.stdout = msg
            self.stderr = ""
            return 1

        try:
            proc = subprocess.Popen(
                ["/bin/bash"], shell=True, cwd=self.projectArea,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.PIPE,
                )
        except Exception, ex:
            msg = "Error thrown while invoking subprocess for scram runtime\n"
            msg += "%s\n" % str(ex)
            msg += "Opening subprocess shell in %s" % self.projectArea
            self.stdout = msg
            self.stderr = ""
            return 1

        # BADPYTHON
        proc.stdin.write("export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/uscmst1/prod/sw/cms/slc5_amd64_gcc434/external/openssl/0.9.7m/lib:/uscmst1/prod/sw/cms/slc5_amd64_gcc434/external/bz2lib/1.0.5/lib\n")
        proc.stdin.write(self.preCommand())
        proc.stdin.write("%s ru -sh\n" % self.command)
        proc.stdin.write("""if [ "$?" -ne "0" ]; then exit 4; fi\n""")
        proc.stdin.write("eval `%s ru -sh`\n" % self.command)

        self.stdout, self.stderr = proc.communicate()
        if proc.returncode == 0:
            for l in self.stdout.split(";\n"):
                if l.strip() == "":
                    continue
                l = l.replace("export ", "")
                var, val = l.split("=", 1)
                self.runtimeEnv[var] = val
        self.code = proc.returncode
        self.lastExecuted = "eval `%s ru -sh`" % self.command
        return proc.returncode


    def __call__(self, command):
        """
        _operator(command)_

        Run the command in the runtime environment

        """
        if self.projectArea == None:
            msg = "Scram Project Area not set/project() not called"
            raise RuntimeError, msg
        if self.runtimeEnv == {}:
            msg = "Scram runtime environment is empty/runtime() not called"
            raise RuntimeError, msg
        logName = 'scramOutput.log'
        if not os.path.exists(logName):
            f = open(logName, 'w')
            f.write('Log for recording SCRAM command-line output\n')
            f.write('-------------------------------------------\n')
            f.close()
        logFile = open(logName, 'a')
        proc = subprocess.Popen(["/bin/bash"], shell=True, cwd=self.projectArea,
                                env = self.runtimeEnv,
                                stdout=logFile,
                                stderr=logFile,
                                stdin=subprocess.PIPE,
                                )

        # BADPYTHON
        proc.stdin.write("export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/uscmst1/prod/sw/cms/slc5_amd64_gcc434/external/openssl/0.9.7m/lib:/uscmst1/prod/sw/cms/slc5_amd64_gcc434/external/bz2lib/1.0.5/lib\n")
        proc.stdin.write("%s\n" % self.preCommand())
        # scram fucks up the python environment from the parent shell
        proc.stdin.write(
            "export PYTHONPATH==%s:$PYTHONPATH\n" % ":".join(sys.path)[1:])
        proc.stdin.write("%s\n" % command)
        proc.stdin.write("""if [ "$?" -ne "0" ]; then exit 5; fi\n""")
        self.stdout, self.stderr = proc.communicate()
        self.code = proc.returncode
        self.lastExecuted = command
        logFile.close()
        return self.code





    def diagnostic(self):
        """
        _diagnostic_

        Diagnostic Error message for a scram command failure

        """

        result = """
        Scram Command Failure:
        Command : %s
        Architecture: %s
        Executed: %s
        Exit Status: %s
        Stdout: %s
        Stderr: %s """ % (
            self.command,
            self.architecture,
            self.lastExecuted,
            self.code,
            self.stdout,
            self.stderr)
        return result
