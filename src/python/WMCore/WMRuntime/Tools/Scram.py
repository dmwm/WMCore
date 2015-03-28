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




import os
import os.path
import sys
import subprocess
import logging


def testWriter(func, *args):
    """
    _testWriter_

    If running Scram object in unittests, this mode will neuter the shell commands to make them
    echo the commands written to the subprocess.

    """
    def newWriter(*args):
        newWriter.__name__ = func.__name__
        newWriter.__doc__ = func.__doc__
        newWriter.__dict__.update(func.__dict__)
        subproc = args[0]
        line = args[1]
        escapedLine = "echo \"%s\"\n" % line
        func(subproc, escapedLine)
    return newWriter


#  //
# // Interceptable function to push commands to the subshell, used to
#//  enable test mode.
procWriter = lambda s, l: s.stdin.write(l)



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
        self.test_mode = options.get("test", False)
        self.envCmd = options.get("envCmd", None)

        # state checks
        self.projectArea = None
        self.runtimeEnv = {}

        # handler to write to subprocesses
        self.procWriter = procWriter
        if self.test_mode:
            # if in test mode, decorate the subprocess writer with the test harness
            self.procWriter = testWriter(procWriter)
            # dont actually try to call a non-existent scram binary in test mode
            self.command = "/bin/echo"

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


        # send commands to the subshell utilising the process writer method
        self.procWriter(proc, "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/openssl/0.9.7m/lib:$VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/bz2lib/1.0.5/lib\n")
        self.procWriter(proc, self.preCommand())
        self.procWriter(proc, "%s --arch %s project CMSSW %s\n" % (self.command, self.architecture, self.version))
        self.procWriter(proc, """if [ "$?" -ne "0" ]; then exit 3; fi\n""")
        self.procWriter(proc, "exit 0")

        self.projectArea = "%s/%s" % (self.directory, self.version)
        self.stdout, self.stderr =  proc.communicate()
        self.code = proc.returncode
        self.lastExecuted = "%s project CMSSW %s" % (
            self.command, self.version)
        if self.test_mode:
            # in test mode, the scram command would create the project area
            # have to emulate this by hand here.
            if not os.path.exists(self.projectArea):
                os.makedirs(self.projectArea)
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

        # write via process writer method
        self.procWriter(proc, "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/openssl/0.9.7m/lib:$VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/bz2lib/1.0.5/lib\n")
        self.procWriter(proc, self.preCommand())
        self.procWriter(proc, "%s ru -sh\n" % self.command)
        self.procWriter(proc, """if [ "$?" -ne "0" ]; then exit 4; fi\n""")
        self.procWriter(proc, "eval `%s ru -sh`\n" % self.command)


        self.stdout, self.stderr = proc.communicate()
        if proc.returncode == 0:
            for l in self.stdout.split(";\n"):
                if l.strip() == "":
                    continue
                if l.strip().startswith('unset'):
                    continue
                l = l.replace("export ", "")
                try:
                    var, val = l.split("=", 1)
                except ValueError, ex:
                    raise ValueError, "Couldn't split line: %s" % l

                self.runtimeEnv[var] = val
        if self.test_mode:
            # ensure that runtime env isnt empty in test mode
            # as that will cause an exception in __call__
            self.runtimeEnv['TEST_MODE'] = 1
        self.code = proc.returncode
        self.lastExecuted = "eval `%s ru -sh`" % self.command
        return proc.returncode


    def __call__(self, command, hackLdLibPath = True,
                 logName = "scramOutput.log", runtimeDir = None, cleanEnv = True):
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
        executeIn = runtimeDir
        if runtimeDir == None:
            executeIn = self.projectArea
        #if the caller passed a filename and not a filehandle and if the logfile does not exist then create one
        if isinstance(logName, basestring) and not os.path.exists(logName):
            f = open(logName, 'w')
            f.write('Log for recording SCRAM command-line output\n')
            f.write('-------------------------------------------\n')
            f.close()
        logFile = open(logName, 'a') if isinstance(logName, basestring) else logName
        bashcmd = "/bin/bash"
        if cleanEnv:
            bashcmd = "env - " + bashcmd
        proc = subprocess.Popen([bashcmd], shell=True, cwd=executeIn,
                                stdout=logFile,
                                stderr=logFile,
                                stdin=subprocess.PIPE,
                                )

        # Passing the environment in to the subprocess call results in all of
        # the variables being quoted which causes problems for search paths.
        # We'll setup the environment the hard way to avoid this.
        rtCmsswBase = None
        rtScramArch = self.architecture
        for varName in self.runtimeEnv:
            self.procWriter(proc, 'export %s=%s\n' % (varName, self.runtimeEnv[varName]))
            if varName == "CMSSW_RELEASE_BASE":
                rtCmsswBase = self.runtimeEnv[varName].replace('\"','')
            if varName == "SCRAM_ARCH":
                rtScramArch = self.runtimeEnv[varName].replace('\"','')

        if os.environ.get('VO_CMS_SW_DIR', None ) != None:
            self.procWriter(proc, 'export VO_CMS_SW_DIR=%s\n'%os.environ['VO_CMS_SW_DIR'])
        if os.environ.get('OSG_APP', None) != None:
            self.procWriter(proc, 'export VO_CMS_SW_DIR=%s/cmssoft/cms\n'%os.environ['OSG_APP'])
        if os.environ.get('CMS_PATH', None) != None:
            self.procWriter(proc, 'export CMS_PATH=%s\n'%os.environ['CMS_PATH'])

        if hackLdLibPath:
            self.procWriter(proc, "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/openssl/0.9.7m/lib:$VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/bz2lib/1.0.5/lib\n")

        self.procWriter(proc, "%s\n" % self.preCommand())

        if self.envCmd != None:
            self.procWriter(proc, "%s\n" % self.envCmd)

        if command.startswith(sys.executable):
            # replace python version with python 2.7 from CMSSW release if possible
            python27exec = "%s/external/%s/bin/python2.7" % (rtCmsswBase, rtScramArch)
            if os.path.islink(python27exec):
                command = command.replace(sys.executable, python27exec)
            elif hackLdLibPath:
                # reset python path for DMWM python (scram will have changed env to point at its own)
                self.procWriter(proc, "export PYTHONPATH==%s:$PYTHONPATH\n" % ":".join(sys.path)[1:])

        self.procWriter(proc, "%s\n" % command)
        self.procWriter(proc,"""if [ "$?" -ne "0" ]; then exit 5; fi\n""")
        self.stdout, self.stderr = proc.communicate()
        self.code = proc.returncode
        self.lastExecuted = command
        #close the logfile if one has been created from the name. Let the caller close it if he passed a file object.
        if isinstance(logName, basestring):
            logFile.close()
        return self.code


    def diagnostic(self):
        """
        _diagnostic_

        Diagnostic Error message for a scram command failure

        """

        result = """
        Scram Command Diagnostic:
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
