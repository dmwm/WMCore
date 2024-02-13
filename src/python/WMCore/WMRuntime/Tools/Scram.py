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

from builtins import range, object

import logging
import os
import os.path
import subprocess
import sys
import platform

from PSetTweaks.WMTweak import readAdValues
from Utils.Utilities import encodeUnicodeToBytes, decodeBytesToUnicode

SCRAM_TO_ARCH = {'amd64': 'X86_64', 'aarch64': 'aarch64', 'ppc64le': 'ppc64le'}
# Scram arch to platform machine values above are unique, so we can reverse the mapping
ARCH_TO_SCRAM = {arch:scram for scram, arch in list(SCRAM_TO_ARCH.items())}
ARCH_TO_OS = {'slc5': ['rhel6'],
              'slc6': ['rhel6'],
              'slc7': ['rhel7'],
              'el8': ['rhel8'], 'cc8': ['rhel8'], 'cs8': ['rhel8'], 'alma8': ['rhel8'],
              'el9': ['rhel9'], 'cs9': ['rhel9'],
              }
OS_TO_ARCH = {}
for arch, oses in ARCH_TO_OS.items():
    for osName in oses:
        if osName not in OS_TO_ARCH:
            OS_TO_ARCH[osName] = []
        OS_TO_ARCH[osName].append(arch)

def getPlatformMachine():
    """
    Get the platform machine. Keep it compatible with HTCondor
    :return: str, platform machine.
    """
    machine = platform.machine()
    # Condor throws X86_64 but machine uses x86_64. Using condor convention
    if machine == 'x86_64':
        machine = machine.upper()

    return machine

def getSingleScramArch(scramArch):
    """
    Figure out which scram arch is compatible with both the request and the release

    Args:
        scramArch: string or list of strings representing valid scram arches for the workflow

    Returns:
        a single scram arch
    """
    if isinstance(scramArch, list):
        try:
            ad = readAdValues(['glidein_required_os'], 'machine')
            runningOS = ad['glidein_required_os'].strip('"')
            validArches = sorted(OS_TO_ARCH[runningOS], reverse=True)
            for requestedArch in sorted(scramArch, reverse=True):
                for validArch in validArches:
                    if requestedArch.startswith(validArch):
                        # Check target machine matches with a valid scram arch
                        if ARCH_TO_SCRAM.get(getPlatformMachine()):
                            return requestedArch
        except KeyError:
            return sorted(scramArch)[-1]  # Give the most recent release if lookup fails
        return None
    else:
        return scramArch


def isCMSSWSupported(thisCMSSW, supportedCMSSW):
    """
    _isCMSSWSupported_

    Function used to validate whether the CMSSW release to be used supports
    a feature that is not available in all releases.
    :param thisCMSSW: release to be used in this job
    :param allowedCMSSW: first (lowest) release that started supporting the
    feature you'd like to use.
    
    NOTE: only the 3 digits version are evaluated, pre and patch releases
    are not taken into consideration
    """
    if not thisCMSSW or not supportedCMSSW:
        logging.info("You must provide the CMSSW version being used by this job and a supported version")
        return False

    if thisCMSSW == supportedCMSSW:
        return True

    thisCMSSW = [int(i) for i in thisCMSSW.split('_', 4)[1:4]]
    supportedCMSSW = [int(i) for i in supportedCMSSW.split('_', 4)[1:4]]
    for idx in range(3):
        if thisCMSSW[idx] > supportedCMSSW[idx]:
            return True
        elif thisCMSSW[idx] == supportedCMSSW[idx] and idx < 2:
            if thisCMSSW[idx + 1] > supportedCMSSW[idx + 1]:
                return True
        else:
            return False

    return False


def isEnforceGUIDInFileNameSupported(thisCMSSW):
    """
    _isEnforceGUIDInFileNameSupported_

    Function used to validate whether the CMSSW release to be used supports
    the enforceGUIDInFileName feature.
    :param thisCMSSW: release to be used in this job

    """
    # a set of CMSSW releases that support the enforceGUIDInFileName feature. Releases in the same
    # cycle with a higher minor revision number also support the feature.
    supportedReleases = set(["CMSSW_10_6_8", "CMSSW_10_2_20", "CMSSW_9_4_16", "CMSSW_9_3_17", "CMSSW_8_0_34"])
    # a set of specific CMSSW releases that supported the enforceGUIDInFileName feature.
    specificSupportedReleases = set(["CMSSW_10_2_20_UL", "CMSSW_9_4_16_UL", "CMSSW_8_0_34_UL", "CMSSW_7_1_45_patch3"])

    if not thisCMSSW:
        logging.info("You must provide the CMSSW version being used by this job.")
        return False

    # true if CMSSW release is >= CMSSW_11_0_0
    if isCMSSWSupported(thisCMSSW, "CMSSW_11_0_0"):
        return True
    # true if CMSSW release is in the specific release set
    elif thisCMSSW in specificSupportedReleases:
        return True
    # true if the CMSSW release's minor revision is >= to one of the supported releases
    else:
        thisMajor, thisMid, thisMinor = [int(i) for i in thisCMSSW.split('_', 4)[1:4]]
        for release in supportedReleases:
            supportedMajor, supportedMid, supportedMinor = [int(i) for i in release.split('_', 4)[1:4]]
            # major and mid revisions need an exact match
            if thisMajor == supportedMajor and thisMid == supportedMid:
                # minor revision need to be >= to the supported minor revision
                if thisMinor >= supportedMinor:
                    return True
    return False


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
# //  enable test mode.
procWriter = lambda s, l: s.stdin.write(encodeUnicodeToBytes(l))


class Scram(object):
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
        self.library_path = "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH\n"
        self.procWriter = procWriter
        if self.test_mode:
            # if in test mode, decorate the subprocess writer with the test harness
            self.procWriter = testWriter(procWriter)
            # dont actually try to call a non-existent scram binary in test mode
            self.command = "/bin/echo"

        # buffers for debug/error reporting
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
        if self.architecture is not None:
            result += "export SCRAM_ARCH=%s\n" % self.architecture

        if self.initialise is not None:
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
        self.procWriter(proc, self.preCommand())
        self.procWriter(proc, "%s --arch %s project CMSSW %s\n" % (self.command, self.architecture, self.version))
        self.procWriter(proc, """if [ "$?" -ne "0" ]; then exit 3; fi\n""")
        self.procWriter(proc, "exit 0")

        self.projectArea = "%s/%s" % (self.directory, self.version)
        self.stdout, self.stderr = proc.communicate()
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
        if self.projectArea is None:
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
        except Exception as ex:
            msg = "Error thrown while invoking subprocess for scram runtime\n"
            msg += "%s\n" % str(ex)
            msg += "Opening subprocess shell in %s" % self.projectArea
            self.stdout = msg
            self.stderr = ""
            return 1

        # write via process writer method
        self.procWriter(proc, self.library_path)
        self.procWriter(proc, self.preCommand())
        self.procWriter(proc, "%s ru -sh\n" % self.command)
        self.procWriter(proc, """if [ "$?" -ne "0" ]; then exit 4; fi\n""")
        self.procWriter(proc, "eval `%s ru -sh`\n" % self.command)

        self.stdout, self.stderr = proc.communicate()
        self.stdout = decodeBytesToUnicode(self.stdout)
        self.stderr = decodeBytesToUnicode(self.stderr)
        if proc.returncode == 0:
            for l in self.stdout.split(";\n"):
                if l.strip() == "":
                    continue
                if l.strip().startswith('unset'):
                    continue
                l = l.replace("export ", "")
                try:
                    var, val = l.split("=", 1)
                except ValueError as ex:
                    raise ValueError("Couldn't split line: %s" % l) from ex

                self.runtimeEnv[var] = val
        if self.test_mode:
            # ensure that runtime env isnt empty in test mode
            # as that will cause an exception in __call__
            self.runtimeEnv['TEST_MODE'] = 1
        self.code = proc.returncode
        self.lastExecuted = "eval `%s ru -sh`" % self.command
        return proc.returncode

    def __call__(self, command, hackLdLibPath=False, runtimeDir=None, cleanEnv=True):
        """
        _operator(command)_

        Run the command in the runtime environment

        """
        if self.projectArea is None:
            msg = "Scram Project Area not set/project() not called"
            raise RuntimeError(msg)
        if self.runtimeEnv == {}:
            msg = "Scram runtime environment is empty/runtime() not called"
            raise RuntimeError(msg)
        executeIn = runtimeDir
        if runtimeDir is None:
            executeIn = self.projectArea

        bashcmd = "/bin/bash"
        if cleanEnv:
            # Start with a clean environment
            bashcmd = "env - " + bashcmd

        logging.info("Creating a subprocess to run the PSet setup.")
        logging.info("Also recording SCRAM command-line related output.")
        proc = subprocess.Popen([bashcmd], shell=True, cwd=executeIn,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
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
                rtCmsswBase = self.runtimeEnv[varName].replace('\"', '')
            if varName == "SCRAM_ARCH":
                rtScramArch = self.runtimeEnv[varName].replace('\"', '')

        if os.environ.get('VO_CMS_SW_DIR', None) is not None:
            self.procWriter(proc, 'export VO_CMS_SW_DIR=%s\n' % os.environ['VO_CMS_SW_DIR'])
        if os.environ.get('OSG_APP', None) is not None:
            self.procWriter(proc, 'export VO_CMS_SW_DIR=%s/cmssoft/cms\n' % os.environ['OSG_APP'])
        # In general, CMSSW releases <= 12_6_0 will use CMS_PATH, while anything beyond that
        # requires the SITECONFIG_PATH variable to properly load storage.xml/json file.
        if os.environ.get('CMS_PATH', None) is not None:
            self.procWriter(proc, 'export CMS_PATH=%s\n' % os.environ['CMS_PATH'])
        if os.environ.get('SITECONFIG_PATH', None) is not None:
            self.procWriter(proc, 'export SITECONFIG_PATH=%s\n' % os.environ['SITECONFIG_PATH'])
        if os.environ.get('_CONDOR_JOB_AD'):
            self.procWriter(proc, 'export _CONDOR_JOB_AD=%s\n' % os.environ['_CONDOR_JOB_AD'])
        if os.environ.get('_CONDOR_MACHINE_AD'):
            self.procWriter(proc, 'export _CONDOR_MACHINE_AD=%s\n' % os.environ['_CONDOR_MACHINE_AD'])

        if hackLdLibPath:
            self.procWriter(proc, self.library_path)

        self.procWriter(proc, "%s\n" % self.preCommand())

        if self.envCmd is not None:
            self.procWriter(proc, "%s\n" % self.envCmd)

        if command.startswith(sys.executable):
            # replace COMP python version with python from CMSSW release if possible
            python27exec = "%s/external/%s/bin/python2.7" % (rtCmsswBase, rtScramArch)
            python26exec = "%s/external/%s/bin/python2.6" % (rtCmsswBase, rtScramArch)
            if os.path.islink(python27exec):
                command = command.replace(sys.executable, python27exec)
            elif os.path.islink(python26exec):
                command = command.replace(sys.executable, python26exec)
            elif hackLdLibPath:
                # reset python path for DMWM python (scram will have changed env to point at its own)
                self.procWriter(proc, "export PYTHONPATH=%s:$PYTHONPATH\n" % ":".join(sys.path)[1:])

        logging.info("    Invoking command: %s", command)
        self.procWriter(proc, "%s\n" % command)
        self.procWriter(proc, """if [ "$?" -ne "0" ]; then exit 5; fi\n""")
        self.stdout, self.stderr = proc.communicate()
        self.stdout = decodeBytesToUnicode(self.stdout)
        self.stderr = decodeBytesToUnicode(self.stderr)
        logging.info("Subprocess stdout was:\n%s", self.stdout)
        logging.info("Subprocess stderr was:\n%s", self.stderr)
        self.code = proc.returncode
        self.lastExecuted = command

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

    def getStdout(self):
        """
        retrieve stdout of the command executed via the __call__ method\
        note that in current implementation stderr of command execution
        is piped to stdout
        :return: a (possibly long) string
        """
        return self.stdout
