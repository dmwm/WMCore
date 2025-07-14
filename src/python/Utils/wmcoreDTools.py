import os
import signal
import subprocess
import time
import json
import psutil

from ptrace.debugger import PtraceProcess, PtraceDebugger
from ptrace.error import PtraceError
from pprint import pformat, pprint
from Utils.Utilities import extractFromXML
from Utils.ProcFS import processStatus
from Utils.ProcessStats import processThreadsInfo
from WMCore.Agent.Daemon.Details import Details
from WMCore.Configuration import loadConfigurationFile, Configuration
from WMCore.WMFactory import WMFactory
from WMCore.WMInit import WMInit

def connectionTest(configFile):
    """
    _connectionTest_

    Create a DB Connection instance to test the connection specified
    in the config file.

    :param configFile:     Either path to the WMAgent configuration file or a WMCore.Configuration instance.
    :return: None
    """
    if isinstance(configFile, Configuration):
        config = configFile
    else:
        config = loadConfigurationFile(configFile)

    wmInit = WMInit()

    print("Checking default database connection...", end=' ')
    if not hasattr(config, "CoreDatabase"):
        print("skipped.")
        return

    dialect, _ = config.CoreDatabase.connectUrl.split(":", 1)
    socket = getattr(config.CoreDatabase, "socket", None)

    try:
        wmInit.setDatabaseConnection(dbConfig = config.CoreDatabase.connectUrl,
                                     dialect = dialect,
                                     socketLoc = socket)
    except Exception as ex:
        msg = "Unable to make connection to using \n"
        msg += "parameters provided in %s\n" % config.CoreDatabase.connectUrl
        msg += str(ex)
        print(msg)
        raise ex

    print("ok.")
    return

def startup(configFile, componentsList=None):
    """
    _startup_

    Start up the component daemons

    :param configFile:     Either path to the WMAgent configuration file or a WMCore.Configuration instance.
    :param componentsList: A list of components to be acted upon.
    :return:               int ExitCode - 0 in case of success, nonzero value otherwise
    """
    exitCode = 0
    if isinstance(configFile, Configuration):
        config = configFile
    else:
        config = loadConfigurationFile(configFile)

    if componentsList == None:
        componentsList = config.listComponents_() + config.listWebapps_()

    print('Starting components: '+str(componentsList))
    for component in componentsList:
        print('Starting : '+component)
        if component in config.listWebapps_():
            from WMCore.WebTools.Root import Root
            webtoolsRoot = Root(config, webApp = component)
            webtoolsRoot.startDaemon(keepParent = True, compName = component)
        else:
            factory = WMFactory('componentFactory')
            try:
                namespace = config.component_(component).namespace
            except AttributeError:
                print ("Failed to start component: Could not find component named %s in config" % component)
                print ("Aborting")
                return 1
            componentObject = factory.loadObject(classname = namespace, args = config)
            componentObject.startDaemon(keepParent = True)

        print('Waiting 1 seconds, to ensure daemon file is created')
        time.sleep(1)
        compDir = config.section_(component).componentDir
        compDir = os.path.expandvars(compDir)
        daemonXML = os.path.join( compDir, "Daemon.xml")
        if os.path.exists(daemonXML):
            daemon = Details(daemonXML)
            # write into component area process status information
            cpath = os.path.join(compDir, "threads.json")
            if os.path.exists(cpath):
                os.remove(cpath)
            cpid = extractFromXML(daemonXML, "ProcessID")
            with open(cpath, 'w', encoding="utf-8") as istream:
                procStatus = processStatus(cpid)
                istream.write(json.dumps(procStatus))

            if not daemon.isAlive():
                print("Error: Component %s Did not start properly..." % component)
                print("Check component log to see why")
                return 1
        else:
            print('Path for daemon file does not exist!')
            return 1
        numThreads = len([proc for proc in procStatus if proc['type'] == 'thread'])
        numProcs = len([proc for proc in procStatus if proc['type'] == 'process'])
        print("Component %s started with %s main process(es) and %s threads, see %s\n" % (component, numProcs, numThreads, cpath))
    return exitCode

def shutdown(configFile, componentsList=None, doLogCleanup=False, doDirCleanup=False):
    """
    _shutdown_

    Shutdown the component daemons

    If cleanup-logs option is specified, wipe out the component logs
    If cleanup-all option is specified, wipe out all component dir
    content and purge the ProdAgentDB

    :param configFile:     Either path to the WMAgent configuration file or a WMCore.Configuration instance.
    :param componentsList: A list of components to be acted upon.
    :return:               int ExitCode - 0 in case of success, nonzero value otherwise
    """
    exitCode = 0
    if isinstance(configFile, Configuration):
        config = configFile
    else:
        config = loadConfigurationFile(configFile)

    if componentsList == None:
        componentsList = config.listComponents_() + config.listWebapps_()

    print('Stopping components: '+str(componentsList))
    for component in componentsList:
        print('Stopping: '+component)
        try:
            compDir = config.section_(component).componentDir
        except AttributeError:
            print ("Failed to shutdown component: Could not find component named %s in config" % component)
            print ("Aborting")
            return 1
        compDir = os.path.expandvars(compDir)
        daemonXml = os.path.join(compDir, "Daemon.xml")
        if not os.path.exists(daemonXml):
            print("Cannot find Daemon.xml for component:", component)
            print("Unable to shut it down")
        else:
            daemon = Details(daemonXml)
            if not daemon.isAlive():
                print("Component %s with process id %s is not running" % (
                    component, daemon['ProcessID'],
                    ))
                daemon.removeAndBackupDaemonFile()
            else:
                daemon.killWithPrejudice()
        # remove component threads.json file
        cpath = os.path.join(compDir, "threads.json")
        if os.path.exists(cpath):
            os.remove(cpath)
        if doLogCleanup:
            #  //
            # // Log Cleanup
            #//
            msg = "Removing %s/ComponentLog" % compDir
            print(msg)
            try:
                os.remove("%s/ComponentLog" % compDir)
            except Exception as ex:
                msg = "Unable to cleanup Component Log: "
                msg += "%s/ComponentLog\n" % compDir
                msg += str(ex)

        if doDirCleanup:
            #  //
            # // Cleanout everything in ComponentDir
            #//  for this component
            print("Removing %s\n" % compDir)
            exitCode = subprocess.call(["rm", "-rf", "%s" % compDir])
            if exitCode:
                msg = "Failed to clean up dir: %s\n" % compDir
                msg += f"with exit code {exitCode}"
                print(msg)

    return exitCode


def status(configFile, componentsList=None):
    """
    _status_

    Print status of all components in config file

    :param configFile:     Either path to the WMAgent configuration file or a WMCore.Configuration instance.
    :param componentsList: A list of components to be acted upon.
    :return:               int ExitCode - 0 in case of success, nonzero value otherwise
    """
    exitCode = 0
    if isinstance(configFile, Configuration):
        config = configFile
    else:
        config = loadConfigurationFile(configFile)

    if componentsList == None:
        componentsList = config.listComponents_() + config.listWebapps_()

    print('Status components: '+str(componentsList))
    for component in componentsList:
        getComponentThreads(configFile, component)
    return exitCode

def getComponentThreads(configFile, component):
    """
    Helper function to check process and its threads for their statuses

    :param configFile: Either path to the WMAgent configuration file or a WMCore.Configuration instance
    :param component:  Component name
    :return: The process tree for the component and prints status of the component process and its threads
    """
    pidTree = {}
    if isinstance(configFile, Configuration):
        config = configFile
    else:
        config = loadConfigurationFile(configFile)

    try:
        compDir = config.section_(component).componentDir
    except AttributeError:
        print ("Failed to check component: Could not find component named %s in config" % component)
        print ("Aborting")
        return pidTree
    compDir = config.section_(component).componentDir
    compDir = os.path.expandvars(compDir)

    # check if component daemon exists
    daemonXml = os.path.join(compDir, "Daemon.xml")
    if not os.path.exists(daemonXml):
        print("Component:%s Not Running" % component)
        return pidTree
    pid = extractFromXML(daemonXml, "ProcessID")

    jsonFile = os.path.join(compDir, "threads.json")
    with open(jsonFile, "r", encoding="utf-8") as istream:
        data = json.load(istream)

    # Extract process and its threads
    threadPids = []

    # Properly parsing the thread.json file
    for entry in data:
        if 'error' in entry:
            print(f"Error recorded at threads.json during component startup: {entry['error']}")
            break
        if str(entry["pid"]) == str(pid) and entry["type"] == "process":
            continue
        elif entry["type"] == "thread":
            threadPids.append(int(entry["pid"]))

    # Check if process is running
    processRunning = psutil.pid_exists(int(pid))
    if not processRunning:
        msg = f"Component:{component} with PID={pid} is no longer available on OS"
        print(msg)
        return pidTree

    # Check if initial threads are running.
    # NOTE: The list threadPids is fetched from the threads.json file
    #       as it has been constructed at Daemon startup time. Any threads spawn later during
    #       the component's lifetime are not tracked by threads.json file, which means
    #       they will always fall either into orphanThreads or lostThreads. That's why the
    #       results from this function cannot be used for components like AgentWatchdog,
    #       where every timer has its own thread and those are regularly restarted.
    #       Such mechanism always results in non-constant and nonzero values in the orphanThreads and
    #       lostThreads fields of the pidTree, which are changing following the child threads life cycle.
    process = psutil.Process(int(pid))
    currThreads = set([thread.id for thread in process.threads()])
    startupThreads = set(threadPids)
    # Remove the parent pid from the list of currently running threads
    try:
        currThreads.remove(int(pid))
    except ValueError or KeyError:
        pass

    runningThreads = currThreads & startupThreads
    orphanThreads = currThreads - startupThreads
    lostThreads = startupThreads - currThreads

    # Output result
    msg=""
    runningMsg=""
    orphanMsg=""
    lostMsg=""
    status = "running" if processRunning else "not-running"
    if status == "running":
        runningMsg = f"with {len(runningThreads)} running threads: {runningThreads}"
        if len(lostThreads) > 0:
            status = f"{status}-partially"
            lostMsg = f", {len(lostThreads)} lost threads: {lostThreads}"
        if len(orphanThreads) > 0:
            status = f"{status}-untracked"
            orphanMsg = f", {len(orphanThreads)} untracked/zombie threads: {orphanThreads}"
    msg = f"Component:{component} {pid} {status} {runningMsg} {lostMsg} {orphanMsg}"
    print(msg)

    pidTree['Parent'] = int(pid)
    pidTree['RunningThreads'] = list(runningThreads)
    pidTree['OrphanThreads'] = list(orphanThreads)
    pidTree['LostThreads'] = list(lostThreads)
    return pidTree

def restart(config, componentsList=None, doLogCleanup=False, doDirCleanup=False):
    """
    _restart_

    do a shutdown and startup again
    :param configFile: Either path to the WMAgent configuration file or a WMCore.Configuration instance
    :return:           int ExitCode - 0 in case of success, nonzero value otherwise
    """
    exitCode = 0
    exitCode += shutdown(config, componentsList, doDirCleanup, doLogCleanup)
    exitCode += startup(config, componentsList)
    return exitCode

def forkRestart(config=None, componentsList=None, useWmcoreD=False):
    """
    _frokRestart_

    Call component restart actions by forking a subprocess in the background
    :param config:         Path to the WMAgent configuration file
    :param componentsList: The list of components to be restarted
    :param useWmcoreD:     Bool Flag to tell if to use wmcoreD for this action or to act directly
                           with python and the functions imported from wmcoreDTools (Default: False)
                           NOTE: if False, requires config to be provided.
    :return:               int ExitCode - 0 in case of success, nonzero value otherwise

    NOTE: This function works only with a path to the WMAgent configuration file, because it is
          supposed to be called independently as a separate process through the subprocess module
          to which we cannot pass python objects as arguments
    """
    try:
        if useWmcoreD:
            if componentsList:
                componentsListStr = ','.join(componentsList)
                res =  subprocess.run(["wmcoreD", "--restart", "--component", f"{componentsListStr}"], capture_output=True, check=True)
            else:
                res =  subprocess.run(["wmcoreD", "--restart"], capture_output=True, check=True)
        else:
            # NOTE: Here follows an alternative and shorter way of calling the above without referring to `wmcoreD`
            #       and the extra burden of converting all python options into strings.
            #       This method results in a longer and a bit more obscure `ps uxf` output line):
            #       Another difference between those two methods is that `wmcoreD` takes WMAGENT_CONFIG
            #       from the environment, while the later method requires the config to be passed explicitly
            if isinstance(config, Configuration):
                configFile = config.getLoadPath()
            else:
                configFile = confg
                if not os.path.exists(str(configFile)):
                    print(f"ERROR: Could not find configuration path: {configFile}")
                    return 1
            cmd = f"from Utils.wmcoreDTools import restart; restart('{configFile}', {componentsList})"
            res = subprocess.run(['python', '-c', cmd], capture_output=True, check=True)
    except subprocess.CalledProcessError as ex:
        print(f"ERROR: The called subprocess returned an error: {ex.returncode}")
        print(f"ERROR: Full subprocess Output: {ex.output}")
        raise
    return res.returncode

def resetWatchdogTimer(configFile, component):
    """
    _resetWatchdogTimer_

    Resets a given watchdog timer. The timer can be identified by component name or by the timer's PID

    :param configFile:     Either path to the WMAgent configuration file or a WMCore.Configuration instance.
    :param component:       The name of the component this timer is associated with. This also determines
                           the place where the component's timer will be searched for.
    :return:               int ExitCode - 0 in case of success, nonzero value otherwise
    """

    exitCode = 0
    try:
        if isinstance(configFile, Configuration):
            config = configFile
        else:
            config = loadConfigurationFile(configFile)

        compDir = config.section_(component).componentDir
        compDir = os.path.expandvars(compDir)
        timerPath = compDir + '/' + 'ComponentTimer'
        with open(timerPath, 'r') as timerFile:
            timer = json.load(timerFile)

            # Reset the timer by sending it the expected signal.
            os.kill(timer['native_id'], timer['expSig'])

    except Exception as ex:
        exitCode = 1
        msg = f"ERROR: Failed to reset {component} component's timer. ERROR: {str(ex)}"
        print(msg)
    return exitCode

def isComponentAlive(config, component=None, pid=None, trace=False, timeout=6):
    """
    _isComponentAlive_
    A function to asses if a component is stuck or is still doing its job in the background.
    It uses psutil and ptrace modules to monitor the component threads' state and system calls instead
    of just declaring the component dead only because of lack of log entries as it was in the past.
    :param config:    Path to WMAgent configuration file or an instance of WMCore.Configuration
    :param component: Component name to be checked (str)
                      NOTE: mutually exclusive with the pid parameter)
    :param pid:       The process ID to be checked if not component name was provided
                      NOTE: component name takes precedence so pid will be ignored if both are to be provided
    :param trace:     Bool flag to chose whether to use strace like mechanisms to examine the component's
                      system calls during the tests or to just check if the process tree of the component is sane
    :param timeout:   The amount of time to wait during a ptrace based test before declaring it failed (Default 60 sec.)
                      NOTE: In case the component's system calls will be traced, this timeout would be used
                          to wait for any system call before entering deeper logic in the tests.
    :return:          Bool - True if all checks has passed, False if any of the checks has returned an error

    NOTE: We basically have three eventual reasons for a process to seemingly has gotten
          stuck and doing nothing:
          * Soft Lockup:
            When a thread or task is not releasing the CPU for long period of time
            and not allowing other tasks to proceed. Typical reason could be -  the CPU
            is stuck in executing code in kernel space.
          * Blocking System Calls:
            When a process is stuck in a system call (e.g. waiting for I/O).
          * Unkillable process:
            When a process does not respond to any signals, e.g. stuck in an Uninterruptable
            Sleep state. Which means, it cannot be woken by any signal, not even SIGKILL.
            Such processes are marked with State:D in the output from the `ps` command.
    """

    checkList = []

    # First create the pidTree and collect information for the examined process:
    if component:
        pidTree = getComponentThreads(config, component)
    elif pid:
        pidTree = {}
        process = psutil.Process(int(pid))
        currThreads = [thread.id for thread in process.threads()]
        # # Remove the parent pid from the list of currently running threads
        # try:
        #     currThreads.remove(int(pid))
        # except ValueError or KeyError:
        #     pass
        pidTree['Parent'] = int(pid)
        pidTree['RunningThreads'] = currThreads
        pidTree['OrphanThreads'] = []
        pidTree['LostThreads'] = []
    else:
        print(f"You must provide PID or Component Name")
        return False

    if not pidTree:
        return False

    # Get the PID status,statistics and major resource usage
    # NOTE: If we've lost some threads or they have run as zombies we will miss them in the structure produced here.
    #       Those must have been caught and accounted for while building the pidTree
    pidInfo = processThreadsInfo(pidTree['Parent'])
    # pprint(pidInfo)
    # pprint(pidTree)

    # If we already have found there are orphaned/lost threads stemming from the current pidTree
    # we already declare the first check as Failed (in such case we can return even from this point here)
    if pidTree['OrphanThreads'] or pidTree['LostThreads']:
        checkList.append(False)
    else:
        checkList.append(True)

    # Check Main process status:
    checkList.append(pidInfo['is_running'] and pidInfo['status'] is not psutil.STATUS_ZOMBIE)

    # Check all threads statuses:
    for threadInfo in pidInfo['threads']:
        checkList.append(threadInfo['is_running'] and threadInfo['status'] is not psutil.STATUS_ZOMBIE)

    # Build strace like tests if it was chosen to
    # Setup the debugger and tracer process objects for every thread from the pidTree
    if trace:
        debugger = PtraceDebugger()
        tracers = {}
        for threadId in pidTree['RunningThreads']:
            try:
                # Initially try to attach the process as a non traced one
                print(f"Tracing {threadId} as unattached process")
                tracers[threadId] = PtraceProcess(debugger, threadId, False, parent=pidTree['Parent'], is_thread=True)
            except PtraceError:
                # in case of an error make an attempt to attach it as already traced one (supposing
                # a previous execution of the current function could not finish and release it.
                print(f"Tracing {threadId} as already attached process")
                tracers[threadId] = PtraceProcess(debugger, threadId, True, parent=pidTree['Parent'], is_thread=True)

        # Now start designing all the tests per each thread in order to cover the three possible problematic
        # states as explained in the function docstring.
        print("Start ptrace based tests")
        for threadId, tracer in tracers.items():
            # .....
            checkList.append(True)

        print("Detaching from all the threads")
        # Detach all tracers before returning:
        for threadId, tracer in tracers.items():
            tracer.detach()

    return all(checkList)
    # return tracers
