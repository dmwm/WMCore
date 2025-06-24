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
from WMCore.Configuration import loadConfigurationFile
from WMCore.WMFactory import WMFactory
from WMCore.WMInit import WMInit

def connectionTest(configFile, componentsList=None):
    """
    _connectionTest_

    Create a DB Connection instance to test the connection specified
    in the config file.

    """
    config = loadConfigurationFile(configFile)
    if componentsList == None:
        componentsList = config.listComponents_() + config.listWebapps_()
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

    """
    exitCode = 0
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
            if not daemon.isAlive():
                print("Error: Component %s Did not start properly..." % component)
                print("Check component log to see why")
                return 1
        else:
            print('Path for daemon file does not exist!')
            return 1
        # write into component area process status information
        cpath = os.path.join(compDir, "threads.json")
        if os.path.exists(cpath):
            os.remove(cpath)
        cpid = extractFromXML(daemonXML, "ProcessID")
        with open(cpath, 'w', encoding="utf-8") as istream:
            procStatus = processStatus(cpid)
            istream.write(json.dumps(procStatus))
            print("Component %s started with %s threads, see %s\n" % (component, len(procStatus), cpath))

    return exitCode

def shutdown(configFile, componentsList=None, doLogCleanup=False, doDirCleanup=False):
    """
    _shutdown_

    Shutdown the component daemons

    If cleanup-logs option is specified, wipe out the component logs
    If cleanup-all option is specified, wipe out all component dir
    content and purge the ProdAgentDB

    """
    exitCode = 0
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

    """
    exitCode = 0
    config = loadConfigurationFile(configFile)
    if componentsList == None:
        componentsList = config.listComponents_() + config.listWebapps_()

    print('Status components: '+str(componentsList))
    for component in componentsList:
        checkComponentThreads(configFile, component)
    return exitCode

def checkComponentThreads(configFile, component):
    """
    Helper function to check process and its threads for their statuses
    :param component: component name
    :return: prints status of the component process and its threads
    """
    config = loadConfigurationFile(configFile)
    try:
        compDir = config.section_(component).componentDir
    except AttributeError:
        print ("Failed to check component: Could not find component named %s in config" % component)
        print ("Aborting")
        return
    compDir = config.section_(component).componentDir
    compDir = os.path.expandvars(compDir)

    # check if component daemon exists
    daemonXml = os.path.join(compDir, "Daemon.xml")
    if not os.path.exists(daemonXml):
        print("Component:%s Not Running" % component)
        return
    pid = extractFromXML(daemonXml, "ProcessID")

    jsonFile = os.path.join(compDir, "threads.json")
    with open(jsonFile, "r", encoding="utf-8") as istream:
        data = json.load(istream)

    # Extract process and its threads
    threadPids = []

    for entry in data:
        if str(entry["pid"]) == str(pid) and entry["type"] == "process":
            continue
        elif entry["type"] == "thread":
            threadPids.append(int(entry["pid"]))
    # Check if process is running
    processRunning = psutil.pid_exists(int(pid))
    if not processRunning:
        msg = f"Component:{component} with PID={pid} is no longer available on OS"
        print(msg)
        return

    # Check if threads are running - the list threadPids is fetched from the threads.json file
    # as it has been constructed at Daemon startup time
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
        runningMsg = f"with threads: {runningThreads}"
        if len(lostThreads) > 0:
            status = f"{status}-partially"
            lostMsg = f", lost threads: {lostThreads}"
        if len(orphanThreads) > 0:
            status = f"{status}-untracked"
            orphanMsg = f", untracked threads: {orphanThreads}"
    msg = f"Component:{component} {pid} {status} {runningMsg} {lostMsg} {orphanMsg}"
    print(msg)
    pidTree = {}
    pidTree['Parent'] = int(pid)
    pidTree['RunningThreads'] = list(runningThreads)
    pidTree['OrphanThreads'] = list(orphanThreads)
    pidTree['LostThreads'] = list(lostThreads)
    return pidTree

def restart(config, componentsList=None, doLogCleanup=False, doDirCleanup=False):
    """
    _restart_

    do a shutdown and startup again

    """
    exitCode = 0
    exitCode += shutdown(config, componentsList, doDirCleanup, doLogCleanup)
    exitCode += startup(config, componentsList)
    return exitCode

def isComponentAlive(config, component=None, pid=None, trace=True, timeout=6):
    """
    _isComponentAlive_
    A function to asses if a component is stuck or is still doing its job in the background.
    It uses the ptrace module to monitor the component's system calls instead of just
    declaring the component dead only because of lack of log entries as it was in the past.
    :param config: Path to WMAgent configuration file
    :param component: Component name to be checked (str)
    :param trace: Bool flag to chose whether to use strace like mechanisms to examine the component's
                  system calls during the tests or to just check if the process tree of the component is sane
    :param timeout: The amount of time to wait during a test before declaring it failed (Default 60 sec.)
                    NOTE: In case the component's system calls will be traced, this timeout would be used
                          to wait for any system call before entering deeper logic in the tests.
    :return: Bool - True if all checks has passed, False if any of the checks has returned an error

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
        pidTree = checkComponentThreads(config, component)
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
        print("Start all tests")
        for threadId, tracer in tracers.items():
            # .....
            checkList.append(True)

        print("Detaching from all the threads")
        # Detach all tracers before returning:
        for threadId, tracer in tracers.items():
            tracer.detach()

    return all(checkList)
    # return tracers
