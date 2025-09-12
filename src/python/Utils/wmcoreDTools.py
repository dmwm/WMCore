import os
import sys
import signal
import subprocess
import time
import json
import psutil
import inspect
import logging
from collections  import namedtuple

from ptrace.debugger import PtraceProcess, PtraceDebugger
from ptrace.debugger.process_event import ProcessExit
from ptrace.debugger.ptrace_signal import ProcessSignal
from ptrace.func_call import FunctionCallOptions
from ptrace.error import PtraceError
from pprint import pformat, pprint
from Utils.Utilities import extractFromXML
from Utils.ProcFS import processStatus
from Utils.ProcessStats import processThreadsInfo
from WMCore.Agent.Daemon.Details import Details
from WMCore.Configuration import loadConfigurationFile, Configuration, ConfigSection
from WMCore.WMFactory import WMFactory
from WMCore.WMInit import WMInit


def _loadConfig(configFile):
    """
    Auxiliary function to check the type of or load a wmagent configuration from file.
    :param configFile: Either path to the WMAgent configuration file or a WMCore.Configuration instance.
    :return:           A WMAgent configuration instance
    """
    if isinstance(configFile, Configuration):
        config = configFile
    else:
        config = loadConfigurationFile(configFile)
    return config

def _getConfigSubsections(configSection):
    """
    Auxiliary function to return any possible subsection defined in a WMАgent configuration section
    :param configSection: An instance of WMCore.Configuratin.ConfigSection
    :return:              A dictionary with all subsections one level down (no recursions are performed)
                          e.g.
                          {'AgentStatusPoller': <WMCore.Configuration.ConfigSection at 0x7f6b0a931370>,
                           'DrainStatusPoller': <WMCore.Configuration.ConfigSection at 0x7f6b0a931220>,
                           'ResourceControlUpdater': <WMCore.Configuration.ConfigSection at 0x7f6b0a931280>}
    """
    return {subSection[0]:subSection[1] for subSection in inspect.getmembers(configSection)
            if isinstance(subSection[1], ConfigSection)}

def getThreadConfigSections(config, compName):
    """
    Auxiliary function to find all threads configured to be spawned by a component
    by parsing all config subsections related to this component and filtering any non thread related subsection.
    :param config:   WMAgent configuration path or instance
    :param compName: The component name to be searched for
    :return:         A dictionary with all subsections one level down (no recursions are performed)
                     e.g.
                     {'AgentStatusPoller': <WMCore.Configuration.ConfigSection at 0x7f6b0a931370>,
                      'DrainStatusPoller': <WMCore.Configuration.ConfigSection at 0x7f6b0a931220>,
                      'ResourceControlUpdater': <WMCore.Configuration.ConfigSection at 0x7f6b0a931280>}
    # NOTE: We expect to have a separate sub sections per every thread defining at least its time parameters.
    #       They should be searched for in the respective subsection first and if not found only then to
    #       fall back to the upper level component config section.

    """
    config = _loadConfig(config)
    compConfigSection = getattr(config, compName, None)
    compSubSections = _getConfigSubsections(compConfigSection)

    return {subSectionName: subSection for subSectionName, subSection in compSubSections.items()
            if getattr(subSection, 'pollInterval', None) or getattr(subSection, 'runTimeEst', None)}

def connectionTest(configFile):
    """
    _connectionTest_

    Create a DB Connection instance to test the connection specified
    in the config file.

    :param configFile:     Either path to the WMAgent configuration file or a WMCore.Configuration instance.
    :return: None
    """
    config = _loadConfig(configFile)

    wmInit = WMInit()

    logging.info("Checking default database connection... ")
    if not hasattr(config, "CoreDatabase"):
        logging.info("skipped.")
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
        logging.error(msg)
        raise ex

    logging.info("ok.")
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
    config = _loadConfig(configFile)

    if componentsList == None:
        componentsList = config.listComponents_() + config.listWebapps_()

    logging.info('Starting components: '+str(componentsList))
    for component in componentsList:
        logging.info('Starting : '+component)
        if component in config.listWebapps_():
            from WMCore.WebTools.Root import Root
            webtoolsRoot = Root(config, webApp = component)
            webtoolsRoot.startDaemon(keepParent = True, compName = component)
        else:
            factory = WMFactory('componentFactory')
            try:
                namespace = config.component_(component).namespace
            except AttributeError:
                logging.error ("Failed to start component: Could not find component named %s in config" % component)
                logging.error ("Aborting")
                return 1
            componentObject = factory.loadObject(classname = namespace, args = config)
            componentObject.startDaemon(keepParent = True)

        logging.info('Waiting 1 seconds, to ensure daemon file is created')
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
                logging.error("Error: Component %s Did not start properly..." % component)
                logging.error("Check component log to see why")
                return 1
        else:
            logging.error('Path for daemon file does not exist!')
            return 1
        numThreads = len([proc for proc in procStatus if proc['type'] == 'thread'])
        numProcs = len([proc for proc in procStatus if proc['type'] == 'process'])
        logging.info("Component %s started with %s main process(es) and %s threads, see %s\n" % (component, numProcs, numThreads, cpath))
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
    :param doLogCleanup:   A Bool flag identifying if all components' logs are to be cleaned upon shutdown
    :param doDirCleanup:   A Bool flag identifying if the components' working area is to be cleaned upon shutdown
    :return:               int ExitCode - 0 in case of success, nonzero value otherwise
    """
    exitCode = 0
    config = _loadConfig(configFile)

    if componentsList == None:
        componentsList = config.listComponents_() + config.listWebapps_()

    logging.info('Stopping components: '+str(componentsList))
    for component in componentsList:
        logging.info('Stopping: '+component)
        try:
            compDir = config.section_(component).componentDir
        except AttributeError:
            logging.error ("Failed to shutdown component: Could not find component named %s in config" % component)
            logging.error ("Aborting")
            return 1
        compDir = os.path.expandvars(compDir)
        daemonXml = os.path.join(compDir, "Daemon.xml")
        if not os.path.exists(daemonXml):
            logging.warning("Cannot find Daemon.xml for component: %s", component)
            logging.warning("Unable to shut it down")
        else:
            daemon = Details(daemonXml)
            if not daemon.isAlive():
                logging.warning("Component %s with process id %s is not running" % (
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
            logging.info(msg)
            try:
                os.remove("%s/ComponentLog" % compDir)
            except Exception as ex:
                msg = "Unable to cleanup Component Log: "
                msg += "%s/ComponentLog\n" % compDir
                msg += str(ex)
                logging.error(msg)

        if doDirCleanup:
            #  //
            # // Cleanout everything in ComponentDir
            #//  for this component
            logging.info("Removing %s\n" % compDir)
            exitCode = subprocess.call(["rm", "-rf", "%s" % compDir])
            if exitCode:
                msg = "Failed to clean up dir: %s\n" % compDir
                msg += f"with exit code {exitCode}"
                logging.error(msg)

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
    config = _loadConfig(configFile)

    if componentsList == None:
        componentsList = config.listComponents_() + config.listWebapps_()

    logging.info('Status components: '+str(componentsList))
    for component in componentsList:
        getComponentThreads(configFile, component)
    return exitCode

def getComponentThreads(configFile, component, quiet=False):
    """
    Helper function to check process and its threads for their statuses

    :param configFile: Either path to the WMAgent configuration file or a WMCore.Configuration instance
    :param component:  Component name
    :param quiet:      Bool flag to set quiet mode - no info messages. (Default: False)
    :return: The process tree for the component and prints status of the component process and its threads

    :Example: getComponentThreads(wmaConfig, "AgentWatchdog")

              {'Parent': 1417248,
               'RunningThreads': [1417249, 1417251, 1417252, 1417253],
               'OrphanThreads': [],
               'LostThreads': []}
    """
    pidTree = {}
    config = _loadConfig(configFile)

    try:
        compDir = config.section_(component).componentDir
    except AttributeError:
        logging.error("Failed to check component: Could not find component named %s in config" % component)
        logging.error("Aborting")
        return pidTree
    compDir = config.section_(component).componentDir
    compDir = os.path.expandvars(compDir)

    # check if component daemon exists
    daemonXml = os.path.join(compDir, "Daemon.xml")
    if not os.path.exists(daemonXml):
        logging.error("Cannot find Daemon.xml. Component:%s Not Running." % component)
        return pidTree
    daemon = Details(daemonXml)
    pid = daemon['ProcessID']
    if not daemon.isAlive():
        msg = f"Component {component}, with process id {pid} is not running"
        msg += ", but Daemon.xml file was present. This might become a serious problem!"
        logging.warning(msg)
        return pidTree

    # NOTE: We should check for os.path.exists(jsonFile) here.
    #       Letting the system throw an exception in this situation actually
    #       breaks other calls e.g. isComonentAlive. Because threads.json file is created at
    #       startup time few steps upon the Daemon.xml file creation. Having one of
    #       the files created and not the other means either:
    #       * Someone has called getComponentTreads during the process of the
    #         component startup - this simply should not work, because the full
    #         set of threads to be spawned by the component is still undetermined. OR:
    #       * Something went terribly wrong during the component startup. OR:
    #       * The component has not shut down properly and a stale Daemon.xml file still exists
    jsonFile = os.path.join(compDir, "threads.json")
    if not os.path.exists(jsonFile):
        logging.error("Component:%s Not Running ... Either had problems at startup or not completed its shutdown sequence" % component)
        return pidTree
    with open(jsonFile, "r", encoding="utf-8") as istream:
        data = json.load(istream)

    # Extract process and its threads
    threadPids = []

    # Properly parsing the thread.json file
    for entry in data:
        if 'error' in entry:
            logging.error(f"Error recorded at threads.json during component startup: {entry['error']}")
            break
        if str(entry["pid"]) == str(pid) and entry["type"] == "process":
            continue
        elif entry["type"] == "thread":
            threadPids.append(int(entry["pid"]))

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

    # Adding an exception for AgentWatchdog when it comes to building the sets of threads
    if component == 'AgentWatchdog':
        runningThreads = currThreads
        orphanThreads = set()
        lostThreads = set()

    # Output result
    msg=""
    runningMsg=""
    orphanMsg=""
    lostMsg=""
    status = "running" if daemon.isAlive() else "not-running"
    if status == "running":
        runningMsg = f"with {len(runningThreads)} running threads: {runningThreads}"
        if len(lostThreads) > 0:
            status = f"{status}-partially"
            lostMsg = f", {len(lostThreads)} lost threads: {lostThreads}"
        if len(orphanThreads) > 0:
            status = f"{status}-untracked"
            orphanMsg = f", {len(orphanThreads)} untracked/zombie threads: {orphanThreads}"
    msg = f"Component:{component} {pid} {status} {runningMsg} {lostMsg} {orphanMsg}"
    if not quiet:
        logging.info(msg)

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
                configFile = config
                if not os.path.exists(str(configFile)):
                    logging.error(f"ERROR: Could not find configuration path: {configFile}")
                    return 1
            cmd = f"from Utils.wmcoreDTools import restart; restart('{configFile}', {componentsList})"
            res = subprocess.run([sys.executable, '-c', cmd], capture_output=True, check=True)
    except subprocess.CalledProcessError as ex:
        logging.error(f"ERROR: The called subprocess returned an error: {ex.returncode}")
        logging.error(f"ERROR: Full subprocess Output: {ex.output}")
        raise
    return res.returncode

def resetWatchdogTimer(wmaObj, compName=None, threadName=None):
    """
    _resetWatchdogTimer_

    Resets a given watchdog timer. The timer can be identified by either:
    * Providing the wmagentConfig file in combination with the component name
      and the thread/process name of the component thread the timer belongs to
    or
    * Providing an instance of a WMAgent thread/process and extracting
      the component and thread name information from the instance itself.

    The timer should always be found at $WMA_INSTALL_DIR/<compName>/Timer-<ThreadInstance>

    :param wmaObj:         Any instance of WMComponent.*.* thread/process or a wmagent configuration file
    :param configFile:     Either path to the WMAgent configuration file or a WMCore.Configuration instance.
    :param compName:       The name of the component this timer is associated with. This also determines
                           the place where the component's timer will be searched for.
                           (Required param if wmaObj is a wmaConfig file)
    :param threadName:     The name of the thread/process this timer is associated with. This also determines
                           the name of the timer.
                           (Required param if wmaObj is a wmaConfig file)
    :return:               int ExitCode - 0 in case of success, nonzero value otherwise
    """

    exitCode = 0
    try:
        # First fetch the needed information
        if isinstance(wmaObj, Configuration):
            if not compName or not threadName:
                logging.error(f"You must provide component name and thread/process name in addition to the wmagent configuration file")
                exitCode = 1
                return exitCode
            config = _loadConfig(wmaObj)
        else:
            config = _loadConfig(wmaObj.config)
            compName = componentName(wmaObj)
            threadName = moduleName(wmaObj)

        # Now find the timer:
        compDir = config.section_(compName).componentDir
        compDir = os.path.expandvars(compDir)
        timerPath = f"{compDir}/Timer-{threadName}"
        with open(timerPath, 'r') as timerFile:
            timer = json.load(timerFile)

            # Reset the timer by sending the expected signal to the timer thread.
            os.kill(timer['native_id'], timer['expSig'])

    except AttributeError:
        exitcode = 1
        logging.error("Failed to load {compName} component config section.")
        logging.error("Aborting")
    except ProcessLookupError as ex:
        logging.warning(f"The timer thread: {timer['native_id']} is missing. Probably the timer has expired.")
        logging.warning(f"Trying to fully restart the timer by sending the signal to its parent thread: {timer['parent_id']}")
        try:
            # Restart the timer by sending the expected signal to the timer' parent(creator) thread.
            os.kill(timer['parent_id'], timer['expSig'])
        except Exception as ex:
            exitCode = 1
            logging.error(f"Failed to restart the timer: {threadName} of component: {compName}. ERROR: {str(ex)}")
    except Exception as ex:
        exitCode = 1
        logging.error(f"Failed to reset timer: {threadName} of component: {compName}. ERROR: {str(ex)}")
    return exitCode

def componentName(obj):
    """
    Returns the component name the current object instance belongs to.
    It relies on the fact that our component modules are always structured as:
    WMComponent.<ComponentName>.<ComponentPoller/Thread>
    :param obj:  Any instance of an object from any of the classes defined  under WMComponent module area
    :return:     String - The Parent module name of the object instance:
                 obj =  WMComponent.AgentStatusWatcher.AgentStatusPoller()
                 findComponentName(obj) -> 'AgentStatusWatcher'
    """
    compName = ""
    try:
        objNamespace = obj.__module__
        logging.debug(f"Current obj namespace: {objNamespace}")
        if not getattr(obj, 'config', None) or not isinstance(obj.config, Configuration):
            logging.error(f"The obj: {obj} is not an instance of WMComponent.* modules.")
            return compName
        for compName in obj.config.listComponents_():
            compSection = obj.config.component_(compName)
            compNamespace = compSection.namespace.split('.', 2)
            compNamespace.pop()
            compNamespace = '.'.join(compNamespace)
            if objNamespace.startswith(compNamespace):
                return compName
        # If we are here then we have not found the component name
        logging.error(f"Could not find component name for: {obj}.")
        return None
    except Exception as ex:
        logging.error(f"Could not find component name for: {obj}. ERROR: {str(ex)}")

def moduleName(obj):
    """
    Returns the module name from which the current object is an instance by parsing
    its namespace.
    :param obj:  Any instances of WMComponent.*.*
    :return:     String - the module name.
    """
    return obj.__module__.split('.')[-1]

def tracePid(pid, interval=10):
    """
    A helper function designed to build ptrace based tests per process
    :param pid:      The process id to be traced
    :param interval: The interval in seconds for which the process  will be traced. (Default: 10sec.)
    :return:         Trace string|buffer|result (to be decided)
    """
    exitcode = 0
    debugger = PtraceDebugger()
    try:
        # Initially try to attach the process as a non traced one
        logging.info(f"Attempting to trace {pid} as unattached process")
        process = debugger.addProcess(pid, is_attached=False)
    except PtraceError:
        # in case of an error make an attempt to attach it as already traced one (supposing
        # a previous execution of the current function could not finish and release it.
        logging.info(f"Tracing {pid} as already attached process")
        process = debugger.addProcess(pid, is_attached=True)

    # Now start sampling:
    logging.info("Start system calls sampling.")

    process.syscall_options = FunctionCallOptions()
    endTime = time.time() + interval
    while time.time() < endTime:
        process.syscall()
        try:
            event = process.debugger.waitSyscall()
        except ProcessExit as event:
            if event.exitcode is not None:
                exitcode = event.exitcode
            continue
        except ProcessSignal as event:
            event.display()
            event.process.syscall(event.signum)
            exitcode = 128  + event.signum
            continue

        state = process.syscall_state
        syscall = state.event(process.syscall_options)
        if syscall and syscall.result is not None:
            logging.info(syscall.format())

    # Detach all tracers before returning:
    logging.info(f"Detaching from PID:{pid}")
    process.detach()
    return exitcode

ProcessTracer = namedtuple('ProcessTracer', ['traceFunc', 'args', 'kwArgs'])

def isComponentAlive(config, component=None, pid=None, trace=False, traceInterval=10):
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
    :param traceInterval:   The amount of time to wait during a ptrace based test before declaring it failed (Default 60 sec.)
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
        try:
            pidTree = getComponentThreads(config, component, quiet=True)
        except Exception as ex:
            logging.error(f"Could not rebuild the the process tree for component: {compName}. ERROR: {str(ex)}")
            pidTree = {}
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
        logging.error(f"You must provide PID or Component Name")
        return False

    if not pidTree:
        return False

    # Get the PID status,statistics and major resource usage
    # NOTE: If we've lost some threads or they have run as zombies we will miss them in the structure produced here.
    #       Those must have been caught and accounted for while building the pidTree
    pidInfo = processThreadsInfo(pidTree['Parent'])
    # logging.debug(pidInfo)
    # logging.debug(pidTree)

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
        tracers = {}
        for threadId in pidTree['RunningThreads']:
            logging.info(f"Creating process tracer for PID: {threadId}")
            tracers[threadId] = ProcessTracer(tracePid, [threadId], {'interval': traceInterval})

        # Now start designing all the tests per each thread in order to cover the three possible problematic
        # states as explained in the function docstring.
        logging.info("Start ptrace based tests")
        for threadId, tracer in tracers.items():
            # As a start, run a basic strace just for proof of concept:
            traceResult = tracer.traceFunc(*tracer.args, **tracer.kwArgs)
            if traceResult == 0:
                checkList.append(True)
            else:
                checkList.append(False)

    return all(checkList)
    # return tracers
