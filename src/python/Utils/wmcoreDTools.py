import os
import subprocess
import time
import json
import psutil

from Utils.Utilities import extractFromXML
from Utils.ProcFS import processStatus
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
                return
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
            return
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
            threadPids.append(entry["pid"])

    # Check if process is running
    processRunning = psutil.pid_exists(int(pid))

    # Check if threads are running
    runningThreads = []
    orphanThreads = []
    process = psutil.Process(int(pid))
    for thread in process.threads():
        if str(thread.id) == str(pid):
            continue  # skip pid thread
        if str(thread.id) in threadPids:
            runningThreads.append(thread.id)
        else:
            orphanThreads.append(thread.id)

    # Output result
    status = "running" if processRunning else "not-running"
    msg = f"Component:{component} {pid} {status} with threads {runningThreads}"
    if status == "running":
        if len(orphanThreads) > 0:
            status = "partially-running" if processRunning else "not-running"
            msg = f"Component:{component} {pid} {status} with threads {runningThreads}, lost {orphanThreads} threads"
    else:
        msg = f"Component:{component} {pid} {status}"
    print(msg)
    pidTree = {}
    pidTree['Parent'] = pid
    pidTree['RunningThreads'] = runningThreads
    pidTree['OrphanThreads'] = orphanThreads
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
