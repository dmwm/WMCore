"""
Pre-requisites:
 1. a valid proxy in your X509_USER_PROXY variable
 2. wmagent env: /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh

Script used during cmsweb-testbed validation (or a new agent validation).
It will:
 1. clone the WMCore repository
 2. fetch all templates available in WMCore/test/data/ReqMgr/requests/*
 3. create a new request for each of them (based on the parameters given
 in the command line)
 4. assign workflows during creation time (also based on the parameters
 provided in command line)
"""
from __future__ import print_function

from builtins import range, str as newstr, bytes as newbytes
from future.utils import viewitems

import sys
import os
import pwd
import shlex
import json
import argparse
import logging
from subprocess import call
from time import sleep, time


def makeSiteWhitelist(jsonName, siteList):
    """
    Provided a template json file name and the site white list from
    the command line options; return the correct site white list based
    on some silly rules
    """
    if 'LHE_PFN' in jsonName:
        siteList = ["T1_US_FNAL"]
        print("Overwritting SiteWhitelist to: %s" % siteList)
    elif 'LHE' in jsonName or 'DQMHarvest' in jsonName:
        siteList = ["T2_CH_CERN"]
        print("Overwritting SiteWhitelist to: %s" % siteList)
    return siteList


def cloneRepo(logger):
    """
    Clone the WMCore repository, if needed
    """
    if os.path.isdir('WMCore'):
        logger.info("WMCore directory found. I'm not going to clone it again.")
        logger.info("You have 5 secs to abort this operation or live with that forever...\n")
        sleep(5)
    else:
        # Cloning WMCore repo
        command = ["git", "clone", "https://github.com/dmwm/WMCore.git"]
        try:
            retcode = call(command)
            if retcode == 0:
                logger.info("WMCore repository successfully cloned!")
            else:
                logger.error("Failed to clone WMCore. Error: %s ", retcode)
                sys.exit(1)
        except OSError as e:
            logger.error("Exception while cloning WMCore. Details: %s", str(e))
            sys.exit(2)


def parseArgs():
    """
    Well, parse the arguments passed in the command line :)
    """
    parser = argparse.ArgumentParser(description="Inject and/or assign requests in reqmgr/2")

    parser.add_argument('-c', '--campaign', required=True,
                        help='Campaign injected during the workflow creation')
    parser.add_argument('-r', '--reqStr', required=True,
                        help='Request string appended to the request name')
    parser.add_argument('-m', '--mode', default='DMWM',
                    help='Base directory for the templates. Options are: DMWM, Integration or Static')
    parser.add_argument('-f', '--filename',
                        help='Specifies a single file for injection (extesion must be included)')
    parser.add_argument('-u', '--url', default='https://cmsweb-testbed.cern.ch',
                        help='Url to inject the requests against')
    parser.add_argument('-t', '--team', default='testbed-vocms0230',
                        help='Team name of the agent for assignment')
    parser.add_argument('-s', '--site', default=["T1_US_FNAL", "T2_CH_CERN"],
                        help='Site white list for assignment')
    parser.add_argument('-a', '--acqEra', default='DMWM_TEST',
                        help='AcquisitionEra for assignment')
    parser.add_argument('-p', '--procStr', default='TEST_Alan_DS3',
                        help='ProcessingString for assignment')
    parser.add_argument('-v', '--procVer', default=11,
                        help='ProcessingVersion for assignment')
    parser.add_argument('-i', '--injectOnly', action='store_true', default=False,
                        help='Only injects requests but do not assign them')
    parser.add_argument('--dryRun', action='store_true', default=False,
                        help='Simulation mode only')
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Set logging to debug mode.')
    args = parser.parse_args()

    # sites argument could be "T1_US_FNAL,T2_CH_CERN" ...
    if isinstance(args.site, (newstr, newbytes)):
        args.site = args.site.split(',')

    return args


def handleAssignment(args, fname, jsonData):
    """
    Tweak the assignment parameters, if needed.
    We only overwrite assignment parameters that are already present
    in the json template
    """
    assignRequest = {}
    assignRequest.setdefault('Team', args.team)
    assignRequest.setdefault('Dashboard', "integration")
    assignRequest.setdefault('SiteWhitelist', makeSiteWhitelist(fname, args.site))
    # merge template name and current procStr, to avoid dups
    tmpProcStr = fname.replace('.json', '_') + args.procStr
    if 'AcquisitionEra' in jsonData['assignRequest']:
        assignRequest.setdefault('AcquisitionEra', args.acqEra)
    if 'ProcessingString' in jsonData['assignRequest']:
        assignRequest.setdefault('ProcessingString', tmpProcStr)
    if 'ProcessingVersion' in jsonData['assignRequest']:
        assignRequest.setdefault('ProcessingVersion', args.procVer)

    # dict args for TaskChain and StepChain
    if jsonData['createRequest']['RequestType'] in ["TaskChain", "StepChain"]:
        requestType = jsonData['createRequest']['RequestType']
        joker = requestType.split("Chain")[0]
        assignDict = jsonData['assignRequest']

        # reuse values provided in the request schema at creation level
        if 'AcquisitionEra' in assignDict and isinstance(assignDict['AcquisitionEra'], dict):
            assignRequest['AcquisitionEra'] = assignDict['AcquisitionEra']
            createDict = jsonData['createRequest']
            for i in range(1, createDict[requestType] + 1):
                innerDictName = "%s%d" % (joker, i)  # Task1, Task2, Step1, Step2 ...
                innerDict = createDict[innerDictName]
                # if there is no Task/Step level value, get it from the main dict
                assignRequest['AcquisitionEra'][innerDict["%sName" % joker]] = innerDict.get('AcquisitionEra', createDict['AcquisitionEra'])

        # always overwrite it as provided in the command line and task/step name
        if 'ProcessingString' in assignDict and isinstance(assignDict['ProcessingString'], dict):
            assignRequest['ProcessingString'] = assignDict['ProcessingString']
            for task, _ in viewitems(assignRequest['ProcessingString']):
                assignRequest['ProcessingString'][task] = task + '_' + tmpProcStr

        # also reuse values as provided in the request schema at creation level
        if 'ProcessingVersion' in assignDict and isinstance(assignDict['ProcessingVersion'], dict):
            assignRequest['ProcessingVersion'] = assignDict['ProcessingVersion']
            createDict = jsonData['createRequest']
            for i in range(1, createDict[requestType] + 1):
                innerDictName = "%s%d" % (joker, i)  # Task1, Task2, Step1, Step2 ...
                innerDict = createDict[innerDictName]
                # if there is no Task/Step level value, get it from the main dict
                assignRequest['ProcessingVersion'][innerDict["%sName" % joker]] = innerDict.get('ProcessingVersion', createDict['ProcessingVersion'])

    jsonData['assignRequest'].update(assignRequest)
    return

def loggerSetup(logLevel=logging.INFO):
    """
    Return a logger which writes everything to stdout.
    """
    logger = logging.getLogger(__name__)
    outHandler = logging.StreamHandler(sys.stdout)
    outHandler.setFormatter(logging.Formatter("%(asctime)s:%(levelname)s:%(module)s: %(message)s"))
    outHandler.setLevel(logLevel)
    logger.addHandler(outHandler)
    logger.setLevel(logLevel)
    return logger

def main():
    """
    Util to create and assign requests based on the templates available
    in WMCore repository.

    NOTE: it will inject and assign ALL templates under DMWM or Integration folder
    """
    args = parseArgs()
    if args.debug:
        logger = loggerSetup(logging.DEBUG)
    else:
        logger = loggerSetup()

    cloneRepo(logger)

    os.chdir("WMCore/test/data/ReqMgr")
    wmcorePath = "requests/" + args.mode + "/"
    if args.filename:
        # then only specified template will be injected
        if os.path.isfile(wmcorePath + args.filename):
            templates = [args.filename]
        else:
            logger.info("File %s not found.", wmcorePath + args.filename)
            templates = []
    else:
        templates = os.listdir(wmcorePath)

    # Filter out templates not allowed to be injected
    disallowedList = ['ReReco_badBlocks.json', 'TaskChain_InclParents.json',
                      'StepChain_InclParents.json', 'SC_Straight.json',
                      'StoreResults.json', 'Resub_MonteCarlo_eff.json', 'Resub_TaskChain_Multicore.json']
    logger.info("Skipping injection for these templates: %s\n", disallowedList)
    templates = [item for item in templates if item not in disallowedList]
    if not templates:
        logging.info("There are no templates to be injected.")
        sys.exit(3)

    startT = time()
    reqMgrCommand = "reqmgr2.py"

    # Temporary place to write the tweaked templates
    tmpFile = '/tmp/%s.json' % pwd.getpwuid(os.getuid()).pw_name
    wfCounter = 0

    # figure out which python version it is
    pythonCmd = "python3" if sys.version_info[0] == 3 else "python"

    for fname in templates:
        logger.info("Processing template: %s", fname)
        strComand = "%s %s -u %s -f %s -i " % (pythonCmd, reqMgrCommand, args.url, tmpFile)

        # read the original json template
        with open(wmcorePath + fname) as fo:
            jsonData = json.load(fo)

        # tweak the create dict
        createRequest = {}
        createRequest['RequestString'] = fname.split('.json')[0] + '_' + args.reqStr
        if 'Campaign' in jsonData['createRequest']:
            createRequest['Campaign'] = args.campaign
        jsonData['createRequest'].update(createRequest)

        # apply assignment overrides
        if not args.injectOnly:
            strComand += "-g "
            handleAssignment(args, fname, jsonData)

        # Dump the modified json in a temp file and use just it
        with open(tmpFile, "w") as outfile:
            json.dump(jsonData, outfile)

        if args.dryRun:
            logger.info("dry-run command: %s", strComand)
            continue

        # Inject and/or assign the request, for real
        injectComand = shlex.split(strComand)
        retcode = call(injectComand)
        if retcode == 0:
            logger.info("%s request successfully created!", fname)
            wfCounter += 1
        else:
            logger.info("%s request FAILED injection!", fname)

    totalT = time() - startT

    logger.info("\nInjected %d workflows out of %d templates in %.2f secs. Good job!",
                 wfCounter, len(templates), totalT)


if __name__ == '__main__':
    main()
