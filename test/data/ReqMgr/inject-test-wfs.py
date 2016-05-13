"""
Pre-requisites:
 1. a valid proxy in your X509_USER_PROXY variable
 2. wmagent env: /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh

Script used during cmsweb-testbed validation (or a new agent validation).
It will:
 1. clone the WMCore repository
 2. fetch all templates available in WMCore/test/data/ReqMgr/requests/DMWMW
 3. create a new request for each of them (based on the parameters given
 in command line)
 4. assign workflows during creation time (also based on the parameters
 provided in command line)
"""
from __future__ import print_function

import sys
import os
import shlex
import json
import argparse
from subprocess import call
from time import sleep


def main():
    """
    Util to create and assign requests based on the templates available
    in WMCore repository.

    NOTE: it will inject and assign ALL templates under DMWM or Integration folder
    """
    parser = argparse.ArgumentParser(description="Inject and/or assign requests in reqmgr/2")
    parser.add_argument('-c', '--campaign', help='Campaign injected during the workflow creation', required=True)
    parser.add_argument('-r', '--reqStr', help='Request string appended to the request name', required=True)
    parser.add_argument('-m', '--mode', help='Uses either the DMWM or the Integration mode/templates')
    parser.add_argument('-f', '--filename', help='Specifies a single file for injection (extesion must be included)')
    parser.add_argument('-u', '--url', help='Url to inject the requests against')
    parser.add_argument('-t', '--team', help='Team name of the agent for assignment')
    parser.add_argument('-s', '--site', help='Site white list for assignment')
    parser.add_argument('-a', '--acqEra', help='AcquisitionEra for assignment')
    parser.add_argument('-p', '--procStr', help='ProcessingString for assignment')
    parser.add_argument('-i', '--injectOnly', help='Only injects requests but do not assign them', action='store_true', default=False)
    parser.add_argument('-d', '--dryRun', help='Simulation mode only', action='store_true', default=False)
    parser.add_argument('-2', '--reqmgr2', help='Request Manager 2 injection', action='store_true', default=False)
    parser.set_defaults(mode='DMWM')
    parser.set_defaults(filename=None)
    parser.set_defaults(url='https://cmsweb-testbed.cern.ch')
    parser.set_defaults(team='testbed-vocms0230')
    parser.set_defaults(site=["T1_US_FNAL", "T2_CH_CERN"])
    parser.set_defaults(acqEra='DMWM_TEST')
    parser.set_defaults(procStr='TEST_Alan_DS3')
    args = parser.parse_args()

    cernTemplates = ['MonteCarlo_LHE.json', 'TaskChainZJetsLNu_LHE.json']

    if isinstance(args.site, basestring):
        args.site = args.site.split(',')

    if os.path.isdir('WMCore'):
        print("WMCore directory found. I'm not going to clone it again.")
        print("You have 5 secs to abort this operation or live with that forever...\n")
        sleep(5)
    else:
        # Cloning WMCore repo
        command = ["git", "clone", "https://github.com/dmwm/WMCore.git"]
        try:
            retcode = call(command)
            if retcode == 0:
                print("WMCore repository successfully cloned!")
            else:
                print("Failed to clone WMCore ", -retcode)
                sys.exit(1)
        except OSError as e:
            print("Execution failed:", e)
            sys.exit(2)

    # Retrieve template names available and filter blacklisted
    os.chdir("WMCore/test/data/ReqMgr")
    wmcorePath = "requests/" + args.mode + "/"
    if args.filename:
        if os.path.isfile(wmcorePath + args.filename):
            templates = [args.filename]
        else:
            print("File %s not found." % (wmcorePath + args.filename))
            sys.exit(3)
    else:
        templates = os.listdir(wmcorePath)
    blacklist = ['StoreResults.json']
    templates = [item for item in templates if item not in blacklist]

    if args.reqmgr2:
        reqMgrCommand = "reqmgr2.py"
    else:
        reqMgrCommand = "reqmgr.py"

    for fname in templates:
        strComm = "python %s -u CMSWEB_TESTBED -f TEMPLATE.json -i " % reqMgrCommand
        # create request setup
        name = fname.split('.json')[0]
        createRequest = {"createRequest": {}}
        createRequest['createRequest']['Campaign'] = args.campaign
        createRequest['createRequest']['RequestString'] = name + '_' + args.reqStr

        if not args.injectOnly:
            strComm += "-g "
            # merge template name and current procStr, to avoid dups
            tmpProcStr = fname.replace('.json', '_') + args.procStr
            # assignment setup
            assignRequest = {"assignRequest": {}}
            assignRequest['assignRequest']['SiteWhitelist'] = "T2_CH_CERN" if fname in cernTemplates else args.site
            assignRequest['assignRequest']['Team'] = args.team
            assignRequest['assignRequest']['Dashboard'] = "integration"
            assignRequest['assignRequest']['AcquisitionEra'] = args.acqEra
            assignRequest['assignRequest']['ProcessingString'] = tmpProcStr

            # assignment override for TaskChain
            if fname.startswith("TaskChain"):
                config = json.loads(open(wmcorePath + fname).read())
                assignRequest['assignRequest']['AcquisitionEra'] = config['assignRequest']['AcquisitionEra']
                assignRequest['assignRequest']['ProcessingString'] = config['assignRequest']['ProcessingString']
                for task, _ in assignRequest['assignRequest']['AcquisitionEra'].iteritems():
                    assignRequest['assignRequest']['AcquisitionEra'][task] = config['createRequest']['CMSSWVersion']
                for task, _ in assignRequest['assignRequest']['ProcessingString'].iteritems():
                    assignRequest['assignRequest']['ProcessingString'][task] = task + '_' + tmpProcStr
            createRequest.update(assignRequest)

        # Hack to go around single, double quotes and format
        with open("/tmp/alan.json", "w") as outfile:
            json.dump(createRequest, outfile)
        configOver = json.loads(open("/tmp/alan.json").read())

        # Adapt parameters for final command
        strComm = strComm.replace("CMSWEB_TESTBED", args.url)
        strComm = strComm.replace("TEMPLATE.json", wmcorePath + fname)
        strComm += "--json='" + json.dumps(configOver) + "'"
        injectComm = shlex.split(strComm)

        if args.dryRun:
            print(injectComm)
            continue

        # Actually injects and assign request
        retcode = call(injectComm)
        if retcode == 0:
            print("%s request successfully created!" % fname)
        else:
            print("%s request FAILED injection!" % fname)

    print("\n%d templates should have been injected. Good job!" % len(templates))


if __name__ == '__main__':
    main()
