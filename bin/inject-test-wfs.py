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

import sys, os, shlex, json
from optparse import OptionParser
from subprocess import call
from time import sleep

def main():
    """
    Util to create and assign requests based on the templates available
    in WMCore repository.

    NOTE: it will inject and assign ALL templates under DMWM folder 
    """
    usage = "Usage: %prog -c Campaign -r requestString [-u url -t team -s site -a acqEra -p procStr -d]"
    parser = OptionParser(usage = usage)
    parser.add_option('-c', '--campaign', help = '(Mandatory) Campaign injected during the workflow creation', dest = 'camp')
    parser.add_option('-r', '--reqStr', help = '(Mandatory) Request string appended to the request name', dest = 'reqStr')
    parser.add_option('-m', '--mode', help = 'Uses either the DMWM or the Integration mode/templates', dest = 'mode')
    parser.add_option('-f', '--file', help = 'Specifies a single file for injection (extesion must be included)', dest = 'file')
    parser.add_option('-u', '--url', help = 'Url to inject the requests against', dest = 'url')
    parser.add_option('-t', '--team', help = 'Team name of the agent for assignment', dest = 'team')
    parser.add_option('-s', '--site', help = 'Site white list for assignment', dest = 'site')
    parser.add_option('-a', '--acqEra', help = 'AcquisitionEra for assignment', dest = 'acqEra')
    parser.add_option('-p', '--procStr', help = 'ProcessingString for assignment', dest = 'procStr')
    parser.add_option('-i', '--injectOnly', action = "store_true", help = 'Only injects requests but do not assign them',
                      dest = 'injOnly', default=False)
    parser.add_option('-d', '--dryRun', action = "store_true", help = 'Simulation mode only', dest = 'dryRun', default=False)
    (options, args) = parser.parse_args()
    if not options.camp and not options.reqStr:
        print "Ex.: python inject-test-wfs.py -c Agent105_Validation -r Robot_Alan -t testbed-vocms009 -a DMWM_Test -p TEST_Alan_LoL_v2"  
        parser.error('Campaign and requestString *must* be provided')
        sys.exit(1)
    mode = options.mode if options.mode else "DMWM"
    file = options.file if options.file else None
    url = options.url if options.url else "https://cmsweb-testbed.cern.ch"
    team = options.team if options.team else "testbed-vocms0230"
    site = options.site if options.site else ["T1_US_FNAL", "T2_CH_CERN"]
    acqEra = options.acqEra if options.acqEra else "DMWM_TEST"
    procStr = options.procStr if options.procStr else "TEST_Alan_LoL"

    if os.path.isdir('WMCore'):
        print "WMCore directory found. I'm not going to clone it again."
        print "You have 5 secs to abort this operation or live with that forever...\n"
        sleep(5)
    else:
        # Cloning WMCore repo
        command = ["git", "clone", "https://github.com/dmwm/WMCore.git"]
        try:
            retcode = call(command)
            if retcode == 0:
                print "WMCore repository successfully cloned!"
            else:
                print "Failed to clone WMCore ", -retcode
                sys.exit(1)
        except OSError as e:
            print "Execution failed:", e
            sys.exit(2)

    # Retrieve template names available and filter blacklisted
    os.chdir("WMCore/test/data/ReqMgr")
    wmcorePath = "requests/" + mode + "/"
    if file:
        if os.path.isfile(wmcorePath + file):
            templates = [ file ]
        else:
            print "File %s not found." % (wmcorePath + file)
            sys.exit(2)
    else:
        templates = os.listdir(wmcorePath)
    blacklist = ['StoreResults.json']
    templates = [ item for item in templates if item not in blacklist ]

    for filename in templates:
        strComm = "python reqmgr.py -u CMSWEB_TESTBED -f TEMPLATE.json -i "
        # create request setup
        name = filename.split('.json')[0]
        createRequest = {"createRequest": {}}
        createRequest['createRequest']['Campaign']      = options.camp
        createRequest['createRequest']['RequestString'] = name + '_' + options.reqStr

        if not options.injOnly:
            strComm += "-g "
            # merge template name and current procStr, to avoid dups
            tmpProcStr = filename.replace('.json','_') + procStr
            # assignment setup
            assignRequest = {"assignRequest": {}}
            assignRequest['assignRequest']['SiteWhitelist']    = site 
            assignRequest['assignRequest']['Team']             = team 
            assignRequest['assignRequest']['Dashboard']        = "integration" 
            assignRequest['assignRequest']['AcquisitionEra']   = acqEra 
            assignRequest['assignRequest']['ProcessingString'] = tmpProcStr 
  
            # assignment override for TaskChain
            if filename.startswith("TaskChain"):
                config = json.loads(open(wmcorePath+filename).read())
                assignRequest['assignRequest']['AcquisitionEra']   = config['assignRequest']['AcquisitionEra'] 
                assignRequest['assignRequest']['ProcessingString'] = config['assignRequest']['ProcessingString'] 
                for task,_ in assignRequest['assignRequest']['AcquisitionEra'].iteritems():
                    assignRequest['assignRequest']['AcquisitionEra'][task] = config['createRequest']['CMSSWVersion']
                for task,_ in assignRequest['assignRequest']['ProcessingString'].iteritems():
                    assignRequest['assignRequest']['ProcessingString'][task] = task + '_' + tmpProcStr
            createRequest.update(assignRequest)

        # Hack to go around single, double quotes and format 
        with open("/tmp/alan.json", "w") as outfile:
            json.dump(createRequest, outfile)
        configOver = json.loads(open("/tmp/alan.json").read())

        # Adapt parameters for final command
        strComm =  strComm.replace("CMSWEB_TESTBED", url)
        strComm =  strComm.replace("TEMPLATE.json", wmcorePath+filename)
        strComm +=  "--json='" + json.dumps(configOver) + "'"
        injectComm = shlex.split(strComm)

        if options.dryRun:
            print injectComm
            continue

        # Actually injects and assign request
        retcode = call(injectComm)
        if retcode == 0:
            print "%s request successfully created!" % filename
        else:
            print "%s request FAILED injection!" % filename

    print "\n%d templates should have been injected. Good job!" % len(templates)
       
if __name__ == '__main__':
    main()

