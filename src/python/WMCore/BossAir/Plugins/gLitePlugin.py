#!/usr/bin/env python

"""
gLite Plugin


"""

import logging
import subprocess
import threading
import multiprocessing, Queue
import socket
import tempfile
import os
import re
import time
import types
import stat

from WMCore.Credential.Proxy import Proxy
from WMCore.FwkJobReport.Report        import Report
from WMCore.BossAir.Plugins.BasePlugin import BasePlugin, BossAirPluginException
from WMCore.DAOFactory import DAOFactory
import WMCore.WMInit
from copy import deepcopy

def processWorker(input, results):
    """
    _outputWorker_

    Runs a subprocessed command.

    This takes whatever you send it (a single ID)
    executes the command
    and then returns the stdout result

    I planned this to do a glite-job-output command
    in massive parallel, possibly using the bulkID
    instead of the gridID.  Either way, all you have
    to change is the command here, and what is send in
    in the complete() function.
    """

    # Get this started
    t1 = None
    jsout = None
    myJSONDecoder = BossAirJsonDecoder()

    while True:
        type = ''
        workid = None
        try:
            #print "Waiting for new work..."
            workid, work, type = input.get()
            #print " -> New work %s " % str(workid)
            t1 = time.time()
        except (EOFError, IOError):
            crashMessage = "Hit EOF/IO in getting new work\n"
            crashMessage += "Assuming this is a graceful break attempt.\n"
            print crashMessage
            break

        if work == 'STOP':
            #print "received stop message"
            break

        command = work
        #print 'Staring %s subprocess for %s ' % (str(t1), command )
        pipe = subprocess.Popen(command, stdout = subprocess.PIPE,
                                 stderr = subprocess.PIPE, shell = True)
        #print 'Waiting %s ' % str(time.time())
        stdout, stderr = pipe.communicate()
        #print 'Error:  ', stderr
        #print 'Output: ', stdout

        try:
            ## TODO: make this dynamic with a dictionary
            if type == 'submit':
                jsout = myJSONDecoder.decodeSubmit( stdout )
            elif type == 'status':
                jsout = stdout
                #myJSONDecoder.decodeStatus( stdout )
            elif type == 'output':
                jsout = stdout
            else:
                jsout = stdout
        except ValueError, val:
            print val, stdout, stderr
            jsout = stdout
        except Exception, err:
            jsout = str(work) + '\n' + str(err)
            stderr = stdout + '\n' + stderr

        #print "Returning work %i " % workid

        results.put( {
                       'workid': workid,
                       'jsout' : jsout,
                       'stderr': stderr,
                       'exit': pipe.returncode
                     })
        #print '%i TOOK: %s' % (workid, str(time.time() - t1))

    #print "Returning"

    return 0


class gLitePlugin(BasePlugin):
    """
    Prototype for gLite Plugin

    Written so I can put the multiprocessing pool somewhere
    """

    defaultjdl = {
                  'myproxyhost': 'myproxy.cern.ch',
                  'nodesarch'  : 'VO-cms-slc5_ia32_gcc434',
                  'vo'         : 'cms',
                  'gridftphost': socket.getfqdn(),
                  'cestatus'   : 'Production',
                  'sbtransfer' : 'gsiftp',
                  'service'    : 'https://gswms01.cern.ch:7443/glite_wms_wmproxy_server'
                 }


    def __init__(self, config):

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger = myThread.logger,
                                dbinterface = myThread.dbi)

        self.locationAction = daoFactory(classname = "Locations.GetSiteSE")
        self.locationDict = {}

        self.delegationid = str(type(self).__name__)

        self.config = config

        # These are just the MANDATORY states
        self.states = [ 'New',
                        'Timeout',
                        'Submitted',
                        'Waiting',
                        'Ready',
                        'Scheduled',
                        'Running',
                        'Done(failed)',
                        'Done',
                        'Aborted',
                        'Cleared',
                        'Cancelled by user',
                        'Cancelled']


        self.defaultDelegation = {
                                  'vo': self.defaultjdl['vo'],
                                  'logger': myThread.logger,
                                  'myProxySvr': self.defaultjdl['myproxyhost'],
                                  'proxyValidity' : '192:00',
                                  'min_time_left' : 36000
                                 }

        self.singleproxy  = getattr(self.config.BossAir, 'manualProxyPath', None)

        if self.singleproxy is None:
            if getattr ( self.config.Agent, 'serverDN' , None ) is None:
                msg = "Error: serverDN parameter required and not provided " + \
                      "in the configuration"
                raise BossAirPluginException( msg )
            if getattr ( self.config.BossAir, 'credentialDir', None) is None:
                msg = "Error: credentialDir parameter required and " + \
                      "not provided in the configuration"
                raise BossAirPluginException( msg )
            else:
                if not os.path.exists(self.config.BossAir.credentialDir):
                    logging.debug("credentialDir not found: creating it...")
                    try:
                        os.mkdir(self.config.BossAir.credentialDir)
                    except Exception, ex:
                        msg = "Error: problem when creating credentialDir " + \
                              "directory - '%s'" % str(ex)
                        raise BossAirPluginException( msg )
                elif not os.path.isdir(self.config.BossAir.credentialDir):
                    msg = "Error: credentialDir '%s' is not a directory" \
                           % str(self.config.BossAir.credentialDir)
                    raise BossAirPluginException( msg )
            self.defaultDelegation['credServerPath'] = self.config.BossAir.credentialDir
            self.defaultDelegation['serverDN'] = self.config.Agent.serverDN
        else:
            logging.debug("Using manually provided proxy '%s' " % self.singleproxy)


        # These are the pool settings.
        self.nProcess       = getattr(self.config.BossAir, 'gLiteProcesses', 10)
        self.collectionsize = getattr(self.config.BossAir, 'gLiteCollectionSize', 200)
        self.trackmaxsize   = getattr(self.config.BossAir, 'gLiteMaxTrackSize', 200)

        self.pool     = []

        self.submitFile   = getattr(self.config.JobSubmitter, 'submitScript', None)
        self.submitDir    = getattr(self.config.JobSubmitter, 'submitDir', '/tmp/')
        self.gliteConfig  = getattr(self.config.BossAir, 'gLiteConf', None)
        self.defaultjdl['service'] = getattr(self.config.BossAir, 'gliteWMS', None)

        self.basetimeout  = getattr(self.config.JobSubmitter, 'getTimeout', 300 )

        self.defaultjdl['myproxyhost'] = self.defaultDelegation['myProxySvr'] = getattr(self.config.BossAir, 'myproxyhost', self.defaultDelegation['myProxySvr'] )

        self.manualenvprefix = getattr(self.config.BossAir, 'gLitePrefixEnv', '')

        self.setupScript = getattr(self.config.BossAir, 'UISetupScript', None)
        if self.setupScript is None:
            msg = "Setup script not provided in the configuration: need to " + \
                  "specify the 'UISetupScript' complete path."
            raise BossAirPluginException( msg )
        elif not os.path.exists( self.setupScript ):
            msg = "Setup script not found: check if '%s' is really there." \
                   % self.setupScript
            raise BossAirPluginException( msg )
        elif not self.checkUI():
            msg = "gLite environment has not been set properly through '%s'." \
                   % self.setupScript
            raise BossAirPluginException( msg )
        self.defaultDelegation['uisource'] = self.setupScript

        self.debugOutput = getattr(self.config.BossAir, 'gLiteCacheOutput', False)

        wmcoreBasedir = WMCore.WMInit.getWMBASE()

        if os.path.exists(os.path.join(wmcoreBasedir, 'src/python/WMCore/WMRuntime/Unpacker.py')):
            self.unpacker = os.path.join(wmcoreBasedir, 'src/python/WMCore/WMRuntime/Unpacker.py')
        else:
            self.unpacker = os.path.join(wmcoreBasedir, 'WMCore/WMRuntime/Unpacker.py')

        return


    @staticmethod
    def stateMap():
        """
        _stateMap_

        For a given name, return a global state
        """

        stateDict = {'New': 'Pending',
                     'Timeout': 'Error',
                     'Submitted': 'Pending',
                     'Waiting': 'Pending',
                     'Ready': 'Pending',
                     'Scheduled': 'Pending',
                     'Running': 'Running',
                     'Done(failed)': 'Running',
                     'Done': 'Running',
                     'Aborted': 'Error',
                     'Cleared': 'Complete',
                     'Cancelled by user': 'Complete',
                     'Cancelled': 'Error'
                    }

        return stateDict


    def fakeReport(self, title, mesg, code, job):
        """
        _fakeReport_

        Prepares a report to be passed to the JobAccountant
        """
        print "MATTIA\njob: \n'"+ str(job) + "'\n"
        if job.get('cache_dir', None) == None or job.get('retry_count', None) == None:
            return
        if not os.path.isdir(job['cache_dir']):
            logging.error("Could not write a kill FWJR due to non-existant cache_dir for job %i\n" % job['id'])
            logging.debug("cache_dir: %s\n" % job['cache_dir'])
            return
        reportName = os.path.join(job['cache_dir'], 'Report.%i.pkl' % job['retry_count'])
        if os.path.exists(reportName) and os.path.getsize(reportName) > 0:
            logging.debug("Not writing report due to pre-existing report for job %i.\n" % job['id'])
            logging.debug("ReportPath: %s\n" % reportName)
            return
        else:
            errorReport = Report()
            errorReport.addError(title, code, title, mesg)
            errorReport.save(filename = reportName)
        return


    def close(self, input, result):
        """
        _close_

        Kill all connections and terminate
        """
        logging.debug("Ready to close all %i started processes " \
                        % len(self.pool) )
        for x in self.pool:
            try:
                logging.debug("Shutting down %s " % str(x))
                input.put( ('-1', 'STOP', 'control') )
            except Exception, ex:
                msg =  "Hit some exception in deletion\n"
                msg += str(ex)
                logging.error(msg)

        for proc in self.pool:
            proc.terminate()

        self.pool = []
        logging.debug('Slave stopped!')
        return


    def start( self, input, result ):
        """
        _start_

        Start the mulitp.
        """

        if len(self.pool) == 0:
            # Starting things up
            for x in range(self.nProcess):
                logging.debug("Starting process %i" %x)
                p = multiprocessing.Process(target = processWorker,
                                            args = (input, result))
                p.start()
                self.pool.append(p)


    def submit(self, jobs, info = None):
        """
        _submit_

        Submits jobs
        """

        input  = multiprocessing.Queue()
        result = multiprocessing.Queue()
        self.start( input, result )

        if not os.path.exists(self.submitDir):
            os.makedirs(self.submitDir)

        successfulJobs = []
        failedJobs     = []

        if len(jobs) == 0:
            # Then we have nothing to do
            return {}

        # Job sorting by sandbox.

        workqueued = {}
        submitDict = {}

        retrievedproxy = {}

        for job in jobs:
            sandbox = job['sandbox']
            if not sandbox in submitDict.keys():
                submitDict[sandbox] = {}
            if job['location'] not in submitDict[sandbox]:
                if job['location']:
                    submitDict[sandbox][job['location']] = []
                else:
                    submitDict[sandbox][''] = []
            submitDict[sandbox][job['location']].append(job)

        tounlink = []

        # Now submit the bastards
        currentwork = len(workqueued)
        for sandbox in submitDict.keys():
            logging.debug("   Handling '%s'" % str(sandbox))
            siteDict = submitDict.get(sandbox, {})
            for site in siteDict.keys():
                logging.debug("   Handling '%s'" % str(site))
                jobList = siteDict.get(site, [])
                while len(jobList) > 0:
                    ## getting the sandbox jobs and splitting  by collection size
                    jobList = siteDict.get(site, [])
                    command = "glite-wms-job-submit --json "
                    if self.debugOutput:
                        unused, uniquename = tempfile.mkstemp(suffix = '.log', prefix = 'glite.submit.%s.' % time.strftime("%Y%m%d%H%M%S", time.localtime()),
                                                              dir = self.submitDir )
                        command += '--logfile %s ' % os.path.join(self.submitDir, uniquename)

                    jobsReady = jobList[:self.collectionsize]
                    jobList   = jobList[self.collectionsize:]

                    ## retrieve user proxy and set the path
                    ownersandbox      = jobsReady[0]['userdn']
                    valid, ownerproxy = (False, None)
                    exportproxy       = 'echo $X509_USER_PROXY'
                    if ownersandbox in retrievedproxy:
                        valid      = True
                        ownerproxy = retrievedproxy[ownersandbox]
                    else:
                        valid, ownerproxy = self.validateProxy( ownersandbox )

                    if valid:
                        retrievedproxy[ownersandbox] = ownerproxy
                        exportproxy = "export X509_USER_PROXY=%s" % ownerproxy
                    else:
                        msg = "Problem retrieving user proxy, or user proxy " + \
                              "expired '%s'" % ownersandbox
                        logging.error( msg )
                        failedJobs.extend( jobsReady )
                        for job in jobsReady:
                            self.fakeReport("SubmissionFailure", msg, -1, job)
                        continue

                    ## getting the job destinations
                    dest      = []
                    logging.debug("Getting location from %s" % str(jobsReady) )
                    try:
                        dest = self.getDestinations( [], jobsReady[0]['location'] )
                    except Exception, ex:
                        import traceback
                        msg = str(traceback.format_exc())
                        msg += str(ex)
                        logging.error("Exception in site selection \n %s " % msg)
                        return {'NoResult': [0]}

                    if len(dest) == 0:
                        logging.error('No site selected, trying to submit without')

                    jdlReady  = self.makeJdl( jobList = jobsReady, dest = dest, info = info )
                    if not jdlReady or len(jdlReady) == 0:
                        # Then we got nothing
                        logging.error("No JDL file made!")
                        return {'NoResult': [0]}

                    # write a jdl into tmpFile
                    tmp, fname = tempfile.mkstemp(suffix = '.jdl', prefix = 'glite',
                                                  dir = self.submitDir )
                    tmpFile = os.fdopen(tmp, "w")
                    tmpFile.write( jdlReady )
                    tmpFile.close()
                    tounlink.append( fname )

                    # delegate proxy
                    if self.delegationid != "" :
                        command += " -d %s " % self.delegationid
                        logging.debug("Delegating proxy...")
                        self.delegateProxy(self.defaultjdl['service'], exportproxy)
                    else :
                        command += " -a "

                    if self.gliteConfig is not None:
                        command += " -c " + self.gliteConfig
                    elif self.defaultjdl['service'] is not None:
                        # eventual note: the '-e' override the ...
                        command += ' -e ' + self.defaultjdl['service']
                    #command += ' -e https://gswms01.cern.ch:7443/glite_wms_wmproxy_server '
                    #command += ' -e https://wms-cms-analysis.grid.cnaf.infn.it:7443/glite_wms_wmproxy_server '

                    command += ' ' + fname

                    # Now submit them
                    logging.debug("About to submit %i jobs" % len(jobsReady) )
                    workqueued[currentwork] = jobsReady
                    completecmd = 'source %s && export LD_LIBRARY_PATH=%s:$LD_LIBRARY_PATH && %s && %s' % (self.setupScript, self.manualenvprefix, exportproxy, command)
                    input.put((currentwork, completecmd, 'submit'))
                    currentwork += 1

        logging.debug("Waiting for %i works to finish.." % len(workqueued))
        for n in xrange(len(workqueued)):
            logging.debug("Waiting for work number %i to finish.." % n)
            res = None
            try:
                res = result.get(block = True, timeout = self.basetimeout)
            except Queue.Empty:
                logging.error("Timeout retrieving result %i out of %i" % (n, xrange(len(workqueued))) )
                continue
            jsout  = res['jsout']
            error  = res['stderr']
            exit   = res['exit']
            workid = res['workid']
            jobsub = workqueued[workid]
            logging.debug("Retrieving id %i " %workid)

            if not error == '':
                logging.error("Printing out command stderr")
                logging.error(error)
            if not exit == 0:
                logging.error("Exit code %s" % str(exit) )

            # {
            #  result: success
            #  parent: https://XYZ
            #  endpoint: https://X
            #  children: {
            #             NodeName_1_0: https://ABC
            #             NodeName_2_0: https://DEF
            #             NodeName_3_0: https://GHI
            #            }
            # }
            if jsout is not None:
                parent   = ''
                endpoint = ''
                if 'result' in jsout:
                    if jsout['result'] != 'success':
                        failedJobs.extend(jobsub)
                        for job in jobsub:
                            self.fakeReport("SubmissionFailure", str(jsout), -1, job)
                        continue
                else:
                    failedJobs.extend(jobsub)
                    for job in jobsub:
                        self.fakeReport("SubmissionFailure", str(jsout), -1, job)
                    continue
                if jsout.has_key('parent'):
                    parent = jsout['parent']
                else:
                    failedJobs.extend(jobsub)
                    for job in jobsub:
                        self.fakeReport("SubmissionFailure", str(jsout), -1, job)
                    continue
                if jsout.has_key('endpoint'):
                    endpoint = jsout['endpoint']
                else:
                    failedJobs.extend(jobsub)
                    for job in jobsub:
                        self.fakeReport("SubmissionFailure", str(jsout), -1, job)
                    continue
                if jsout.has_key('children'):
                    jobnames = jsout['children'].keys()
                    for jj in jobsub:
                        jobnamejdl = 'Job_%i_%s' % (jj['id'], jj['retry_count'])
                        if jobnamejdl in jobnames:
                            job = jj
                            job['bulkid']       = parent
                            job['gridid']       = jsout['children'][jobnamejdl]
                            job['sched_stattus'] = 'Submitted'
                            successfulJobs.append(job)
                        else:
                            failedJobs.append(jj)
                            self.fakeReport("SubmissionFailure", str(jsout), -1, jj)
                else:
                    failedJobs.extend(jobsub)
                    for job in jobsub:
                        self.fakeReport("SubmissionFailure", str(jsout), -1, job)
                    continue

        logging.debug("Submission completed and processed at time %s " \
                       % str(time.time()) )

        # unlinking all the temporary files used for jdl
        if not self.debugOutput:
            for tempf in tounlink:
                os.unlink( tempf )

        # need to shut down the subprocesses
        logging.debug("About to close the subprocesses...")
        self.close(input, result)

        # We must return a list of jobs successfully submitted,
        # and a list of jobs failed
        logging.debug("Returning jobs..")
        return successfulJobs, failedJobs


    def track(self, jobs):
        """
        _track_

        Tracks jobs
        Returns three lists:
        1) the running jobs
        2) the jobs that need to be updated in the DB
        3) the complete jobs
        """

        logging.debug("Staring gLite track method..")
        # Retrieve the location of GLiteStatusQuery.py ...
        cmdquerypath = ''
        queryfilename    = 'GLiteStatusQuery.py'
        wmcoreBasedir = WMCore.WMInit.getWMBASE()


        if wmcoreBasedir  :
            if os.path.exists(os.path.join(wmcoreBasedir, 'src/python/WMCore/BossAir/Plugins/', queryfilename)):
                cmdquerypath = os.path.join(wmcoreBasedir, 'src/python/WMCore/BossAir/Plugins/', queryfilename)
            else:
                cmdquerypath = os.path.join(wmcoreBasedir, 'WMCore/BossAir/Plugins/', queryfilename)

            if not os.path.exists( cmdquerypath ):
                msg = 'Impossible to locate %s' % cmdquerypath
                raise BossAirPluginException( msg )
        else :
            # Impossible to locate GLiteQueryStatus.py ...
            msg = 'Impossible to locate %s, WMBASE = %s ' \
                   % (queryfilename, str(wmcoreBasedir))
            raise BossAirPluginException( msg )

        logging.debug("Using %s to check the status " % cmdquerypath)


        changeList   = []
        completeList = []
        runningList  = []

        workqueued  = {}
        currentwork = len(workqueued)

        # Preparing job ids grouped by user DN
        dnjobs = {}

        for jj in jobs:
            if dnjobs.has_key( jj['userdn'] ):
                dnjobs[ jj['userdn'] ].append(jj)
            else:
                dnjobs[ jj['userdn'] ] = [ jj ]

        ## Start up processes
        input  = multiprocessing.Queue()
        result = multiprocessing.Queue()
        self.start(input, result)

        #creates chunks of work per multiprocesses
        # in principle each multiprocess can get one list of job ids associated
        # to a user dn in some cases a thread can get too many or few jobs
        # also splitting the jobs of a user may help (each chunk of work has at
        # max N jobs)

        for user in dnjobs.keys():

            valid, ownerproxy = self.validateProxy( user )
            if not valid:
                logging.error("Problem getting proxy for user '%s'" % str(user))
                continue
                ## TODO evaluate if jobs need to be set as failed
            exportproxy = "export X509_USER_PROXY=%s" % ownerproxy

            jobList = dnjobs.get(user, [])
            while len(jobList) > 0:
                jobIds = []
                ## TODO: try to avoid iterating over all the jobs
                for jj in jobList:
                    jobIds.append( jj['gridid'] )
                formattedJobIds = ','.join(jobIds)
                ## TODO: understand how to solve the python 2.4
                command = 'python2.4 %s --jobId=%s' % (cmdquerypath, \
                                                       formattedJobIds)
                jobsReady = jobList[:self.trackmaxsize]
                jobList   = jobList[self.trackmaxsize:]
                logging.debug("Status check for %i jobs" %len(jobsReady))
                workqueued[currentwork] = jobsReady
                completecmd = 'source %s && export LD_LIBRARY_PATH=%s:$LD_LIBRARY_PATH && %s && %s' % (self.setupScript, self.manualenvprefix, exportproxy, command)
                input.put((currentwork, completecmd, 'status'))
                currentwork += 1

        # Now we should have sent all jobs to be submitted
        # Going to do the rest of it now
        logging.debug("Waiting for %i works to finish..." % len(workqueued))
        for n in xrange(len(workqueued)):
            logging.debug("Waiting for work number %i to finish.." % n)
            res = None
            try:
                res = result.get(block = True, timeout = self.basetimeout)
            except Queue.Empty:
                logging.error("Timeout retrieving result %i out of %i" % (n, xrange(len(workqueued))) )
                continue
            jsout  = res['jsout']
            error  = res['stderr']
            exit   = res['exit']
            workid = res['workid']
            # Check error
            if exit != 0:
                msg = 'Error executing %s:\n\texit code: %i\n' %(cmdquerypath, exit)
                msg += '\tstderr: %s\n\tjson: %s'  % (error, str(jsout.strip()))
                logging.error( msg )
                continue
            else:
                # parse JSON output
                out = None
                try:
                    out = json.loads(jsout)
                except ValueError, va:
                    msg = 'Error parsing JSON:\n\terror: %s\n\t:exception: %s' \
                           % (error, str(va))
                    raise BossAirPluginException( msg )
                ## out example
                ##  {'https://cert-rb-01.cnaf.infn.it:9000/fucrLsxVXal9mzE3UaFBFg':
                ##           {'status': 'K'
                ##            'scheduledAtSite': None,
                ##            'service': 'https://wms020.cnaf.infn.it:7443/glite_wms_wmproxy_server',
                ##            'statusScheduler': 'Cancelled',
                ##            'destination': 'grid-ce-01.ba.infn.it:2119/jobmanager-lcgpbs-cms',
                ##            'statusReason': '',
                ##            'schedulerParentId': None,
                ##            'schedulerId': 'https://cert-rb-01.cnaf.infn.it:9000/fucrLsxVXal9mzE3UaFBFg',
                ##            'lbTimestamp': None,
                ##            'startTime': None,
                ##            'stopTime': None}
                ##  }
                for jj in jobs:
                    status = None
                    if jj['gridid'] in out.keys():
                        logging.debug("Job scheduler id: %s " % (jj['gridid']))
                        jobStatus   = out[jj['gridid']]
                        status      = jobStatus['statusScheduler']
                        destination = jobStatus['destination']

                        if 'lbTimestamp' in jobStatus:
                            jj['status_time'] = jobStatus['lbTimestamp']
                        else:
                            ## we do not want jobs without timestamp to abort...
                            ## (probably), so I just set current time and print as an error for the operator
                            logging.error("Impossible to retrieve timestamp from status: job %s in status %s! Setting current time." %(jj['gridid'], status))
                            jj['status_time'] = time.time()

                        # Get the global state
                        jj['globalState'] = gLitePlugin.stateMap()[status]


                        if status != jj['status']:
                            # Then the status has changed
                            jj['status']      = status
                            changeList.append(jj)

                        if status not in ['Done', 'Aborted', 'Cleared',
                                          'Cancelled by user', 'Cancelled']:
                            runningList.append(jj)
                        else:
                            completeList.append(jj)
                    #else:
                    #    logging.error('Job %s not returned!' % jj['gridid'])

        ## Shut down processes
        logging.debug("About to close the subprocesses...")
        self.close(input, result)

        return runningList, changeList, completeList


    def getoutput(self, jobs):
        """
        _getoutput_

        1) get finished jobs
        2) retrieve job output
        3) return done id + failed to process id + aborted jobs lists
        """

        logging.debug("Staring gLite getoutput method..")

        command   = "glite-wms-job-output --json --noint"
        outdiropt = "--dir"

        workqueued  = {}
        currentwork = len(workqueued)

        completedJobs = []
        failedJobs    = []
        abortedJobs   = []

        retrievedproxy = {}

        ## Start up processes
        input  = multiprocessing.Queue()
        result = multiprocessing.Queue()
        self.start(input, result)

        #creates chunks of work per multi-processes
        # TODO: evaluate if passing just one job per work is too much overhead

        for jj in jobs:
            ownersandbox      = jj['userdn']
            valid, ownerproxy = (False, None)
            exportproxy       = 'echo $X509_USER_PROXY'
            if ownersandbox in retrievedproxy:
                valid      = True
                ownerproxy = retrievedproxy[ownersandbox]
            else:
                valid, ownerproxy = self.validateProxy( ownersandbox )

            if valid:
                retrievedproxy[ownersandbox] = ownerproxy
                exportproxy = "export X509_USER_PROXY=%s" % ownerproxy
            else:
                msg = "Problem retrieving user proxy, or user proxy " + \
                      "expired '%s'" % ownersandbox
                logging.error( msg )
                failedJobs.append( jj )
                self.fakeReport("GetOutputFailure", msg, -1, jj)
                continue

            logging.info("Processing job %s " %str(jj['status']))

            if jj['status'] not in ['Done']:
                if jj['status'] in ['Aborted']:
                    abortedJobs.append( jj )
                continue

            cmd = '%s %s %s %s' \
                   % (command, outdiropt, jj['cache_dir'], jj['gridid'])
            logging.debug("Enqueuing getoutput for job %i" % jj['jobid'] )
            workqueued[currentwork] = jj['jobid']
            completecmd = 'source %s && export LD_LIBRARY_PATH=%s:$LD_LIBRARY_PATH && %s && %s' % (self.setupScript, self.manualenvprefix, exportproxy, cmd)
            input.put((currentwork, completecmd, 'output'))
            currentwork += 1

        # Now we should have sent all jobs to be submitted
        # Going to do the rest of it now
        logging.debug("Waiting for %i works to finish..." % len(workqueued))
        for n in xrange(len(workqueued)):
            logging.debug("Waiting for work number %i to finish.." % n)
            res = None
            try:
                res = result.get(block = True, timeout = self.basetimeout)
            except Queue.Empty:
                logging.error("Timeout retrieving result %i out of %i" % (n, xrange(len(workqueued))) )
                continue
            jsout  = res['jsout']
            error  = res['stderr']
            exit   = res['exit']
            workid = res['workid']
            # Check error
            if exit != 0:
                msg = 'Error executing get-output:\n\texit code: %i\n' % exit
                msg += '\tstderr: %s\n\tjson: %s' % (error, str(jsout.strip()))
                logging.error( msg )
                failedJobs.append(workqueued[workid])
                for jj in jobs:
                    if jj['jobid'] == workqueued[workid]:
                        self.fakeReport("GetOutputFailure", msg, -1, jj)
                        break
                continue
            else:
                # parse JSON output, but is not a correct json format, so don't do it!
                out = json

                ### out example
                # {
                # result: success
                # endpoint: https://wms202.cern.ch:7443/glite_wms_wmproxy_server
                # jobid: https://wms202.cern.ch:9000/MwNUhRUsC2HaSfCFzxETVw {
                # No output files to be retrieved for the job:
                # https://wms202.cern.ch:9000/MwNUhRUsC2HaSfCFzxETVw
                #
                # }
                #}

                if jsout is not None:
                    jobid    = workqueued[workid]
                    if jsout.find('success') != -1:
                        completedJobs.append(jobid)
                    else:
                        failedJobs.append(jobid)
                        for jj in jobs:
                            if jj['jobid'] == jobid:
                                self.fakeReport("GetOutputFailure", msg, -1, jj)
                                break

        ## Shut down processes
        logging.debug("About to close the subprocesses...")
        self.close(input, result)

        return completedJobs, failedJobs, abortedJobs


    def postMortem(self, jobs):
        """
        _postMortem_
        """
        logging.debug("Staring gLite postMortem method..")

        command   = "glite-wms-job-logging-info -v 3"

        workqueued  = {}
        currentwork = len(workqueued)

        completedJobs = []
        failedJobs    = []

        retrievedproxy = {}

        ## Start up processes
        input  = multiprocessing.Queue()
        result = multiprocessing.Queue()
        self.start(input, result)

        #creates chunks of work per multi-processes
        # TODO: evaluate if passing just one job per work is too much overhead

        for jj in jobs:

            ownersandbox      = jj['userdn']
            valid, ownerproxy = (False, None)
            exportproxy       = 'echo $X509_USER_PROXY'
            if ownersandbox in retrievedproxy:
                valid      = True
                ownerproxy = retrievedproxy[ownersandbox]
            else:
                valid, ownerproxy = self.validateProxy( ownersandbox )

            if valid:
                retrievedproxy[ownersandbox] = ownerproxy
                exportproxy = "export X509_USER_PROXY=%s" % ownerproxy
            else:
                msg = "Problem retrieving user proxy, or user proxy " + \
                      "expired '%s'" % ownersandbox
                logging.error( msg )
                failedJobs.append( jj )
                self.fakeReport("PostMortemFailure", msg, -1, jj)
                continue

            cmd = '%s %s > %s/loggingInfo.%i.log'\
                   % (command, jj['gridid'], jj['cache_dir'], jj['retry_count'])
            logging.debug("Enqueuing logging-info command for job %i" \
                           % jj['jobid'] )
            workqueued[currentwork] = jj['jobid']
            completecmd = 'source %s && export LD_LIBRARY_PATH=%s:$LD_LIBRARY_PATH && %s && %s' % (self.setupScript, self.manualenvprefix, exportproxy, cmd)
            input.put( (currentwork, completecmd, 'output') )
            currentwork += 1

        # Now we should have sent all jobs to be submitted
        # Going to do the rest of it now
        logging.debug("Waiting for %i works to finish..." % len(workqueued))
        for n in xrange(len(workqueued)):
            logging.debug("Waiting for work number %i to finish.." % n)
            res = None
            try:
                res = result.get(block = True, timeout = self.basetimeout)
            except Queue.Empty:
                logging.error("Timeout retrieving result %i out of %i" % (n, xrange(len(workqueued))) )
                continue
            jsout  = res['jsout']
            error  = res['stderr']
            exit   = res['exit']
            workid = res['workid']
            logging.debug ('result : \n %s' % str(res) )
            # Check error
            if exit != 0:
                msg = 'Error getting logging-info:\n\texit code: %i\n' % exit
                msg += '\tstderr: %s\n\tjson: %s' % (error, str(jsout.strip()))
                logging.error( msg )
                failedJobs.append(workqueued[workid])
                for jj in jobs:
                    if jj['jobid'] == workqueued[workid]:
                        self.fakeReport("PostMortemFailure", msg, -1, jj)
                        break
                continue
            else:
                completedJobs.append(workqueued[workid])

        ## Shut down processes
        logging.debug("About to close the subprocesses...")
        self.close(input, result)

        return completedJobs, failedJobs


    def complete(self, jobs):
        """
        _complete_

        Run any complete code
        """
        # Run your command in parallel
        # This sends the outputWorker function
        # Whatever's in gridid as an argument
        # And at the end waits for a return

        # NOTE: This is a blocking function


        #input = [x['gridid'] for x in jobs]

        #results = self.pool.map(outputWorker, input,
        #                        chunksize = self.chunksize)

        #return results
        completed, failed, aborted = self.getoutput(jobs)
        if len( aborted ) > 0:
            abortcompl, abortfail = self.postMortem( jobs = aborted )

        return



    def kill(self, jobs):
        """
        _kill_

        Kill any and all jobs
        """

        workqueued  = {}
        currentwork = len(workqueued)

        completedJobs = []
        failedJobs    = []

        retrievedproxy = {}

        ## Start up processes
        input  = multiprocessing.Queue()
        result = multiprocessing.Queue()
        self.start(input, result)

        for job in jobs:

            ownersandbox      = job['userdn']
            valid, ownerproxy = (False, None)
            exportproxy       = 'echo $X509_USER_PROXY'
            if ownersandbox in retrievedproxy:
                valid      = True
                ownerproxy = retrievedproxy[ownersandbox]
            else:
                valid, ownerproxy = self.validateProxy( ownersandbox )

            if valid:
                retrievedproxy[ownersandbox] = ownerproxy
                exportproxy = "export X509_USER_PROXY=%s" % ownerproxy
            else:
                msg = "Problem retrieving user proxy, or user proxy " + \
                      "expired '%s'" % ownersandbox
                logging.error( msg )
                failedJobs.append( job )
                self.fakeReport("KillFailure", msg, -1, job)
                continue

            gridID = job['gridid']
            command = 'glite-wms-job-cancel --json --noint %s' % (gridID)
            logging.debug("Enqueuing cancel command for gridID %s" % gridID )

            workqueued[currentwork] = gridID
            completecmd = 'source %s && export LD_LIBRARY_PATH=%s:$LD_LIBRARY_PATH && %s && %s' % (self.setupScript, self.manualenvprefix, exportproxy, command)
            input.put( (currentwork, completecmd, 'output') )

            currentwork += 1

        # Now we should have sent all jobs to be submitted
        # Waiting for results
        logging.debug("Waiting for %i JOBS to be killed..." % len(workqueued))

        for n in xrange(len(workqueued)):
            logging.debug("Waiting for job number %i to be killed.." % n)
            res = None
            try:
                res = result.get(block = True, timeout = self.basetimeout)
            except Queue.Empty:
                logging.error("Timeout retrieving result %i out of %i" % (n, xrange(len(workqueued))) )
                continue
            jsout  = res['jsout']
            error  = res['stderr']
            exit   = res['exit']
            workid = res['workid']
            logging.debug ('result : \n %s' % str(res) )
            # Checking error
            if exit != 0:
                msg = 'Error executing kill:\n\texit code: %i\n' % exit
                msg += '\tstderr: %s\n\tjson: %s' % (error, str(jsout.strip()))
                logging.error( msg )
                failedJobs.append(workqueued[workid])
                for jj in jobs:
                    if jj['jobid'] == workqueued[workid]:
                        self.fakeReport("KillFailure", msg, -1, jj)
                        break
                continue
            elif jsout.find('success') != -1:
                logging.debug('Success killing %s' % str(workqueued[workid]))
                completedJobs.append(workqueued[workid])
            else:
                logging.error('Error success != -1')
                failedJobs.append(workqueued[workid])
                for jj in jobs:
                    if jj['jobid'] == workqueued[workid]:
                        self.fakeReport("KillFailure", msg, -1, jj)
                        break
                continue

        ## Shut down processes
        logging.debug("Close the subprocesses...")
        self.close(input, result)

        return completedJobs, failedJobs



    def delegateProxy( self, wms = None, exportcmd = None ):
        """
        _delegateProxy_

        delegate proxy to _all_ wms or to specific one (if explicitly passed)
        """
        command = "glite-wms-job-delegate-proxy -d " + self.delegationid
        if exportcmd is not None:
            command =  exportcmd + ' && ' + command

        if wms is not None:
            command1 = command + " -e " + wms
            pipe = subprocess.Popen(command1, stdout = subprocess.PIPE,
                                 stderr = subprocess.PIPE, shell = True)
            logging.debug('Waiting delegation at time %s ' % str(time.time()))
            stdout, stderr = pipe.communicate()
            logging.debug('Retrieved subprocess at time %s ' % str(time.time()))
            if len(stderr) > 0 or pipe.returncode != 0:
                msg = 'Problem on delegating the proxy:\n\twms: "%s"\n\t' \
                        % wms
                msg += 'std error: "%s"\n\texit code: "%s"' \
                        % (stderr, str(pipe.returncode))
                logging.error( msg )
            else:
                logging.debug("Proxy delegated using %s endpoint" % wms )


        if self.gliteConfig is not None :
            command2 = command + " -c " + self.gliteConfig
            pipe = subprocess.Popen(command2, stdout = subprocess.PIPE,
                                 stderr = subprocess.PIPE, shell = True)
            logging.debug('Waiting delegation at time %s ' % str(time.time()))
            stdout, stderr = pipe.communicate()
            errcode = pipe.returncode
            if len(stderr) > 0 or errcode != 0:
                msg = 'Problem on delegating the proxy:\n\twms: "%s"\n' % wms
                msg +='\tstd error: "%s"\n\texit code: "%s"' \
                       % (stderr, str(pipe.returncode))
                logging.error( msg )
            else:
                logging.debug("Proxy delegated using %s " % self.gliteConfig)


    def makeJdl(self, jobList, dest, info):
        """
        _makeJdl_

        Prepares the jdl for the collection
        """

        jdl = "[\n"
        jdl += 'Type = "collection";\n'

        # global task attributes :
        globalSandbox = ''
        # nodes files
        commonFiles = ''
        # task input files handling:
        startdir = ''
        if self.defaultjdl['sbtransfer'] == 'gsiftp':
            startdir = 'gsiftp://%s' % self.defaultjdl['gridftphost']
        isb = ''
        commonFiles = ''
        ind = 0
        ## this should include tgz file
        if jobList[0].has_key('sandbox') and jobList[0]['sandbox'] is not None:
            isb += '"%s%s",' % ( startdir, jobList[0]['sandbox'] )
            commonFiles += "root.inputsandbox[%i]," % ind
            ind += 1
        ## this should include the JobPackage.pkl
        #if jobList[0].has_key('packageDir') and \
        #   jobList[0]['packageDir'] is not None:
        #    isb += '"%s%s",' % ( startdir, \
        #             os.path.join(jobList[0]['packageDir'], 'JobPackage.pkl'))
        #    commonFiles += "root.inputsandbox[%i]," % ind
        #    ind += 1
        ## this should include the job starter on the WN
        if self.submitFile is not None:
            isb += '"%s%s",' % ( startdir, self.submitFile )
            commonFiles += "root.inputsandbox[%i]," % ind
            ind += 1
        ## this should include the Unpacker.py of the jobs on the WN
        if self.unpacker is not None:
            isb += '"%s%s",' % ( startdir, self.unpacker )
            commonFiles += "root.inputsandbox[%i]," % ind
            ind += 1

        ## how to include extra files?
        ## TODO: --> need to generalize

        ## removing extracommas from isb file names and from nodes
        if len(isb) > 0 and isb.endswith(','):
            isb = isb[:-1]
        if len(commonFiles) > 0 and commonFiles.endswith(','):
            commonFiles = commonFiles[:-1]

        # single job definition
        jdl += "Nodes = {\n"

        for job in jobList:
            jobid = job['id']
            jobretry = job['retry_count']
            jdl += '[\n'
            jdl += 'NodeName   = "Job_%i_%s";\n' % (job['id'], jobretry)
            jdl += 'Executable = "%s";\n' % os.path.basename(self.submitFile)
            if job.has_key('sandbox') and job['sandbox'] is not None:
                jdl += 'Arguments  = "%s %s";\n' \
                            % (os.path.basename(job['sandbox']), jobid)
            jdl += 'StdOutput  = "%s_%s.stdout";\n' % (jobid, jobretry)
            jdl += 'StdError   = "%s_%s.stderr";\n' % (jobid, jobretry)

            jdl += 'OutputSandboxBaseDestURI = "%s%s";\n' \
                            % (startdir, job['cache_dir'])

            jdl += 'OutputSandbox = {"Report.%i.pkl",".BrokerInfo", "%i_%i.stdout","%i_%i.stderr"};\n' \
                    % (jobretry, jobid, jobretry, jobid, jobretry)

            inputfiles = ''
            if 'packageDir' in job and job['packageDir'] is not None:
                if len(commonFiles) > 0:
                    inputfiles = '"%s%s",%s' % (startdir, os.path.join(job['packageDir'], 'JobPackage.pkl'),commonFiles)
                else:
                    inputfiles = '"%s%s"' % (startdir, os.path.join(job['packageDir'], 'JobPackage.pkl'))

            if len(inputfiles) > 0:
                jdl += 'InputSandbox = {%s};\n' % inputfiles

            jdl += '],\n'
        jdl  = jdl[:-2] + "\n};\n"

        # global sandbox definition
        if len(isb) > 0:
            jdl += 'InputSandbox = {%s};\n' % isb

        #### BUILD REQUIREMENTS ####
        jdl += 'Requirements = Member("%s", ' % self.defaultjdl['nodesarch'] + \
               'other.GlueHostApplicationSoftwareRunTimeEnvironment) ' + \
               '&& (other.GlueHostNetworkAdapterOutboundIP) ' + \
               '&& other.GlueCEStateStatus == "%s" ' \
                % self.defaultjdl['cestatus'] + \
               '&&  other.GlueCEPolicyMaxCPUTime>=130 %s ;\n' \
                % self.sewhite(dest)

        logging.debug("Got destination %s " % str(dest) )
        logging.debug("Translate destination %s " % str( self.sewhite(dest) ) )

        jdl += 'MyProxyServer = "%s";\n' % self.defaultjdl['myproxyhost']
        jdl += 'VirtualOrganization = "%s";\n' % self.defaultjdl['vo']
        jdl += 'RetryCount = 0;\n' + \
               'DefaultNodeRetryCount = 0;\n' + \
               'ShallowRetryCount = -1;\n' + \
               'DefaultNodeShallowRetryCount = -1;\n'

        # close jdl
        jdl += 'SignificantAttributes = {"Requirements", "Rank", "FuzzyRank"};'
        jdl += "\n]\n"

        # return values
        logging.error( str(jdl) )
        return jdl

    def sewhite(self, sesites):
        """
        _sewhite__

        Preparing clause to select ce close to storage data
        """
        sr = ''
        if len(sesites) > 0:
            sr = ' && ('
            for se in sesites:
                sr += ' Member("%s", other.GlueCESEBindGroupSEUniqueID) ||' % se
            sr = sr[:-3] + ')'
        return sr

    def getDestinations(self, destlist, location):
        """
        _getDestinations_

        get a string or list of location, translate from cms name to ce name
          and add the ce if not already in the destination
        """
        destlist = []
        if type(location) == types.StringType or \
           type(location) == types.UnicodeType:
            destlist.append( self.locationAction.execute( \
                               cesite = location)[0].get('se_name', None) )
        elif type(location) == types.ListType:
            for site in location:
                destlist.append( self.locationAction.execute(site)[0].get('se_name', None) )
        return destlist

        #if type(location) == types.StringType or \
        #   type(location) == types.UnicodeType:
        #    if location not in destlist:
        #        jobCE = self.getCEName(jobSite = location)
        #        if jobCE not in destlist:
        #            destlist.append(jobCE)
        #elif type(location) == types.ListType:
        #    for dest in location:
        #        cename = self.getCEName(jobSite = dest)
        #        if cename not in destlist:
        #            destlist.append(cename)

        #return destlist


    def getCEName(self, jobSite):
        """
        _getCEName_

        This is how you get the name of a CE for a job
        """

        if not jobSite in self.locationDict.keys():
            siteInfo = self.locationAction.execute(siteName = jobSite)
            self.locationDict[jobSite] = siteInfo[0].get('se_name', None)
        return self.locationDict[jobSite]

    def checkUI(self, requestedversion = '3.2'):
        """
        _checkUI_

        check if the glite UI is setup
        input: string
        output: boolean
        """

        result = False

        cmd = 'source %s && export LD_LIBRARY_PATH=%s:$LD_LIBRARY_PATH && glite-version' % (self.setupScript, self.manualenvprefix)
        pipe = subprocess.Popen(cmd, stdout = subprocess.PIPE,
                                stderr = subprocess.PIPE, shell = True)
        stdout, stderr = pipe.communicate()
        errcode = pipe.returncode

        logging.info("gLite UI '%s' version detected" % stdout.strip() )
        if len(stderr) == 0 and errcode == 0:
            gliteversion = stdout.strip()
            if gliteversion.find( str(requestedversion) ) == 0:
                result = True

        return result


    def validateProxy(self, userdn):
        """
        _validateProxy_

        Return the proxy path to be used and a boolean to indicates if the proxy is good or not
        """
        if self.singleproxy is not None:
            proxy = Proxy(self.defaultDelegation)
            timeleft = proxy.getTimeLeft( self.singleproxy )
            if timeleft is not None and timeleft > 60:
                logging.info("Remaining timeleft for proxy %s is %s" % (self.singleproxy, str(timeleft)))
                return (True, self.singleproxy)
            else:
                return (False, self.singleproxy)
        else:
            return self.getProxy(userdn)


    def getProxy(self, userdn):
        """
        _getProxy_
        """

        logging.debug("Retrieving proxy for %s" % userdn)
        config = self.defaultDelegation
        config['userDN'] = userdn
        proxy = Proxy(self.defaultDelegation)
        proxyPath = proxy.getProxyFilename( True )
        timeleft = proxy.getTimeLeft( proxyPath )
        if timeleft is not None and timeleft > 3600:
            return (True, proxyPath)
        proxyPath = proxy.logonRenewMyProxy()
        timeleft = proxy.getTimeLeft( proxyPath )
        if timeleft is not None and timeleft > 0:
            return (True, proxyPath)
        return (False, None)


# manage json library using the appropriate WMCore wrapper
from WMCore.Wrappers import JsonWrapper as json

##########################################################################

class BossAirJsonDecoder(json.JSONDecoder):
    """
    Override JSON decode
    """

    def __init__(self):

        # call super
        super(BossAirJsonDecoder, self).__init__()

        # cache pattern to optimize reg-exp substitution
        self.pattern1 = re.compile('([^ \t\n\r\f\v\{\}]+)\s')
        self.pattern2 = re.compile(':"(["|\{])')
        self.pattern3 = re.compile('"[\s]*"')

    def decodeSubmit(self, jsonString):
        """
        specialized method to decode JSON output of glite-wms-job-submit
        """

        # pre-processing the string before decoding
        toParse = jsonString.replace( '\n' , ' ' )
        toParse = self.pattern1.sub(r'"\1"', toParse )
        toParse = self.pattern2.sub(r'":\1', toParse )
        toParse = self.pattern3.sub(r'","', toParse )

        parsedJson = self.decode(toParse)

        return parsedJson

##########################################################################

