#!/usr/bin/env python

"""
gLite Plugin


"""

import logging
import subprocess
import threading
import multiprocessing
import socket
import tempfile
import os
import re
import time
import types

from WMCore.FwkJobReport.Report        import Report
from WMCore.BossAir.Plugins.BasePlugin import BasePlugin,BossAirPluginException
from WMCore.DAOFactory import DAOFactory
import WMCore.WMInit
from copy import deepcopy


def hackTheEnv(prependCommand = ''):
    """
    HaCk ThE eNv  *IMPORTANT NOTES* 
    - the hack is necessary only for CLI which are python(2) script
    - the hack reverts PATH & LD_LYBRARY_PATH if an external PYTHON is present
    - during the hack, replicate entries will be dropped
    - the hack MUST be placed between the 'proxyString'and the CLI command
    """

    newEnv = prependCommand + ' '

    try :
        pyVersionToRemove = ''
        if os.environ.has_key('PYTHON_VERSION'):
            pyVersionToRemove = os.environ['PYTHON_VERSION']
        else:
            import platform
            pyVersionToRemove = "%s-cmp" % platform.python_version()

        originalPath = os.environ['PATH']
        originalLdLibPath = os.environ['LD_LIBRARY_PATH']

        newPath = ''
        newLdLibPath = ''

        # build new PATH
        for x in list(set(originalPath.split(':'))) :
            if x.find(pyVersionToRemove) == -1 :
                newPath += x + ':'
        newEnv += 'PATH=' + newPath[:-1] + ' '

        # build new LD_LIBRARY_PATH
        for x in list(set(originalLdLibPath.split(':'))) :
            if x.find(pyVersionToRemove) == -1 :
                newLdLibPath += x + ':'
        newEnv += 'LD_LIBRARY_PATH=' + newLdLibPath[:-1] + ' '

    except :
        # revert not necessary or something went wrong during the hacking
        pass

    return newEnv

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
                  'service'    : None
                 }


    def __init__(self, config):

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger = myThread.logger,
                                dbinterface = myThread.dbi)

        self.locationAction = daoFactory(classname = "Locations.GetSiteSE")
        self.locationDict = {}

        self.delegationid = str(type(self).__name__)

        ## env check
        if not self.checkUI():
            raise BossAirPluginException("gLite environment has not been set properly!")
        ## encapsulating env
        self.hackEnv = hackTheEnv()

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


        # These are the pool settings.
        self.nProcess       = getattr(self.config.BossAir, \
                                          'gLiteProcesses', 10)
        self.collectionsize = getattr(self.config.BossAir, \
                                          'gLiteCollectionSize', 200)
        self.trackmaxsize   = getattr(self.config.BossAir, \
                                          'gLiteMaxTrackSize', 200)

        self.pool     = []

        self.submitFile   = getattr(self.config.JobSubmitter, \
                                           'submitScript', None)
        self.unpackerFile = getattr(self.config.JobSubmitter, \
                                           'unpackerScript', None)
        self.submitDir    = getattr(self.config.JobSubmitter, \
                                           'submitDir', '/tmp/')
        self.gliteConfig  = getattr(self.config.BossAir, \
                                           'gLiteConf', None)
        self.defaultjdl['service'] = getattr(self.config.BossAir, \
                                           'gliteWMS', None)

        
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


    def close(self, input, result):
        """
        _close_

        Kill all connections and terminate
        """
        logging.debug("Ready to close all %i started processes " % len(self.pool))
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

        for job in jobs:
            sandbox = job['sandbox']
            if not sandbox in submitDict.keys():
                submitDict[sandbox] = []
            submitDict[sandbox].append(job)

        tounlink = []
        # Now submit the bastards
        currentwork = len(workqueued)
        for sandbox in submitDict.keys():
            jobList = submitDict.get(sandbox, [])
            while len(jobList) > 0:
                command = "glite-wms-job-submit --json "
                jobsReady = jobList[:self.collectionsize]
                jobList   = jobList[self.collectionsize:]
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

                jdlReady  = self.makeJdl( jobList = jobsReady, 
                                          dest = dest,
                                          info = info
                                        )
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
                    self.delegateProxy( self.defaultjdl['service'] )
                else :
                    command += " -a "

                if self.gliteConfig is not None:
                    command += " -c " + self.gliteConfig
                elif self.defaultjdl['service'] is not None:
                    # eventual note: the '-e' override the ...
                    command += ' -e ' + self.defaultjdl['service']

                command += ' ' + fname

                # Now submit them
                logging.debug("About to submit %i jobs" % len(jobsReady) )
                workqueued[currentwork] = jobsReady
                input.put((currentwork, self.hackEnv + ' ' + command, 'submit'))
                currentwork += 1

        logging.debug("Waiting for %i works to finish.." % len(workqueued))
        for n in xrange(len(workqueued)):
            logging.debug("Waiting for work number %i to finish.." % n)
            res = result.get()
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
                if jsout.has_key('result'):
                    if jsout['result'] != 'success':
                        failedJobs.extend(jobsub)
                        continue
                else:
                    failedJobs.extend(jobsub)
                    continue
                if jsout.has_key('parent'): 
                    parent = jsout['parent']
                else:
                    failedJobs.extend(jobsub)
                    continue
                if jsout.has_key('endpoint'): 
                    endpoint = jsout['endpoint']
                else:
                    failedJobs.extend(jobsub)
                    continue
                if jsout.has_key('children'): 
                    for key in jsout['children'].keys():
                        jobid, retrycount = (key.split('NodeName_')[-1]).split('_')
                        job   = jobsub[int(jobid)-1]
                        job['bulkid']       = parent
                        job['gridid']       = jsout['children'][key]
                        job['retry_count']  = int(retrycount)
                        job['sched_status'] = 'Submitted'
                        #job['endpoint'] = endpoint
                        successfulJobs.append(job)
                else:
                    failedJobs.extend(jobsub)
                    continue

        logging.debug("Submission completed and processed at time %s " \
                       % str(time.time()) )

        # unlinking all the temporary files used for jdl
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
            cmdquerypath = wmcoreBasedir + \
                           '/src/python/WMCore/BossAir/Plugins/' + queryfilename
            if not os.path.exists( cmdquerypath ):
                raise BossAirPluginException('Impossible to locate %s' % cmdquerypath)
        else :
            # Impossible to locate GLiteQueryStatus.py ...
            raise BossAirPluginException('Impossible to locate %s, WMBASE = %s ' % (queryfilename, str(wmcoreBasedir)) )

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
                input.put((currentwork, self.hackEnv + ' ' + command, 'status'))
                currentwork += 1

        # Now we should have sent all jobs to be submitted
        # Going to do the rest of it now
        logging.debug("Waiting for %i works to finish..." % len(workqueued))
        for n in xrange(len(workqueued)):
            logging.debug("Waiting for work number %i to finish.." % n)
            res = result.get()
            jsout  = res['jsout']
            error  = res['stderr']
            exit   = res['exit']
            workid = res['workid']
            # Check error
            if exit != 0:
                logging.error('Error executing %s: \n\texit code: %i\n\tstderr: %s\n\tjson: %s' % (cmdquerypath, exit, error, str(jsout.strip()) )
                              )
                continue
            else:
                # parse JSON output
                out = None
                try:
                    out = json.loads(jsout)
                except ValueError, va:
                    raise BossAirPluginException('Error parsing JSON: \n\terror: %s\n\t:exception: %s' % (error, str(va)) )
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
                        ## TODO: need to handle the status time stamp
                        #lbts        = jobStatus['lbTimestamp']
                        lbts         = 0
                 
                        # Get the global state
                        jj['globalState'] = gLitePlugin.stateMap()[status]


                        if status != jj['status']:
                            # Then the status has changed
                            jj['status']      = status
                            jj['status_time'] = lbts
                            changeList.append(jj)

                        if status not in ['Done', 'Aborted', 'Cleared', 'Cancelled by user', 'Cancelled']: 
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

        ## Start up processes
        input  = multiprocessing.Queue()
        result = multiprocessing.Queue()
        self.start(input, result)

        #creates chunks of work per multi-processes 
        # TODO: evaluate if passing just one job per work is too much overhead

        for jj in jobs:

            if jj['status'] is not ['Done']:
                if jj['status'] in ['Aborted']:
                    abortedJobs.append( jj )
                continue

            cmd = '%s %s %s %s' % (command, outdiropt, jj['cache_dir'], jj['gridid'])
            logging.debug("Enqueuing getoutput for job %i" % jj['jobid'] )
            workqueued[currentwork] = jj['jobid']
            input.put((currentwork, self.hackEnv + ' ' + cmd, 'output'))
            currentwork += 1

        # Now we should have sent all jobs to be submitted
        # Going to do the rest of it now
        logging.debug("Waiting for %i works to finish..." % len(workqueued))
        for n in xrange(len(workqueued)):
            logging.debug("Waiting for work number %i to finish.." % n)
            res = result.get()
            jsout  = res['jsout']
            error  = res['stderr']
            exit   = res['exit']
            workid = res['workid']
            # Check error
            if exit != 0:
                logging.error('Error executing %s: \n\texit code: %i\n\tstderr: %s\n\tjson: %s' % (command, exit, error, str(jsout.strip()) )
                               )
                failedJobs.append(workqueued[workid])
                continue
            else:
                # parse JSON output
                out = None
                try:
                    out = json #.loads(jsout)
                except ValueError, va:
                    raise BossAirPluginException('Error parsing JSON: \n\terror: %s\n\t:exception: %s' % (error, str(va)) )

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
                    """
                    if not jsout.has_key('result'):
                        failedJobs.extend(jobid)
                        continue
                    elif jsout['result'] != 'success':
                        failedJobs.extend(jobid)
                        continue
                    else:
                        completedJobs.extend(jobid)
                    """
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

        ## Start up processes
        input  = multiprocessing.Queue()
        result = multiprocessing.Queue()
        self.start(input, result)

        #creates chunks of work per multi-processes 
        # TODO: evaluate if passing just one job per work is too much overhead

        for jj in jobs:

            cmd = '%s %s > %s/loggingInfo.%i.log' % (command, jj['gridid'], jj['cache_dir'], jj['retry_count'])
            logging.debug("Enqueuing logging info command for job %i" % jj['jobid'] )
            workqueued[currentwork] = jj['jobid']
            input.put( (currentwork, self.hackEnv + ' ' + cmd, 'output') )
            currentwork += 1

        # Now we should have sent all jobs to be submitted
        # Going to do the rest of it now
        logging.debug("Waiting for %i works to finish..." % len(workqueued))
        for n in xrange(len(workqueued)):
            logging.debug("Waiting for work number %i to finish.." % n)
            res = result.get()
            jsout  = res['jsout']
            error  = res['stderr']
            exit   = res['exit']
            workid = res['workid']
            logging.debug ('result : \n %s' % str(res) )
            # Check error
            if exit != 0:
                logging.error('Error executing %s: \n\texit code: %i\n\tstderr: %s\n\tjson: %s' % (command, exit, error, str(jsout.strip()) )
                               )
                failedJobs.append(workqueued[workid])
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

        ## Start up processes
        input  = multiprocessing.Queue()
        result = multiprocessing.Queue()
        self.start(input, result)
        
        for job in jobs:

            gridID = job['gridid']
            command = 'glite-wms-job-cancel --json --noint %s' % (gridID)
            logging.debug("Enqueuing cancel command for gridID %s" % gridID )

            workqueued[currentwork] = gridID
            input.put( (currentwork, self.hackEnv + ' ' + command, 'output') )
  
            currentwork += 1

        # Now we should have sent all jobs to be submitted
        # Waiting for results
        logging.debug("Waiting for %i JOBS to be killed..." % len(workqueued))

        for n in xrange(len(workqueued)):

            logging.debug("Waiting for job number %i to be killed.." % n)
            res = result.get()
            jsout  = res['jsout']
            error  = res['stderr']
            exit   = res['exit']
            workid = res['workid']
            logging.debug ('result : \n %s' % str(res) )
            # Checking error
            if exit != 0:
                logging.error('Error executing %s: \n\texit code: %i\n\tstderr: %s\n\tjson: %s' % (command, exit, error, str(jsout.strip()) )
                               )
                failedJobs.append(workqueued[workid])
            elif  jsout.find('success') != -1:
                logging.debug('Succesfully killed job %s' % str(workqueued[workid]))
                completedJobs.append(workqueued[workid]) 
            else:
                logging.error('Error success != -1')
                failedJobs.append(workqueued[workid])

        ## Shut down processes
        logging.debug("Close the subprocesses...")
        self.close(input, result)

        return completedJobs, failedJobs



    def delegateProxy( self, wms = None ):
        """
        _delegateProxy_

        delegate proxy to _all_ wms or to specific one (if explicitly passed)
        """
        command = "glite-wms-job-delegate-proxy -d " + self.delegationid
     
        if wms is not None:
            command1 = command + " -e " + wms
            pipe = subprocess.Popen(command1, stdout = subprocess.PIPE,
                                 stderr = subprocess.PIPE, shell = True)
            logging.debug('Waiting delegation at time %s ' % str(time.time()))
            stdout, stderr = pipe.communicate()
            logging.debug('Retrieved subprocess at time %s ' % str(time.time()))
            if len(stderr) > 0 or pipe.returncode != 0:
                logging.error('Problem on delegating the proxy: \n\twms: "%s"\n\tstd error: "%s"\n\texit code: "%s"' % (wms, stderr, str(pipe.returncode)) )
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
                logging.error('Problem on delegating the proxy: \n\tcfg: "%s"\n\tstd error: "%s"\n\texit code: "%s"' % (self.gliteConfig, stderr, str(errcode)) )
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
        if jobList[0].has_key('packageDir') and \
           jobList[0]['packageDir'] is not None:
            isb += '"%s%s",' % ( startdir, \
                     os.path.join(jobList[0]['packageDir'], 'JobPackage.pkl'))
            commonFiles += "root.inputsandbox[%i]," % ind
            ind += 1
        ## this should include the job starter on the WN
        if self.submitFile is not None:
            isb += '"%s%s",' % ( startdir, self.submitFile )
            commonFiles += "root.inputsandbox[%i]," % ind
            ind += 1
        ## this should include the Unpacker.py of the jobs on the WN
        if self.unpackerFile is not None:
            isb += '"%s%s",' % ( startdir, self.unpackerFile )
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

        counter = 0
        for job in jobList:
            jobid = job['id']
            jobretry = job['retry_count']
            jdl += '[\n'
            jdl += 'NodeName   = "NodeName_%i_%s";\n' % (counter, jobretry)
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

            if len(commonFiles) > 0:
                jdl += 'InputSandbox = {%s};\n' % commonFiles

            jdl += '],\n'
            counter += 1
        jdl  = jdl[:-2] + "\n};\n"

        # global sandbox definition
        if len(isb) > 0:
            jdl += "InputSandbox = {%s};\n" % (isb)

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

        cmd = 'glite-version'
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

