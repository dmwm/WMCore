#!/usr/bin/env python
#pylint: disable-msg=W0102, W6501, C0301
# W0102: We want to pass blank lists by default
# for the whitelist and the blacklist
# W6501: pass information to logging using string arguments
# C0301: I'm ignoring this because breaking up error messages is painful

"""
Creates jobs for new subscriptions

"""

__revision__ = "$Id: JobSubmitterPollerMatchMaking.py,v 1.45 2010/08/11 19:33:26 mnorman Exp $"
__version__ = "$Revision: 1.45 $"


#This job currently depends on the following config variables in JobSubmitter:
# pluginName
# pluginDir

import logging
import threading
import os.path
import cPickle
import traceback

# WMBS objects
from WMCore.DAOFactory        import DAOFactory

from WMCore.JobStateMachine.ChangeState       import ChangeState
from WMCore.WorkerThreads.BaseWorkerThread    import BaseWorkerThread
from WMCore.ProcessPool.ProcessPool           import ProcessPool
from WMCore.ResourceControl.ResourceControl   import ResourceControl
from WMCore.DataStructs.JobPackage            import JobPackage
from WMCore.WMBase        import getWMBASE

from WMComponent.JobSubmitter.JobSubmitterPoller import JobSubmitterPoller

class JobSubmitterPollerMatchMaking(JobSubmitterPoller):
    """
    _JobSubmitterPollerMatchMaking_

    The jobSubmitterPoller takes the jobs and organizes them into packages
    before sending them to the individual plugin submitters.
    """
    def __init__(self, config):
        #super(JobSubmitterPollerMatchMaking, self).__init__(config)
        JobSubmitterPoller.__init__(self, config)

        self.jobGroup = self.daoFactory(classname = "JobGroup.GetGroupFromCreatedJob")
        self.jobsFromGroup = self.daoFactory(classname = "Jobs.LoadFromGroup")

        return

    def matcha(self, sitelist):
        """
        _matcha_

        Performing the matchmaking 
        """

        import re
        import os
        import tempfile
        from WMCore.BossLite.Common.System import executeCommand

        patternCE = re.compile('(?<= - ).*(?=:)', re.M)
        req = 'Requirements = (other.GlueHostNetworkAdapterOutboundIP) ' + \
              '&& other.GlueCEStateStatus == "Production" ' + \
              '&& other.GlueCEPolicyMaxCPUTime>=130 '
        ses = ''
        if len(sitelist) > 0:
            ses += ' && ( '
            for ss in sitelist:
                ses += 'Member("%s" , other.GlueCESEBindGroupSEUniqueID) || ' \
                        % str(ss)
            ses = ses[:-3]
            ses += ' ) '
        req += ses
        jdl = '[\nType = "job";\nExecutable = "/bin/echo";\n' + \
               req + ';\n' + \
              'MyProxyServer = "myproxy.cern.ch";\n' + \
              'VirtualOrganisation = "cms";\nRetryCount = 0;\n' + \
              'DefaultNodeRetryCount = 0;\nShallowRetryCount = -1;\n' + \
              'DefaultNodeShallowRetryCount = -1;\n' + \
              'SignificantAttributes = ' + \
                 '{"Requirements", "Rank", "FuzzyRank"};\n' + \
              ']\n'
        tmp, fname = tempfile.mkstemp( "", "glite_list_match_", '.' )
        tmpFile = os.fdopen(tmp, "w")
        tmpFile.write( jdl )
        tmpFile.close()

        command = "glite-wms-job-list-match -a " + fname

        outRaw, ret = executeCommand( command )

        os.unlink( fname )

        out = None
        if ret == 0 : 
            out = patternCE.findall(outRaw)
        else:
            logging.error("Problem executing list match, code '%s'" % str(ret))
            logging.debug( str(command) )
            logging.debug( str(outRaw) )
            return []
        
        # return CE without duplicate
        listCE = list(set(out))
        if len(listCE) == 0:
            logging.debug(
                'List match performed with following requirements:\n %s'
                                                            % str(fakeJdl) )  
        if 'Unable to perform the operation' in listCE:
            listCE.remove('Unable to perform the operation')

        return listCE



    def matchMaking(self, job):
        """
        _matchMaking_
        """

        pickledJobPath = os.path.join(job["cache_dir"], "job.pkl")
        jobHandle      = open(pickledJobPath, "r")
        loadedJob      = cPickle.load(jobHandle)
        rawLocations   = loadedJob["input_files"][0]["locations"]
        jobHandle.close()

        logging.info("Locations: '%s'" % str(rawLocations))

        possibleLocations = set()
        for loc in rawLocations:
            possibleLocations.add(loc)
        
        if len(loadedJob["siteWhitelist"]) > 0:
            logging.info("  applying se white list")
            possibleLocations = possibleLocations & set(loadedJob.get("siteWhitelist"))
        if len(loadedJob["siteBlacklist"]) > 0:
            logging.info("  applying site blak list")
            possibleLocations = possibleLocations - set(loadedJob.get("siteBlacklist"))

        ####
        #### EVENTUALLY REMOVE FROM possibleLocations SITES NOT IN RES-CONTROL
        ####

        return list(possibleLocations), self.matcha(possibleLocations)
        

    def submitJobs(self, work): #jobs, sites):
        """
        _submitJobs_

        Actually do the submission of the jobs
        """

        agentName = self.config.Agent.agentName
        lenWork   = 0
   
        for jobs, sites in work:

            jobsReady = []
            for job in jobs:
                jobsReady.append( {
                                   'id':          job['id'],
                                   'retry_count': job['retry_count'],
                                   'custom':      {'location': sites},
                                   'cache_dir':   job['cache_dir']
                                 })

            while len(jobsReady) > 0: 
                jobSubmit = jobsReady[:self.config.JobSubmitter.jobsPerWorker]
                jobsReady = jobsReady[self.config.JobSubmitter.jobsPerWorker:]

#                package = ''
#                loadedJob = None

#                for jj in jobSubmit:
                pickledJobPath = os.path.join(jobSubmit[0]['cache_dir'], "job.pkl")
                jobHandle = open(pickledJobPath, "r")
                loadedJob = cPickle.load(jobHandle)
                jobHandle.close()
                package   = self.addJobsToPackage(loadedJob)
                self.flushJobPackages()

                sandbox   = loadedJob['sandbox']

                self.processPool.enqueue([{'jobs':       jobSubmit,
                                           'packageDir': package,
                                           'sandbox':    sandbox,
                                           'agentName':  agentName,
                                           'matching':   True
                                          }])
                lenWork += 1

        # And then, at the end of the day, we have to dequeue them.
        result = []
        result = self.processPool.dequeue(lenWork)
       
        return

    def algorithm(self, parameters = None):
        """
        _algorithm_

        Try to, in order:
        1) Gets the job group id where there are jobs in created status
        2) Performs the match making
        3) Submits the jobs to the plugin
        """
        try:
            tosubmit = []
            ## extraction of the jobgroupid from the first created job found
            jobgroup = self.jobGroup.execute()
            logging.info(" %i groups of jobs to be processed." % len(jobgroup))
            ## if jobgroup is found
            if jobgroup is not None and len(jobgroup) > 0:
                logging.info("group :  %s" %str(jobgroup) )
                for group in jobgroup:
                    jobs = self.jobsFromGroup.execute(group)
                    logging.info("Working on group %i with %i jobs " \
                                  % (group, len(jobs)) )
                    if len(jobs) > 0:
                        ## matchmaking
                        locationstorage, sites = self.matchMaking(jobs[0])
                        if len(sites) > 0:
                            ## job to be submitted
                            tosubmit.append( (jobs, locationstorage) )
                        else:
                            logging.warning('No sites for group %i ' % group)
                    else:
                        logging.error('No jobs in the group %i!' % group)
            else:
                logging.info('No group to process ')

            if len( tosubmit ) > 0:
                logging.info('Starting job submission...')
                ## submit
                self.submitJobs( tosubmit )
                logging.info('...job submission completed!')
        except Exception, ex:
            msg = 'Fatal error in JobSubmitter:\n'
            msg += str(ex)
            msg += str(traceback.format_exc())
            msg += '\n\n'
            logging.error(msg)
            raise Exception(msg)


        return

