#!/usr/bin/env python
#pylint: disable-msg=E1103
#pylint: disable-msg=R0902
# E1103: The thread will have a logger and a dbi before it gets here
# R0902: Too many instance attributes

"""
_GLitePlugin_

A plug-in to submit to gLite WMS

"""


import os
import logging
import threading
import subprocess
import types
import socket

from WMCore.DAOFactory import DAOFactory
#from WMCore.WMInit import getWMBASE
from WMComponent.JobSubmitter.Plugins.PluginBase import PluginBase

# BossLite DBObjects
from WMCore.BossLite.DbObjects.Job         import Job
from WMCore.BossLite.DbObjects.Task        import Task

# BossLite API
from WMCore.BossLite.API.BossLiteAPI       import BossLiteAPI
from WMCore.BossLite.API.BossLiteAPISched  import BossLiteAPISched
from WMCore.BossLite.Common.Exceptions import SchedulerError

from WMCore.Services.UUID import makeUUID


subprocess._cleanup = lambda: None



class BossLiteGLitePlugin(PluginBase):
    """
    _GLitePlugin_
    
    A plug-in to submit to gLite WMS
    """

    def __init__(self, **configDict):

        PluginBase.__init__(self, config = configDict)
        
        self.config = configDict

        self.locationDict = {}

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger = myThread.logger,
                                dbinterface = myThread.dbi)

        self.locationAction = daoFactory(classname = "Locations.GetSiteInfo")

        self.packageDir = None
        self.unpacker   = None
        self.sandbox    = None
        self.executable = os.path.basename(self.config['submitScript'])

        return



    def __call__(self, parameters):
        """
        _submitJobs_
        
        If this class actually did something, this would handle submissions
        """

        ## INPUT
        # 'jobs': jobsReady
        # 'packageDir': packagePath
        # 'sandbox': sandbox
        # 'agentName': self.config.Agent.agentName


        if parameters == {} or parameters == []:
            return {'NoResult': [0]}

        result = {'Success': [], 'Failed': []}

        for entry in parameters:
            jobList         = entry.get('jobs')
            self.packageDir = entry.get('packageDir', None)
            self.sandbox    = entry.get('sandbox', None)
            self.agent      = entry.get('agentName', 'test')
            self.matching   = entry.get('matching', False)

            logging.info("Got %i jobs to submit" %len(jobList) )

            if self.packageDir is None:
                self.packageDir = jobList[0]['batch_dir']

            # Create a task and some jobs
            myBossLiteAPI = BossLiteAPI()

            jpkgPath = os.path.join(self.packageDir, 'JobPackage.pkl')
            ## Prepares the input sandbox string for the whole task
            inputsbstr = "%s,%s,%s,%s" % (jpkgPath, self.sandbox, \
                         self.config['unpackerScript'], self.config['submitScript'])
            #inputsbstr = self.config['submitScript']

            taskParams = {'name' : makeUUID(),
                          'globalSandbox' : inputsbstr,
                          'serverName': self.agent,
                          'startDirectory' :  'gsiftp://' + socket.getfqdn()
                         }

            task = Task(taskParams)
            task.create(myBossLiteAPI.db)
            task.exists(myBossLiteAPI.db)

            successList = []
            failList    = []
            dest        = []

            for job in jobList:

                logging.debug("Preparing job '%s'..." % str(job) )

                args = '%s %i' % (os.path.basename(self.sandbox), job['id'])
                name = "%s_job%s" % (taskParams['name'], str(job['id']))

                jobParams = {
                             'executable' : self.executable,
                             'arguments'  : args,
                             'name'       : name,
                             'wmbsJobId'  : job['id'],
                             'standardError' : '%s.err' % taskParams['name'],
                             'standardOutput' : '%s.out' % taskParams['name'],
                             'outputFiles': [
                                              'Report.pkl',
                                              '.BrokerInfo',
                                              '%s.err' % taskParams['name'],
                                              '%s.out' % taskParams['name']
                                            ],
                             'submissionNumber': job['retry_count'] - 1,
                             'outputDirectory' : taskParams['startDirectory'] + \
                                                 job['cache_dir']
                            }
                # outfile_basename - list of str
                # output_files - list of str
                
                ## Translating CMS names into ce name (through resource control)
                if not self.matching:
                    dest = self.getDestinations( dest, job['custom']['location'] )
                else:
                    dest = job['custom']['location']

                #jobParams['dlsDestination'] = dest

                j = Job( parameters = jobParams )
                j.newRunningInstance(myBossLiteAPI.db)
                task.addJob(j)

            task.save(myBossLiteAPI.db)

            mySchedConfig =  { \
             'name' : 'SchedulerGLite',
             'tmpDir': jobList[0]['cache_dir'],
             'service': 'https://wms020.cnaf.infn.it:7443/' + \
                          'glite_wms_wmproxy_server'
                             }

            ## instantiate boss lite api for scheduler interaction
            mySchedAPI = BossLiteAPISched( bossLiteSession = myBossLiteAPI, 
                                           schedulerConfig = mySchedConfig)

            ## performing real submission
            try:
                logging.info("submitting...")
                mySchedAPI.submit( taskObj = task,
                   requirements = self.requirements(dest) + self.otherParJdl()
                                 )
                logging.info('...done!')
            except SchedulerError, ex:
                logging.error("Problem in submission: '%s'" % str(ex))

            ## This below is the slow way of doing it: O(job^2)
            ## IDEA: replacing this with an ad hoc dao
            for job in jobList:
                for blj in task.jobs:
                    if job['id'] == blj['wmbsJobId']:
                        if blj.runningJob['schedulerId']:
                            result['Success'].append(job['id'])
                            job['couch_record'] = None
                            successList.append( job )
                        else:
                            result['Failed'].append(job['id'])
                            job['couch_record'] = None
                            failList.append( job )

        logging.debug("Submission results '%s' " % str(result) )

        # setting submitted jobs
        if len(successList) > 0:
            self.passJobs(jobList = successList)

        # setting unsubmitted jobs
        if len(failList) > 0:
            self.failJobs(jobList = failList)

        return result


    def requirements(self, sesites):
        """
        _requirements_
        
        Preparing the requirement for jdl
        """
        # Missing for the moment the cmssw version
        #  Member("VO-cms-CMSSW_3_6_1_patch7", 
        #  other.GlueHostApplicationSoftwareRunTimeEnvironment) &&

        return 'Requirements = ' + \
               'Member("VO-cms-slc5_ia32_gcc434", ' + \
                'other.GlueHostApplicationSoftwareRunTimeEnvironment) ' + \
               '&& (other.GlueHostNetworkAdapterOutboundIP) ' + \
               '&& other.GlueCEStateStatus == "Production"  ' + \
               '&&  other.GlueCEPolicyMaxCPUTime>=130 %s ;\n' \
                % self.sewhite(sesites)

    def otherParJdl(self):
        """
        _otherParJdl_

        Some hardcoded value for jdl
        """
        return 'MyProxyServer = "myproxy.cern.ch";\n' + \
               'VirtualOrganisation = "cms";\n' + \
               'RetryCount = 0;\n' + \
               'DefaultNodeRetryCount = 0;\n' + \
               'ShallowRetryCount = -1;\n' + \
               'DefaultNodeShallowRetryCount = -1;\n'


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
                logging.info('\t selected SE: [%s]' % se)
            sr = sr[:-3] + ')'
        return sr


    def getDestinations(self, destlist, location):
        """
        _getDestinations_

        get a string or list of location, translate from cms name to ce name
          and add the ce if not already in the destination
        """
        if type(location) == types.StringType or \
           type(location) == types.UnicodeType:
            if location not in destlist:
                jobCE = self.getCEName(jobSite = location)
                if jobCE not in destlist:
                    destlist.append(jobCE)
        elif type(location) == types.ListType:
            for dest in location:
                cename = self.getCEName(jobSite = dest)
                if cename not in destlist:
                    destlist.append(cename)

        return destlist


    def getCEName(self, jobSite):
        """
        _getCEName_

        This is how you get the name of a CE for a job
        """

        if not jobSite in self.locationDict.keys():
            siteInfo = self.locationAction.execute(siteName = jobSite)
            self.locationDict[jobSite] = siteInfo[0].get('se_name', None)
        return self.locationDict[jobSite]


