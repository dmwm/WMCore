#! /usr/bin/env python
"""
_SchedulerCondorCommon_
Base class for CondorG and GlideIn schedulers
"""




import os
import commands
from subprocess import *
import re
import shutil
import cStringIO

from socket import getfqdn

from WMCore.BossLite.Scheduler.SchedulerInterface import SchedulerInterface
from WMCore.BossLite.Common.Exceptions import SchedulerError
from WMCore.BossLite.DbObjects.Job import Job
from WMCore.BossLite.DbObjects.Task import Task
from WMCore.BossLite.DbObjects.RunningJob import RunningJob

class SchedulerCondorCommon(SchedulerInterface) :
    """
    basic class to handle glite jobs through wmproxy API
    """
    def __init__( self, **args ):
        # call super class init method
        super(SchedulerCondorCommon, self).__init__(**args)
        os.environ['_CONDOR_GRIDMANAGER_MAX_SUBMITTED_JOBS_PER_RESOURCE'] = '20'
        self.hostname   = getfqdn()
        self.condorTemp = args.get('tmpDir', None)
        self.outputDir  = args.get('outputDirectory', None)
        self.jobDir     = args.get('jobDir', None)
        self.useGlexec  = args.get('useGlexec', False)
        self.glexec     = args.get('glexec', None)
        self.renewProxy    = args.get('renewProxy', None)
        self.glexecWrapper = args.get('glexecWrapper', None)
        self.condorQCacheDir     = args.get('CondorQCacheDir', None)
        self.userRequirements = ''


    def submit( self, obj, requirements='', config ='', service='' ):
        """
        user submission function

        obj is job or list of jobs

        takes as arguments:
        - a finite, dedicated jdl
        - eventually a wms list
        - eventually a config file

        the passed config file or, if not provided, the default one is
        used to extract basic ui configurations and, if not provided, a
        list o candidate wms

        the function returns the grid parent id, the wms of the
        successfully submission and a map associating the jobname to the
        node id. If the submission is not bulk, the parent id is the node
        id of the unique entry of the map

        """

        # Make directory for Condor returned files
        seDir = "/".join((obj['globalSandbox'].split(',')[0]).split('/')[:-1])
        if self.jobDir:
            seDir = self.jobDir
        self.userRequirements = obj['commonRequirements']

        if os.path.isdir(self.condorTemp):
            pass
        else:
            os.mkdir(self.condorTemp)

        taskId = ''
        ret_map = {}

        jobRegExp = re.compile(
                "\s*(\d+)\s+job\(s\) submitted to cluster\s+(\d+)*")
        if type(obj) == RunningJob or type(obj) == Job :
            raise NotImplementedError
        elif type(obj) == Task :
            taskId = obj['name']
            jobCount = 0
            jdl = ''
            for job in obj.getJobs():
                submitOptions = ''

                jobRequirements = requirements
                execHost = self.findExecHost(jobRequirements)
                filelist = self.inputFiles(obj['globalSandbox'])
                if filelist:
                    jobRequirements += "transfer_input_files = %s\n" % filelist

                # Build JDL file
                if not jobCount:
                    jdl, sandboxFileList, ce = self.commonJdl(job, jobRequirements)
                    jdl += 'Executable = %s/%s\n' % (seDir, job['executable'])
                    jdl += '+BLTaskID = "' + taskId + '"\n'
                jdl += self.singleApiJdl(job, jobRequirements)
                jdl += "Queue 1\n"
                jobCount += 1
            # End of loop over jobs to produce JDL

            # Write and submit JDL
            jdlFileName = self.condorTemp + '/' + job['name'] + '.jdl'
            jdlFile = open(jdlFileName, 'w')
            jdlFile.write(jdl)
            jdlFile.close()

            command = 'cd %s; ' % self.condorTemp

            if self.useGlexec:
                # Set up environment in thread safe manner
                userProxy = obj['user_proxy']
                seProxy   = seDir + '/userProxy'
                commonEnv = 'export GLEXEC_CLIENT_CERT=%s; ' \
                            'export GLEXEC_SOURCE_PROXY=%s; ' \
                            'export X509_USER_PROXY=%s; ' % \
                            (userProxy, userProxy, seProxy)
                proxyEnv  = 'export GLEXEC_TARGET_PROXY=/tmp/x509_ugeneric; '
                submitEnv = 'export GLEXEC_TARGET_PROXY=%s; ' % seProxy

                diffTime = str(os.path.getmtime(obj['user_proxy']))
                proxycmd = commonEnv + proxyEnv
                proxycmd += "%s %s %s %s" % (self.renewProxy, userProxy, seDir, diffTime)
                (status, output) = commands.getstatusoutput(proxycmd)
                self.logging.debug("Result of %s\n%s\n%s" %
                                    (proxycmd,status,output))
                command += commonEnv + submitEnv
                command += "%s %s %s %s" % (self.glexec, self.glexecWrapper,
                                            seDir, jdlFileName)
            else:
                command += 'condor_submit ' + submitOptions + jdlFileName
            (status, output) = commands.getstatusoutput(command)
            self.logging.debug("Result of %s\n%s\n%s" %
                    (command, status, output))

            # Parse output, build numbers
            jobsSubmitted = False
            if not status:
                for line in output.split('\n'):
                    matchObj = jobRegExp.match(line)
                    if matchObj:
                        jobsSubmitted = True
                        jobCount = 0
                        for job in obj.getJobs():
                            if ce:
                                job.runningJob['destination'] = ce.split(':')[0]
                            else:
                                job.runningJob['destination'] = execHost

                            condorID = self.hostname + "//" \
                               + matchObj.group(2) + "." + str(jobCount)
                            ret_map[job['name']] = condorID
                            job.runningJob['schedulerId'] = condorID
                            jobCount += 1
            if not jobsSubmitted:
                job.runningJob.errors.append('Job not submitted:\n%s' \
                                                % output )
                self.logging.error("Job not submitted:")
                self.logging.error(output)

        success = self.hostname
        self.logging.debug("Returning %s\n%s\n%s" %
                (ret_map, taskId, success))
        return ret_map, taskId, success


    def findExecHost(self, requirements=''):
        """
        What host was the job submitted to?
        """

        if not requirements:
            return 'Unknown'
        jdlLines = requirements.split(';')
        execHost = 'Unknown'
        for line in jdlLines:
            if line.find("globusscheduler") != -1:
                parts = line.split('=')
                sched = parts[1]
                parts = sched.split(':')
                execHost = parts[0]

        return execHost.strip()


    def inputFiles(self, globalSandbox):
        """
        Parse out list of input files in sandbox
        """

        filelist = ''
        if globalSandbox is not None:
            for sbFile in globalSandbox.split(','):
                if sbFile == '' :
                    continue
                filename = os.path.abspath(sbFile)
                filename.strip()
                filelist += filename + ','
        return filelist[:-1] # Strip off last ","


    def commonJdl(self, job, requirements=''):
        """
        Bulk mode, common things for all jobs
        """
        jdl  = self.specificBulkJdl(job, requirements='')
        jdl += 'stream_output = false\n'
        jdl += 'stream_error  = false\n'
        jdl += 'notification  = never\n'
        jdl += 'should_transfer_files   = YES\n'
        jdl += 'when_to_transfer_output = ON_EXIT\n'
        jdl += 'copy_to_spool           = false\n'

        # Things in the requirements/jobType field
        jdlLines = requirements.split(';')
        ce = None
        for line in jdlLines:
            [key, value] = line.split('=', 1)
            if key.strip() == "schedulerList":
                ceList = value.split(',')
                ce = ceList[0]
                jdl += "globusscheduler = " + ce + '\n'
            else:
                jdl += line.strip() + '\n'
        filelist = ''
        return jdl, filelist, ce

    def specificBulkJdl(self, job, requirements=''):
        """
        Dummy routine for Common
        """
        return ''


    def singleApiJdl(self, job, requirements=''):
        """
        build a job jdl
        """

        jdl  = ''
        jobId = int(job['jobId'])
        # Make arguments condor friendly (space delimited w/o backslashes)
        jobArgs = job['arguments']
        # Server args already correct
        if not self.useGlexec:
            jobArgs = jobArgs.replace(',',' ')
            jobArgs = jobArgs.replace('\\ ',',')
            jobArgs = jobArgs.replace('\\','')
            jobArgs = jobArgs.replace('"','')

        jdl += 'Arguments  = %s\n' % jobArgs
        if job['standardInput'] != '':
            jdl += 'input = %s\n' % job['standardInput']
        jdl += 'output  = %s\n' % job['standardOutput']
        jdl += 'error   = %s\n' % job['standardError']
        # Make logfile with same root filename
        jdl += 'log     = %s.log\n' % os.path.splitext(job['standardError'])[0]

        # HACK: Figure out where the request for .BrokerInfo comes from
        outputFiles = []
        for fileName in job['outputFiles']:
            if not fileName.endswith('BrokerInfo'):
                outputFiles.append(fileName)
        if outputFiles:
            jdl += 'transfer_output_files   = ' + ','.join(outputFiles) + '\n'


        return jdl


    def query(self, obj, service='', objType='node'):
        """
        query status of jobs
        """

        from xml.sax import make_parser
        from CondorHandler import CondorHandler
        from xml.sax.handler import feature_external_ges

        jobIds = {}
        bossIds = {}

        # FUTURE:
        #  Remove Condor < 7.3 when OK
        #  Use condor_q -attributes to limit the XML size. Faster on both ends
        # Convert Condor integer status to BossLite Status codes
        statusCodes = {'0':'RE', '1':'S', '2':'R',
                       '3':'K',  '4':'D', '5':'A'}
        textStatusCodes = {
                '0':'Ready',
                '1':'Submitted',
                '2':'Running',
                '3':'Cancelled',
                '4':'Done',
                '5':'Aborted'
        }

        if type(obj) == Task:
            taskId = obj['name']

            for job in obj.jobs:
                if not self.valid(job.runningJob):
                    continue
                
                schedulerId = job.runningJob['schedulerId']
                
                # fix: skip if the Job was created but never submitted
                if job.runningJob['status'] == 'C' :
                    continue
		        
                # Jobs are done by default
                bossIds[schedulerId] = {'status':'SD', 'statusScheduler':'Done'}
                schedd = schedulerId.split('//')[0]
                jobNum = schedulerId.split('//')[1]

                # Fill dictionary of schedd and job #'s to check
                if schedd in jobIds.keys():
                    jobIds[schedd].append(jobNum)
                else :
                    jobIds[schedd] = [jobNum]
        else:
            raise SchedulerError('Wrong argument type or object type',
                                  str(type(obj)) + ' ' + str(objType))

        for schedd in jobIds.keys() :
            cmd = 'condor_q -xml '
            if schedd != self.hostname:
                cmd += '-name ' + schedd + ' '
            cmd += """-constraint 'BLTaskID=?="%s"'""" % taskId

            pObj = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE,
                         stderr=STDOUT, close_fds=True)
            (inputFile, outputFp) = (pObj.stdin, pObj.stdout)
            try:
                xmlLine = ''
                while xmlLine.find('<?xml') == -1:
                    # Throw away junk for condor < 7.3, remove when obsolete
                    xmlLine = outputFp.readline()

                outputFile = cStringIO.StringIO(xmlLine+outputFp.read())
                #outputFile = cStringIO.StringIO(outputFp.read()) # >7.3 vers.
            except:
                raise SchedulerError('Problem reading output of command', cmd)

            # If the command succeeded, close returns None
            # Otherwise, close returns the exit code
            if outputFp.close():
                raise SchedulerError("condor_q command or cache file failed.")

            handler = CondorHandler('GlobalJobId',
                       ['JobStatus', 'GridJobId','ProcId','ClusterId',
                        'MATCH_GLIDEIN_Gatekeeper', 'GlobalJobId'])
            parser = make_parser()
            try:
                parser.setContentHandler(handler)
                parser.setFeature(feature_external_ges, False)
                parser.parse(outputFile)
            except:
                raise SchedulerError('Problem parsing output of command', cmd)

            jobDicts = handler.getJobInfo()

            for globalJobId in jobDicts.keys():
                clusterId = jobDicts[globalJobId].get('ClusterId', None)
                procId    = jobDicts[globalJobId].get('ProcId',    None)
                jobId = str(clusterId) + '.' + str(procId)
                jobStatus = jobDicts[globalJobId].get('JobStatus', None)

                # Host can be either in GridJobId or Glidein match
                execHost = None
                gridJobId = jobDicts[globalJobId].get('GridJobId', None)
                if gridJobId:
                    uri = gridJobId.split(' ')[1]
                    execHost = uri.split(':')[0]
                glideinHost = jobDicts[globalJobId].get('MATCH_GLIDEIN_Gatekeeper', None)
                if glideinHost:
                    execHost = glideinHost

                # Don't mess with jobs we're not interested in,
                # put what we found into BossLite statusRecord
                if bossIds.has_key(schedd+'//'+jobId):
                    statusRecord = {}
                    statusRecord['status']          = statusCodes.get(jobStatus, 'UN')
                    statusRecord['statusScheduler'] = textStatusCodes.get(jobStatus, 'Undefined')
                    statusRecord['statusReason']    = ''
                    statusRecord['service']         = service
                    if execHost:
                        statusRecord['destination'] = execHost

                    bossIds[schedd + '//' + jobId] = statusRecord

        for job in obj.jobs:
            schedulerId = job.runningJob['schedulerId']
            if bossIds.has_key(schedulerId):
                for key, value in bossIds[schedulerId].items():
                    job.runningJob[key] = value
        return


    def kill( self, obj ):
        """
        Kill jobs submitted to a given WMS. Does not perform status check
        """

        for job in obj.jobs:
            if not self.valid( job.runningJob ):
                continue
            schedulerId = str(job.runningJob['schedulerId']).strip()
            submitHost, jobId  = schedulerId.split('//')
            if self.glexec:
                # Set up environment in thread safe manner
                seDir = "/".join((obj['globalSandbox'].split(',')[0]).split('/')[:-1])
                userProxy = obj['user_proxy']
                seProxy   = seDir + '/userProxy'
                commonEnv = 'export GLEXEC_TARGET_PROXY=/tmp/x509_ugeneric; '\
                            'export GLEXEC_CLIENT_CERT=%s; ' \
                            'export GLEXEC_SOURCE_PROXY=%s; ' \
                            'export X509_USER_PROXY=%s; ' % \
                            (userProxy, userProxy, seProxy)

                command  = commonEnv + 'cd %s; ' % seDir
                command += "%s `which condor_rm` -name %s %s" % (self.glexec, submitHost, jobId)
            else:
                command = "condor_rm -name %s %s" % (submitHost, jobId)

            try:
                retcode = call(command, shell=True)
            except OSError, ex:
                raise SchedulerError('condor_rm failed', ex)
            return


    def getOutput( self, obj, outdir='' ):
        """
        Retrieve (move) job output from cache directory to outdir
        User files from CondorG appear asynchronously in the cache directory
        """

        if type(obj) == RunningJob: # The object passed is a RunningJob
            raise SchedulerError('Operation not possible',
                  'CondorG cannot retrieve files when passed RunningJob')
        elif type(obj) == Job: # The object passed is a Job

            # check for the RunningJob integrity
            if not self.valid( obj.runningJob ):
                raise SchedulerError('invalid object', str( obj.runningJob ))

            # retrieve output
            self.getCondorOutput(obj, outdir)

        # the object passed is a Task
        elif type(obj) == Task :

            if outdir == '':
                outdir = obj['outputDirectory']

            for job in obj.jobs:
                if self.valid( job.runningJob ):
                    self.getCondorOutput(job, outdir)

        # unknown object type
        else:
            raise SchedulerError('wrong argument type', str( type(obj) ))


    def getCondorOutput(self, job, outdir):
        """
        Move the files for Condor from temp directory to
        final resting place
        """
        fileList = []
        fileList.append(job['standardOutput'])
        fileList.append(job['standardError'])
        fileList.extend(job['outputFiles'])

        for fileName in fileList:
            try:
                shutil.move(self.condorTemp+'/'+fileName, outdir)
            except IOError:
                self.logging.error( "Could not move file %s" % fileName)



    def postMortem( self, schedulerId, outfile, service):
        """
        Get detailed postMortem job info
        """

        if not outfile:
            raise SchedulerError('Empty filename',
                                 'postMortem called with empty logfile name')

        submitHost, jobId = schedulerId.split('//')
        cmd = "condor_q -l -name  %s %s > %s" % (submitHost, jobId, outfile)
        return self.ExecuteCommand(cmd)


    def jobDescription(self, obj, requirements='', config='', service = ''):
        """
        retrieve scheduler specific job description
        """

        return "Check jdl files in " + self.condorTemp + " after submit\n"


    def x509Proxy(self):
        """
        Return the name of the X509 proxy file (must exist)
        """
        x509 = None
        x509tmp = '/tmp/x509up_u' + str(os.getuid())
        if 'X509_USER_PROXY' in os.environ:
            x509 = os.environ['X509_USER_PROXY']
        elif os.path.isfile(x509tmp):
            x509 = x509tmp
        return x509
