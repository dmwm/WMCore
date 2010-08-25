#!/usr/bin/env python
"""
SchedulerFake
"""

__revision__ = "$Id: SchedulerFake.py,v 1.1 2010/05/21 14:16:41 spigafi Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "filippo.spiga@cern.ch"

from WMCore.BossLite.Scheduler.SchedulerInterface import SchedulerInterface
from WMCore.BossLite.Common.Exceptions import SchedulerError

from WMCore.BossLite.DbObjects.Job import Job
from WMCore.BossLite.DbObjects.Task import Task
# from WMCore.BossLite.DbObjects.RunningJob import RunningJob

class SchedulerFake(SchedulerInterface) :
    """
    SchedulerFake
    """
    
    def __init__( self, **args):
        
        # call super class init method
        super(SchedulerFake, self).__init__(**args)
        
        self.warnings = []
        self.vo = args.get( "vo", "cms" )
        self.service = args.get( "service", "" )
        self.config = args.get( "config", "" )
        self.delegationId = args.get( "proxyname", "bossproxy" )
        
        return
    
    
    def delegateProxy( self, wms = '' ):
        """
        Fake delegate proxy
        """

        command = "glite-wms-job-delegate-proxy -d " + self.delegationId
        
        if wms :
            command += " -e " + wms
            
        if len(self.config) != 0 :
            command += " -c " + self.config
     
        return command
    
    
    def submit( self, obj, requirements='', config ='', service='' ):
        """
        fake submit
        """
        
        if not config :
            config = self.config
        
        fname = "fakeJob.jdl"
        
        if self.delegationId != "" :
            command = "glite-wms-job-submit --json -d " \
                                                + self.delegationId
            self.delegateProxy(service)
            
        else :
            command = "glite-wms-job-submit --json -a "
        
        if len(config) != 0 :
            command += " -c " + config
        
        if service != '' :
            command += ' -e ' + service

        command += ' ' + fname
        
        return command
    
    
    def getOutput( self, obj, outdir='' ):
        """
        Fake getOutput
        """
        
        if outdir == '' or outdir is None :
            outdir = '.'
        
        if type(obj) == Job :
            if not self.valid( obj.runningJob ):
                raise SchedulerError('invalid object', str( obj.runningJob ))
            
            command = "glite-wms-job-output --json --noint --dir " + \
                        outdir + " " + obj.runningJob['schedulerId']
                    
        elif type(obj) == Task :
            for selJob in obj.jobs:
                if not self.valid( selJob.runningJob ):
                    continue
                
                command = "glite-wms-job-output --json --noint --dir " + \
                            outdir + " " + selJob.runningJob['schedulerId']
                
                command += "\n"
        
        else:
            raise SchedulerError('wrong argument type', str( type(obj) ))      

        return command
    
    
    def purgeService( self, obj ):
        """
        Fake purgeService
        """
        
        if type(obj) == Job and self.valid( obj.runningJob ):
            command = "glite-wms-job-output --json --noint --dir /tmp/ " \
                      + obj.runningJob['schedulerId']
            
        elif type(obj) == Task :
            for job in obj.jobs:
                if not self.valid( job.runningJob ):
                    continue
                
                command = "glite-wms-job-output --json --noint --dir /tmp/ " \
                          + job.runningJob['schedulerId']
                
                command += "\n"
        
        return command
            
    
    def kill( self, obj ):
        """
        Fake kill
        """
        
        if type(obj) == Job and self.valid( obj.runningJob ):
            schedIdList = str( obj.runningJob['schedulerId'] ).strip()
        
        elif type(obj) == Task :
            schedIdList = ""
            for job in obj.jobs:
                if not self.valid( job.runningJob ):
                    continue
                schedIdList += " " + \
                               str( job.runningJob['schedulerId'] ).strip()
        
        command = "glite-wms-job-cancel --json --noint " + schedIdList
        
        return command
    
    
    def matchResources( self, obj, requirements='', config='', service='' ):
        """
        Fake matchResources
        """
        
        fname = "fakeMatchingJdl.jdl"
        
        if self.delegationId == "" :
            command = "glite-wms-job-list-match -d " + self.delegationId
            self.delegateProxy(service)
            
        else :
            command = "glite-wms-job-list-match -a "
        
        if len(config) != 0 :
            command += " -c " + config

        if service != '' :
            command += ' -e ' + service

        command += " " + fname
        
        return command
    
    
    def postMortem( self, schedulerId, outfile, service):
        """
        Fake postMortem
        """
        
        command = "glite-wms-job-logging-info -v 3 " + schedulerId + \
                  " > " + outfile
            
        return command
    
    
    def query(self, obj, service='', objType='node') :
        """
        Fake query
        """
        
        jobIds = {}
        parentIds = []
        count = 0
                
        if type(obj) == Task :
            if objType == 'node' :
                for job in obj.jobs :              
                    if job.runningJob is None \
                           or job.runningJob.active != True \
                           or job.runningJob['schedulerId'] is None \
                           or job.runningJob['closed'] == "Y" \
                           or job.runningJob['status'] in self.invalidList :
                        count += 1
                        continue
                    
                    jobIds[ str(job.runningJob['schedulerId']) ] = count    
                    count += 1
                
                if jobIds :
                    formatJobIds = ','.join(jobIds)
                                   
                    command = 'GLiteStatusQuery.py --jobId=%s' % formatJobIds
                    
            elif objType == 'parent' :
                for job in obj.jobs :
                    if self.valid( job.runningJob ) :
                        jobIds[ str(job.runningJob['schedulerId']) ] = count
                        
                        if job.runningJob['schedulerParentId'] \
                                not in parentIds:
                            parentIds.append( 
                                str(job.runningJob['schedulerParentId']))
                        
                    count += 1
            
                if jobIds :
                    formattedParentIds = ','.join(parentIds)
                    formattedJobIds = ','.join(jobIds)
                    
                    command = 'GLiteStatusQuery.py --parentId=%s --jobId=%s' \
                            % (formattedParentIds, formattedJobIds)
            
        else:
            raise SchedulerError('wrong argument type', str( type(obj) ))

        return command
    
    
    def jobDescription (self, obj, requirements='', config='', service = ''):
        """
        Fake jobDescription
        """
        
        return """Routine jobDescription done: requirements = %s\n""" + \
                """\t config = %s\n\t, %s\n""" + \
                """\t service = %s\n\t, %s\n""" % \
                                        (requirements, config, service)
    
    
    def decode  ( self, obj, requirements='' ) :
        """
        fake decode
        """
        
        return "Routine jobDescription done: requirements = %s\n" % \
                                                            (requirements)
        