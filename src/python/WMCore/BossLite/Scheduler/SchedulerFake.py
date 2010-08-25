#!/usr/bin/env python
"""
SchedulerFake
"""





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
     
        # DEBUG
        #print command
        
        return 
    
    
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
        
        # DEBUG
        #print command
        
        returnMap = {}
        if type(obj) == Task :                                    
            for job in obj.jobs:
                    returnMap[job['name']] = "%s-schedulerId" % (job['name'])

            return returnMap, str("XXX1"), str("XX2")
        
        elif type(obj) == Job :
            returnMap[obj['name']] = "%s-schedulerId" % (jobjob['name'])

            return returnMap, str("XXX1"), str("XX2")
        
        else : 
            raise SchedulerError( 'unexpected error',  type(obj) )
    
    
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
            
            # DEBUG
            #print command
                 
        elif type(obj) == Task :
            for selJob in obj.jobs:
                if not self.valid( selJob.runningJob ):
                    continue
                
                command = "glite-wms-job-output --json --noint --dir " + \
                            outdir + " " + selJob.runningJob['schedulerId']
                
                command += "\n"
            
            # DEBUG
            #print command
        
        else:
            raise SchedulerError('wrong argument type', str( type(obj) ))      
    
        return
    
    
    
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
        
        # DEBUG
        #print command
                    
        return
            
    
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
        
        # DEBUG
        #print command
                    
        return
    
    
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
        
        # DEBUG
        #print command
                    
        return
    
    
    def postMortem( self, schedulerId, outfile, service):
        """
        Fake postMortem
        """
        
        command = "glite-wms-job-logging-info -v 3 " + schedulerId + \
                  " > " + outfile
            
        # DEBUG
        #print command
                    
        return 
    
    
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
                    
                    job.runningJob['status'] = 'SD'
                    
                    count += 1
                
                if jobIds :
                    formatJobIds = ','.join(jobIds)
                                   
                    command = 'GLiteStatusQuery.py --jobId=%s' % formatJobIds
                    
                    # DEBUG
                    #print command
                    
            elif objType == 'parent' :
                for job in obj.jobs :
                    if self.valid( job.runningJob ) :
                        jobIds[ str(job.runningJob['schedulerId']) ] = count
                        
                        job.runningJob['status'] = 'SD'
                        
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
                            
                    # DEBUG
                    #print command
            
        else:
            raise SchedulerError('wrong argument type', str( type(obj) ))

        return
    
    
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
        