#!/usr/bin/env python
"""
_BossLiteAPI_

"""





# db interaction
from WMCore.BossLite.API.BossLiteAPI import BossLiteAPI

# Scheduler interaction
from WMCore.BossLite.Scheduler import Scheduler
from WMCore.BossLite.Common.BossLiteLogger import BossLiteLogger
from WMCore.BossLite.Common.Exceptions import BossLiteError, SchedulerError


class BossLiteAPISched(object):
    """
    High level API class for DBObjects and Scheduler interaction.
    It allows load/operate/update Jobs and Tasks using just id ranges
    """


    def __init__(self, bossLiteSession, schedulerConfig, task=None):
        """
        Initialize the scheduler API instance:
        - bossLiteSession is a BossLiteAPI instance
        - schedulerConfig is a dictionary with these fields
           {'name' : 'SchedulerGLiteAPI', 'user_proxy' : '/proxy/path',
            'service' : 'https://wms104.cern.ch:7443/glite_wms_wmproxy_server',
            'config' : '/etc/glite_wms.conf' }

        """

        # set BossLiteLogger
        self.bossLiteLogger = None

        # use bossLiteSession for DB interactions
        if type( bossLiteSession ) is not BossLiteAPI:
            raise TypeError( "first argument must be a BossLiteAPI object")

        self.bossLiteSession = bossLiteSession
        #global GlobalLogger

        # update scheduler config
        self.schedConfig = {'user_proxy' : '', 'service' : '', 'config' : '' }
        self.schedConfig.update( schedulerConfig )

        # something to be retrieved from task object?
        if task is not None :

            # retrieve the scheduler
            if not self.schedConfig.has_key('name') :
                for job in task.jobs :
                    if job.runningJob is not None:
                        if job.runningJob['scheduler'] is not None:
                            self.schedConfig['name'] = \
                                        job.runningJob['scheduler']

            # retrieve the user_proxy
            if task['user_proxy'] is not None:
                self.schedConfig['user_proxy'] = task['user_proxy']



        if not self.schedConfig.has_key('name') :
            raise SchedulerError( 'Invalid scheduler', \
                                  'Missing scheduler name in configuration' )

        # scheduler - it uses WMFactory internally
        self.scheduler = Scheduler.Scheduler(
            self.schedConfig['name'], self.schedConfig
            )
        
        return
    
    
    def getSchedulerInterface(self):
        """
        returns the SchedulerInterface object
        """

        return self.scheduler.schedObj
    
    
    def getLogger(self):
        """
        returns the BossLiteLogger object
        """

        return self.bossLiteLogger
    
    
    def submit( self, taskId = None, taskObj = None, jobRange='all', 
                        requirements='', schedulerAttributes=None ):
        """
        Submit to the scheduler (eventually it creates running instances
        - taskId can be both a Task object or the task id
        - jobRange can be of the form: 'a,b:c,d,e' OR ['a',b','c'] OR 'all'
        - requirements are scheduler attributes affecting all jobs
        - jobAttributes is a map of running attributes
        """
        
        # checks...
        if not taskId and not taskObj :
            raise SchedulerError( 'Invalid parameter', \
                                  'Missing taskId or taskObj parameter' )
            
        if taskId and taskObj :
            raise SchedulerError( 'Too many parameter', \
                            'Use only taskId or taskObj at the same time' )
            
        task = None
        
        try:

            if taskObj and (not taskId):
                # task already loaded
                task = taskObj
                
            elif taskId and (not taskObj) :
                # load task
                task = self.bossLiteSession.loadTask( taskId = taskId, \
                                                        jobRange = jobRange )
            else :
                # something goes wrong ...
                raise SchedulerError( 'Invalid parameter', \
                                  'Check taskId or taskObj parameter' )
            
            # create or load running instances
            for job in task.jobs:
                if job.runningJob is not None :
                    job.runningJob['schedulerAttributes'] = schedulerAttributes

            # scheduler submit
            self.scheduler.submit( task, requirements )

            # update & set logger
            self.bossLiteSession.updateDB( task )
            self.bossLiteLogger = BossLiteLogger( task )

        except BossLiteError, e:

            # update & set logger
            self.bossLiteSession.updateDB( task )
            self.bossLiteLogger = BossLiteLogger( task, e )

            # re-throw exception
            raise

        # return updated task
        return task
    
    
    def resubmit( self, taskId = None, taskObj = None, jobRange='all', 
                        requirements='', schedulerAttributes=None ):
        """
        It archives existing submission and creates a new entry for the next
        submission (i.e. duplicate the entry with an incremented submission 
        number) instances and submit to the scheduler.
        - taskId can be both a Task object or the task id
        - jobRange can be of the form: 'a,b:c,d,e' OR ['a',b','c'] OR 'all'
        - requirements are scheduler attributes affecting all jobs
        - jobAttributes is a map of running attributes
        """
        
        # checks...
        if not taskId and not taskObj :
            raise SchedulerError( 'Invalid parameter', \
                                  'Missing taskId or taskObj parameter' )
            
        if taskId and taskObj :
            raise SchedulerError( 'Too many parameter', \
                            'Use only taskId or taskObj at the same time' )
            
        task = None

        try:
            
            if taskObj and (not taskId):
                # task already loaded
                task = taskObj
                
            elif taskId and (not taskObj) :
                # load task
                task = self.bossLiteSession.loadTask( taskId = taskId, \
                                                        jobRange = jobRange )
            else :
                # something goes wrong ...
                raise SchedulerError( 'Invalid parameter', \
                                  'Check taskId or taskObj parameter' )

            # get new running instance
            for job in task.jobs:
                self.bossLiteSession.getNewRunningInstance(
                    job, { 'schedulerAttributes' : schedulerAttributes }
                    )

            # scheduler submit
            self.scheduler.submit( task, requirements )

            # update & set logger
            self.bossLiteSession.updateDB( task )
            self.bossLiteLogger = BossLiteLogger( task )

        except BossLiteError, e:

            # update & set logger
            self.bossLiteSession.updateDB( task )
            self.bossLiteLogger = BossLiteLogger( task, e )

            # re-throw exception
            raise

        # return updated task
        return task
    
    
    def query( self, taskId = None, taskObj = None, 
                                    jobRange='all', queryType='node' ):
        """
        query status and eventually other scheduler related information
        - taskId can be both a Task object or the task id
        - jobRange can be of the form: 'a,b:c,d,e' OR ['a',b','c'] OR 'all'
        - query type can be 'parent' status check performed via bulk id
                            'node' otherwise
        """
        
        # checks...
        if not taskId and not taskObj :
            raise SchedulerError( 'Invalid parameter', \
                                  'Missing taskId or taskObj parameter' )
            
        if taskId and taskObj :
            raise SchedulerError( 'Too many parameter', \
                            'Use only taskId or taskObj at the same time' )
        
        task = None

        try:
            
            if taskObj and (not taskId):
                # task already loaded
                task = taskObj
                
            elif taskId and (not taskObj) :
                # load task
                task = self.bossLiteSession.loadTask( taskId = taskId, \
                                                        jobRange = jobRange )
            else :
                # something goes wrong ...
                raise SchedulerError( 'Invalid parameter', \
                                  'Check taskId or taskObj parameter' )
            
            # scheduler query
            self.scheduler.query( task, queryType )

            # update & set logger
            self.bossLiteSession.updateRunningInstances( task )
            self.bossLiteLogger = BossLiteLogger( task )

        except BossLiteError, e:

            # update & set logger
            self.bossLiteSession.updateRunningInstances( task )
            self.bossLiteLogger = BossLiteLogger( task, e )

            # re-throw exception
            raise

        # return task updated
        return task
    
    
    def getOutput( self, taskId = None, taskObj = None, 
                                            jobRange='all', outdir='' ):
        """
        It retrieves the output or just put it in the destination directory
        - taskId can be both a Task object or the task id
        - jobRange can be of the form: 'a,b:c,d,e' OR ['a',b','c'] OR 'all'
        - outdir is the output directory for files retrieved
        """
        
        # checks...
        if not taskId and not taskObj :
            raise SchedulerError( 'Invalid parameter', \
                                  'Missing taskId or taskObj parameter' )
            
        if taskId and taskObj :
            raise SchedulerError( 'Too many parameter', \
                            'Use only taskId or taskObj at the same time' )
        
        task = None

        try:

            if taskObj and (not taskId):
                # task already loaded
                task = taskObj
                
            elif taskId and (not taskObj) :
                # load task
                task = self.bossLiteSession.loadTask( taskId = taskId, \
                                                        jobRange = jobRange )
            else :
                # something goes wrong ...
                raise SchedulerError( 'Invalid parameter', \
                                  'Check taskId or taskObj parameter' )
            
            # scheduler query
            self.scheduler.getOutput( task, outdir )

            # update & set logger
            self.bossLiteSession.updateRunningInstances( task )
            self.bossLiteLogger = BossLiteLogger( task )

        except BossLiteError, e:

            # update & set logger
            self.bossLiteSession.updateRunningInstances( task )
            self.bossLiteLogger = BossLiteLogger( task, e )

            # re-throw exception
            raise

        # return task updated
        return task
    
    
    def kill( self, taskId = None, taskObj = None, jobRange='all' ):
        """
        It kills job instances
        - taskId can be both a Task object or the task id
        - jobRange can be of the form: 'a,b:c,d,e' OR ['a',b','c'] OR 'all'
        """
        
        # checks...
        if not taskId and not taskObj :
            raise SchedulerError( 'Invalid parameter', \
                                  'Missing taskId or taskObj parameter' )
            
        if taskId and taskObj :
            raise SchedulerError( 'Too many parameter', \
                        'Use only taskId or taskObj at the same time' )
        
        task = None

        try:

            if taskObj and (not taskId):
                # task already loaded
                task = taskObj
                
            elif taskId and (not taskObj) :
                # load task
                task = self.bossLiteSession.loadTask( taskId = taskId, \
                                                        jobRange = jobRange )
            else :
                # something goes wrong ...
                raise SchedulerError( 'Invalid parameter', \
                                  'Check taskId or taskObj parameter' )
            
            # scheduler query
            self.scheduler.kill( task )

            # update & set logger
            self.bossLiteSession.updateRunningInstances( task )
            self.bossLiteLogger = BossLiteLogger( task )

        except BossLiteError, e:

            # update & set logger
            self.bossLiteSession.updateRunningInstances( task )
            self.bossLiteLogger = BossLiteLogger( task, e )

            # re-throw exception
            raise

        # return task updated
        return task
    
    
    def matchResources( self, taskId = None, taskObj = None, jobRange='all', 
                                requirements='', schedulerAttributes=None ) :
        """
        It performs a resources discovery
        - taskId can be both a Task object or the task id
        - jobRange can be of the form: 'a,b:c,d,e' OR ['a',b','c'] OR 'all'
        """
        
        # checks...
        if not taskId and not taskObj :
            raise SchedulerError( 'Invalid parameter', \
                                  'Missing taskId or taskObj parameter' )
            
        if taskId and taskObj :
            raise SchedulerError( 'Too many parameter', \
                            'Use only taskId or taskObj at the same time' )
        
        task = None

        try:

            if taskObj and (not taskId):
                # task already loaded
                task = taskObj
                
            elif taskId and (not taskObj) :
                # load task
                task = self.bossLiteSession.loadTask( taskId = taskId, \
                                                        jobRange = jobRange )
            else :
                # something goes wrong ...
                raise SchedulerError( 'Invalid parameter', \
                                  'Check taskId or taskObj parameter' )
            
            # retrieve running instances
            for job in task.jobs:
                if job.runningJob is not None :
                    job.runningJob['schedulerAttributes'] = schedulerAttributes

            # scheduler matchResources
            resources = self.scheduler.matchResources( task, requirements )

            # update & set logger
            self.bossLiteSession.updateRunningInstances( task )
            self.bossLiteLogger = BossLiteLogger( task )

        except BossLiteError, e:

            # update & set logger
            self.bossLiteSession.updateRunningInstances( task )
            self.bossLiteLogger = BossLiteLogger( task, e )

            # re-throw exception
            raise

        # return list of sites
        return resources
    
    
    def jobDescription ( self, taskId = None, taskObj = None, jobRange='all', 
                                requirements='', schedulerAttributes=None ):
        """
        It queries status and eventually other scheduler related information
        - taskId can be both a Task object or the task id
        - jobRange can be of the form: 'a,b:c,d,e' OR ['a',b','c'] OR 'all'
        - requirements are scheduler attributes affecting all jobs
        - jobAttributes is a map of running attributes
        """
        
        # checks...
        if not taskId and not taskObj :
            raise SchedulerError( 'Invalid parameter', \
                                  'Missing taskId or taskObj parameter' )
            
        if taskId and taskObj :
            raise SchedulerError( 'Too many parameter', \
                            'Use only taskId or taskObj at the same time' )
        
        task = None

        try:
            
            if taskObj and (not taskId):
                # task already loaded
                task = taskObj
                
            elif taskId and (not taskObj) :
                # load task
                task = self.bossLiteSession.loadTask( taskId = taskId, \
                                                        jobRange = jobRange )
                
            else :
                # something goes wrong ...
                raise SchedulerError( 'Invalid parameter', \
                                  'Check taskId or taskObj parameter' )
            
            # retrieve running instances
            for job in task.jobs:
                if job.runningJob is not None :
                    job.runningJob['schedulerAttributes'] = schedulerAttributes

            jdString = self.scheduler.jobDescription ( task, requirements )

        except BossLiteError, e:

            # set logger
            self.bossLiteLogger = BossLiteLogger( task, e )

            # re-throw exception
            raise

        # return updated task
        return jdString
    
    
    def purgeService( self, taskId = None, taskObj = None, jobRange='all') :
        """
        It purges the service used by the scheduler
        - taskId can be both a Task object or the task id
        - jobRange can be of the form: 'a,b:c,d,e' OR ['a',b','c'] OR 'all'
        """
        
        # checks...
        if not taskId and not taskObj :
            raise SchedulerError( 'Invalid parameter', \
                                  'Missing taskId or taskObj parameter' )
            
        if taskId and taskObj :
            raise SchedulerError( 'Too many parameter', \
                            'Use only taskId or taskObj at the same time' )
        
        task = None

        try:
            
            if taskObj and (not taskId):
                # task already loaded
                task = taskObj
                
            elif taskId and (not taskObj) :
                # load task
                task = self.bossLiteSession.loadTask( taskId = taskId, \
                                                        jobRange = jobRange )
                
            else :
                # something goes wrong ...
                raise SchedulerError( 'Invalid parameter', \
                                  'Check taskId or taskObj parameter' )
            
            # purge task
            self.scheduler.purgeService( task )

            # update & set logger
            self.bossLiteSession.updateRunningInstances( task )
            self.bossLiteLogger = BossLiteLogger( task )

        except BossLiteError, e:

            # update & set logger
            self.bossLiteSession.updateRunningInstances( task )
            self.bossLiteLogger = BossLiteLogger( task, e )

            # re-throw exception
            raise

        # return updated task
        return task
    
    
    def postMortem ( self, taskId = None, taskObj = None, 
                                jobRange='all', outfile='loggingInfo.log') :
        """
        It executes a post-mortem command
        - taskId can be both a Task object or the task id
        - jobRange can be of the form: 'a,b:c,d,e' OR ['a',b','c'] OR 'all'
        - outfile is the physical file where to log post-mortem informations
        """
        
        # checks...
        if not taskId and not taskObj :
            raise SchedulerError( 'Invalid parameter', \
                                  'Missing taskId or taskObj parameter' )
            
        if taskId and taskObj :
            raise SchedulerError( 'Too many parameter', \
                            'Use only taskId or taskObj at the same time' )
        
        
        if taskObj and (not taskId):
            # task already loaded
            task = taskObj
                
        elif taskId and (not taskObj) :
            # load task
            task = self.bossLiteSession.loadTask( taskId = taskId, \
                                                    jobRange = jobRange )
            
        else :
            # something goes wrong ...
            raise SchedulerError( 'Invalid parameter', \
                              'Check taskId or taskObj parameter' )
        
        # scheduler query
        self.scheduler.postMortem( task, outfile )

        return
    