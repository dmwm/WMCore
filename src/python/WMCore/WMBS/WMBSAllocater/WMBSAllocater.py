#!/usr/bin/env python

from sqlalchemy import create_engine
import sqlalchemy.pool as pool
from sqlalchemy.exceptions import IntegrityError, OperationalError
from WMCore.WMBS.Factory import SQLFactory 

from WMCore.WMBS.WMBSAllocater.Registry import retrieveAllocaterImpl, \
                                                        RegistryError
from ProdCommon.ThreadTools import WorkQueue

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.File import File

import logging


def callAllocater(subscription, ms, spec_dir):
    """
    Call relevant external source and get file details
    """
    try:
        feeder = retrieveAllocaterImpl(subscription.type, ms, spec_dir)
    except RegistryError:
        msg = "WMBSAllocater plugin \'%s\' unknown" % subscription.type
        logging.error(msg)
        logging.error("Cant relese jobs for %s" % subscription.workflow.spec)
        raise RuntimeError, msg
    return feeder(subscription)


class WMBSAllocater:
    """
	WMBSAllocater
	
	Class that matches data to jobs in the constraint of the given subscription
	
	Typical workflow: 
		Alerted data exists for a dataset
		find all subscriptions for that dataset
		check input parent data available if required for subscription
		call plugin depending on subscription type to divide files between jobs
		create job specs and publish
		mark taken files as such
	"""
	
    def __init__(self, dbparams, label, spec_dir, **params):
        """
		conect to wmbs db etc..
		"""
		#logger = logging.getLogger()
        
        #TODO: create connection string properly
        engine = create_engine(dbparams['dbName'], convert_unicode=True,
                                    encoding='utf-8', pool_size=10,
                                    pool_recycle=30)
        #factory = SQLFactory(logging)
        factory = SQLFactory(logging)
        self.wmbs = factory.connect(engine)       
        try:
            #TODO: move into client setup script
            self.wmbs.createWMBS()
        except OperationalError:
            pass

        self.label = label
        self.spec_dir = spec_dir
        self.ms = None
        #self.workq = WorkQueue.WorkQueue([callAllocater for x in range(params.get('threads', 5)])
        self.params = params
    
    def setMessager(self, ms):
        """
        set message system, ms is a function that will be called to send
        messages
        """
        self.ms = ms
        
    
    def sendMessage(self, type, payload='', delay='00:00:00'):
        """
        function that should be overriden by implementation to send messages
        """
        
        if self.ms is not None:
            return self.ms(type, payload, delay)
    
        raise NotImplementedError, 'sendMessage not set via setMessager()'
   


    def allocateJobs(self, filesetName):
        """
    	Allocate jobs for the given dataset
    	
    	find all subscriptions for that dataset
		check input parent data available if required for subscription
		call plugin depending on subscription type to divide files between jobs
		create job specs
		mark taken files as such
    	"""
        
        jobsCreated = []
        
        fileset = Fileset(filesetName, self.wmbs).populate()
        
        for subscription in fileset.subscriptions():
            subscription.load()
            availableFiles = subscription.availableFiles() 
            
            if not availableFiles:
                logging.info("No files for subscription")
                continue
            
            #Prob not worth this - do in line
            #self.workq.enqueue(subscription, subscription, 
            #                   availableFiles, self.params)
            jobs = callAllocater(subscription, availableFiles, self.params)
            jobsCreated.extend(jobs)

        # No longer needed if workQ not used
        #for subscription, jobs in self.workq:
        #    jobsCreated.extend(jobs)
            
        # let caller do what it wants with job specs
        return jobsCreated
        
        




            
            
    	
    	