#!/usr/bin/env python

import os, logging, random, commands, time

from WMCore_t.Database_t.Performance import Performance
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Workflow import Workflow
from sets import Set

class WMBSBase(Performance):
    """
    __WMBSBase__

    Base class for Performance Tests at WMBS

    This class is abstract, proceed to the DB specific testcase
    to run the test


    """

    def genFileObjects(self, number=0):
        # Create a Fileset of random, parentless, childless, unlocatied file
        filelist = []

        #Generating Files
        if number == 0:
            rangemax = random.randint(1000,3000)
        else:
            rangemax = number

        for x in range(rangemax):
            file = File(lfn='/store/data/genfile'+str(x)+'.root',
                        size=random.randint(1000, 2000),
                        events = 1000,
                        run = random.randint(0, 2000),
                        lumi = random.randint(0, 8), 
                        logger=self.logger, 
                        dbfactory=self.dbf)
            
            filelist.append(file)
        return filelist

    def genFiles(self, number=0):
        
        filelist = self.genFileObjects(number)

        setfiles = set(filelist)

        fileset = Fileset(name='genFilesSet', 
                            files=setfiles, 
                            logger=self.logger, 
                            dbfactory=self.dbf)
        print fileset.name
        fileset.create()
    
        filelist = list(fileset.getFiles())

        return filelist

    def genLocations(self, number=0):
        list = []

        if number == 0:
            rangemax = random.randint(1000,3000)
        else:
            rangemax = number

        for x in range(rangemax):
            list.append("Location"+str(x))

        return list

    def genFileset(self, number=0):

        list = []

        if number == 0:
            rangemax = random.randint(1000,3000)
        else:
            rangemax = number

        
        for i in range(rangemax):        
            filelist = self.genFileObjects(1)
            fileset = Fileset(name='testFileSet'+str(i), 
                            files=set(filelist), 
                            logger=self.logger, 
                            dbfactory=self.dbf) 
            fileset.create()
            list.append(fileset)        
     
        return list

    def genWorkflow(self, number=0):

        list = []

        if number == 0:
            rangemax = random.randint(1000,3000)
        else:
            rangemax = number

        for i in range(rangemax):        
            workflow = Workflow(spec='Test', owner='PerformanceTestCase', name='TestWorkflow'+str(i), logger=self.logger, dbfactory=self.dbf)
            workflow.create()
            list.append(workflow)

        return list

    def genSubscription(self, number=0):

        list = []

        if number == 0:
            rangemax = random.randint(1000,3000)
        else:
            rangemax = number

        for i in range(rangemax):        
            list = self.genWorkflow(number=1)
            workflow = list[0]

            list = self.genFileset(number=1)
            fileset = list[0]

            subscription = Subscription(fileset=fileset, 
                        workflow=workflow, logger=self.logger, 
                        dbfactory=self.dbf)
            subscription.create()

            list.append(subscription)

        return list


    def genJob(self, number=0):

        list = []

        if number == 0:
            rangemax = random.randint(1000,3000)
        else:
            rangemax = number

        for i in range(rangemax):        
            list = self.genFileset(number=1)
            fileset = list[0]
            list = self.genSubscription(number=1)
            subscription = list[0]
        
            job = Job(name='TestJob'+str(i),files=fileset, logger=self.logger, dbfactory=self.dbf)
            jobset = Set()
            jobset.add(job)
        
            jobgroup = JobGroup(subscription=subscription, jobs=jobset)
            job.create(group = jobgroup)
            
            list.append(job)

            return list

    def genJobGroup(self, number=0):

        list = []
        jobs = []
        set = Set()

        if number == 0:
            rangemax = random.randint(1000,3000)
        else:
            rangemax = number

        for i in range(rangemax):        

            subscription = self.genSubscription(number=1)[0]            

            jobs = self.genJob(number=0)

            for j in jobs:
                set.add(j)

                jobgroup = JobGroup(subscription=subscription, jobs=set)
                list.append(jobgroup)

        return list
 
    def setUp(self, dbf):
        """
        Common setUp for all Performance tests

        """
        
        Performance.setUp(self)
        #Place common execute method arguments here        
        #TODO -Still to be implemented
        self.baseexec=''

        #Threshold settings:
        self.threshold = 0 
        self.totalthreshold = 0

        #possibly deprecated, need to use selist instead
        self.sename='localhost'        
        
        self.tearDown()

        self.dbf=dbf

        self.dao = DAOFactory(package='WMCore.WMBS', logger=self.logger, 
                        dbinterface=self.dbf.connect())
        
        assert self.dao(classname='CreateWMBS').execute()       

        #Creating the Locations at the Database
        self.selist = ['localhost']        
        for se in self.selist:
            self.dao(classname='Locations.New').execute(sename=se)      

    def tearDown(self):
        #Base tearDown method for the DB Performance test
        Performance.tearDown(self)
        pass

    
