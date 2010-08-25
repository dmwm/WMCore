#!/usr/bin/env python
#pylint: disable-msg=W0613,E1103
"""
_Feeder_
"""



import logging
import threading
import time
import os
from traceback import format_exc

from ProdCommon.FwkJobRep.ReportParser import readJobReport

from WMCore.WMBSFeeder.FeederImpl import FeederImpl 
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.Job import Job
#from WMCore.WMFactory import WMFactory
from WMCore.Services.Requests import JSONRequests
from WMCore.WMInit import WMInit

LOCK = threading.Lock()

class Feeder(FeederImpl):
    """
    feeder implementation
    """

    def __init__(self):
        """
        Configure the feeder
        """

        FeederImpl.__init__(self)

        self.maxRetries = 3
        self.purgeTime = 480 
        self.reopenTime = 120

    def __call__(self, filesetToProcess):
        """
        The algorithm itself
        """

        # Get configuration
        initObj = WMInit()
        initObj.setLogging()
        initObj.setDatabaseConnection(os.getenv("DATABASE"), \
            os.getenv('DIALECT'), os.getenv("DBSOCK"))

        myThread = threading.currentThread()

        daofactory = DAOFactory(package = "WMCore.WMBS" , \
              logger = myThread.logger, \
              dbinterface = myThread.dbi)

        lastFileset = daofactory(classname = "Fileset.ListFilesetByTask")
        lastWorkflow = daofactory(classname = "Workflow.LoadFromTask")
        subsRun = daofactory(\
classname = "Subscriptions.LoadFromFilesetWorkflow")
        successJob = daofactory(classname = "Subscriptions.SucceededJobs")
        allJob = daofactory(classname = "Subscriptions.Jobs")
        fileInFileset = daofactory(classname = "Files.InFileset")


        # Get the start Run if asked
        startRun = (filesetToProcess.name).split(":")[3]
        logging.debug("the T0Feeder is processing %s" % \
                 filesetToProcess.name) 
        logging.debug("the fileset name %s" % \
         (filesetToProcess.name).split(":")[0])

        fileType = (filesetToProcess.name).split(":")[2]
        crabTask = filesetToProcess.name.split(":")[0]
        LASTIME = filesetToProcess.lastUpdate

        tries = 1
        while True:

            try:

                myRequester = JSONRequests(url = "vocms52.cern.ch:8889")
                requestResult = myRequester.get("/tier0/runs")

            except:

                logging.debug("T0Reader call error...")
                if tries == self.maxRetries:
                    return
                else:
                    tries += 1
                    continue


            logging.debug("T0ASTRunChain feeder queries done ...")
            now = time.time()

            break


        for listRun in requestResult[0]:

            
            if startRun != 'None' and int(listRun['run']) >= int(startRun):             
                if listRun['status'] =='CloseOutExport' or listRun\
        ['status']=='Complete' or listRun['status']=='CloseOutT1Skimming':

                    crabWorkflow = lastWorkflow.execute(task=crabTask)
 
                    crabFileset = lastFileset.execute\
                                (task=crabTask)                  

                    crabrunFileset = Fileset(\
    name = crabFileset[0]["name"].split(':')[0].split\
   ('-Run')[0]+ '-Run' + str(listRun['run']) + ":" + \
     ":".join(crabFileset[0]['name'].split(':')[1:]) )

                    if crabrunFileset.exists() > 0: 

                        crabrunFileset.load()
                        currSubs = subsRun.execute\
           (crabrunFileset.id, crabWorkflow[0]['id'])         

                        if currSubs:                         

                            listsuccessJob = successJob.execute(\
                                 subscription=currSubs['id'])
                            listallJob = allJob.execute(\
                                subscription=currSubs['id'])  

                            if len(listsuccessJob) == len(listallJob): 

                                for currid in listsuccessJob:
                                    currjob = Job( id = currid )
                                    currjob.load()

                                    logging.debug("Reading FJR %s" %currjob['fwjr_path'])
        
                                    jobReport = readJobReport(currjob['fwjr_path'])

                                    if len(jobReport) > 0:

                                        
                                        if jobReport[0].files:

                                            for newFile in jobReport[0].files:
                         
                                                logging.debug(\
                               "Output path %s" %newFile['LFN'])
                                                newFileToAdd = File(\
                             lfn=newFile['LFN'], locations ='caf.cern.ch')

                                                LOCK.acquire()
 
                                                if newFileToAdd.exists\
                                                      () == False :

                                                    newFileToAdd.create()
                                                else:
                                                    newFileToAdd.loadData()

                                                LOCK.release()
 
                                                listFile = \
                             fileInFileset.execute(filesetToProcess.id)
                                                if {'fileid': \
                                 newFileToAdd['id']} not in listFile:

                                                    filesetToProcess.addFile(\
                                                        newFileToAdd)
                                                    filesetToProcess\
                                                    .setLastUpdate(now)
                                                    filesetToProcess.commit()
                                                    logging.debug(\
                                                     "new file created/loaded and added by T0ASTRunChain...")
                                    
                                        elif jobReport[0].analysisFiles:

                                            for newFile in jobReport\
                                                [0].analysisFiles:


                                                logging.debug(\
                             "Ouput path %s " %newFile['LFN'])
                                                newFileToAdd = File(\
                               lfn=newFile['LFN'], locations ='caf.cern.ch')

                                                LOCK.acquire()

                                                if newFileToAdd.exists\
                                                     () == False :
                                                    newFileToAdd.create()
                                                else:
                                                    newFileToAdd.loadData()

                                                LOCK.release()
  
                                                listFile = \
                              fileInFileset.execute(filesetToProcess.id)
                                                if {'fileid': newFileToAdd\
                                                  ['id']} not in listFile:

                                                    logging.debug\
                                             ("%s loaded and added by T0ASTRunChain" %newFile['LFN'])
                                                    filesetToProcess.addFile\
                                                         (newFileToAdd)
                                                    filesetToProcess.\
                                                       setLastUpdate(now)
                                                    filesetToProcess.commit()
                                                    logging.debug(\
                                                      "new file created/loaded added by T0ASTRunChain...")
                             
                                        else: break #Missed fjr - Try next time 
                                        

        # Commit the fileset
        logging.debug("Test purge in T0ASTRunChain ...")
        filesetToProcess.load()
        LASTIME = filesetToProcess.lastUpdate


        # For re-opned fileset or empty, try until the purge time
        if (int(now)/3600 - LASTIME/3600) > self.reopenTime:

            filesetToProcess.setLastUpdate(time.time())
            filesetToProcess.commit()

        if (int(now)/3600 - LASTIME/3600) > self.purgeTime:

            filesetToProcess.markOpen(False)
            logging.debug("Purge Done...")
                    	         

    def persist(self):
        """
        To overwrite
        """ 
        pass

