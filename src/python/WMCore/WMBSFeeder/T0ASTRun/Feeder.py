#!/usr/bin/env python
#pylint: disable-msg=W0613,E1103
"""
_Feeder_
"""
__revision__ = "$Id: Feeder.py,v 1.1 2010/05/04 22:37:46 riahi Exp $"
__version__ = "$Revision: 1.1 $"

import logging
import threading

from WMCore.WMBSFeeder.FeederImpl import FeederImpl 
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.DataStructs.Run import Run
from WMCore.DAOFactory import DAOFactory
#from WMCore.WMFactory import WMFactory
from traceback import format_exc

import time
from WMCore.Services.Requests import JSONRequests

#StartTime = int(time.time())
LASTIME = int(time.time()) 

class Feeder(FeederImpl):
    """
    feeder implementation
    """

    def __init__(self, \
                 urlT0 = "/tier0/listbulkfilesoverinterval/"): 
        """
        Configure the feeder
        """

        FeederImpl.__init__(self)

        self.maxRetries = 3
        self.purgeTime = 3600 #(86400 1 day)

        myThread = threading.currentThread()
        self.daofactory = DAOFactory(package = "WMCore.WMBS" , \
              logger = myThread.logger, \
              dbinterface = myThread.dbi)

    def __call__(self, filesetToProcess):
        """
        The algorithm itself
        """
        global LASTIME    

        myThread = threading.currentThread()

        locationNew = self.daofactory(classname = "Locations.New")
        getFileLoc = self.daofactory(classname = "Files.GetLocation")

        logging.debug("the T0Feeder is processing %s" % \
                 filesetToProcess.name) 
        logging.debug("the fileset name %s" % \
         (filesetToProcess.name).split(":")[0])

        fileType = (filesetToProcess.name).split(":")[2]

        logging.debug("fileType is %s with start run %s" % \
        ((filesetToProcess.name).split(":")[3], \
         (filesetToProcess.name).split(":")[2]))

        # url builder
        primaryDataset = ((filesetToProcess.name).split(":")[0]).split('/')[1]
        processedDataset = ((filesetToProcess.name).split(":")[0]).split('/')[2]
        dataTier = (((filesetToProcess.name\
            ).split(":")[0]).split('/')[3]).split('-')[0]


        #Add if fileset is empty , set LASTIME to 0
        filesetToProcess.loadData()

        # Fisrt call to T0 db for this fileset 
        # Add test for the closed fileset 
        if not filesetToProcess.files:
            LASTIME = 0

        url = "/tier0/listfilesoverinterval/%s/%s/%s/%s/%s" % \
              (fileType, LASTIME, primaryDataset,processedDataset, dataTier)

        tries = 1
        while True:

            try:

                myRequester = JSONRequests(url = "vocms52.cern.ch:8889")
                requestResult = myRequester.get(\
              url+"/"+"?return_type=text/json%2Bdas")
                newFilesList = requestResult[0]["results"] 

            except:

                logging.debug("T0Reader call error...")
                if tries == self.maxRetries:
                    return  
                else:
                    tries += 1 
                    continue

            logging.debug("T0ASTRun queries done ...")
            now = time.time()
            filesetToProcess.last_update = now
            LASTIME = int(newFilesList['end_time']) + 1

            break

        if len(newFilesList['files']):  

     
            try:
                locationNew.execute(siteName = "caf.cern.ch")
            except Exception,e:
                logging.debug("Error when adding new location...")
                logging.debug(e)
                logging.debug( format_exc() )

            for files in newFilesList['files']:
                logging.debug("Files to add %s" %files)

                # Assume parents and LumiSection aren't asked 
                newfile = File(str(files['lfn']), \
           size = files['file_size'], events = files['events'])


                try:
                    if newfile.exists() == False :
                        newfile.create()

                    else:
                        newfile.loadData()

                    for run in files['runs']:

                        filesetRun = Fileset( name = \
       (((filesetToProcess.name).split(':')[0]).split\
       ('/')[0])+'/'+(((filesetToProcess.name).split(\
       ':')[0]).split('/')[1])+'/'+(((filesetToProcess.name\
       ).split(':')[0]).split('/')[2])+'/'+((((filesetToProcess.name\
       ).split(':')[0]).split('/')[3]).split('-')[0])+'-'+'Run'+str\
       (run)+":"+":".join((filesetToProcess.name).split(':')[1:] ) )

                        if filesetRun.exists() == False :
                            filesetRun.create()

                        else:
                            filesetRun.loadData()
   
                        if not newfile['runs']:

                            runSet = set()
                            runSet.add(Run( run, *files['runs'][run]))
                            newfile.addRunSet(runSet)

                        fileLoc = getFileLoc.execute(file = files['lfn'])

                        if 'caf.cern.ch' not in fileLoc:
                            newfile.setLocation(["caf.cern.ch"])

                        else:
                            logging.debug("File already associated to %s\
                                             " %fileLoc)

                        filesetRun.addFile(newfile)
                        logging.debug("new file added...")
                        filesetRun.commit()

                except Exception,e:
                    logging.debug("Error when adding new location...")
                    logging.debug(e)
                    logging.debug( format_exc() )



        else:
            logging.debug("nothing to do...")

        if LASTIME: 

            myRequester = JSONRequests(url = "vocms52.cern.ch:8889")
            requestResult = myRequester.get("/tier0/runs")

            for listRun in requestResult[0]:

                if listRun['status'] == 'CloseOutExport' \
           or listRun['status'] == 'Complete' or listRun\
               ['status'] == 'CloseOutT1Skimming':        

                    logging.debug("Try to find this fileset %s" \
          %(((filesetToProcess.name).split(':')[0]).split('/')[0]\
         )+'/'+(((filesetToProcess.name).split(':')[0]).split('/')\
         [1])+'/'+(((filesetToProcess.name).split(':')[0]).split('/')\
         [2])+'/'+((((filesetToProcess.name).split(':')[0]).split('/'\
         )[3]).split('-')[0])+'-'+'Run'+str(listRun['run'])+":"+\
         ":".join((filesetToProcess.name).split(':')[1:] ) )
 
                    closeFileset = Fileset( name = (((\
        filesetToProcess.name).split(':')[0]).split('/')[0])+'/'+\
        (((filesetToProcess.name).split(':')[0]).split('/')[1])+'/'+\
        (((filesetToProcess.name).split(':')[0]).split('/')[2])+'/'+\
        ((((filesetToProcess.name).split(':')[0]).split('/')[3]).split\
        ('-')[0])+'-'+'Run'+str(listRun['run'])+":"+":".join((\
        filesetToProcess.name).split(':')[1:] ) )
     
                    if closeFileset.exists() != False :

                        logging.debug("Fileset exist %s and will be closed" \
                                %closeFileset.exists())

                        closeFileset = Fileset( id = closeFileset.exists())
                        closeFileset.loadData()

                        if closeFileset.open == True:      
                            logging.debug("Fileset is open")  
                            closeFileset.markOpen(False)

        # Commit the fileset
        filesetToProcess.commit()

    def persist(self):
        """
        To overwrite
        """ 
        pass



