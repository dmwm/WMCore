#!/usr/bin/env python
#pylint: disable-msg=W0613,E1103
"""
_Feeder_
"""
__revision__ = "$Id: Feeder.py,v 1.5 2010/05/04 22:41:04 riahi Exp $"
__version__ = "$Revision: 1.5 $"

import logging
import threading

from WMCore.WMBSFeeder.FeederImpl import FeederImpl 
from WMCore.WMBS.File import File
#from WMCore.WMBS.Fileset import Fileset
from WMCore.DataStructs.Run import Run
from WMCore.DAOFactory import DAOFactory
#from WMCore.WMFactory import WMFactory
from traceback import format_exc

import time
from WMCore.Services.Requests import JSONRequests

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

        locationNew = self.daofactory(classname = "Locations.New")
        getFileLoc = self.daofactory(classname = "Files.GetLocation")

        logging.debug("the T0Feeder is processing %s" % \
                 filesetToProcess.name) 
        logging.debug("the fileset name %s" % \
         (filesetToProcess.name).split(":")[0])

        # Get the start Run if asked
        startRun = (filesetToProcess.name).split(":")[3]

        fileType = (filesetToProcess.name).split(":")[2]
        logging.debug("the fileType is %s" % \
        (filesetToProcess.name).split(":")[2])
        
        #Add if fileset is empty , set LASTIME to 0
        logging.debug("The fileset object %s" %filesetToProcess.files) 

        # Fisrt call to T0 db for this fileset 
        if not filesetToProcess.files: 
            LASTIME = 0
 
        # url builder
        primaryDataset = ((filesetToProcess.name).split(":")[0]).split('/')[1]
        processedDataset = ((filesetToProcess.name).split(":")[0]).split('/')[2]
        dataTier = ((filesetToProcess.name\
            ).split(":")[0]).split('/')[3]
        url = "/tier0/listfilesoverinterval/%s/%s/%s/%s/%s" % \
              (fileType, LASTIME, primaryDataset,processedDataset, dataTier)

        tries = 1
        while True:

            try:

                myRequester = JSONRequests(url = "vocms52.cern.ch:8889")
                requestResult = myRequester.get(\
            url+"/"+"?return_type=text/json%2Bdas")
                logging.debug("Res %s" %str(requestResult))
                newFilesList = requestResult[0]["results"]

            except:

                logging.debug("T0Reader call error...")
                if tries == self.maxRetries:
                    return  
                else:
                    tries += 1 
                    continue

            logging.debug("T0 queries done ...")
            now = time.time()
            filesetToProcess.last_update = now
            LASTIME = int(newFilesList['end_time']) + 1

            break

        # process all files
        if len(newFilesList['files']):  

     
            try:
                locationNew.execute(siteName = "caf.cern.ch")
            except Exception,e:
                logging.debug("Error when adding new location...")
                logging.debug(e)
                logging.debug( format_exc() )

            for files in newFilesList['files']:
                logging.debug("Files to add %s" %files)

                newfile = File(str(files['lfn']), \
           size = files['file_size'], events = files['events'])


                try:
                    if newfile.exists() == False :
                        newfile.create()

                    else:
                        newfile.loadData()

                    #Add run test if already exist
                    for run in files['runs']:

                        if not newfile['runs']:

                            runSet = set()
                            runSet.add(Run( run, *files['runs'][run]))
                            newfile.addRunSet(runSet)


                    fileLoc = getFileLoc.execute(file = files['lfn'])

                    if 'caf.cern.ch' not in fileLoc:
                        newfile.setLocation(["caf.cern.ch"])

                    else:
                        logging.debug("File already associated to %s" %fileLoc)

                    if len(newfile["runs"]):

                        val = 0
                        for run in newfile['runs']:

                            if run.run < int(startRun):
                                val = 1 
                                break 

                        if not val:
                            filesetToProcess.addFile(newfile)
                            logging.debug("new file added...")

                except Exception,e:
                    logging.debug("Error when adding new location...")
                    logging.debug(e)
                    logging.debug( format_exc() )



        else:
            logging.debug("nothing to do...")

        # Commit the fileset
        filesetToProcess.commit()

    def persist(self):
        """
        To overwrite
        """ 
        pass



