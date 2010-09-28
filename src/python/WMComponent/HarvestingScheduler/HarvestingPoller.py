#!/usr/bin/env python
"""
_HarvestingPoller_

Polls PhEDEx and Global DBS for complete datasets and launches harvesting jobs
"""
import time
import threading
import logging
import sys
import os
import re
import traceback

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.Agent.Harness import Harness
from WMCore.DAOFactory import DAOFactory

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset

from WMCore.DataStructs.Run import Run
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.DBS.DBSReader import DBSReader

from WMCore.WMSpec.StdSpecs.Harvesting import HarvestingWorkloadFactory
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker

from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow

from WMComponent.HarvestingScheduler.DQMCouchAPI import DQMCouchAPI


class HarvestingPoller(BaseWorkerThread):
    def __init__(self, config):
        BaseWorkerThread.__init__(self)
        self.config = config
        return

    def setup(self, parameters = None):
        """
        _setup_

        Setup all the parameters.
        """
        logging.info("HarvestingScheduler Component Started")

        # Workload related parameters
        self.harvestingFactory = HarvestingWorkloadFactory()
        self.workloads = {}
        self.workloadCache = self.config.HarvestingScheduler.workloadCache
        if not os.path.exists(self.workloadCache):
            msg = "Workload cache directory does not exists. Creating it."
            logging.info(msg)
            os.makedirs(self.workloadCache)
        else:
            msg = "Worload cache directory already exists."
            logging.info(msg)

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMCore.WMBS", logger = logging,
                                     dbinterface = myThread.dbi)

        # Scram arch and path to cmssw needed to generate workflows.
        self.scramArch = self.config.HarvestingScheduler.scramArch
        self.cmsPath = self.config.HarvestingScheduler.cmsPath

        # DQM upload and stage out options
        self.proxy = self.config.HarvestingScheduler.proxy
        self.dqmGuiUrl = self.config.HarvestingScheduler.dqmGuiUrl
        self.doStageOut = self.config.HarvestingScheduler.doStageOut
        self.doDqmUpload = self.config.HarvestingScheduler.doDqmUpload
        self.dqmBaseLFN = getattr(self.config.HarvestingScheduler,
                                  'dqmBaseLFN',
                                  '/store/temp/WMAgent/dqm')

        # Dataset polling control
        self.expiryTime = self.config.HarvestingScheduler.expiryTime
        self.cooloffTime = self.config.HarvestingScheduler.cooloffTime

        # TODO: make sure the number of files per jobs is not bounded to 1000

        self.targetSite = self.config.HarvestingScheduler.targetSite
        self.dqmDatabaseAPI = DQMCouchAPI(self.config)
        self.phedex = PhEDEx(
                    {"endpoint": self.config.HarvestingScheduler.phedexURL},
                    "json")
        self.reader = DBSReader(self.config.HarvestingScheduler.dbsUrl)
        return

    def checkTargetSite(self):
        """
        _checkTargetNode_

        Consistency check for the target site in configuration
        """
        se = self.targetSite
        if not self.phedex.getNodeNames(se):
            return False
        return True

    def getTransferredBlocks(self, dataset):
        """
        _getTransferredBlocks_

        Queries PhEDEx datasvc and returns a set of blocks from the
        input dataset. This set indicates if the blocks are complete at
        the target node.

        If a block is not complete, it won't be in the returned set
        """
        se = self.targetSite
        blockReplicas = self.phedex.getReplicaInfoForBlocks(dataset=dataset,
                                                            se=se)
        blocks = blockReplicas['phedex']['block']

        if not blocks:
            return set()

        results = set()
        for block in blocks:
            for replica in block['replica']:
                if replica['complete'].lower() == 'y':
                    results.add(block['name'])

        return results

    def getInfoFromDBS(self, dataset, newBlocks, inputRelease=None):
        """
        _getInfoFromDBS_

        Queries all the files in the dataset from global DBS. Then composes
        one dictionary:
            {run: {"blocks": {block1: [files],
                              block2: [files]},
                   "release": release,
                   "newBlocks": boolean}}

        newBlocks is used to set the "newBlocks" key to true as soon as a run
        has a single new block.

        If release is different than none, then the DBS query will be less
        heavy.

        """
        runs = {}
        unknownRelease = inputRelease in (None, '')

        if unknownRelease:
            retrieveList = ['retrive_lumi', 'retrive_run', 'retrive_algo']
        else:
            retrieveList = ['retrive_lumi', 'retrive_run']

        dbsResult = self.reader.dbs.listFiles(dataset,
                                              retriveList=retrieveList)

        for item in dbsResult:
            # I Assume that one file will have info from only one run
            run = item["RunsList"][0]["RunNumber"]
            blockName = item["Block"]["Name"]

            if unknownRelease: 
                release = item['AlgoList'][0]['ApplicationVersion']
            else:
                release = inputRelease

            runs.setdefault(run, {}).setdefault("blocks", {}).setdefault(blockName, []).append(item)

            # I assume that there's only one release per run
            if runs[run].get("release", None) is None:
                runs[run]["release"] = release

            if not runs[run].setdefault("newBlocks", False) and \
                                                    blockName in newBlocks:
                runs[run]["newBlocks"] = True

        return runs

    def getDatasetParts(self, dataset):
        """
        _getDatasetParts_

        Extract primaryDataset, processedDataset and dataTier and
        retuns a tuple
        """
        regExp = r'^/([-A-Za-z0-9_]+)/([-A-Za-z0-9_]+)/([-A-Za-z0-9_]+)$'
        m = re.match(regExp, dataset)
        return m.groups()

    def getGlobalTagFromDBS(self, dataset):
        """
        _getGlobalTagFromDBS_

        Query DBS in order to retrieve the global tag used during the
        production of the given dataset.

        """
        parts = self.getDatasetParts(dataset)
        globalTag = self.reader.dbs.listProcessedDatasets(
                                        patternPrim=parts[0],
                                        patternProc=parts[1],
                                        patternDT=parts[2]
                                        )[0]['GlobalTag']
        return globalTag

    def getDatasetsToWatch(self):
        """
        _getDatasetsToWatch_

        Queries the couch db in order to fecth all the 'open' datasets whose
        'last_check' timestamp is older than a cooloff time.

        The whole idea of doing this is not to poll for the same dataset
        every polling cycle, is new blocsk come available, let's give it
        some time to be many enough.

        Return a list of all the documents fetched from the couch db
        """
        openDatasets = self.dqmDatabaseAPI.listOpenDatasets()
        self.watchedDatasets = []
        total = 0
        cooloff = 0
        toProcess = 0
        # Filter older datasets
        for dataset in openDatasets:
            # If this is the first time we check the dataset, then the
            # creation date is the same last_check time stamp.
            if int(dataset["last_check"]) == int(dataset["creation_date"]):
                self.watchedDatasets.append(dataset)
                toProcess += 1
            elif int(int(time.time() - dataset["last_check"])) > self.cooloffTime:
                self.watchedDatasets.append(dataset)
                toProcess += 1
            else:
                cooloff += 1
            total += 1

        msg = "Total open datasets: %s. " \
              "Datasets in cooloff: %s. " \
              "Datasets to process: %s." % (total, cooloff, toProcess)
        logging.info(msg)

    def closeOldDatasets(self):
        """
        _closeOldDatasets_

        If the 'last_update' time is older than the expiryTime, then the
        dataset will be declared as closed.

        'last_update' means the last time a new block was added to the
        processed list.

        The expiryTime might be a value around two or four weeks.

        After closing the dataset, all its blocks will be deleted from the
        database, it's expected that this dataset won't be processed anymore.
        """
        datasetsToRemove = []
        datasetsToKeep = []
        for dataset in self.watchedDatasets:
            if int(int(time.time() - dataset["last_update"])) > self.expiryTime:
                # Setting the deletion flag to True
                dataset["status"] = "closed"
                datasetsToRemove.append(dataset)
            else:
                datasetsToKeep.append(dataset)

        # New list of watched datasets
        self.watchedDatasets = datasetsToKeep

        # Deleting datasets
        self.dqmDatabaseAPI.updateDatasets(datasetsToRemove, 'ready')
        logging.info("Closed %s dataset in couch db" % len(datasetsToRemove))

        # Deleting blocks
        self.dqmDatabaseAPI.deleteAllBlocksFromDataset(
                                        [x["_id"] for x in datasetsToRemove])

    def algorithm(self, parameters = None):
        """
        _algorithm_

        """

        # Some checks...
        if not self.checkTargetSite():
            msg = 'The following target se\'s do not have associated node' \
                  ' in PhEDEx:\n%s\nPlease fix this.' \
                  ' Waiting for next polling cycle' % missingSites.join(',')
            logging.error(msg)
            return

        logging.info("Polling for watched datasets in couch db.")
        self.getDatasetsToWatch()
        logging.info("Found %s datasets," % len(self.watchedDatasets))

        logging.info("Closing old datasets.")
        self.closeOldDatasets()

        # AFAIK, if and exception occurs up to here, to should be handled
        # upstream

        # At this point self.watchedDatasets is the list of datasets we want
        # to check.
        # The main idea is to work on this list and at the end use it to
        # update the database

        toBulkUpdate = []
        for dataset in self.watchedDatasets:
            datasetName = dataset["_id"]
            logging.info("Begin dataset: %s" % datasetName)
            # Touching dataset un couch db
            dataset["last_check"] = int(time.time())

            # Looking up ready blocks at site in PhEDEx
            try:
                blocksInSite = self.getTransferredBlocks(datasetName)
            except Exception, ex:
                msg = "Error querying PhEDEx datasvc:\n%s\n" % str(ex)
                msg += str(traceback.format_exc())
                msg += "\nSkipping dataset.\n"
                logging.error(msg)
                continue

            if not blocksInSite:
                msg = "Dataset %s has no blocks in site %s. " \
                      "Skipping." % (datasetName, self.targetSite)
                logging.warning(msg)
                toBulkUpdate.append(dataset)
                continue

            # Fetching list of processed blocks
            try:
                processedBlocks = self.dqmDatabaseAPI.listBlocks(datasetName)
            except Exception, ex:
                msg = "Error querying couchdb:\n%s\n" % str(ex)
                msg += str(traceback.format_exc())
                msg += "\nSkipping dataset.\n"
                logging.error(msg)
                continue
            blocksInDB = [x["_id"] for x in processedBlocks]

            # Filtering new blocks
            filter_new = lambda x: x not in blocksInDB
            new_blocks = filter(filter_new, blocksInSite)
            if not new_blocks:
                msg = "Dataset %s has no new blocks to process. " \
                      "Skipping." % datasetName
                logging.warning(msg)
                toBulkUpdate.append(dataset)
                continue

            logging.info("Found %s new blocks for dataset %s" % (
                                            len(new_blocks), datasetName))

            # Am I here? Then I have new blocks to process.
            # First, I have to find out the runs contained in those blocks.
            # Then, for each run I have to fetch ALL the files belonging to it.
            # This basically means running all over again in order to include
            # missing files.

            try:
                runsInDBS = self.getInfoFromDBS(datasetName, new_blocks,
                                                dataset["release"])
            except Exception, ex:
                msg = "Error querying DBS:\n%s\n" % str(ex)
                msg += str(traceback.format_exc())
                msg += "\nSkipping dataset.\n"
                logging.error(msg)
                continue

            runsWithNewBlocks = {}
            for run in runsInDBS:
                if runsInDBS[run]["newBlocks"]:
                    runsWithNewBlocks[run] = runsInDBS[run]

            # Retrieving global tag. If it's not in the couch db, get it from
            # DBS and store in the couch, so next time it wont be queried.
            if dataset["global_tag"] in (None, ''):
                msg = "Global tag is not in couch db. " \
                      "Retrieving Global Tag from DBS " \
                      "storing it in couch db."
                logging.warning(msg)
                try:
                    dataset["global_tag"] = \
                                        self.getGlobalTagFromDBS(datasetName)
                except Exception, ex:
                    msg = "Failed to fetch a global tag from DBS for " \
                        "dataset %s. Error while querying DBS:\n" % datasetName
                    msg += str(ex) + "\n"
                    msg += str(traceback.format_exc())
                    msg += "\nSkipping dataset.\n"
                    logging.error(msg)
                    toBulkUpdate.append(dataset)
                    continue
            if dataset["global_tag"] in (None, ''):
                msg = "Dataset %s does not have an associated " \
                    "global tag in couchdb nor in DBS. Please insert " \
                    "one in couch db. Skipping dataset.\n" % datasetName
                logging.error(msg)
                toBulkUpdate.append(dataset)
                continue
            logging.info("Dataset has global tag: %s" % dataset["global_tag"])
                    
            # If there are many releases per dataset, group all the runs per
            # release version in 'releasesInDataset'.
            # If there's only one release associated to this dataset in DBS,
            # then it will be store in couchdb, in order to make the query
            # less heavy next time we poll for this dataset.

            releasesInDataset = {}
            if dataset["release"] in (None, ''):
                msg = "Release version is not in couch db. " \
                      "Retrieving CMSSW version from DBS " \
                      "storing it in couch db (" \
                      "Only if I find a single release " \
                      "for this dataset)."
                logging.warning(msg)
                for run in runsWithNewBlocks:
                    releasesInDataset.setdefault(
                        runsWithNewBlocks[run]["release"], []).append(run)

                # Updating couch document if only one dataset is found.
                if len(releasesInDataset.keys()) == 1:
                    dataset["release"] = releasesInDataset.keys()[0]

                if len(releasesInDataset.keys()) == 0:
                    msg = "Dataset %s does not have an associated " \
                          "release in couchdb nor in DBS. Please insert " \
                          "one in couch db. Skipping dataset.\n" % datasetName
                    logging.error(msg)
                    toBulkUpdate.append(dataset)
                    continue
            else:
                releasesInDataset[dataset["release"]] = []
                for run in runsWithNewBlocks:
                    releasesInDataset[dataset["release"]].append(run)

            # OK. Here's where the real magic starts.
            myThread = threading.currentThread()
            myThread.transaction.begin()

            # Insert files from runs with new blocks in WMBS
            self.getInputFilesetName(datasetName, dataset["last_check"])
            # TODO: maybe this does not work
            self.insertFilesIntoWMBS(datasetName, runsWithNewBlocks)

            # Creating a workflow per release
            # The agent will be in charge of doing the job splitting by run

            success = True
            for release in releasesInDataset:
                logging.info("Creating workflow in release %s" % release)
                self.getWorkloadName(datasetName, release, dataset["last_check"])

                self.workloadDir = None

                try:
                    self.createWorkload(datasetDoc=dataset, release=release,
                                        runs=releasesInDataset[release],
                                        runsMap=runsWithNewBlocks)
                except Exception, ex:
                    msg = "Error making workloads:\n%s\n" % str(ex)
                    msg += str(traceback.format_exc())
                    msg += "\nSkipping dataset.\n"
                    logging.error(msg)

                    # Delete any workload directory created
                    if self.workloadDir is not None and \
                                            os.path.exists(self.workloadDir):
                        msg = "Removing created cache dir:\n%s" % (
                                                            self.workloadDir)
                        logging.info(msg)
                        os.system("rm -rf %s" % self.workloadDir)

                    success = False
                    break

            if success:
                msg = "Worklods for %s created succesfully. " \
                      "Now committing in databases..." % datasetName 
                logging.info(msg)

                try:
                    errors = self.dqmDatabaseAPI.insertBlocks(datasetName,
                                                              new_blocks)
                except Exception, ex:
                    msg = "Error committing changes in couchdb:\n%s\n" % str(ex)
                    msg += str(traceback.format_exc())
                    msg += "\nSkipping dataset.\n"
                    logging.error(msg)
                    myThread.transaction.rollback()
                    continue

                try:
                    dataset["last_update"] = int(time.time())
                    self.dqmDatabaseAPI.updateDatasets([dataset], 'ready')
                except Exception, ex:
                    msg = "Error committing changes in couchdb:\n%s\n" % str(ex)
                    msg += str(traceback.format_exc())
                    msg += "\nSkipping dataset.\n"
                    logging.error(msg)
                    myThread.transaction.rollback()
                    continue

                myThread.transaction.commit()
            else:
                myThread.transaction.rollback()

        # Update timestamp for skipped datasets
        self.dqmDatabaseAPI.updateDatasets(toBulkUpdate, 'ready')

        return

    def getWorkloadName(self, dataset, release, timeStamp):
        """
        _getWorkloadName_

        Generates a name for the current workload. It will be like this:

        Harvesting-<Release>-PrimaryDatasetName-ProcessedDatasetName-DataTierName-TimeStamp

        """
        parts = self.getDatasetParts(dataset)
        self.workloadName = "Harvesting-%s-%s-%s-%s-%s" % (release,
                                                           parts[0],
                                                           parts[1],
                                                           parts[2],
                                                           timeStamp)

    def getInputFilesetName(self, dataset, timeStamp):
        """
        _getInputFilesetName_

        Generate a name for the fileset that will be used by the workloads
        created in a single iteration. It will be like:

        Harvesting-PrimaryDatasetName-ProcessedDatasetName-DataTierName-TimeStamp
        """
        parts = self.getDatasetParts(dataset)
        self.inputFilesetName = "Harvesting-%s-%s-%s-%s" % (parts[0],
                                                            parts[1],
                                                            parts[2],
                                                            timeStamp)

    def insertFilesIntoWMBS(self, dataset, runsMap):
        """
        _insertFilesIntoWMBS_

        Crete a fileset for the runs with newblocks only.
        """
        logging.info("Creating fileset")
        self.inputFileset = Fileset(name=self.inputFilesetName)
        self.inputFileset.create()

        for run in runsMap:
            for block in runsMap[run]["blocks"]:
                for file in runsMap[run]["blocks"][block]:
                    newFile = File(lfn=file["LogicalFileName"],
                                   size=file["FileSize"],
                                   events=file["NumberOfEvents"],
                                   checksums={"cksum": file["Checksum"]},
                                   locations=self.targetSite,
                                   merged=True)
                    newRun = Run(runNumber=file["LumiList"][0]["RunNumber"])
                    for lumi in file["LumiList"]:
                        newRun.lumis.append(lumi["LumiSectionNumber"])
                    newFile.addRun(newRun)
                    newFile.create()
                    self.inputFileset.addFile(newFile)

        self.inputFileset.commit()
        self.inputFileset.markOpen(False)

    def getDatasetCacheDir(self, dataset):
        """
        _getDatasetCacheDir_

        Given a dataset, it will return the dataset cache dir name
        """
        parts = self.getDatasetParts(dataset)
        return os.path.join(*parts)

    def createWorkload(self, datasetDoc, release, runs, runsMap):
        """
        _createWorkload_

        """

        wParams = {
            "CmsPath": self.cmsPath,
            "Requestor": self.config.Agent.contact,
            "InputDataset": datasetDoc["_id"],
            "CMSSWVersion": release,
            "ScramArch": self.scramArch,
            "ProcessingVersion": "v1",
            "GlobalTag": datasetDoc["global_tag"],
            "Scenario": datasetDoc["scenario"],
            "Proxy": self.proxy,
            "DqmGuiUrl": self.dqmGuiUrl,
            "CouchUrl": None,
            "DoStageOut": self.doStageOut,
            "DoDqmUpload": self.doDqmUpload,
            "DqmBaseLFN": self.dqmBaseLFN,
            "RefHistogram": datasetDoc["ref_file"],
            "RunWhitelist": runs,
            "JobSplitAlgo": "RunBased"
            }

        # Creating dataset cache directory
        datasetCache = os.path.join(self.workloadCache,
                                    self.getDatasetCacheDir(datasetDoc["_id"]))

        if not os.path.exists(datasetCache):
            os.makedirs(datasetCache)

        # Creating workload and task
        workload = self.harvestingFactory(self.workloadName, wParams)

        self.workloadDir = os.path.join(datasetCache, self.workloadName)
        taskMaker = TaskMaker(workload, self.workloadDir)
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()

        specPath = os.path.join(datasetCache, self.workloadName, "spec.pkl")
        workload.save(specPath)

        #TODO: Line to remove. Maybe this works.
        #self.insertFilesIntoWMBS(datasetDoc["_id"], runsMap)
        for workloadTask in workload.taskIterator():

            newWorkflow = Workflow(spec=specPath, owner=wParams["Requestor"],
                                   name=self.workloadName,
                                   task=workloadTask.getPathName())

            newWorkflow.create()

            subscription = Subscription(fileset=self.inputFileset,
                                        workflow=newWorkflow,
                                        split_algo=workloadTask.jobSplittingAlgorithm(),
                                        type=workloadTask.taskType())

            subscription.create()

        return


