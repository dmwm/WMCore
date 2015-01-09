#!/usr/bin/env python
"""
_DBSWriter_

Interface object for writing data to DBS

"""

import time

from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsStorageElement import *
from DBSAPI.dbsApiException import *


import WMCore.Services.DBS.DBSWriterObjects as DBSWriterObjects
from   WMCore.Services.DBS.DBSErrors import DBSWriterError, formatEx
from   WMCore.Services.DBS.DBSReader import DBSReader

#from ProdCommon.MCPayloads.DatasetTools import getOutputDatasetsWithPSet
#from ProdCommon.MCPayloads.DatasetTools import getOutputDatasets
#from ProdCommon.MCPayloads.MergeTools import createMergeDatasetWorkflow

from DBSAPI.dbsFile import DbsFile
from DBSAPI.dbsUtil import get_path
from DBSAPI.dbsFileBlock import DbsFileBlock
from DBSAPI.dbsStorageElement import DbsStorageElement
from DBSAPI.dbsRun import DbsRun
from DBSAPI.dbsLumiSection import DbsLumiSection

from xml.dom import minidom
import logging
import base64

class _CreateDatasetOperator:
    """
    _CreateDatasetOperator_

    Operator for creating datasets from a workflow node

    """
    def __init__(self, apiRef, workflow):
        self.apiRef = apiRef
        self.workflow = workflow

    def __call__(self, pnode):
        if pnode.type != "CMSSW":
            return
        datasets = self.getOutputDatasetsWithPSet(pnode)
        cfgMeta = None
        try:
            cfgInt = pnode.cfgInterface
            cfgMeta = cfgInt.configMetadata
            cfgMeta['Type'] = self.workflow.parameters["RequestCategory"]
        except Exception as ex:
            msg = "Unable to Extract cfg data from workflow"
            msg += str(ex)
            logging.error(msg)
            return

        for dataset in datasets:
            primary = DBSWriterObjects.createPrimaryDataset(
                dataset, self.apiRef)
            algo = DBSWriterObjects.createAlgorithm(
                dataset, cfgMeta, self.apiRef)

            processed = DBSWriterObjects.createProcessedDataset(
                primary, algo, dataset, self.apiRef)

        return

class _CreateMergeDatasetOperator:
    """
    _CreateMergeDatasetOperator_

    Operator for creating merge datasets from a workflow node

    """
    def __init__(self, apiRef, workflow):
        self.apiRef = apiRef
        self.workflow = workflow

    def __call__(self, pnode):
        if pnode.type != "CMSSW":
            return
        for dataset in pnode._OutputDatasets:

            primary = DBSWriterObjects.createPrimaryDataset(
                dataset, self.apiRef)

            mergeAlgo = DBSWriterObjects.createMergeAlgorithm(dataset,
                                                              self.apiRef)
            DBSWriterObjects.createProcessedDataset(
                primary, mergeAlgo, dataset, self.apiRef)

            inputDataset = dataset.get('ParentDataset', None)
            if inputDataset == None:
                continue
            processedDataset = dataset["ProcessedDataset"]
            self.apiRef.insertMergedDataset(
                inputDataset, processedDataset, mergeAlgo)

            # algorithm used when process jobs produce merged files directly
            # doesnt contain pset content - taken from processing (same hash)
            mergeDirectAlgo = DBSWriterObjects.createAlgorithm(
                dataset, None, self.apiRef)
            self.apiRef.insertAlgoInPD(makeDSName2(dataset), mergeDirectAlgo)

            logging.debug("ProcessedDataset: %s"%processedDataset)
            logging.debug("inputDataset: %s"%inputDataset)
            logging.debug("mergeAlgo: %s"%mergeAlgo)
        return


def _remapBlockParentage(dsPath, data):
    """
    _RemapBlockParentage

    Remap the parentage of a block and its constiuent files

    o Remove child relations - to be set by child ds when exported
    o Remove unmerged file and processed dataset parents

    """

    # TODO: Throw on unmerged migrations?

    def dropNode(node):
        logging.debug("_remapBlockParentage: Dropping %s node" % node.nodeName)
        logging.debug("_remapBlockParentage: Node contents: %s" % node.toxml())
        node.parentNode.removeChild(node)

    def unmergedDropper(node, name):
        # strip un-merged tags - how to do this better?
        if node.getAttribute(name).count('unmerged') != 0:
            dropNode(node)

    dsContents = minidom.parseString(data)

    # remove other paths from proc ds - screws up ds parentage
    for proc in dsContents.getElementsByTagName('processed_dataset'):
        for path in proc.getElementsByTagName('path'):
            if path.getAttribute('dataset_path') != dsPath:
                dropNode(path)

    # remove file children - let this be set by a file setting its parents
    for child in dsContents.getElementsByTagName('file_child'):
        dropNode(child)

    # remap processing ds parentage
    for proc_parent in \
        dsContents.getElementsByTagName('processed_dataset_parent'):
        unmergedDropper(proc_parent, 'path')

    # remap file parentage
    for afile in dsContents.getElementsByTagName('file'):
        for aparent in afile.getElementsByTagName('file_parent'):
            unmergedDropper(aparent, 'lfn')

    result = dsContents.toxml()
    dsContents.unlink()
    return result




#  //
# // Util lambda for matching files with the same dataset and se name
#//
fileMatcher = lambda x, dataset, pnn: (x['CompleteDatasetName'] == dataset) and (x['PNN'] == pnn)
makeDSName = lambda x: "/%s/%s/%s" % (x['PrimaryDataset'],
                                      x['DataTier'],
                                      x['ProcessedDataset'])
makeDSName2 = lambda x: "/%s/%s/%s" % (x['PrimaryDataset'],
                                      x['ProcessedDataset'],
                                      x['DataTier'],)
makeDBSDSName = lambda x: "/%s/%s/%s" % (
    x['Dataset']['PrimaryDataset']['Name'],
    '-'.join(sorted(x['Dataset']['TierList'])),
    x['Dataset']['Name'])


class _InsertFileList(list):
    def __init__(self, pnn, dataset):
        list.__init__(self)
        self.pnn = pnn
        self.dataset = dataset

class DBSWriter:
    """
    _DBSWriter_

    General API for writing data to DBS


    """
    def __init__(self, url,  **contact):
        args = { "url" : url, "level" : 'ERROR'}
        args.update(contact)
        try:
            self.dbs           = DbsApi(args)
            self.args          = args
            self.version       = args.get('version', None)
            self.globalDBSUrl  = args.get('globalDBSUrl', None)
            self.globalVersion = args.get('globalVersion', None)
            if self.globalDBSUrl:
                globalArgs = {'url': url, 'level': 'ERROR'}
                globalArgs.update(contact)
                self.globalDBS = DbsApi(globalArgs)

        except DbsException as ex:
            msg = "Error in DBSWriterError with DbsApi\n"
            msg += "%s\n" % formatEx(ex)
            raise DBSWriterError(msg)
        self.reader = DBSReader(**args)

    def createDatasets(self, workflowSpec):
        """
        _createDatasets_

        Create All the output datasets found in the workflow spec instance
        provided

        """
        try:
            workflowSpec.payload.operate(
                _CreateDatasetOperator(self.dbs, workflowSpec)
                )
        except DbsException as ex:
            msg = "Error in DBSWriter.createDatasets\n"
            msg += "For Workflow: %s\n" % workflowSpec.workflowName()
            msg += "%s\n" % formatEx(ex)
            raise DBSWriterError(msg)
        return

    def insertFilesForDBSBuffer(self, files, procDataset, algos,
                                jobType = "NotMerge", insertDetectorData = False,
                                maxFiles = 100, maxSize = 99999999, timeOut = None,
                                fileCommitLength = 5):
        """
        _insertFiles_

        list of files inserted in DBS
        """
        #TODO: Whats the purpose of insertDetectorData

        if len(files) < 1:
            return
        affectedBlocks = []
        insertFiles =  []
        addedRuns=[]
        pnn = None

        #Get the algos in insertable form
        # logging.error("About to input algos")
        # logging.error(algos)
        ialgos = [DBSWriterObjects.createAlgorithmForInsert(dict(algo)) for algo in algos ]

        #print ialgos

        for outFile in files:
            #  //
            # // Convert each file into a DBS File object
            #//
            lumiList = []

            #Somehing similar should be the real deal when multiple runs/lumi could be returned from wmbs file

            for runlumiinfo in outFile.getRuns():
                lrun=long(runlumiinfo.run)
                run = DbsRun(
                    RunNumber = lrun,
                    NumberOfEvents = 0,
                    NumberOfLumiSections = 0,
                    TotalLuminosity = 0,
                    StoreNumber = 0,
                    StartOfRun = 0,
                    EndOfRun = 0,
                    )
                #Only added if not added by another file in this loop, why waste a call to DBS
                if lrun not in addedRuns:
                    self.dbs.insertRun(run)
                    addedRuns.append(lrun) #save it so we do not try to add it again to DBS
                    logging.debug("run %s added to DBS " % str(lrun))
                for alsn in runlumiinfo:
                    lumi = DbsLumiSection(
                            LumiSectionNumber = long(alsn),
                            StartEventNumber = 0,
                            EndEventNumber = 0,
                            LumiStartTime = 0,
                            LumiEndTime = 0,
                            RunNumber = lrun,
                    )
                    lumiList.append(lumi)

            logging.debug("lumi list created for the file")

            dbsfile = DbsFile(
                              #Checksum = str(outFile['cksum']),
                              NumberOfEvents = outFile['events'],
                              LogicalFileName = outFile['lfn'],
                              FileSize = int(outFile['size']),
                              Status = "VALID",
                              ValidationStatus = 'VALID',
                              FileType = 'EDM',
                              Dataset = procDataset,
                              TierList = DBSWriterObjects.makeTierList(procDataset['Path'].split('/')[3]),
                              AlgoList = ialgos,
                              LumiList = lumiList,
                              ParentList = outFile.getParentLFNs(),
                              #BranchHash = outFile['BranchHash'],
                            )
            #Set checksums by hand
            #dbsfile['Checksum'] = 0  #Set a default?
            for entry in outFile['checksums'].keys():
                #This should be a dictionary with a cktype key and cksum value
                if entry.lower() == 'cksum':
                    dbsfile['Checksum'] = str(outFile['checksums'][entry])
                elif entry.lower() == 'adler32':
                    dbsfile['Adler32'] = str(outFile['checksums'][entry])
                elif entry.lower() == 'md5':
                    dbsfile['Md5'] = str(outFile['checksums'][entry])



            #This check comes from ProdAgent, not sure if its required
            if len(outFile["locations"]) > 0:
                pnn = list(outFile["locations"])[0]
                logging.debug("PNN associated to file is: %s"%pnn)
            else:
                msg = "Error in DBSWriter.insertFiles\n"
                msg += "No PNN associated to file"
                #print "FAKING seName for now"
                #seName="cmssrm.fnal.gov"
                raise DBSWriterError(msg)
            insertFiles.append(dbsfile)
        #  //Processing Jobs:
        # // Insert the lists of sorted files into the appropriate
        #//  fileblocks


        sumSize   = 0
        sumFiles  = 0
        tmpFiles  = []
        blockList = []
        #First, get the block.  See if the block already exists
        try:
            fileBlock = DBSWriterObjects.getDBSFileBlock(
                self.dbs,
                procDataset,
                pnn)
            fileBlock['files'] = []
            #if not fileBlock in affectedBlocks:
            #    affectedBlocks.append(fileBlock)
        except DbsException as ex:
            msg = "Error in DBSWriter.insertFilesForDBSBuffer\n"
            msg += "Cannot retrieve FileBlock for dataset:\n"
            msg += " %s\n" % procDataset['Path']
            msg += "%s\n" % formatEx(ex)
            raise DBSWriterError(msg)



        filesToCommit = []
        for file in insertFiles:
            # First see if the block is full
            if self.manageFileBlock(fileBlock = fileBlock, maxFiles = maxFiles,
                                    maxSize = maxSize, timeOut = timeOut, algos = ialgos,
                                    filesToCommit = filesToCommit, procDataset = procDataset):
                fileBlock['OpenForWriting'] = 0
                if not fileBlock in affectedBlocks:
                    affectedBlocks.append(fileBlock)
                # Then we need a new block
                try:
                    fileBlock = DBSWriterObjects.getDBSFileBlock(
                        self.dbs,
                        procDataset,
                        pnn)
                    fileBlock['files'] = []
                except DbsException as ex:
                    msg = "Error in DBSWriter.insertFilesForDBSBuffer\n"
                    msg += "Cannot retrieve FileBlock for dataset:\n"
                    msg += " %s\n" % procDataset['Path']
                    msg += "%s\n" % formatEx(ex)
                    raise DBSWriterError(msg)

            fileBlock['files'].append(file['LogicalFileName'])
            filesToCommit.append(file)
            if len(filesToCommit) >= fileCommitLength:
                    # Only commit the files if there are more of them then the maximum length
                try:
                    self.dbs.insertFiles(procDataset, filesToCommit, fileBlock)
                    filesToCommit = []
                    logging.debug("Inserted files: %s to FileBlock: %s" \
                                  % ( ([ x['LogicalFileName'] for x in insertFiles ]),fileBlock['Name']))

                except DbsException as ex:
                    msg = "Error in DBSWriter.insertFiles\n"
                    msg += "Cannot insert processed files:\n"
                    msg += " %s\n" % ([ x['LogicalFileName'] for x in insertFiles ],)
                    msg += "%s\n" % formatEx(ex)
                    raise DBSWriterError(msg)


        if len(filesToCommit) > 0:
            try:
                self.dbs.insertFiles(procDataset, filesToCommit, fileBlock)
                filesToCommit = []
                logging.debug("Inserted files: %s to FileBlock: %s" \
                              % ( ([ x['LogicalFileName'] for x in insertFiles ]),fileBlock['Name']))

            except DbsException as ex:
                msg = "Error in DBSWriter.insertFiles\n"
                msg += "Cannot insert processed files:\n"
                msg += " %s\n" % ([ x['LogicalFileName'] for x in insertFiles ],)
                msg += "%s\n" % formatEx(ex)
                raise DBSWriterError(msg)


        if not fileBlock in affectedBlocks:
            affectedBlocks.append(fileBlock)




        ## Do bulk inserts now for DBS
        #filesToCommit = []
        #count         = 0
        #count2        = 0
        #for file in insertFiles:
        #    count += 1
        #    #Try and close the box
        #    logging.error("Should have a file")
        #    logging.error(len(filesToCommit))
        #    count2 += len(filesToCommit)
        #    if self.manageFileBlock(fileBlock = fileBlock, maxFiles = maxFiles,
        #                            maxSize = maxSize, timeOut = timeOut, algos = ialgos,
        #                            filesToCommit = filesToCommit, procDataset = procDataset):
        #        fileBlock['OpenForWriting'] = '0'
        #        if not fileBlock in affectedBlocks:
        #            affectedBlocks.append(fileBlock)
        #
        #
        #
        #        # Then we need a new block
        #        try:
        #            fileBlock = DBSWriterObjects.getDBSFileBlock(
        #                self.dbs,
        #                procDataset,
        #                seName)
        #            fileBlock['files'] = []
        #        except DbsException, ex:
        #            msg = "Error in DBSWriter.insertFilesForDBSBuffer\n"
        #            msg += "Cannot retrieve FileBlock for dataset:\n"
        #            msg += " %s\n" % procDataset['Path']
        #            msg += "%s\n" % formatEx(ex)
        #            raise DBSWriterError(msg)
        #    #At this point, we should commit the block as is
        #    fileBlock['files'].append(file['LogicalFileName'])
        #    if jobType == "MergeSpecial":
        #        for file in fileList:
        #            file['Block'] = fileBlock
        #            msg="calling: self.dbs.insertMergedFile(%s, %s)" % (str(file['ParentList']),str(file))
        #            logging.debug(msg)
        #            try:
        #                #
        #                #
        #                # NOTE To Anzar From Anzar (File cloning as in DBS API can be done here and then I can use Bulk insert on Merged files as well)
        #                self.dbs.insertMergedFile(file['ParentList'],
        #                                          file)
        #
        #            except DbsException, ex:
        #                msg = "Error in DBSWriter.insertFiles\n"
        #                msg += "Cannot insert merged file:\n"
        #                msg += "  %s\n" % file['LogicalFileName']
        #                msg += "%s\n" % formatEx(ex)
        #                raise DBSWriterError(msg)
        #            logging.debug("Inserted merged file: %s to FileBlock: %s"%(file['LogicalFileName'],fileBlock['Name']))
        #    else:
        #        filesToCommit.append(file)
        #        if len(filesToCommit) >= fileCommitLength:
        #            # Only commit the files if there are more of them then the maximum length
        #            try:
        #                logging.error("About to commit %i files" %(len(filesToCommit)))
        #                count2 += len(filesToCommit)
        #                self.dbs.insertFiles(procDataset, filesToCommit, fileBlock)
        #                filesToCommit = []
        #                logging.debug("Inserted files: %s to FileBlock: %s" \
        #                              % ( ([ x['LogicalFileName'] for x in insertFiles ]),fileBlock['Name']))
        #
        #            except DbsException, ex:
        #                msg = "Error in DBSWriter.insertFiles\n"
        #                msg += "Cannot insert processed files:\n"
        #                msg += " %s\n" % ([ x['LogicalFileName'] for x in insertFiles ],)
        #                msg += "%s\n" % formatEx(ex)
        #                raise DBSWriterError(msg)
        #
        #
        #
        #
        ## If we still have files to commit, commit them
        #logging.error("Got to the end of the loop")
        #logging.error(len(filesToCommit))
        #logging.error(count2)
        #if len(filesToCommit) > 0:
        #    try:
        #        logging.error("About to insert some files")
        #        self.dbs.insertFiles(procDataset, filesToCommit, fileBlock)
        #        filesToCommit = []
        #        logging.debug("Inserted files: %s to FileBlock: %s" \
        #                      % ( ([ x['LogicalFileName'] for x in insertFiles ]),fileBlock['Name']))
        #
        #    except DbsException, ex:
        #        msg = "Error in DBSWriter.insertFiles\n"
        #        msg += "Cannot insert processed files:\n"
        #        msg += " %s\n" % ([ x['LogicalFileName'] for x in insertFiles ],)
        #        msg += "%s\n" % formatEx(ex)
        #        raise DBSWriterError(msg)


        if not fileBlock in affectedBlocks:
            affectedBlocks.append(fileBlock)

        return list(affectedBlocks)








    def insertFiles(self, fwkJobRep, insertDetectorData = False):
        """
        _insertFiles_

        Process the files in the FwkJobReport instance and insert
        them into the associated datasets

        A list of affected fileblock names is returned both for merged
        and unmerged fileblocks. Only merged blocks will have to be managed.
        #for merged file
        #blocks to facilitate management of those blocks.
        #This list is not populated for processing jobs since we dont really
        #care about the processing job blocks.

        """

        insertLists = {}
        orderedHashes = []
        affectedBlocks = set()

        if len(fwkJobRep.files)<=0:
            msg = "Error in DBSWriter.insertFiles\n"
            msg += "No files found in FrameWorkJobReport for:\n"
            msg += "==> JobSpecId: %s"%fwkJobRep.jobSpecId
            msg += " Workflow: %s"%fwkJobRep.workflowSpecId
            raise DBSWriterError(msg)


        for outFile in fwkJobRep.sortFiles():
            #  //
            # // Convert each file into a DBS File object
            #//
            pnn = None
            if outFile.has_key("PNN"):
                if outFile['PNN'] :
                    pnn = outFile['PNN']
                    logging.debug("PNN associated to file is: %s"%pnn)
## remove the fallback to site se-name if no SE is associated to File
## because it's likely that there is some stage out problem if there
## is no SEName associated to the file.
#            if not seName:
#                if fwkJobRep.siteDetails.has_key("se-name"):
#                   seName = fwkJobRep.siteDetails['se-name']
#                   seName = str(seName)
#                   logging.debug("site SEname: %s"%seName)
            if not pnn:
                msg = "Error in DBSWriter.insertFiles\n"
                msg += "No PNN associated to files in FrameWorkJobReport for "
#                msg += "No SEname found in FrameWorkJobReport for "
                msg += "==> JobSpecId: %s"%fwkJobRep.jobSpecId
                msg += " Workflow: %s"%fwkJobRep.workflowSpecId
                raise DBSWriterError(msg)
            try:
                if ( insertDetectorData ):
                    dbsFiles = DBSWriterObjects.createDBSFiles(outFile,
                                                               fwkJobRep.jobType,
                                                               self.dbs)
                else:
                    dbsFiles = DBSWriterObjects.createDBSFiles(outFile,
                                                               fwkJobRep.jobType)
            except DbsException as ex:
                msg = "Error in DBSWriter.insertFiles:\n"
                msg += "Error creating DbsFile instances for file:\n"
                msg += "%s\n" % outFile['LFN']
                msg += "%s\n" % formatEx(ex)
                raise DBSWriterError(msg)

            if len(dbsFiles)<=0:
                msg="No DbsFile instances created. Not enough info in the FrameWorkJobReport for"
                msg += "==> JobSpecId: %s"%fwkJobRep.jobSpecId
                msg += " Workflow: %s"%fwkJobRep.workflowSpecId
                raise DBSWriterError(msg)

            for f in dbsFiles:
                datasetName = makeDBSDSName(f)
                hashName = "%s-%s" % (pnn, datasetName)

                if hashName not in insertLists:
                    insertLists[hashName] = _InsertFileList(pnn,
                                                            datasetName)
                insertLists[hashName].append(f)

                if not orderedHashes.count(hashName):
                    orderedHashes.append(hashName)


        #  //Processing Jobs:
        # // Insert the lists of sorted files into the appropriate
        #//  fileblocks

        for hash in orderedHashes:

            fileList = insertLists[hash]
            procDataset = fileList[0]['Dataset']


            try:
                fileBlock = DBSWriterObjects.getDBSFileBlock(
                    self.dbs,
                    procDataset,
                    fileList.pnn)

            except DbsException as ex:
                msg = "Error in DBSWriter.insertFiles\n"
                msg += "Cannot retrieve FileBlock for dataset:\n"
                msg += " %s\n" % procDataset
#                msg += "In Storage Element:\n %s\n" % fileList.seName
                msg += "In PNN:\n %s\n" % fileList.pnn
                msg += "%s\n" % formatEx(ex)
                raise DBSWriterError(msg)

            if fwkJobRep.jobType == "Merge":
                #  //
                # // Merge files
                #//
                for mergedFile in fileList:
                    mergedFile['Block'] = fileBlock
                    affectedBlocks.add(fileBlock['Name'])
                    msg="calling: self.dbs.insertMergedFile(%s, %s)" % (str(mergedFile['ParentList']),str(mergedFile))
                    logging.debug(msg)
                    try:
                        self.dbs.insertMergedFile(mergedFile['ParentList'],
                                                  mergedFile)

                    except DbsException as ex:
                        msg = "Error in DBSWriter.insertFiles\n"
                        msg += "Cannot insert merged file:\n"
                        msg += "  %s\n" % mergedFile['LogicalFileName']
                        msg += "%s\n" % formatEx(ex)
                        raise DBSWriterError(msg)
                    logging.debug("Inserted merged file: %s to FileBlock: %s"%(mergedFile['LogicalFileName'],fileBlock['Name']))
            else:
                #  //
                # // Processing files
                #//
                affectedBlocks.add(fileBlock['Name'])
                msg="calling: self.dbs.insertFiles(%s, %s, %s)" % (str(procDataset),str(list(fileList)),str(fileBlock))
                logging.debug(msg)

                try:
                    self.dbs.insertFiles(procDataset, list(fileList),
                                         fileBlock)
                except DbsException as ex:
                    msg = "Error in DBSWriter.insertFiles\n"
                    msg += "Cannot insert processed files:\n"
                    msg += " %s\n" % (
                        [ x['LogicalFileName'] for x in fileList ],
                        )

                    msg += "%s\n" % formatEx(ex)
                    raise DBSWriterError(msg)
                logging.debug("Inserted files: %s to FileBlock: %s"%( ([ x['LogicalFileName'] for x in fileList ]),fileBlock['Name']))

        return list(affectedBlocks)



    def manageFileBlock(self, fileBlock, maxFiles = 100, maxSize = None,
                        timeOut = None, algos = [], filesToCommit = [],
                        procDataset = None):
        """
        _manageFileBlock_

        Check to see wether the fileblock with the provided name
        is closeable based on number of files or total size.

        If the block equals or exceeds wither the maxFiles or maxSize
        parameters, close the block and return True, else do nothing and
        return False

        """

        #  //
        # // Check that the block exists, and is open before we close it
        #//

        fileblockName = fileBlock['Name']

        blockInstance = self.dbs.listBlocks(block_name=fileblockName)
        if len(blockInstance) > 1:
            msg = "Multiple Blocks matching name: %s\n" % fileblockName
            msg += "Unable to manage file block..."
            raise DBSWriterError(msg)

        if len(blockInstance) == 0:
            msg = "Block name %s not found\n" % fileblockName
            msg += "Cant manage a non-existent fileblock"
            raise DBSWriterError(msg)
        blockInstance = blockInstance[0]
        isClosed = blockInstance.get('OpenForWriting', '1')
        if isClosed != '1':
            msg = "Block %s already closed" % fileblockName
            logging.warning(msg)
            # Now we need to commit files
            if len(filesToCommit) > 0:
                try:
                    self.dbs.insertFiles(procDataset, filesToCommit, fileBlock)
                    filesToCommit = []


                except DbsException as ex:
                    msg = "Error in DBSWriter.insertFiles\n"
                    msg += "Cannot insert processed files:\n"
                    raise DBSWriterError(msg)

            # Attempting to migrate to global
            if self.globalDBSUrl:

                self.dbs.dbsMigrateBlock(srcURL = self.args['url'],
                                         dstURL = self.globalDBSUrl,
                                         block_name = fileblockName,
                                         srcVersion = self.version,
                                         dstVersion = self.globalVersion,
                                         )
                #for algo in algos:
                #    self.globalDBS.insertAlgoInPD(dataset = get_path(fileblockName.split('#')[0]),
                #                                  algorithm = algo)
                logging.info("Migrated block %s to global due to pre-closed status" %(fileblockName))
            else:
                logging.error("Should've migrated block %s because it was already closed, but didn't" % (fileblockName))
            return True



        #  //
        # // We have an open block, sum number of files and file sizes
        #//

        #fileCount = int(blockInstance.get('NumberOfFiles', 0))
        fileCount = len(fileBlock['files'])
        totalSize = float(blockInstance.get('BlockSize', 0))

        msg = "Fileblock: %s\n ==> Size: %s Files: %s\n" % (
            fileblockName, totalSize, fileCount)
        logging.warning(msg)

        #  //
        # // Test close block conditions
        #//
        closeBlock = False
        if timeOut:
            if int(time.time()) - int(blockInstance['CreationDate']) > timeOut:
                closeBlock = True
                msg = "Closing Block based on timeOut: %s" % fileblockName
                logging.debug(msg)
        if fileCount >= maxFiles:
            closeBlock = True
            msg = "Closing Block Based on files: %s" % fileblockName
            logging.debug(msg)

        if maxSize != None:
            if totalSize >= maxSize:
                closeBlock = True
                msg = "Closing Block Based on size: %s" % fileblockName
                logging.debug(msg)


        if closeBlock:
            # Now we need to commit files
            if len(filesToCommit) > 0:
                try:
                    self.dbs.insertFiles(procDataset, filesToCommit, fileBlock)
                    filesToCommit = []
                    #logging.debug("Inserted files: %s to FileBlock: %s" \
                    #              % ( ([ x['LogicalFileName'] for x in insertFiles ]),fileBlock['Name']))

                except DbsException as ex:
                    msg = "Error in DBSWriter.insertFiles\n"
                    msg += "Cannot insert processed files:\n"
                    #msg += " %s\n" % ([ x['LogicalFileName'] for x in insertFiles ],)
                    msg += "%s\n" % formatEx(ex)
                    raise DBSWriterError(msg)
            #  //
            # // Close the block
            #//
            self.dbs.closeBlock(
                DBSWriterObjects.createDBSFileBlock(fileblockName)
                )
            if self.globalDBSUrl:
                self.dbs.dbsMigrateBlock(srcURL = self.args['url'],
                                         dstURL = self.globalDBSUrl,
                                         block_name = fileblockName,
                                         srcVersion = self.version,
                                         dstVersion = self.globalVersion
                                         )
                for algo in algos:
                    pass
                    #self.globalDBS.insertAlgoInPD(dataset = get_path(fileblockName.split('#')[0]),
                    #                              algorithm = algo)

                logging.info("Migrated block %s to global" %(fileblockName))
            else:
                logging.error("Should've migrated block %s, but didn't" % (fileblockName))
        return closeBlock



    def migrateDatasetBlocks(self, inputDBSUrl, datasetPath, blocks):
        """
        _migrateDatasetBlocks_

        Migrate the list of fileblocks provided by blocks, belonging
        to the dataset specified by the dataset path to this DBS instance
        from the inputDBSUrl provided

        - *inputDBSUrl* : URL for connection to input DBS
        - *datasetPath* : Name of dataset in input DBS (must exist in input
                          DBS)
        - *blocks*      : list of block names to be migrated (must exist
                          in input DBS)

        """
        if len(blocks) == 0:
            msg = "FileBlocks not provided.\n"
            msg += "You must provide the name of at least one fileblock\n"
            msg += "to be migrated"
            raise DBSWriterError(msg)
        #  //
        # // Hook onto input DBSUrl and verify that the dataset & blocks
        #//  exist
        reader = DBSReader(inputDBSUrl)

        inputBlocks = reader.listFileBlocks(datasetPath)

        for block in blocks:
            #  //
            # // Test block exists at source
            #//
            if block not in inputBlocks:
                msg = "Block name:\n ==> %s\n" % block
                msg += "Not found in input dataset:\n ==> %s\n" % datasetPath
                msg += "In DBS Instance:\n ==> %s\n" % inputDBSUrl
                raise DBSWriterError(msg)

            #  //
            # // Test block does not exist in target
            #//
            if self.reader.blockExists(block):
                #  //
                # // block exists
                #//  If block is closed dont attempt transfer
                if not self.reader.blockIsOpen(block):
                    msg = "Block already exists in target DBS and is closed:\n"
                    msg += " ==> %s\n" % block
                    msg += "Skipping Migration of that block"
                    logging.warning(msg)
                    continue

            try:
                xferData = reader.dbs.listDatasetContents(datasetPath,  block)
            except DbsException as ex:
                msg = "Error in DBSWriter.migrateDatasetBlocks\n"
                msg += "Could not read content of dataset:\n ==> %s\n" % (
                    datasetPath,)
                msg += "Block name:\n ==> %s\n" % block
                msg += "%s\n" % formatEx(ex)
                raise DBSWriterError(msg)

            xferData = _remapBlockParentage(datasetPath, xferData)

            try:
                self.dbs.insertDatasetContents(xferData)
            except DbsException as ex:
                msg = "Error in DBSWriter.migrateDatasetBlocks\n"
                msg += "Could not write content of dataset:\n ==> %s\n" % (
                    datasetPath,)
                msg += "Block name:\n ==> %s\n" % block
                msg += "%s\n" % formatEx(ex)
                raise DBSWriterError(msg)
            del xferData


        return

    def importDatasetWithExistingParents(self, sourceDBS, sourceDatasetPath, targetDBS,
                      onlyClosed = True):
        """
        _importDataset_

        Import a dataset into the local scope DBS.
        It complains if the parent dataset ar not there!!

        - *sourceDBS* : URL for input DBS instance

        - *sourceDatasetPath* : Dataset Path to be imported

        - *targetDBS* : URL for DBS to have dataset imported to

        """
        reader = DBSReader(sourceDBS)
        inputBlocks = reader.getFileBlocksInfo(sourceDatasetPath, onlyClosed, locations = False)
        for inputBlock in inputBlocks:
            block = inputBlock['Name']
            #  //
            # // Test block does not exist in target
            #//
            if self.reader.blockExists(block):
                #  //
                # // block exists
                #//  If block is closed dont attempt transfer
                if not str(inputBlock['OpenForWriting']) != '1':
                    msg = "Block already exists in target DBS and is closed:\n"
                    msg += " ==> %s\n" % block
                    msg += "Skipping Import of that block"
                    logging.warning(msg)
                    locations = reader.listFileBlockLocation(block)
                    # only empty file blocks can have no location
                    if not locations and str(inputBlock['NumberOfFiles']) != "0":
                        msg = "Error in DBSWriter.importDatasetWithExistingParents\n"
                        msg += "Block has no locations defined: %s" % block
                        raise DBSWriterError(msg)
                    logging.info("Update block locations to:")
                    for pnn in locations:
                        self.dbs.addReplicaToBlock(block,pnn)
                        logging.info(pnn)
                    continue


            try:
                xferData = reader.dbs.listDatasetContents(
                    sourceDatasetPath,  block
                    )
            except DbsException as ex:
                msg = "Error in DBSWriter.importDatasetWithExistingParents\n"
                msg += "Could not read content of dataset:\n ==> %s\n" % (
                    sourceDatasetPath,)
                msg += "Block name:\n ==> %s\n" % block
                msg += "%s\n" % formatEx(ex)
                raise DBSWriterError(msg)
            try:
                self.dbs.insertDatasetContents(xferData)
            except DbsException as ex:
                msg = "Error in DBSWriter.importDatasetWithExistingParents\n"
                msg += "Could not write content of dataset:\n ==> %s\n" % (
                    sourceDatasetPath,)
                msg += "Block name:\n ==> %s\n" % block
                msg += "%s\n" % formatEx(ex)
                raise DBSWriterError(msg)
            del xferData

            locations = reader.listFileBlockLocation(block)
            # only empty file blocks can have no location
            if not locations and str(inputBlock['NumberOfFiles']) != "0":
                msg = "Error in DBSWriter.importDatasetWithExistingParents\n"
                msg += "Block has no locations defined: %s" % block
                raise DBSWriterError(msg)
            for pnn in locations:
                self.dbs.addReplicaToBlock(block,pnn)

        return

    def importDataset(self, sourceDBS, sourceDatasetPath, targetDBS,
                      onlyClosed = True):
        """
        _importDataset_

        Import a dataset into the local scope DBS with full parentage hirerarchy
        (at least not slow because branches info is dropped)

        - *sourceDBS* : URL for input DBS instance

        - *sourceDatasetPath* : Dataset Path to be imported

        - *targetDBS* : URL for DBS to have dataset imported to

        """
        reader = DBSReader(sourceDBS)
        inputBlocks = reader.getFileBlocksInfo(sourceDatasetPath, onlyClosed, locations = False)
        blkCounter=0
        for inputBlock in inputBlocks:
            block = inputBlock['Name']
            #  //
            # // Test block does not exist in target
            #//
            blkCounter=blkCounter+1
            msg="Importing block %s of %s: %s " % (blkCounter,len(inputBlocks),block)
            logging.debug(msg)
            if self.reader.blockExists(block):
                #  //
                # // block exists
                #//  If block is closed dont attempt transfer
                if str(inputBlock['OpenForWriting']) != '1':
                    msg = "Block already exists in target DBS and is closed:\n"
                    msg += " ==> %s\n" % block
                    msg += "Skipping Import of that block"
                    logging.warning(msg)
                    locations = reader.listFileBlockLocation(block)
                    # only empty file blocks can have no location
                    if not locations and str(inputBlock['NumberOfFiles']) != "0":
                        msg = "Error in DBSWriter.importDataset\n"
                        msg += "Block has no locations defined: %s" % block
                        raise DBSWriterError(msg)
                    logging.info("Update block locations to:")
                    for pnn in locations:
                        self.dbs.addReplicaToBlock(block,pnn)
                        logging.info(pnn)
                    continue

            try:

                self.dbs.migrateDatasetContents(sourceDBS, targetDBS, sourceDatasetPath, block_name=block, noParentsReadOnly = False)
            except DbsException as ex:
                msg = "Error in DBSWriter.importDataset\n"
                msg += "Could not write content of dataset:\n ==> %s\n" % (
                    sourceDatasetPath,)
                msg += "Block name:\n ==> %s\n" % block
                msg += "%s\n" % formatEx(ex)
                raise DBSWriterError(msg)

            locations = reader.listFileBlockLocation(block)
            # only empty file blocks can have no location
            if not locations and str(inputBlock['NumberOfFiles']) != "0":
                msg = "Error in DBSWriter.importDataset\n"
                msg += "Block has no locations defined: %s" % block
                raise DBSWriterError(msg)
            for pnn in locations:
                self.dbs.addReplicaToBlock(block,pnn)

        return


    def importDatasetWithoutParentage(self, sourceDBS, sourceDatasetPath, targetDBS,
                      onlyClosed = True):
        """
        _importDataset_

        Import a dataset into the local scope DBS with one level parentage,
        however it has severe limitation on its use due to the "ReadOnly" concept.

        - *sourceDBS* : URL for input DBS instance

        - *sourceDatasetPath* : Dataset Path to be imported

        - *targetDBS* : URL for DBS to have dataset imported to

        """
        reader = DBSReader(sourceDBS)
        inputBlocks = reader.getFileBlocksInfo(sourceDatasetPath, onlyClosed, locations = False)
        for inputBlock in inputBlocks:
            block = inputBlock['Name']
            #  //
            # // Test block does not exist in target
            #//
            if self.reader.blockExists(block):
                #  //
                # // block exists
                #//  If block is closed dont attempt transfer
                if str(inputBlock['OpenForWriting']) != '1':
                    msg = "Block already exists in target DBS and is closed:\n"
                    msg += " ==> %s\n" % block
                    msg += "Skipping Import of that block"
                    logging.warning(msg)
                    locations = reader.listFileBlockLocation(block)
                    # only empty file blocks can have no location
                    if not locations and str(inputBlock['NumberOfFiles']) != "0":
                        msg = "Error in DBSWriter.importDatasetWithoutParentage\n"
                        msg += "Block has no locations defined: %s" % block
                        raise DBSWriterError(msg)
                    logging.info("Update block locations to:")
                    for pnn in locations:
                        self.dbs.addReplicaToBlock(block,pnn)
                        logging.info(pnn)
                    continue

            try:
                self.dbs.migrateDatasetContents(sourceDBS, targetDBS, sourceDatasetPath, block_name=block, noParentsReadOnly = True )
            except DbsException as ex:
                msg = "Error in DBSWriter.importDatasetWithoutParentage\n"
                msg += "Could not write content of dataset:\n ==> %s\n" % (
                    sourceDatasetPath,)
                msg += "Block name:\n ==> %s\n" % block
                msg += "%s\n" % formatEx(ex)
                raise DBSWriterError(msg)

            locations = reader.listFileBlockLocation(block)
            # only empty file blocks can have no location
            if not locations and str(inputBlock['NumberOfFiles']) != "0":
                msg = "Error in DBSWriter.importDatasetWithoutParentage\n"
                msg += "Block has no locations defined: %s" % block
                raise DBSWriterError(msg)
            for pnn in locations:
                self.dbs.addReplicaToBlock(block,pnn)

        return


    def getOutputDatasetsWithPSet(payloadNode):
        """
        _getOutputDatasetsWithPSet_

        Extract all the information about output datasets from the
        payloadNode object provided, including the {{}} format PSet cfg

        Returns a list of DatasetInfo objects including App details
        from the node.

        """
        result = []

        for item in payloadNode._OutputDatasets:
            resultEntry = DatasetInfo()
            resultEntry.update(item)
            resultEntry["ApplicationName"] = payloadNode.application['Executable']
            resultEntry["ApplicationProject"] = payloadNode.application['Project']
            resultEntry["ApplicationVersion"] = payloadNode.application['Version']
            resultEntry["ApplicationFamily"] = item.get("OutputModuleName", "AppFamily")

            try:
                config = payloadNode.cfgInterface
                psetStr = config.originalContent()
                resultEntry['PSetContent'] = psetStr
            except Exception as ex:
                resultEntry['PSetContent'] = None

            result.append(resultEntry)

        return _sortDatasets(result)



class DatasetInfo(dict):
    """
    _DatasetInfo_

    Serialisable container for a Dataset including the information
    required to create/locate a dataset in DBS and the details required
    to match the dataset to CMSSW objects

    Inherit from dict, assume all values are strings.

    """

    def __init__(self):
        dict.__init__(self)
        self.setdefault("PrimaryDataset", None)
        self.setdefault("ProcessedDataset", None)
        self.setdefault("AnalysisDataset", None)
        self.setdefault("ParentDataset", None)

        self.setdefault("ApplicationName",  None)
        self.setdefault("ApplicationProject" , None)
        self.setdefault("ApplicationVersion" , None)
        self.setdefault("ApplicationFamily", None)

        self.setdefault("DataTier" , None)

        self.setdefault("Conditions" , None)
        self.setdefault("PSetHash", None)

        self.setdefault("InputModuleName" , None)
        self.setdefault("OutputModuleName" , None)


    def __str__(self):
        """string rep as XML for printouts"""
        return str(self.save())

    def name(self):
        """
        _name_

        Construct a string giving the name of this dataset as a path,
        using /<PrimaryDataset>/<DataTier>/<ProcessedDataset>

        ParentDataset will be a string of this format

        """
        result = "/%s" % self['PrimaryDataset']
        result += "/%s" % self['ProcessedDataset']
        if self['DataTier'] != None:
            result += "/%s" % self['DataTier']

        if self['AnalysisDataset'] != None:
            result += "/%s" % self['AnalysisDataset']

        return result


#    def save(self):
#        """
#        _save_
#
#        Return an improvNode structure containing details
#        of this object so it can be saved to a file
#
#        """
#        improvNode = IMProvNode(self.__class__.__name__)
#        for key, val in self.items():
#            if val == None:
#                continue
#            node = IMProvNode("Entry", str(val), Name = key)
#            improvNode.addNode(node)
#        return improvNode
#
#
#    def load(self, improvNode):
#        """
#        _load_
#
#        Populate this instance with data extracted from the improvNode
#        provided. The Argument should be an improvNode created with the
#        same structure as the result of the save method of this class
#
#        """
#        if improvNode.name != self.__class__.__name__:
#            #  //
#            # // Not the right node type
#            #//
#            return
#
#        entryQ = IMProvQuery("/%s/Entry" % self.__class__.__name__)
#        entries = entryQ(improvNode)
#        for entry in entries:
#            key = str(entry.attrs["Name"])
#            value = str(entry.chardata)
#            self[key] = value
#        return
