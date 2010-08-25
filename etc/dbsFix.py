#!/usr/bin/env python

import threading
import os
import sys

from DBSAPI.dbsApi import DbsApi
from dbsException import DbsException

from WMCore.WMInit import WMInit
from WMCore.Configuration import loadConfigurationFile

from WMComponent.DBSUpload import DBSInterface
from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile

from DBSAPI.dbsPrimaryDataset import DbsPrimaryDataset
from DBSAPI.dbsProcessedDataset import DbsProcessedDataset
from DBSAPI.dbsAlgorithm import DbsAlgorithm
from DBSAPI.dbsQueryableParameterSet import DbsQueryableParameterSet

def connectToDB():
    """
    _connectToDB_
    
    Connect to the database specified in the WMAgent config.
    """
    if not os.environ.has_key("WMAGENT_CONFIG"):
        print "Please set WMAGENT_CONFIG to point at your WMAgent configuration."
        sys.exit(1)
        
    if not os.path.exists(os.environ["WMAGENT_CONFIG"]):
        print "Can't find config: %s" % os.environ["WMAGENT_CONFIG"]
        sys.exit(1)

    wmAgentConfig = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])
    
    if not hasattr(wmAgentConfig, "CoreDatabase"):
        print "Your config is missing the CoreDatabase section."

    socketLoc = getattr(wmAgentConfig.CoreDatabase, "socket", None)
    connectUrl = getattr(wmAgentConfig.CoreDatabase, "connectUrl", None)
    (dialect, junk) = connectUrl.split(":", 1)

    myWMInit = WMInit()
    myWMInit.setDatabaseConnection(dbConfig = connectUrl, dialect = dialect,
                                   socketLoc = socketLoc)
    return

sql = """SELECT blockname, lfn, se_name FROM dbsbuffer_block
           INNER JOIN dbsbuffer_file ON
             dbsbuffer_block.id = dbsbuffer_file.block_id
           INNER JOIN dbsbuffer_location ON
             dbsbuffer_block.location = dbsbuffer_location.id
         WHERE dbsbuffer_file.status = 'InPHEDEx'"""

connectToDB()
myThread = threading.currentThread()

results = []
for result in myThread.dbi.processData(sql):
    results.extend(result.fetchall())

blocks = {}
blockLocation = {}
for row in results:
    if not blocks.has_key(row[0]):
        blocks[row[0]] = []
        blockLocation[row[0]] = row[2]

    blocks[row[0]].append(row[1])

args = {}
#args["url"] = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
args["url"] = "https://cmst0dbs.cern.ch:8443/cms_dbs_prod_tier0_writer/servlet/DBSServlet"
args["version"] = "DBS_2_0_9"
args["mode"] = "GET"
dbsApi = DbsApi(args)

badBlocks = []
badFiles = {}

for blockName in blocks.keys():
    print "%s:" % blockName
    blockFiles = []
    try:
        dbsFiles = dbsApi.listFiles(blockName = blockName)
    except Exception, ex:
        dbsFiles = []
        
    for dbsFile in dbsFiles:
        blockFiles.append(dbsFile["LogicalFileName"])

    if len(blockFiles) != len(blocks[blockName]):
        print "\tFile count mismatch: %s local, %s global" % (len(blocks[blockName]), len(blockFiles))
        if blockName not in badBlocks:
            badBlocks.append(blockName)

    for blockFile in blocks[blockName]:
        if blockFile not in blockFiles:
            if not badFiles.has_key(blockName):
                badFiles[blockName] = []
                
            badFiles[blockName].append(blockFile)

    #sys.exit(0)

psetInstance = DbsQueryableParameterSet(Hash = "GIBBERISH")

for newBlockName in badFiles.keys():
    seName = blockLocation[newBlockName]
    (datasetPath, junk) = newBlockName.split("#", 1)
    dbsApi.insertBlock(datasetPath, newBlockName, storage_element_list = [seName])

    blockRef = dbsApi.listBlocks(dataset = datasetPath, block_name = newBlockName)[0]
    print blockRef

    newFiles = []
    for newFileLFN in badFiles[newBlockName]:
        localFile = DBSBufferFile(lfn = newFileLFN)
        localFile.load(parentage = 1)

        (primaryDS, procDS, tier) = datasetPath[1:].split("/", 3)
        primary = DbsPrimaryDataset(Name = primaryDS, Type = "mc")
        algo = DbsAlgorithm(ExecutableName = localFile["appName"],
                            ApplicationVersion = localFile["appVer"],
                            ApplicationFamily = localFile["appFam"],
                            ParameterSetID = psetInstance)
        processed = DbsProcessedDataset(PrimaryDataset = primary,
                                        AlgoList = [algo],
                                        Name = procDS,
                                        TierList = [tier],
                                        ParentList = [],
                                        PhysicsGroup = "NoGroup",
                                        Status = "VALID",
                                        GlobalTag = "")
        newFiles.append(DBSInterface.createDBSFileFromBufferFile(localFile, processed))

    dbsApi.insertFiles(datasetPath, newFiles, blockRef)
    dbsApi.closeBlock(block = newBlockName)
