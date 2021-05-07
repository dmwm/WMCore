#!/usr/bin/env python

from __future__ import print_function
import threading
import os
import sys

from DBSAPI.dbsApi import DbsApi
from dbsException import DbsException

from WMCore.WMInit import WMInit
from WMCore.Configuration import loadConfigurationFile

def connectToDB():
    """
    _connectToDB_

    Connect to the database specified in the WMAgent config.
    """
    if "WMAGENT_CONFIG" not in os.environ:
        print("Please set WMAGENT_CONFIG to point at your WMAgent configuration.")
        sys.exit(1)

    if not os.path.exists(os.environ["WMAGENT_CONFIG"]):
        print("Can't find config: %s" % os.environ["WMAGENT_CONFIG"])
        sys.exit(1)

    wmAgentConfig = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])

    if not hasattr(wmAgentConfig, "CoreDatabase"):
        print("Your config is missing the CoreDatabase section.")

    socketLoc = getattr(wmAgentConfig.CoreDatabase, "socket", None)
    connectUrl = getattr(wmAgentConfig.CoreDatabase, "connectUrl", None)
    (dialect, junk) = connectUrl.split(":", 1)

    myWMInit = WMInit()
    myWMInit.setDatabaseConnection(dbConfig = connectUrl, dialect = dialect,
                                   socketLoc = socketLoc)
    return

sql = """SELECT blockname, lfn FROM dbsbuffer_block
           INNER JOIN dbsbuffer_file ON
             dbsbuffer_block.id = dbsbuffer_file.block_id
         WHERE dbsbuffer_file.status = 'InPHEDEx'"""

connectToDB()
myThread = threading.currentThread()

results = []
for result in myThread.dbi.processData(sql):
    results.extend(result.fetchall())

blocks = {}
for row in results:
    if row[0] not in blocks:
        blocks[row[0]] = []

    blocks[row[0]].append(row[1])

args = {}
args["url"] = "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader"
args["version"] = "DBS_2_0_9"
args["mode"] = "GET"
dbsApi = DbsApi(args)

badBlocks = []
badFiles = []
for blockName in blocks:
    print("%s:" % blockName)
    blockFiles = []
    try:
        dbsFiles = dbsApi.listFileArray(blockName = blockName)
    except Exception as ex:
        dbsFiles = []

    for dbsFile in dbsFiles:
        blockFiles.append(dbsFile["LogicalFileName"])

    if len(blockFiles) != len(blocks[blockName]):
        print("\tFile count mismatch: %s local, %s global" % (len(blocks[blockName]), len(blockFiles)))
        if blockName not in badBlocks:
            badBlocks.append(blockName)

    for blockFile in blocks[blockName]:
        if blockFile not in blockFiles:
            badFiles.append(blockFile)
            #print "\t File missing: %s" % blockFile
            if blockName not in badBlocks:
                badBlocks.append(blockName)

    #sys.exit(0)

for badFile in badFiles:
    try:
        dbsFile = dbsApi.listFileArray(patternLFN = badFile)[0]
        if dbsFile["Block"]["Name"] not in badBlocks:
            badBlocks.append(dbsFile["Block"]["Name"])
    except Exception as ex:
        continue

print("Bad:")
for blockName in badBlocks:
    print(blockName)

