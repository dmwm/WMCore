#!/usr/bin/env python

import threading
import os
import sys

from WMCore.Services.PhEDEx import PhEDEx

from WMCore.WMInit import WMInit
from WMCore.Configuration import loadConfigurationFile

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
    if not blocks.has_key(row[0]):
        blocks[row[0]] = []

    blocks[row[0]].append(row[1])

phedex = PhEDEx.PhEDEx({"endpoint": "https://cmsweb.cern.ch/phedex/datasvc/json/prod/"}, "json")

for blockName in blocks.keys():
    print "%s:" % blockName

    args = {}
    args["block"] = blockName
    result = phedex._getResult("filereplicas", args = args, verb = "POST")

    blockFiles = []
    for phedexFile in result["phedex"]["block"][0]["file"]:
        blockFiles.append(phedexFile["name"])

    if len(blockFiles) != len(blocks[blockName]):
        print "\tFile count mismatch: %s local, %s global" % (len(blocks[blockName]), len(blockFiles))

    for blockFile in blocks[blockName]:
        if blockFile not in blockFiles:
            print "\t File missing: %s" % blockFile
        
    #sys.exit(0)

