#!/usr/bin/env python

from xml.dom.minidom import parseString
from DbsCli import sendMessage as callDbs

class DbsQueryHelper:
    """
    Contains queries for DBS needed by the RunTransfer feeder
    """
    def __init__(self, dbsHost, dbsPort, dbsInstance):
        """
        Configures the DBS Query helper
        """
        self.dbsHost = dbsHost
        self.dbsPort = dbsPort
        self.dbsInstance = dbsInstance
    
    def getText(self, nodelist):
        """
        Returns the inner text from a list of XML nodes
        """
        rc = ""
        for node in nodelist:
            if node.nodeType == node.TEXT_NODE:
                rc = rc + node.data
        return rc

    def getXmlNodeValues(self, xml, nodelist):
        """
        Gets the inner text for a number of nodes given a search path
        """
        if len(nodelist) == 1:
            nodes = xml.getElementsByTagName(nodelist[0])
            retList = []
            for node in nodes:
                retList.append(self.getText(node.childNodes))
            return retList
        else:
            node = xml.getElementsByTagName(nodelist[0])[0]
            return self.getXmlNodeValues(node, nodelist[1:])
    
    def query(self, query):
        """
        Queries DBS and returns XML
        """
        return parseString(callDbs(self.dbsHost, self.dbsPort, self.dbsInstance,
                                   query, 0, -1, 1))
    
    def queryBlockInfo(self, query):
        """
        Queries DBS and extracts block info from the result
        """
        result = self.query(query)
        return self.getXmlNodeValues(result, ["ddresponse","output","column"])
    
    def queryFileInfo(self, query):
        """
        Queries DBS and extracts file info from the result
        """
        result = self.query(query)
        return self.getXmlNodeValues(result, ["ddresponse","output","column"])

    def queryRunInfo(self, query):
        """
        Queries DBS and extracts run info from the result
        """
        result = self.query(query)
        return self.getXmlNodeValues(result, ["ddresponse","output","column"])
    
    def getParentFiles(self, files):
        """
        Returns all parent files for the given primary files
        """
        parentFiles = Set()
        for f in files:
            pars = self.queryFileInfo("find file.parent where file = %s" % f)
            parentFiles.update(pars)
        return parentFiles
    
    def getFileBlocks(self, files):
        """
        Queries DBS to get the blocks containing all the passed files
        """
        allBlocks = Set()
        for f in files:
            blocks = self.queryBlockInfo("find block where file = %s" % f)
            allBlocks.update(blocks)
        return allBlocks
        
    def getBlockFiles(self, blocks):
        """
        Queries DBS to get all files in the listed blocks
        """
        files = []
        for block in blocks:
            bfs = self.queryFileInfo("find file where block = %s" % block)
            files.extend(bfs)
        return files

    def getBlockInfo(self, run, dataset):
        """
        Queries DBS to get all file blocks for a given run and dataset
        """
        return self.queryBlockInfo("find block where dataset = %s and run = %d" % (dataset, run))

    def getRuns(self, lastRun = None):
        """
        Queries DBS to extract run info
        """
        if not lastRun:
            return queryRunInfo("find run")
        else:
            return queryRunInfo("find run where run > %d" % lastRun)
