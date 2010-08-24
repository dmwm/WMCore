#!/usr/bin/env python

from sets import Set
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
        Gets the inner text for a number of nodes given a search path. Can also
        return key:value pairs if the last item in the nodeList is an n item
        list detailing the key and all associated value [key,value1,value2]
        relations in the XML, returned as:
        {"key_1" : {'value_1' : val_1, 'value_n' : val_n}, "key_n" : {...}}
        """
        def AdditiveUpdate(primary, secondary):
            """
            Adds the given dictionaries together, joining attributes duplicated
            by primary key (as described above) into Sets as required. This
            handles, e.g., multiple files returned by DBS with the same LFN
            but different lumi section each time
            """
            for key in secondary:
                if primary.has_key(key):
                    for valKey in primary[key]:
                        primary[key][valKey].update(secondary[key][valKey])
                else:
                    primary[key] = secondary[key]
            
        if len(nodelist) == 1:
            # Handle top search level and extract required info
            if isinstance(nodelist[0], str):
                # We only want one data member, find it and return
                nodes = xml.getElementsByTagName(nodelist[0])
                retList = []
                for node in nodes:
                    retList.append(self.getText(node.childNodes))
                    return retList
            elif isinstance(nodelist[0], list):
                # We want multiple data members, assume first in the list
                # is the key, and 1:n entries are values to link to the key
                keyNode = xml.getElementsByTagName(nodelist[0][0])
                key = self.getText(keyNode[0].childNodes)
                vals = {}
                for valKey in nodelist[0][1:]:
                    valNodes = xml.getElementsByTagName(valKey)
                    val = self.getText(valNodes[0].childNodes)
                    vals[valKey] = Set([val])
                return {key:vals}
        else:
            # Traverse the node structure until we come to the top level in the
            # required nodelist
            ret = None
            retFunc = None
            if isinstance(nodelist[-1], str):
                # We only want one field at the top level
                ret = []
                retFunc = ret.extend
            elif isinstance(nodelist[-1], list):
                # We want multiple fields at the top level
                ret = {}
                retFunc = lambda x : AdditiveUpdate(ret, x)
            nodes = xml.getElementsByTagName(nodelist[0])
            for node in nodes:
                # Recursion ahoy!
                retFunc(self.getXmlNodeValues(node, nodelist[1:]))
            return ret
    
    def query(self, query):
        """
        Queries DBS and returns XML
        """
        ret = callDbs(self.dbsHost, self.dbsPort, self.dbsInstance, query, 0, -1, 1)
        ret = ret.replace("<>", "")
        print ret
        return parseString(ret)

    def getParentDataset(self, dataset):
        """
        Queries DBS to find the parent dataset of the given dataset
        """
        result = self.query("find dataset.parent where dataset = %s" % dataset)
        return self.getXmlNodeValues(result, ["ddresponse","output","column"])
    
    def getFileInfo(self, run, dataset):
        """
        Gets all files, parent files, and primary blocks for a run and dataset
        """
        ret = self.query("find file, file.parent, file.numevents, lumi, file.size, block where dataset = %s and run = %s" % (dataset, run))
        fileList = self.getXmlNodeValues(ret, ["output","row","file"])
        blockList = self.getXmlNodeValues(ret, ["output","row","block"])
        fileInfoMap = self.getXmlNodeValues(ret, ["ddresponse","output","row",["file","file.parent","file.numevents","file.size","lumi"]])
        return (Set(fileList), Set(blockList), fileInfoMap)
    
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
        return self.queryBlockInfo("find block where dataset = %s and run = %s" % (dataset, run))

    def getRuns(self, lastRun = None):
        """
        Queries DBS to extract run info
        """
        if not lastRun:
            return queryRunInfo("find run")
        else:
            return queryRunInfo("find run where run > %s" % lastRun)
