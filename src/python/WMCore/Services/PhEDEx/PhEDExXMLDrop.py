#!/usr/bin/env python
"""
_DropMaker_

Generate the XML file for injecting data into PhEDEx
Modified from ProdCommon.DataMgmt.PhEDEx.DropMaker.py

TODO: Need to merge with ProdCommon.DataMgmt.PhEDEx.DropMaker.py - Talk to Stuart
"""

import logging
from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvDoc import IMProvDoc

from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader


class XMLFileblock(list):
    """
    _XMLFileblock_

    Object representing a fileblock for conversion to XML

    """
    def __init__(self, fileblockName, isOpen = "y"):
        list.__init__(self)
        self.fileblockName = fileblockName
        self.isOpen = isOpen

    def addFile(self, lfn, checksum, size):
        """
        _addFile_

        Add a file to this fileblock

        """
        self.append(
            ( lfn, checksum, size, )
            )
        return

    def save(self):
        """
        _save_

        Serialise this to XML compatible with PhEDEx injection

        """
        result = IMProvNode("block")
        result.attrs['name'] = self.fileblockName
        result.attrs['is-open'] = self.isOpen
        for entry in self:
            #To do: check this appending is needed anymore
            checksum="cksum:%s"%entry[1] #add cksum:
            file = IMProvNode("file")
            file.attrs['name'] =  entry[0]
            file.attrs['checksum'] = checksum
            file.attrs['bytes'] =  entry[2]

            result.addNode(file)
        return result
    
class XMLDataset(list):
    """
    <dataset name='DatasetNameHere' is-open='boolean' is-transient='boolean'>
    <block name='fileblockname' is-open='boolean'>
    <file lfn='lfn1Here' checksum='cksum:0' size ='fileSize1Here'/>
    <file lfn='lfn2Here' checksum='cksum:0' size ='fileSize2Here'/> 
    </block>
    </dataset>
    """
    
    def __init__(self, datasetName, datasetOpen = "y", 
                 datasetTransient = "n" ):
        
        self.datasetName = datasetName    
        self.datasetIsOpen = datasetOpen
        self.datasetIsTransient = datasetTransient

        #  //
        # // Fileblocks
        #//
        self.fileblocks = {}


    def getFileblock(self, fileblockName, isOpen = "y"):
        """
        _getFileblock_

        Add a new fileblock with name provided if not present, if it exists,
        return it

        """
        if self.fileblocks.has_key(fileblockName):
            return self.fileblocks[fileblockName]
        
        newFileblock = XMLFileblock(fileblockName, isOpen)
        self.fileblocks[fileblockName] = newFileblock
        return newFileblock

    def save(self):
        """
        _save_

        serialise object into PhEDEx injection XML format

        """
        
        dataset = IMProvNode("dataset")
        dataset.attrs['name'] = self.datasetName
        dataset.attrs['is-open'] = self.datasetIsOpen
        dataset.attrs['is-transient'] = self.datasetIsTransient
        
        for block in self.fileblocks.values():
            dataset.addNode(block.save())
        
        return dataset

class XMLInjectionSpec:
    """
    _XMLInjectionSpec_
    <data version='2'>
    <dbs name='DBSNameHere'>

    <dataset name='DatasetNameHere' is-open='boolean' is-transient='boolean'>
    <block name='fileblockname' is-open='boolean'>
    <file lfn='lfn1Here' checksum='cksum:0' size ='fileSize1Here'/>
    <file lfn='lfn2Here' checksum='cksum:0' size ='fileSize2Here'/> 
    </block>
    </dataset>
    <dataset name='DatasetNameHere' is-open='boolean' is-transient='boolean'>
    <block name='fileblockname' is-open='boolean'>
    <file lfn='lfn1Here' checksum='cksum:0' size ='fileSize1Here'/>
    <file lfn='lfn2Here' checksum='cksum:0' size ='fileSize2Here'/> </block>
    </dataset>
    
    </dbs>
    </data> 
    """
    def __init__(self, dbs):
        self.dbs = dbs
        #  //
        # // dataset attributes
        #//

        #  //
        # // Fileblocks
        #//
        self.datasetPaths = {}


    def getDataset(self, datasetName, isOpen = "y", isTransient = "n" ):
        """
        _getFileblock_

        Add a new fileblock with name provided if not present, if it exists,
        return it

        """
        if self.datasetPaths.has_key(datasetName):
            return self.datasetPaths[datasetName]
        
        newDataset = XMLDataset(datasetName, isOpen, isTransient)
        self.datasetPaths[datasetName] = newDataset
        return newDataset

    def save(self):
        """
        _save_

        serialise object into PhEDEx injection XML format

        """
        result = IMProvNode("data")
        # hard coded as version 2. might need to change
        result.attrs['version'] = '2'
        dbs = IMProvNode("dbs")
        dbs.attrs['name'] = self.dbs
        dbs.attrs['dls'] = 'dbs'
        result.addNode(dbs)
        
        for dataset in self.datasetPaths.values():
            dbs.addNode(dataset.save())
                
        return result

    def write(self, filename):
        """
        _write_

        Write to file using name provided

        """
        handle = open(filename, 'w')
        improv = self.save()
        handle.write(improv.makeDOMElement().toprettyxml())
        handle.close()
        return
        
def makePhEDExDrop(dbsUrl, datasetPath, *blockNames):
    """
    _makePhEDExDrop_

    Given a DBS2 Url, dataset name and list of blockNames,
    generate an XML structure for injection

    """
    spec = XMLInjectionSpec(dbsUrl)


    reader = DBSReader(dbsUrl)

    dataset = spec.getDataset(datasetPath)   
        
    for block in blockNames:
        blockContent = reader.getFileBlock(block)
        isOpen = reader.blockIsOpen(block)
        if isOpen:
            xmlBlock = dataset.getFileblock(block, "y")
        else:
            xmlBlock = dataset.getFileblock(block, "n")

        [ xmlBlock.addFile(
            x['LogicalFileName'],x['Checksum'] ,x['FileSize']
            ) for x in blockContent[block]['Files'] ]

    improv = spec.save()
    xmlString = improv.makeDOMElement().toprettyxml()
    return xmlString


def makePhEDExXMLForDatasets(dbsUrl, datasetPaths):        
    
    """
    _makePhEDExDropForDataset_

    Given a DBS Url, list of dataset path name, 
    generate an XML structure for injection
   
   TODO: not sure whether merge this interface with makePhEDExDrop
    """
    spec = XMLInjectionSpec(dbsUrl)
    for datasetPath in datasetPaths:
        spec.getDataset(datasetPath)
        
    improv = spec.save()
    xmlString = improv.makeDOMElement().toprettyxml()
    return xmlString

    
    
    

