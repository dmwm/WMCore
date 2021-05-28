#!/usr/bin/env python
"""
_DropMaker_

Generate the XML file for injecting data into PhEDEx
Modified from ProdCommon.DataMgmt.PhEDEx.DropMaker.py

TODO: Need to merge with ProdCommon.DataMgmt.PhEDEx.DropMaker.py - Talk to Stuart
"""

from xml.dom.minidom import getDOMImplementation

class XMLFileblock(list):
    """
    _XMLFileblock_

    Object representing a fileblock for conversion to XML

    """
    def __init__(self, fileblockName, isOpen = "y"):
        list.__init__(self)
        self.fileblockName = fileblockName
        self.isOpen = isOpen

    def addFile(self, lfn, checksums, size):
        """
        _addFile_

        Add a file to this fileblock

        """
        self.append(
            ( lfn, checksums, size )
            )
        return

    def save(self):
        """
        _save_

        Serialise this to XML compatible with PhEDEx injection

        """
        impl = getDOMImplementation()

        doc = impl.createDocument(None, "block", None)
        result = doc.createElement("block")
        result.setAttribute('name', self.fileblockName)
        result.setAttribute('is-open', self.isOpen)
        for lfn, checksums, size in self:
            # checksums is a comma separated list of key:value pair
            formattedChecksums = ",".join(["%s:%s" % (x.lower(), y) for x, y \
                                           in checksums.items() \
                                           if y not in (None, '')])
            ifile = doc.createElement("file")
            ifile.setAttribute('name', lfn)
            ifile.setAttribute('checksum', formattedChecksums)
            ifile.setAttribute('bytes', str(size))
            result.appendChild(ifile)

        return result

class XMLDataset(list):
    """
    <dataset name='DatasetNameHere' is-open='boolean' is-transient='boolean'>
    <block name='fileblockname' is-open='boolean'>
    <file name='lfn1Here' checksum='cksum:0' bytes ='fileBytes1Here'/>
    <file name='lfn2Here' checksum='cksum:0' bytes ='fileBytes2Here'/>
    </block>
    </dataset>
    """

    def __init__(self, datasetName, datasetOpen = "y",
                 datasetTransient = "n" ):
        list.__init__(self)
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
        if fileblockName in self.fileblocks:
            return self.fileblocks[fileblockName]

        newFileblock = XMLFileblock(fileblockName, isOpen)
        self.fileblocks[fileblockName] = newFileblock
        return newFileblock

    def save(self):
        """
        _save_

        serialise object into PhEDEx injection XML format

        """

        impl = getDOMImplementation()
        doc = impl.createDocument(None, "dataset", None)
        dataset = doc.createElement("dataset")
        dataset.setAttribute('is-open', self.datasetIsOpen)
        dataset.setAttribute('is-transient', self.datasetIsTransient)
        dataset.setAttribute('name', self.datasetName)

        for block in self.fileblocks.values():
            dataset.appendChild(block.save())

        return dataset

class XMLInjectionSpec:
    """
    _XMLInjectionSpec_
    <data version='2'>
    <dbs name='DBSNameHere'>

    <dataset name='DatasetNameHere' is-open='boolean' is-transient='boolean'>
    <block name='fileblockname' is-open='boolean'>
    <file name='lfn1Here' checksum='cksum:0' bytes ='fileSize1Here'/>
    <file name='lfn2Here' checksum='cksum:0' bytes ='fileSize2Here'/>
    </block>
    </dataset>
    <dataset name='DatasetNameHere' is-open='boolean' is-transient='boolean'>
    <block name='fileblockname' is-open='boolean'>
    <file name='lfn1Here' checksum='cksum:0' bytes ='fileSize1Here'/>
    <file name='lfn2Here' checksum='cksum:0' bytes ='fileSize2Here'/> </block>
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
        _getDataset_

        """
        if datasetName in self.datasetPaths:
            return self.datasetPaths[datasetName]

        newDataset = XMLDataset(datasetName, isOpen, isTransient)
        self.datasetPaths[datasetName] = newDataset
        return newDataset

    def save(self):
        """
        _save_

        serialise object into PhEDEx injection XML format

        """
        impl   = getDOMImplementation()
        doc    = impl.createDocument(None, "data", None)
        result = doc.createElement("data")
        result.setAttribute('version', '2')

        dbs = doc.createElement('dbs')
        dbs.setAttribute('dls', 'dbs')
        dbs.setAttribute('name', self.dbs)
        result.appendChild(dbs)

        for dataset in self.datasetPaths.values():
            dbs.appendChild(dataset.save())

        return result.toprettyxml()

    def write(self, filename):
        """
        _write_

        Write to file using name provided

        """
        with open(filename, 'w') as handle:
            improv = self.save()
            handle.write(improv)
        return


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

    xmlString = spec.save()
    return xmlString

def makePhEDExXMLForBlocks(dbsUrl, datasets):
    """
    _makePhEDExXMLForBlocks_

    Given a DBS Url, dictionary of datasets and blocks,
    generate an XML structure for injection
    It assumes that all blocks are closed
    """
    spec = XMLInjectionSpec(dbsUrl)
    for datasetPath in datasets:
        xmlDataset = spec.getDataset(datasetPath)
        for blockPath in datasets[datasetPath]:
            xmlDataset.getFileblock(blockPath, 'n')

    xmlString = spec.save()
    return xmlString

def makePhEDExDrop(dbsUrl, datasetPath, *blockNames):
    """
    _makePhEDExDrop_

    Given a DBS Url, dataset name and list of blockNames,
    generate an XML structure for injection

    """
    from WMCore.Services.DBS.DBS3Reader import DBS3Reader
    reader = DBS3Reader(dbsUrl)

    spec = XMLInjectionSpec(dbsUrl)

    dataset = spec.getDataset(datasetPath)

    for block in blockNames:
        blockContent = reader.getFileBlock(block)
        if blockContent['IsOpen']:
            xmlBlock = dataset.getFileblock(block, "y")
        else:
            xmlBlock = dataset.getFileblock(block, "n")

        # Any Checksum from DBS is type cksum
        for x in blockContent[block]['Files']:
            checksums = {'cksum' : x['Checksum']}
            if x.get('Adler32') not in (None, ''):
                checksums['adler32'] = x['Adler32']
            xmlBlock.addFile(x['LogicalFileName'], checksums, x['FileSize'])

    xml = spec.save()
    return xml