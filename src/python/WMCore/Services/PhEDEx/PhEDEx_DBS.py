"""
Module which depends on both PhEDEx and DBS.
It provides function to create XML struction for injection.
"""

from __future__ import division

from WMCore.Services.PhEDEx.XMLDrop import XMLInjectionSpec
from WMCore.Services.DBS.DBS3Reader import DBS3Reader


def makePhEDExDrop(dbsUrl, datasetPath, *blockNames):
    """
    _makePhEDExDrop_

    Given a DBS Url, dataset name and list of blockNames,
    generate an XML structure for injection

    """
    spec = XMLInjectionSpec(dbsUrl)


    reader = DBS3Reader(dbsUrl)

    dataset = spec.getDataset(datasetPath)

    for block in blockNames:
        blockContent = reader.getFileBlock(block)
        isOpen = reader.blockIsOpen(block)
        if isOpen:
            xmlBlock = dataset.getFileblock(block, "y")
        else:
            xmlBlock = dataset.getFileblock(block, "n")

        #Any Checksum from DBS is type cksum
        for x in blockContent[block]['Files']:
            checksums = {'cksum' : x['Checksum']}
            if x.get('Adler32') not in (None, ''):
                checksums['adler32'] = x['Adler32']
            xmlBlock.addFile(x['LogicalFileName'], checksums, x['FileSize'])

    xml = spec.save()
    return xml
