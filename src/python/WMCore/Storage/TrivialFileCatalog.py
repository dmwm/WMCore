#!/usr/bin/env python
"""
_TrivialFileCatalog_

Object to contain LFN to PFN mappings from a Trivial File Catalog
and provide functionality to match LFNs against them

Usage:

given a TFC file, invoke readTFC on it. This will return
a TrivialFileCatalog instance that can be used to match LFNs
to PFNs.

Usage: Given a TFC constact string: trivialcatalog_file:/path?protocol=proto


    filename = tfcFilename(tfcContactString)
    protocol = tfcProtocol(tfcContactString)
    tfcInstance = readTFC(filename)

    lfn = "/store/PreProd/unmerged/somelfn.root"

    pfn = tfcInstance.matchLFN(protocol, lfn)


"""

from builtins import next, str, range
from future.utils import viewitems

from future import standard_library
standard_library.install_aliases()

import os
import re

from urllib.parse import urlsplit
from xml.dom.minidom import Document

from WMCore.Algorithms.ParseXMLFile import xmlFileToNode

_TFCArgSplit = re.compile("\?protocol=")


class TrivialFileCatalog(dict):
    """
    _TrivialFileCatalog_

    Object that can map LFNs to PFNs based on contents of a Trivial
    File Catalog
    """

    def __init__(self):
        dict.__init__(self)
        self['lfn-to-pfn'] = []
        self['pfn-to-lfn'] = []
        self.preferredProtocol = None  # attribute for preferred protocol

    def addMapping(self, protocol, match, result,
                   chain=None, mapping_type='lfn-to-pfn'):
        """
        _addMapping_

        Add an lfn to pfn mapping to this instance

        """
        entry = {}
        entry.setdefault("protocol", protocol)
        entry.setdefault("path-match-expr", re.compile(match))
        entry.setdefault("path-match", match)
        entry.setdefault("result", result)
        entry.setdefault("chain", chain)
        self[mapping_type].append(entry)

    def _doMatch(self, protocol, path, style, caller):
        """
        Generalised way of building up the mappings.
        caller is the method from there this method was called, it's used
        for resolving chained rules

        Return None if no match

        """
        for mapping in self[style]:
            if mapping['protocol'] != protocol:
                continue
            if mapping['path-match-expr'].match(path) or mapping["chain"] != None:
                if mapping["chain"] != None:
                    oldpath = path
                    path = caller(mapping["chain"], path)
                    if not path:
                        continue
                splitList = []
                if len(mapping['path-match-expr'].split(path, 1)) > 1:
                    for split in range(len(mapping['path-match-expr'].split(path, 1))):
                        s = mapping['path-match-expr'].split(path, 1)[split]
                        if s:
                            splitList.append(s)
                else:
                    path = oldpath
                    continue
                result = mapping['result']
                for split in range(len(splitList)):
                    result = result.replace("$" + str(split + 1), splitList[split])
                return result

        return None

    def matchLFN(self, protocol, lfn):
        """
        _matchLFN_

        Return the result for the LFN provided if the LFN
        matches the path-match for that protocol

        Return None if no match

        """
        result = self._doMatch(protocol, lfn, "lfn-to-pfn", self.matchLFN)
        return result

    def matchPFN(self, protocol, pfn):
        """
        _matchLFN_

        Return the result for the LFN provided if the LFN
        matches the path-match for that protocol

        Return None if no match

        """
        result = self._doMatch(protocol, pfn, "pfn-to-lfn", self.matchPFN)
        return result

    def getXML(self):
        """
        Converts TFC implementation (dict) into a XML string representation.
        The method reflects this class implementation - dictionary containing
        list of mappings while each mapping (i.e. entry, see addMapping
        method) is a dictionary of key, value pairs.

        """

        def _getElementForMappingEntry(entry, mappingStyle):
            xmlDocTmp = Document()
            element = xmlDocTmp.createElement(mappingStyle)
            for k, v in viewitems(entry):
                # ignore empty, None or compiled regexp items into output
                if not v or (k == "path-match-expr"):
                    continue
                element.setAttribute(k, str(v))
            return element

        xmlDoc = Document()
        root = xmlDoc.createElement("storage-mapping")  # root element name
        for mappingStyle, mappings in viewitems(self):
            for mapping in mappings:
                mapElem = _getElementForMappingEntry(mapping, mappingStyle)
                root.appendChild(mapElem)
        return root.toprettyxml()

    def __str__(self):
        result = ""
        for mapping in ['lfn-to-pfn', 'pfn-to-lfn']:
            for item in self[mapping]:
                result += "\t%s: protocol=%s path-match-re=%s result=%s" % (
                    mapping,
                    item['protocol'],
                    item['path-match-expr'].pattern,
                    item['result'])
                if item['chain'] != None:
                    result += " chain=%s" % item['chain']
                result += "\n"
        return result


def tfcProtocol(contactString):
    """
    _tfcProtocol_

    Given a Trivial File Catalog contact string, extract the
    protocol from it.

    """
    args = urlsplit(contactString)[3]
    value = args.replace("protocol=", '')
    return value


def tfcFilename(contactString):
    """
    _tfcFilename_

    Extract the filename from a TFC contact string.

    """
    value = contactString.replace("trivialcatalog_file:", "")
    value = _TFCArgSplit.split(value)[0]
    path = os.path.normpath(value)
    return path


def readTFC(filename):
    """
    _readTFC_

    Read the file provided and return a TrivialFileCatalog
    instance containing the details found in it

    """
    if not os.path.exists(filename):
        msg = "TrivialFileCatalog not found: %s" % filename
        raise RuntimeError(msg)

    try:
        node = xmlFileToNode(filename)
    except Exception as ex:
        msg = "Error reading TrivialFileCatalog: %s\n" % filename
        msg += str(ex)
        raise RuntimeError(msg)

    parsedResult = nodeReader(node)

    tfcInstance = TrivialFileCatalog()
    for mapping in ['lfn-to-pfn', 'pfn-to-lfn']:
        for entry in parsedResult[mapping]:
            protocol = entry.get("protocol", None)
            match = entry.get("path-match", None)
            result = entry.get("result", None)
            chain = entry.get("chain", None)
            if True in (protocol, match == None):
                continue
            tfcInstance.addMapping(str(protocol), str(match), str(result), chain, mapping)
    return tfcInstance


def loadTFC(contactString):
    """
    _loadTFC_

    Given the contact string for the tfc, parse out the file location
    and the protocol and create a TFC instance

    """
    protocol = tfcProtocol(contactString)
    catalog = tfcFilename(contactString)
    instance = readTFC(catalog)
    instance.preferredProtocol = protocol
    return instance


def coroutine(func):
    """
    _coroutine_

    Decorator method used to prime coroutines

    """

    def start(*args, **kwargs):
        cr = func(*args, **kwargs)
        next(cr)
        return cr

    return start


def nodeReader(node):
    """
    _nodeReader_

    Given a node, see if we can find what we're looking for
    """

    processLfnPfn = {
        'path-match': processPathMatch(),
        'protocol': processProtocol(),
        'result': processResult(),
        'chain': processChain()
    }

    report = {'lfn-to-pfn': [], 'pfn-to-lfn': []}
    processSMT = processSMType(processLfnPfn)
    processor = expandPhEDExNode(processStorageMapping(processSMT))
    processor.send((report, node))
    return report


@coroutine
def expandPhEDExNode(target):
    """
    _expandPhEDExNode_

    If pulling a TFC from the PhEDEx DS, its wrapped in a top level <phedex> node,
    this routine handles that extra node if it exists
    """
    while True:
        report, node = (yield)
        sentPhedex = False
        for subnode in node.children:
            if subnode.name == "phedex":
                target.send((report, subnode))
                sentPhedex = True
        if not sentPhedex:
            target.send((report, node))


@coroutine
def processStorageMapping(target):
    """
    Process everything

    """
    while True:
        report, node = (yield)
        for subnode in node.children:
            if subnode.name == 'storage-mapping':
                target.send((report, subnode))


@coroutine
def processSMType(targets):
    """
    Process the type of storage-mapping

    """
    while True:
        report, node = (yield)
        for subnode in node.children:
            if subnode.name in ['lfn-to-pfn', 'pfn-to-lfn']:
                tmpReport = {'path-match-expr': subnode.name}
                targets['protocol'].send((tmpReport, subnode.attrs.get('protocol', None)))
                targets['path-match'].send((tmpReport, subnode.attrs.get('path-match', None)))
                targets['result'].send((tmpReport, subnode.attrs.get('result', None)))
                targets['chain'].send((tmpReport, subnode.attrs.get('chain', None)))
                report[subnode.name].append(tmpReport)


@coroutine
def processPathMatch():
    """
    Process path-match

    """
    while True:
        report, value = (yield)
        report['path-match'] = value


@coroutine
def processProtocol():
    """
    Process protocol

    """
    while True:
        report, value = (yield)
        report['protocol'] = value


@coroutine
def processResult():
    """
    Process result

    """
    while True:
        report, value = (yield)
        report['result'] = value


@coroutine
def processChain():
    """
    Process chain

    """
    while True:
        report, value = (yield)
        if value == "":
            report['chain'] = None
        else:
            report['chain'] = value
