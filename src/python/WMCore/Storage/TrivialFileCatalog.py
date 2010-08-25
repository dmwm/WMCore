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

import os
import re
import urlparse
import xml.parsers.expat

from IMProv.IMProvLoader import loadIMProvFile
from IMProv.IMProvQuery import IMProvQuery

from WMCore.Algorithms.ParseXMLFile import Node, xmlFileToNode

_TFCArgSplit = re.compile("\?protocol=")

class TrivialFileCatalog(list):
    """
    _TrivialFileCatalog_

    Object that can map LFNs to PFNs based on contents of a Trivial
    File Catalog
    """

    def __init__(self):
        list.__init__(self)
        self.preferredProtocol = None # attribute for preferred protocol
        

    def addMapping(self, protocol, match, result, chain = None):
        """
        _addMapping_

        Add an lfn to pfn mapping to this instance

        """
        entry = {}
        entry.setdefault("protocol", protocol)
        entry.setdefault("path-match-expr", match)
        entry.setdefault("path-match", re.compile(match))
        entry.setdefault("result", result)
        entry.setdefault("chain", chain)
        self.append(entry)
        

    def matchLFN(self, protocol, lfn):
        """
        _matchLFN_

        Return the result for the LFN provided if the LFN
        matches the path-match for that protocol

        Return None if no match
        """
        for mapping in self:
            if mapping['protocol'] != protocol:
                continue
            if mapping['path-match'].match(lfn):
                if mapping['chain'] != None:
                    lfn = self.matchLFN(mapping['chain'], lfn)
                try:
                    splitLFN = mapping['path-match'].split(lfn, 1)[1]
                except IndexError:
                    continue
                result = mapping['result'].replace("$1", splitLFN)
                return result

        return None

    def __str__(self):
        result = ""
        for item in self:
            result += "LFN-to-PFN: %s %s %s" % (
                item['protocol'],
                item['path-match-expr'],
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
    args = urlparse.urlsplit(contactString)[3]
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
        raise RuntimeError, msg


    try:
        node = xmlFileToNode(filename)
    except StandardError, ex:
        msg = "Error reading TrivialFileCatalog: %s\n" % filename
        msg += str(ex)
        raise RuntimeError, msg

    parsedResult = nodeReader(node)

    tfcInstance = TrivialFileCatalog()

    for entry in parsedResult:
        protocol = entry.get("protocol", None)
        match    = entry.get("path-match", None)
        result   = entry.get("result", None)
        chain    = entry.get("chain", None)
        if True in (protocol, match == None):
            continue
        tfcInstance.addMapping(str(protocol), str(match), str(result), chain)


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
    def start(*args,**kwargs):
        cr = func(*args,**kwargs)
        cr.next()
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

    report = []

    processSMT = processSMType(processLfnPfn)

    processor = processStorageMapping(processSMT)

    processor.send((report, node))

    return report


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
            if subnode.name == 'lfn-to-pfn':
                tmpReport = {'path-match-expr': subnode.name}
                targets['protocol'].send((tmpReport, subnode.attrs.get('protocol', None)))
                targets['path-match'].send((tmpReport, subnode.attrs.get('path-match', None)))
                targets['result'].send((tmpReport, subnode.attrs.get('result', None)))
                targets['chain'].send((tmpReport, subnode.attrs.get('chain', None)))
                report.append(tmpReport)
        #print report

                
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
        report['chain'] = value







