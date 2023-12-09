#!/usr/bin/env python
"""
_RucioFileCatalog_

Object to contain LFN to PFN mappings from a Rucio File Catalog
and provide functionality to match LFNs against them

Usage:

given a storage description file, storage.json, execute readRFC. This will return
a RucioFileCatalog instance that can be used to match LFNs to PFNs.
"""

from builtins import next, str, range
from future.utils import viewitems

from future import standard_library
standard_library.install_aliases()

import os
import re

import json

from urllib.parse import urlsplit
from xml.dom.minidom import Document

from WMCore.Algorithms.ParseXMLFile import xmlFileToNode

class RucioFileCatalog(dict):
    """
    _RucioFileCatalog_

    Object that can map LFNs to PFNs based on contents of a Rucio
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
        :param protocol: the name of a protocol, for example XRootD
        :path: a LFN path, for example /store/abc/xyz.root
        :style: type of conversion. lfn-to-pfn is to convert LFN to PFN and pfn-to-pfn is for PFN to LFN
        :caller is the method from there this method was called. It's used for resolving chained rules
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


def storageJsonPath(currentSite, currentSubsite, storageSite):
    """
    Return a path to storage.json from site names
    :para currentSite and currentSubsite are the names of facilities where jobs will be executed, storageSite is the storage facility for a stage-out
    :return /pathToStorageJson/storage.json
    """
    #get site config
    siteConfigPath = os.getenv('SITECONFIG_PATH',None)
    if not siteConfigPath:
        raise RuntimeError('SITECONFIG_PATH is not defined')
    subPath = ''
    #the storage site is where jobs are executed so use local path given in SITECONFIG_PATH to locate storage.json
    if currentSite == storageSite:
        #it is a site (no defined subSite), storage.json is located at the path given in SITECONFIG_PATH
        if currentSubsite is None:
            subPath = siteConfigPath
        #it is a subsite, move one level up
        else:
            subPath = siteConfigPath + '/..'
    #cross site
    else:
        #it is a site (no defined subSite), move one level up
        if currentSubsite is None:
            subPath = siteConfigPath + '/../' + storageSite
        #it is a subsite, move two levels up
        else:
            subPath = siteConfigPath + '/../../' + storageSite
    pathToStorageDescription = subPath + '/storage.json'
    pathToStorageDescription = os.path.normpath(os.path.realpath(pathToStorageDescription))#resolve symbolic link and relative path?
    return pathToStorageDescription 

def readRFC(filename, storageSite, volume, protocol):
    """
    _readRFC_

    Read the provided storage.json and return a RucioFileCatalog
    instance containing the details found in it
    """
    rfcInstance = RucioFileCatalog()
    try:
        jsonFile = open(filename,encoding="utf-8")
        jsElements = json.load(jsonFile)
    except Exception as ex:
        msg = "Error reading storage description file: %s\n" % filename
        msg += str(ex)
        raise RuntimeError(msg)
    #now loop over elements, select the right one and fill lfn-to-pfn
    for jsElement in jsElements:
        #check to see if the storageSite and volume matchs with "site" and "volume" in storage.json
        if jsElement['site'] == storageSite and jsElement['volume'] == volume: 
            rfcInstance.preferredProtocol = protocol
            #now loop over protocols to add all mappings (needed for chained rule cases)
            for prot in jsElement['protocols']:
                #check if prefix is in protocol block
                if 'prefix' in prot.keys():
                    #lfn-to-pfn
                    match = '/(.*)' #match all
                    result = prot['prefix']+'/$1'
                    #prefix case should not be chained
                    chain = None 
                    rfcInstance.addMapping(str(prot['protocol']), str(match), str(result), chain, 'lfn-to-pfn')
                    #pfn-to-lfn
                    match = prot['prefix']+'/(.*)'
                    result = '/$1'
                    rfcInstance.addMapping(str(prot['protocol']), str(match), str(result), chain, 'pfn-to-lfn')
                #here is rules  
                else:
                    #loop over rules
                    for rule in prot['rules']:
                        match = rule['lfn']
                        result = rule['pfn']
                        chain = rule['chain'] if 'chain' in rule.keys() else None
                        rfcInstance.addMapping(str(prot['protocol']), str(match), str(result), chain, 'lfn-to-pfn')
                        #pfn-to-lfn: not sure about this!!!
                        match = rule['pfn'].replace('$1','(.*)')
                        result = rule['lfn'].replace('/+','/').replace('^/','/')
                        #now replace anything inside () with $1, for example (.*) --> $1, (store/.*) --> $1
                        result = re.sub('\(.*\)','$1',result)
                        rfcInstance.addMapping(str(prot['protocol']), str(match), str(result), chain, 'pfn-to-lfn')
    
    return rfcInstance

def rseName(currentSite,currentSubsite,storageSite,volume):
    """
    Return Rucio storage element name, for example https://gitlab.cern.ch/SITECONF/T1_DE_KIT/-/blob/master/storage.json?ref_type=heads#L39
    :currentSite is the site where jobs are executing
    :currentSubsite is the sub site if jobs are running here
    :storageSite is the site for storage
    :volume is the volume name, for example https://gitlab.cern.ch/SITECONF/T1_DE_KIT/-/blob/master/storage.json?ref_type=heads#L3
    """
    rse = None
    storageJsonName = storageJsonPath(currentSite,currentSubsite,storageSite)
    try:
        with open(storageJsonName,encoding="utf-8") as jsonFile:
            jsElements = json.load(jsonFile)
    except Exception as ex:
        msg = "RucioFileCatalog.py:rseName_rucio() Error reading storage.json: %s\n" % storageJsonName 
        msg += str(ex)
        raise RuntimeError(msg)
    for jsElement in jsElements:
        if jsElement['site'] == storageSite and jsElement['volume'] == volume:
            rse = jsElement['rse']
            break
    return rse
