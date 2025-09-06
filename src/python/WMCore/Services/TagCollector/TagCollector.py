from __future__ import (division, print_function)
from future import standard_library

from Utils.Utilities import decodeBytesToUnicode

standard_library.install_aliases()

import logging
import cgi

from urllib.parse import urlparse

from collections import defaultdict
from WMCore.Services.Service import Service
from WMCore.Services.TagCollector.XMLUtils import xml_parser

class TagCollector(Service):
    """
    Class which provides interface to CMS TagCollector web-service.
    Provides non-deprecated CMSSW releases in all their ScramArchs (not only prod)
    """

    def __init__(self, url=None, logger=None, configDict=None, **kwargs):
        """
        responseType will be either xml or json
        """
        defaultURL = "https://cmssdt.cern.ch/SDT/cgi-bin/ReleasesXML"
        url = url or defaultURL
        parsedUrl = urlparse(url)
        self.cFileUrlPath = parsedUrl.path.replace("/", "_")
        # all releases types and all their archs
        self.tcArgs = kwargs
        self.tcArgs.setdefault("anytype", 1)
        self.tcArgs.setdefault("anyarch", 1)

        configDict = configDict or {}
        configDict.setdefault('endpoint', url)
        configDict.setdefault("timeout", 300)
        configDict.setdefault('cacheduration', 1)
        configDict['logger'] = logger if logger else logging.getLogger()
        super(TagCollector, self).__init__(configDict)
        self['logger'].debug("Initializing TagCollector with url: %s", self['endpoint'])

    def parseReleasesXML(outputFile='/tmp/ReleasesXML'):

        form = cgi.FieldStorage()

        production = "type=Production;"
        if ("anytype" in form) and form["anytype"].value=="1":
            production=""

        announced="state=Announced;"
        if ("deprel" in form) and form["deprel"].value=="1":
            announced="state=Deprecated;"
        
        anyarch=False
        if ("anyarch" in form) and form["anyarch"].value=="1":
            anyarch=True

        architecture=""
        if "architecture" in form:
            architecture="architecture="+form["architecture"].value+";"

        releasesFilename = "/cvmfs/cms.cern.ch/releases.map"
        releasesFile = open(releasesFilename, "r")

        archs = {}
        rels  = []
        for line in releasesFile:
            if (not anyarch) and ('prodarch=1;' not in line): 
                continue
        
            if production   and production   not in line: 
                continue
        
            if announced    and announced    not in line: 
                continue
        
            if architecture and architecture not in line: 
                continue
  
            data = {}
            for item in line.split(";"):
                if "=" not in item: 
                    continue 
                k,v = item.split("=")
                data[k]= v



            if ("architecture" in data) and ("label" in data) and ("type" in data) and ("state" in data):
                if (not anyarch) and (data["label"] in rels): 
                    continue
    
                rels.append(data["label"])
                arch = data["architecture"]
                if arch not in archs: 
                    archs[arch]=[]

                extraTag = ""
                if "default_micro_arch" in data:
                    extraTag = " default_micro_arch=\"%s\"" % data["default_micro_arch"]

                data["extra_tag"]=extraTag
                archs[arch].append("""<project label="%(label)s" type="%(type)s" state="%(state)s"%(extra_tag)s/>""" % data)
    
            releasesFile.close()

            with open(outputFile, "w", encoding="utf-8") as f:
                f.write("<projects>\n")
                for arch in archs:
                    f.write('  <architecture name="%s">\n' % arch)
                    for rel in archs[arch]:
                        f.write("    %s\n" % rel)
                    f.write("  </architecture>\n")
                f.write("</projects>\n")


    def _getResult(self, callname="", clearCache=False,
                   args=None, verb="GET", encoder=None, decoder=None,
                   contentType=None):
        """
        _getResult_

        retrieve JSON/XML formatted information given the service name and the
        argument dictionaries

        TODO: Probably want to move this up into Service
        """
        
        try:
            self.parseReleasesXML()
            f = '/tmp/ReleasesXML'
            result = f.read()
            f.close()
        except:
            logging.exception(f'Something went wrong parsing /cvmfs/cms.cern.ch/releases.map into XML format')
            raise

        # overhead from REST model which returns results as strings or None
        # therefore they can be encoded by JSON to None, etc.
        if result == 'None':
            return
        if result and decoder:
            result = decoder(result)
        return result

    def data(self):
        "Fetch data from tag collector or local cache"
        data = self._getResult()
        pkey = 'architecture'
        for row in xml_parser(data, pkey):
            yield row[pkey]

    def releases(self, arch=None):
        "Yield CMS releases known in tag collector"
        arr = []
        for row in self.data():
            if arch:
                if arch == row['name']:
                    for item in row['project']:
                        arr.append(item['label'])
            else:
                for item in row['project']:
                    arr.append(item['label'])
        return list(set(arr))

    def architectures(self):
        "Yield CMS architectures known in tag collector"
        arr = []
        for row in self.data():
            arr.append(row['name'])
        return list(set(arr))

    def releases_by_architecture(self):
        "returns CMS architectures and realease in dictionary format"
        arch_dict = defaultdict(list)
        for row in self.data():
            releases = set()
            for item in row['project']:
                releases.add(item['label'])
            arch_dict[row['name']].extend(list(releases))
        return dict(arch_dict)


    def defaultMicroArchVersionNumberByRelease(self, default_microarch=0):
        """
        Yield default microarchitecture by CMS release known in tag collector.

        :param default_microarch: int, default microar when not found in TagCollector XML file.
        :return: dictionary ("release": default_microarch (or default_microarch if not found)).
        """
        rel_microarchs = {}
        for row in self.data():
            for item in row['project']:
                microarch = item.get('default_micro_arch', default_microarch)
                if isinstance(microarch, str):
                    # format of this value is "arch-vN"
                    microarch = int((microarch.split("-")[-1]).lstrip("v"))
                rel_microarchs[item['label']] = microarch
        return rel_microarchs

    def getGreaterMicroarchVersionNumber(self, releases=None, default_microarch=0, rel_microarchs=None):
        """
        Return the greated default microarchitecture, given a list of releases.

        :param releases: str, comma-separated list of releases.
        :param default_microarch: int, default microar when not found in TagCollector XML file.
        :return: int, greater default microarchitecture version number.
        """
        if releases is None:
            return default_microarch

        if rel_microarchs is None:
            rel_microarchs = self.defaultMicroArchVersionNumberByRelease()

        releases = releases.split(",")
        microarch = default_microarch
        for r in releases:
            m = rel_microarchs.get(r, default_microarch)
            if m > microarch:
                microarch = m

        return microarch
