from __future__ import (division, print_function)
from future import standard_library

from Utils.Utilities import decodeBytesToUnicode

standard_library.install_aliases()

import logging
from os import path
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

        self.cvmfsReleasesMap = "/cvmfs/cms.cern.ch/releases.map"
        self.tmpReleasesXML = "/tmp/ReleasesXML"

    def parseCvmfsReleasesXML(self, releasesMap, releasesXML):
        """
        _parseCvmfsReleasesXML_
        
        Parses the ReleasesXML file from the releases.map in cvmfs
        """
        production = "type=Production;"
        announced = "state=Announced;"
        anyarch = False
        architecture = ""

        with open(releasesMap, "r", encoding="utf-8") as releasesFile:
            archs = {}
            for line in releasesFile:
                rels = []
                if not anyarch and 'prodarch=1;' not in line:
                    continue

                if production and production not in line:
                    continue

                if announced and announced not in line:
                    continue

                if architecture and architecture not in line:
                    continue

                data = {}
                for item in line.split(";"):
                    if "=" not in item:
                        continue
                    k, v = item.split("=")
                    data[k] = v

                if "architecture" in data and "label" in data and "type" in data and "state" in data:
                    if not anyarch and data["label"] in rels:
                        continue

                    rels.append(data["label"])
                    arch = data["architecture"]

                    if arch not in archs:
                        archs[arch] = []

                    extraTag = ""
                    if "default_micro_arch" in data:
                        extraTag = ' default_micro_arch="%s"' % data["default_micro_arch"]

                    data["extra_tag"] = extraTag
                    archs[arch].append(
                        """<project label="%(label)s" type="%(type)s" state="%(state)s"%(extra_tag)s/>"""
                        % data
                        )

        with open(releasesXML, "w", encoding="utf-8") as xml:
            xml.write("<projects>\n")
            for arch in archs:
                xml.write('  <architecture name="%s">\n' % arch)
                for rel in archs[arch]:
                    xml.write("    %s\n" % rel)
                xml.write("  </architecture>\n")
            xml.write("</projects>\n")

        return

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
            if not args:
                args = self.tcArgs

            cFile = '%s_%s' % (self.cFileUrlPath, callname.replace("/", "_"))
            # If no callname or url path, the base host is getting queried
            if cFile == '_':
                cFile = 'baseRequest'

            if clearCache:
                self.clearCache(cFile, args, verb)

            # Note cFile is just the base name pattern, args 
            # are also considered for the end filename in the method below
            f = self.refreshCache(cFile, callname, args, encoder=encoder, decoder=decodeBytesToUnicode,
                                verb=verb, contentType=contentType)
            result = f.read()
            f.close()

        except Exception:
            logging.error('Something went wrong accessing ReleasesXML from cmssdt, perhaps the service is temporarily down')
            logging.info('Checking if cvmfs is mounted')
            cvmfsMounted = path.ismount('/cvmfs')
            if cvmfsMounted:
                logging.info('cvmfs is mounted. Retrying to access ReleasesXML from cvmfs')
                try:
                    self.parseCvmfsReleasesXML(releasesMap=self.cvmfsReleasesMap, releasesXML=self.tmpReleasesXML)
                    with open(self.tmpReleasesXML, 'r', encoding='utf-8') as f:
                        result = f.read()
                    f.close()

                except Exception:
                    logging.error('Something went wrong parsing /cvmfs/cms.cern.ch/releases.map into XML format')
                    logging.exception('Unable to access ReleasesXML from cmssdt and cvmfs')
                    raise
            else:
                logging.info('cvmfs is not mounted, nothing else to do')
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
