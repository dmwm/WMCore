from __future__ import (division, print_function)

from collections import defaultdict
from WMCore.Services.Service import Service
from WMCore.Services.TagCollector.XMLUtils import xml_parser


class TagCollector(Service):
    """
    Class which provides interface to CMS TagCollector web-service.
    Provides non-deprecated CMSSW releases in all their ScramArchs (not only prod)
    """

    def __init__(self, url=None, **kwargs):
        """
        responseType will be either xml or json
        """
        defaultURL = "https://cmssdt.cern.ch/SDT/cgi-bin/ReleasesXML"
        # all releases types and all their archs
        self.tcArgs = kwargs
        self.tcArgs.setdefault("anytype", 1)
        self.tcArgs.setdefault("anyarch", 1)

        params = {}
        params["timeout"] = 300
        params['endpoint'] = url or defaultURL
        params.setdefault('cacheduration', 3600)
        Service.__init__(self, params)

    def _getResult(self, callname="", clearCache=False,
                   args=None, verb="GET", encoder=None, decoder=None,
                   contentType=None):
        """
        _getResult_

        retrieve JSON/XML formatted information given the service name and the
        argument dictionaries

        TODO: Probably want to move this up into Service
        """
        if not args:
            args = self.tcArgs

        cfile = callname.replace("/", "_")
        if clearCache:
            self.clearCache(cfile, args, verb)

        f = self.refreshCache(cfile, callname, args, encoder=encoder,
                              verb=verb, contentType=contentType)
        result = f.read()
        f.close()

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
        return arch_dict
