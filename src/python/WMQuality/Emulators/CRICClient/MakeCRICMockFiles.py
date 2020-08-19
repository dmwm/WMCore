#!/usr/bin/env python
"""
MakeCRICMockFiles
Program to create mock SiteDB JSON files used by the SiteDB mock-based emulator
"""

from __future__ import division, print_function
from future import standard_library
standard_library.install_aliases()

import json
import os
from urllib.error import HTTPError

from WMCore.Services.CRIC.CRIC import CRIC
from WMCore.WMBase import getTestBase

if __name__ == '__main__':
    calls = ['people', 'site-names', 'data-processing']

    additionals = {
        'site-names': [
            {"site_name": "T2_XX_SiteA", "type": "psn", "alias": "T2_XX_SiteA"},
            {"site_name": "T2_XX_SiteB", "type": "psn", "alias": "T2_XX_SiteB"},
            {"site_name": "T2_XX_SiteC", "type": "psn", "alias": "T2_XX_SiteC"},
            {"site_name": "T2_XX_SiteA", "type": "phedex", "alias": "T2_XX_SiteA"},
            {"site_name": "T2_XX_SiteB", "type": "phedex", "alias": "T2_XX_SiteB"},
            {"site_name": "T2_XX_SiteC", "type": "phedex", "alias": "T2_XX_SiteC"},
        ],
        'data-processing': [
            {"phedex_name": "T2_XX_SiteA", "psn_name": "T2_XX_SiteA"},
            {"phedex_name": "T2_XX_SiteB", "psn_name": "T2_XX_SiteB"},
            {"phedex_name": "T2_XX_SiteC", "psn_name": "T2_XX_SiteC"},
        ]
    }

    lookup = {}

    outFile = 'CRICMockData.json'
    outFilename = os.path.join(getTestBase(), '..', 'data', 'Mock', outFile)
    print("Creating the file %s with mocked CRIC data" % outFilename)

    cric = CRIC()
    for callname in calls:
        print("Querying %s ..." % callname)
        try:
            if callname == 'people':
                result = cric._CRICUserQuery(callname)
            else:
                result = cric._CRICSiteQuery(callname)
        except HTTPError as exc:
            result = 'Raises HTTPError'

        if callname in ["site-names", "data-processing"]:
            result.extend(additionals[callname])
        else:
            result = result
        lookup.update({callname:result})

    with open(outFilename, 'w') as mockData:
        json.dump(lookup, mockData, indent=1, separators=(',', ': '), sort_keys=True)
