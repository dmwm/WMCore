#!/usr/bin/env python
"""
_DBSRESTFormatter_

A basic REST formatter. The formatter takes the data from the API call, turns it into the
appropriate format and sets the CherryPy header appropriately.

Could add YAML via http://pyyaml.org/
"""
from __future__ import (division, print_function)
from WMCore.WebTools.RESTFormatter import RESTFormatter


class DBSRESTFormatter(RESTFormatter):
    def __init__(self, config):
        RESTFormatter.__init__(self, config)
        self.supporttypes = {'application/xml': self.xml,
                             'application/atom+xml': self.atom,
                             'text/json': self.json,
                             'text/x-json': self.json,
                             'application/json': self.json,
                             'text/html': self.to_string,
                             'text/plain': self.to_string,
                             '*/*': self.json}
