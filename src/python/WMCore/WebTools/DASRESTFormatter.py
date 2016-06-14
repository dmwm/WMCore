#!/usr/bin/env python
"""
_DASRESTFormatter_

A REST formatter that appends the DAS headers to the result data
"""


# expires is used by the decorator to set the expires header
# pylint: disable=W0613
# I want dasjson and plist to be methods instead of functions
# pylint: disable=R0201

import json
import plistlib

from WMCore.WebTools.RESTFormatter import RESTFormatter

class DASRESTFormatter(RESTFormatter):
    """
    A REST formatter that appends the DAS headers to the result data
    """
    def __init__(self, config):
        "Initialise the formatter and set the mime types it supports"
        RESTFormatter.__init__(self, config)
        mimes = {'text/json+das':self.dasjson, 'application/xml+das':self.xml,
                 'application/plist':self.plist}
        self.supporttypes.update(mimes)

    def runDas(self, data, expires):
        """
        Run a query and produce a dictionary for DAS formatting
        """
        start_time = request.time
        results    = data
        call_time  = time.time() - start_time
        res_expire = make_timestamp(expires)

        keyhash = hashlib.md5()

        keyhash.update(str(results))
        res_checksum = keyhash.hexdigest()
        dasdata = {'application':'%s.%s' % (self.config.application, func.__name__),
                   'request_timestamp': start_time,
                   'request_url': request.base + request.path_info + '?' + \
                                                request.query_string,
                   'request_method' : request.method,
                   'request_params' : request.params,
                   'response_version': res_version,
                   'response_expires': res_expire,
                   'response_checksum': res_checksum,
                   'request_call': func.__name__,
                   'call_time': call_time,
                   'results': results,
                  }
        return dasdata

    def dasjson(self, data):
        "Return DAS compliant json"
        data = runDas(self, func, data, expires)
        thunker = JSONThunker()
        data = thunker.thunk(data)
        return json.dumps(data)

    def xml(self, data):
        "Return DAS compliant xml"
        das = runDas(self, func, data, expires)
        header = "<?xml version='1.0' standalone='yes'?>"
        keys = das.keys()
        keys.remove('results')
        string = ''
        for key in keys:
            string = '%s %s="%s"' % (string, key, das[key])
        header = "%s\n<das %s>" % (header, string)
        xmldata = header + das['results'].__str__() + "</das>"
        return xmldata

    def plist(self, data):
        "Return DAS compliant plist xml"

        data_struct = runDas(self, func, data, expires)
        plist_str = plistlib.writePlistToString(data_struct)
        return plist_str
