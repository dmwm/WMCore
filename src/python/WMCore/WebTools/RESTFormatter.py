#!/usr/bin/env python
"""
_RESTFormatter_

A basic REST formatter. The formatter takes the data from the API call, turns it into the
appropriate format and sets the CherryPy header appropriately.

Could add YAML via http://pyyaml.org/
"""

from builtins import str

import json
from types import GeneratorType

from cherrypy import response, HTTPError, request

from WMCore.WebTools.Page import TemplatedPage, _setCherryPyHeaders
from WMCore.Wrappers.JsonWrapper.JSONThunker import JSONThunker

from Utils.Utilities import decodeBytesToUnicodeConditional
from Utils.PythonVersion import PY3

class RESTFormatter(TemplatedPage):
    def __init__(self, config):
        self.supporttypes = {
                    'text/json': self.json,
                    'application/xml': self.xml,
                   'application/atom+xml': self.atom,
                   'text/x-json': self.json,
                   'application/json': self.json,
                   'text/html': self.to_string,
                   'text/plain': self.to_string,
                   '*/*': self.to_string}

        TemplatedPage.__init__(self, config)

    def genstreamer(self, data):
        yield "[\n"
        firstItem  = True
        for item in data:
            if not firstItem:
                yield "\n,\n"
            else:
                firstItem = False
            yield json.dumps(item)
        yield "\n]"

    def json(self, data):
        if isinstance(data, GeneratorType):
            out = ''.join([r for r in self.genstreamer(data)])
            return out
        thunker = JSONThunker()
        data = thunker.thunk(data)
        return json.dumps(data)

    def xml(self, data):
        if isinstance(data, GeneratorType):
            data = [i for i in data]
        return self.templatepage('XML', data = data,
                                config = self.config,
                                path = request.path_info)

    def atom(self, data):
        if isinstance(data, GeneratorType):
            data = [i for i in data]
        return self.templatepage('Atom', data = data,
                                config = self.config,
                                path = request.path_info)

    def to_string(self, data):
        """
        Since json.dumps returns unicode in py3 and bytes in py2 (it behaves
        returning "native str" in both versions), then we do the same with all
        the data that is not GeneratorType, dict nor list by calling str(data)
        Moreover, we need to properly decode bytes.

        :returns: "native str" (unicode in py3, bytes in py2)
        """
        if isinstance(data, GeneratorType):
            return self.json(data)
        if isinstance(data, dict) or isinstance(data, list):
            return json.dumps(data)
        if not isinstance(data, bytes):
            return str(data)
        return decodeBytesToUnicodeConditional(data, condition=PY3)

    def format(self, data, datatype, expires):
        response_data = ''
        func = self.supporttypes[datatype]
        if datatype not in self.supporttypes:
            response.status = 406
            expires=0
            response_data = self.supporttypes['text/plain']({'exception': 406,
                                                'type': 'HTTPError',
            'message': '%s is not supported. Valid accept headers are: %s' %\
                    (datatype, list(self.supporttypes))})

        try:
            response_data = self.supporttypes[datatype](data)
        except HTTPError as h:
            # This won't be triggered with a default formatter, but could be by a subclass
            response.status = h.args[0]
            expires=0
            rec = {'exception': h.args[0],'type': 'HTTPError','message': h.args[1]}
            response_data = func(rec)
        except Exception as e:
            response.status = 500
            expires=0
            rec = {'exception': 500, 'type': e.__class__.__name__, 'message': 'Server Error'}
            response_data = func(rec)
        _setCherryPyHeaders(response_data, datatype, expires)
        return response_data
