#!/usr/bin/env python
"""
_JSONParser_

API for parsing JSON URLs and returning as python objects.

"""

__revision__ = "$Id: JSONParser.py,v 1.9 2009/08/07 14:32:15 ewv Exp $"
__version__ = "$Revision: 1.9 $"

import urllib
import cStringIO
import tokenize
import logging
import os
import pwd

from WMCore.Services.Service import Service

class JSONParser:
    """
    Parser for dealing with broken json from SiteDB
    """
    def parse(self, token, src):
        """
        Dictionary string parser from
        Fredrik Lundh (fredrik at pythonware.com)
        on python-list
        """
        if token[1] == "{":
            out = {}
            token = src.next()
            while token[1] != "}":
                key = self.parse(token, src)
                token = src.next()
                if token[1] != ":":
                    raise SyntaxError("Malformed dictionary")
                value = self.parse(src.next(), src)
                out[key] = value
                token = src.next()
                if token[1] == ",":
                    token = src.next()
            return out
        elif token[1] == "[":
            out = []
            token = src.next()
            while token[1] != "]":
                out.append(self.parse(token, src))
                token = src.next()
                if token[1] == ",":
                    token = src.next()
            return out
        elif token[0] == tokenize.STRING:
            return token[1][1:-1].decode("string-escape")
        elif token[0] == tokenize.NUMBER:
            try:
                return int(token[1], 0)
            except ValueError:
                return float(token[1])
        else:
            raise SyntaxError("Malformed expression: %s" % token)


    def dictParser(self, source):
        """
        Dictionary string parser from
        Fredrik Lundh (fredrik at pythonware.com)
        on python-list
        """
        src = cStringIO.StringIO(source).readline
        src = tokenize.generate_tokens(src)
        return self.parse(src.next(), src)
