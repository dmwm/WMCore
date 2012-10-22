#!/usr/bin/env python
"""
_JSONParser_

API for parsing JSON URLs and returning as python objects.

"""

import urllib
import cStringIO
import tokenize
import logging
import os
import pwd
from WMCore.Wrappers.JsonWrapper import JSONDecoder

class JSONParser:
    """
    Parser for dealing with broken json from SiteDB
    """
    decoder = JSONDecoder()
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
                key, src = self.parse(token, src)
                token = src.next()
                if token[1] != ":":
                    raise SyntaxError("Malformed dictionary")
                value, src = self.parse(src.next(), src)
                out[key] = value
                token = src.next()
                if token[1] == ",":
                    token = src.next()
            return out, src
        elif token[1] == "[":
            out = []
            token = src.next()
            while token[1] != "]":
                item, junk_src = self.parse(token, src)
                out.append(item)
                token = src.next()
                if token[1] == ",":
                    token = src.next()
            return out, src
        elif token[0] == tokenize.STRING:
            start = token[2]
            start_char = start[1]
            end = token[3]
            end_char = end[1] - 1
            if token[4][end[1]:end[1] + 1] not in [',', ':', '}', '{', '[', ']']:
                # We have a single quote in the string
                append_this = src.next()
                end = append_this[3]
                if token[4][append_this[2][1]] == "'":
                    # We have a single quote at the end of the string
                    end_char = append_this[2][1]
                else:
                    end_char = end[1]
                # The tokeniser is broken, e.g. because the string contains the
                # separator it uses. So make a new tokeniser.
                new_src_string = token[4][0:start_char+1] + "REPLACED" + token[4][end_char:]
                new_src = cStringIO.StringIO(new_src_string).readline
                src = tokenize.generate_tokens(new_src)
                catchup_token = src.next()
                while catchup_token[1] != "'REPLACED'":
                    catchup_token = src.next()
                # src should now be where we started, but is a new generator function
            ret_val = token[4][start_char+1:end_char]
            return ret_val.decode("string-escape"), src
        elif token[0] == tokenize.NUMBER:
            try:
                return int(token[1], 0), src
            except ValueError:
                return float(token[1]), src
        else:
            print token
            raise SyntaxError("Malformed expression")


    def dictParser(self, source):
        """
        Dictionary string parser from
        Fredrik Lundh (fredrik at pythonware.com)
        on python-list
        """
        try:
            # Future proofing here - SiteDB 2 will return valid
            # json, at which time this class should be deprecated
            # This allows intermediate testing
            return self.decoder.decode(source)
        except:
            src = cStringIO.StringIO(source).readline
            src = tokenize.generate_tokens(src)
            parsed_dict, final_src = self.parse(src.next(), src)
            return parsed_dict
