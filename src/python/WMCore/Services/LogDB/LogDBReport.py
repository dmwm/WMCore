#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : LogDBReport.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: LogDB report class to represent LogDB messages
"""
from __future__ import print_function

from builtins import range
from builtins import object

class LogDBReport(object):
    """LogDBReport class to represent LogDB messages"""
    def __init__(self, logdb):
        self.logdb = logdb

    def docs(self, request):
        """Fetch LogDB messages for given request"""
        if  request == 'all':
            docs = self.logdb.get_all_requests()
        else:
            docs = self.logdb.get(request)
        return docs

    def orderby(self, docs, order):
        """Order LogDB messages by given type"""
        odict = {}
        for item in docs:
            odict[item['ts']] = item
        keys = sorted(odict.keys())
        keys.reverse()
        out = []
        for key in keys:
            out.append(odict[key])
        return out

    def to_json(self, request, order='ts'):
        """Represent given messages in JSON data-format for given set of requests"""
        docs = self.orderby(self.docs(request), order)
        return docs

    def to_txt(self, request, order='ts', sep=' '):
        """Represent given messages in ASCII text format for given set of requests"""
        docs = self.orderby(self.docs(request), order)
        keys = list(docs[0])
        out = sep.join(keys) + '\n'
        for doc in docs:
            values = []
            for key in keys:
                values.append('%s' % doc[key])
            out += sep.join(values) + '\n'
        return out

    def to_html(self, request, order='ts'):
        """Represent given messages in ASCII text format for given set of requests"""
        out = '<table id="logdb-report">\n'
        for doc in self.to_txt(request, order, sep='</td><td>').split('\n'):
            if  doc:
                out += '<tr><td>'+doc+'</td></tr>\n'
        out += '</table>'
        return out

    def to_stdout(self, request, order='ts'):
        """Yield to stdout LogDB messages for given request/type"""
        msg = '\nReport for %s' % request
        print(msg, '\n', '-'*len(msg))
        docs = self.orderby(self.docs(request), order)
        times = []
        messages = []
        mtypes = []
        for doc in docs:
            times.append(str(doc['ts']))
            messages.append(doc['msg'])
            mtypes.append(doc['type'])
        tstpad = max([len(t) for t in times])
        msgpad = max([len(m) for m in messages])
        mtppad = max([len(m) for m in mtypes])
        out = []
        for idx in range(len(times)):
            tcol = '%s%s' % (times[idx], ' '*(tstpad-len(times[idx])))
            mcol = '%s%s' % (messages[idx], ' '*(msgpad-len(messages[idx])))
            ecol = '%s%s' % (mtypes[idx], ' '*(mtppad-len(mtypes[idx])))
            print("%s %s %s" % (tcol, mcol, ecol))
