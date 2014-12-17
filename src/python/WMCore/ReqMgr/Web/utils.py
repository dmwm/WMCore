#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : utils.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description:
"""

# system modules
import cgi
import json
import time
import hashlib
import cherrypy
from urllib2 import URLError

def tstamp():
    "Generic time stamp"
    return time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())

def gen_color(val):
    "Generate unique color code for given string value"
    keyhash = hashlib.md5()
    keyhash.update(val)
    col = '#%s' % keyhash.hexdigest()[:6]
    return col

def quote(data):
    """
    Sanitize the data using cgi.escape.
    """
    if  isinstance(data, int) or isinstance(data, float):
        res = data
    elif  isinstance(data, dict):
        res = data
    elif  isinstance(data, list):
        res = data
    elif  isinstance(data, long) or isinstance(data, int) or\
          isinstance(data, float):
        res = data
    else:
        try:
            if  data:
                res = cgi.escape(data, quote=True)
            else:
                res = ""
        except Exception as exc:
            print_exc(exc)
            print "Unable to cgi.escape(%s, quote=True)" % data
            res = ""
    return res

def json2table(jsondata, web_ui_map):
    """
    Convert input json dict into HTML table based on assumtion that
    input json is in a simple key:value form.
    """
    table = """<table class="table-bordered width-100">\n"""
    table += "<thead><tr><th>Field</th><th>Value</th></tr></thead>\n"
    keys = sorted(jsondata.keys())
    for key in keys:
        val = jsondata[key]
        if  isinstance(val, list) and not val: # empty list replace with input text tag
            val = ""
        if  isinstance(val, list):
            sel = "<select name=\"%s\">" % key
            values = sorted(val)
            if  key in ['releases', 'software_releases', 'CMSSWVersion', 'ScramArch']:
                values.reverse()
            for item in values:
                sel += "<option value=\"%s\">%s</option>" % (item, item)
            sel += "</select>"
            val = sel
        elif isinstance(val, basestring):
            if  val.startswith('REPLACE-'):
                val = '<input type="text" name="%s" placeholder="%s" class="width-100">'\
                        % (key, val)
            elif  len(val) < 80:
                val = '<input type="text" name="%s" value="%s" class="width-100" />' % (key, val)
            else:
                val = '<textarea name="%s" class="width-100">%s</textarea>' % (key, val)
        else:
            val = '<input type="text" name="%s" value="%s" class="width-100" />' % (key, val)
        if  key in web_ui_map:
            kname = web_ui_map[key]
        else:
            kname = key.capitalize().replace('_', ' ')
        table += "<tr><td>%s</td><td>%s</td></tr>\n" % (kname, val)
    table += "</table>"
    return table

def genid(kwds):
    "Generate id for given field"
    if  isinstance(kwds, dict):
        record = dict(kwds)
        data = json.JSONEncoder(sort_keys=True).encode(record)
    else:
        data = str(kwds)
    keyhash = hashlib.md5()
    keyhash.update(data)
    return keyhash.hexdigest()

def checkarg(kwds, arg):
    """Check arg in a dict that it has str/unicode type"""
    data = kwds.get(arg, None)
    cond = data and (isinstance(data, str) or isinstance(data, unicode))
    return cond

def checkargs(supported):
    """
    Decorator to check arguments in provided supported list
    """
    def wrap(func):
        """Wrap input function"""

        def require_string(val):
            """Check that provided input is a string"""
            if not (isinstance(val, str) or isinstance(val, unicode)):
                code = web_code('Invalid input')
                raise URLError('code=%s' % code)

        def wrapped_f(self, *args, **kwds):
            """Wrap function arguments"""
            # check request headers. For methods POST/PUT
            # we need to read request body to get parameters
            if  cherrypy.request.method == 'POST' or\
                cherrypy.request.method == 'PUT':
                try:
                    body = cherrypy.request.body.read()
                except:
                    body = None
                if  args and kwds:
                    code = web_code('Misleading request')
                    raise URLError('code=%s' % code)
                if  body:
                    jsondict = json.loads(body, encoding='latin-1')
                else:
                    jsondict = kwds
                for key, val in jsondict.iteritems():
                    kwds[str(key)] = str(val)

            if  not kwds:
                if  args:
                    kwds = args[-1]
            keys = []
            if  not isinstance(kwds, dict):
                code  = web_code('Unsupported kwds')
                raise URLError('code=%s' % code)
            if  kwds:
                keys = [i for i in kwds.keys() if i not in supported]
            if  keys:
                code  = web_code('Unsupported key')
                raise URLError('code=%s' % code)
            if  checkarg(kwds, 'status'):
                if  kwds['status'] not in \
                        ['new', 'assigned']:
                    code  = web_code('Unsupported view')
                    raise URLError('code=%s' % code)
            data = func (self, *args, **kwds)
            return data
        wrapped_f.__doc__  = func.__doc__
        wrapped_f.__name__ = func.__name__
        wrapped_f.exposed  = True
        return wrapped_f
    wrap.exposed = True
    return wrap

WEB_CODES = [
        (0  , 'N/A'),
        (1  , 'Unsupported key'),
        (2  , 'Unsupported value'),
        (3  , 'Unsupported method'),
        (4  , 'Unsupported collection'),
        (5  , 'Unsupported database'),
        (6  , 'Unsupported view'),
        (7  , 'Unsupported format'),
        (8  , 'Wrong type'),
        (9  , 'Misleading request'),
        (10 , 'Invalid query'),
        (11 , 'Exception'),
        (12 , 'Invalid input'),
        (13 , 'Unsupported expire value'),
        (14 , 'Unsupported order value'),
        (15 , 'Unsupported skey value'),
        (16 , 'Unsupported idx value'),
        (17 , 'Unsupported limit value'),
        (18 , 'Unsupported dir value'),
        (19 , 'Unsupported sort value'),
        (20 , 'Unsupported ajax value'),
        (21 , 'Unsupported show value'),
        (22 , 'Unsupported dasquery value'),
        (23 , 'Unsupported dbcoll value'),
        (24 , 'Unsupported msg value'),
        (25 , 'Unable to start DASCore'),
        (26 , 'No file id'),
        (27 , 'Unsupported id value'),
        (28 , 'Server error'),
        (29 , 'Query is not suitable for this view'),
        (30 , 'Parser error'),
        (31 , 'Unsupported pid value'),
        (32 , 'Unsupported interval value'),
        (33 , 'Unsupported kwds'),
]
def decode_code(code):
    """Return human readable string for provided code ID"""
    for idx, msg in WEB_CODES:
        if  code == idx:
            return msg
    return 'N/A'

def web_code(error):
    """Return WEB code for provided error string"""
    for idx, msg in WEB_CODES:
        if  msg.lower() == error.lower():
            return idx
    return -1

def sort(docs, sortby):
    "Sort given documents by sortby attribute"
    for doc in docs:
        yield doc
