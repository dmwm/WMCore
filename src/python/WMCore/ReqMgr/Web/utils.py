#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : utils.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description:
"""
from __future__ import print_function
from builtins import str as newstr, bytes, int
from future.utils import viewitems

from future import standard_library

from Utils.PythonVersion import PY3

standard_library.install_aliases()

# system modules
import json
import time
import hashlib
import cherrypy
from urllib.error import URLError

# WMCore Modules
from Utils.Utilities import encodeUnicodeToBytes, encodeUnicodeToBytesConditional


def tstamp():
    "Generic time stamp"
    return time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())

def gen_color(val):
    "Generate unique color code for given string value"
    keyhash = hashlib.md5()
    keyhash.update(encodeUnicodeToBytesConditional(val, condition=PY3))
    col = '#%s' % keyhash.hexdigest()[:6]
    return col

def json2form(jsondata, indent=2, keep_first_value=True):
    "Convert input json dict into one used by HTML form"
    if  keep_first_value:
        for key, val in viewitems(jsondata):
            if  isinstance(val, list):
                if len(val) == 0:
                    jsondata[key] = ""
                else:
                    jsondata[key] = val[0]
    return json.dumps(jsondata, indent=2)

def json2table(jsondata, web_ui_map, visible_attrs=None, selected={}):
    """
    Convert input json dict into HTML table based on assumtion that
    input json is in a simple key:value form.
    """
    table = """<table class="table-bordered width-100">\n"""
    table += "<thead><tr><th>Field</th><th>Value</th></tr></thead>\n"
    keys = sorted(jsondata.keys())
    # move up keys whose values have REPLACE prefix
    priority_keys = []
    rest_keys = []
    for key in keys:
        val = jsondata[key]
        if  isinstance(val, (newstr, bytes)) and val.startswith('REPLACE-'):
            priority_keys.append(key)
        else:
            rest_keys.append(key)
    cells = {}
    for key in priority_keys+rest_keys:
        val = jsondata[key]
        if  isinstance(val, list) and not val: # empty list replace with input text tag
            val = ""
        if  isinstance(val, list):
            if  not visible_attrs:
                sel = '<textarea name="%s" class="width-100">%s</textarea>' \
                        % (key, json.dumps(val))
            else:

                MULTI_SELECTION_KEYS = ['SiteWhitelist', 'SiteBlacklist']
                if key in MULTI_SELECTION_KEYS:
                    sel = "<select class=\"width-100\" name=\"%s\" multiple>" % key
                else:
                    sel = "<select class=\"width-100\" name=\"%s\">" % key

                if key in selected:
                    values = val
                else:
                    values = sorted(val)

                if  key in ['CMSSWVersion', 'ScramArch']:
                    values.reverse()
                # when there is no value to be selected
                if key in selected and not selected[key]:
                    sel += "<option selected disabled>--select an option--</option>"
                for item in values:
                    if key in selected and item in selected[key]:
                        sel += "<option value=\"%s\" selected=\"selected\">%s</option>" % (item, item)
                    else:
                        sel += "<option value=\"%s\">%s</option>" % (item, item)
                sel += "</select>"
            val = sel
        elif isinstance(val, (newstr, bytes)):
            if  val.startswith('REPLACE-'):
                val = '<input type="text" name="%s" placeholder="%s" class="width-100">'\
                        % (key, val)
            elif  len(val) < 80:
                val = '<input type="text" name="%s" value="%s" class="width-100" />' % (key, val)
            else:
                val = '<textarea name="%s" class="width-100">%s</textarea>' % (key, val)
        elif isinstance(val, (dict, list)):
            val = '<textarea name="%s" class="width-100">%s</textarea>' % (key, json.dumps(val))
        else:
            val = '<input type="text" name="%s" value="%s" class="width-100" />' % (key, val)
        if  key in web_ui_map:
            kname = web_ui_map[key]
        else:
            # use original key
            kname = key
        cells[key] = (kname, val)
    if  visible_attrs and isinstance(visible_attrs, list):
        for attr in visible_attrs:
            key, val = cells.pop(attr)
            if  key in web_ui_map:
                kname = web_ui_map[key]
            else:
                # use original key
                kname = key
            val = val.replace('width-100', 'width-100 visible_input')
            table += "<tr><td>%s</td><td class=\"visible\">%s</td></tr>\n" % (kname, val)
    for key, pair in viewitems(cells):
        kname, val = pair
        if  not visible_attrs:
            val = val.replace('<input', '<input readonly')
            val = val.replace('<textarea', '<textarea readonly')
            val = val.replace('<select', '<select disabled')
            val = val.replace('width-100', 'width-100 invisible_input')
        table += "<tr><td>%s</td><td>%s</td></tr>\n" % (kname, val)
    table += "</table>"
    return table

def genid(kwds):
    "Generate id for given field"
    if  isinstance(kwds, dict):
        record = dict(kwds)
        data = json.JSONEncoder(sort_keys=True).encode(record)
    else:
        data = str(kwds)  # it is fine both in py2 and in py3
    keyhash = hashlib.md5()
    keyhash.update(encodeUnicodeToBytesConditional(data, condition=PY3))
    return keyhash.hexdigest()

def checkarg(kwds, arg):
    """Check arg in a dict that it has str/unicode type"""
    data = kwds.get(arg, None)
    cond = data and isinstance(data, (newstr, bytes))
    return cond

def checkargs(supported):
    """
    Decorator to check arguments in provided supported list
    """
    def wrap(func):
        """Wrap input function"""

        def require_string(val):
            """Check that provided input is a string"""
            if not isinstance(val, (newstr, bytes)):
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
                for key, val in viewitems(jsondict):
                    kwds[str(key)] = str(val)

            if  not kwds:
                if  args:
                    kwds = args[-1]
            keys = []
            if  not isinstance(kwds, dict):
                code  = web_code('Unsupported kwds')
                raise URLError('code=%s' % code)
            if  kwds:
                keys = [i for i in kwds if i not in supported]
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

def reorder_list(org_list, selected):
    """
    if the first is in the list.
    move the first in front of the list
    if not, add first to the list
    """
    if isinstance(selected, list) and len(selected) == 0:
        return org_list, selected
    if not isinstance(selected, list):
        selected = [selected]
    new_list = list(org_list)
    for item in selected:
        try:
            new_list.remove(item)
        except ValueError:
            pass
    updated_list = list(selected)
    updated_list.extend(new_list)
    return updated_list, selected
