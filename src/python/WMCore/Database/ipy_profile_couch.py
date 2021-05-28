#!/usr/bin/env python
# -*- coding: ISO-8859-1 -*-
# pylint: disable=E1101,C0103,R0902,E0602
# E0602: sneakily importing a lot of stuff, turn off for false positives

"""
Couch DB command line admin tool
"""
from __future__ import print_function
from future import standard_library
standard_library.install_aliases()

import urllib.parse

__license__ = "GPL"
__maintainer__ = "Valentin Kuznetsov"
__email__ = "vkuznet@gmail.com"
__status__ = "Alpha"

# system modules
import os
import sys
import inspect
import traceback

# ipython modules
from   IPython import Release
import IPython.ipapi
import __main__


class PrintManager:
    def __init__(self):
        from IPython import ColorANSI
        self.term = ColorANSI.TermColors

    def print_red(self, msg):
        """print message using red color"""
        print(self.msg_red(msg))

    def print_green(self, msg):
        """print message using blue color"""
        print(self.msg_green(msg))

    def print_blue(self, msg):
        """print message using blue color"""
        print(self.msg_blue(msg))

    def msg_red(self, msg):
        """yield message using red color"""
        if not msg:
            msg = ''
        return self.term.Red + msg + self.term.Black

    def msg_green(self, msg):
        """yield message using green color"""
        if not msg:
            msg = ''
        return self.term.Green + msg + self.term.Black

    def msg_blue(self, msg):
        """yield message using blue color"""
        if not msg:
            msg = ''
        return self.term.Blue + msg + self.term.Black


def load_config(func_list=[]):
    """
    Defines default configuration for Couch DB. We need the following:
    URI - the Couch DB URI
    DB  - the Couch DB name
    DESIGN - the Couch DB design name
    DEBUG  - debug level, used to setup HTTPConnection debug level
    """
    msg = """
import os, re, sys, time, types, traceback, inspect
import urllib, urllib2, httplib
import json
from json import JSONDecoder, JSONEncoder
try:
    from path import path
except ImportError:
    pass
try:
    from ipipe import *
except ImportError:
    pass

# global variables
URI="http://localhost:5984"
DB="das"
DESIGN="dasadmin"
DEBUG=0
"""
    msg += "\n%s\n" % inspect.getsource(PrintManager)
    msg += "PM = PrintManager()"
    for func in func_list:
        msg += "\n%s\n" % inspect.getsource(func)
    return msg


def httplib_request(host, path, params, request='POST', debug=0):
    """request method using provided HTTP request and httplib library"""
    if debug:
        httplib.HTTPConnection.debuglevel = 1
    if not isinstance(params, str):
        params = urllib.parse.urlencode(params, doseq=True)
    if debug:
        print("input parameters", params)
    headers = {"Content-type": "application/x-www-form-urlencoded",
               "Accept": "text/plain"}
    if host.find('https://') != -1:
        host = host.replace('https://', '')
        conn = httplib.HTTPSConnection(host)
    else:
        host = host.replace('http://', '')
        conn = httplib.HTTPConnection(host)
    if request == 'GET':
        conn.request(request, path)
    else:
        conn.request(request, path, params, headers)
    response = conn.getresponse()

    if response.reason != "OK":
        print(response.status, response.reason, response.read())
        res = None
    else:
        res = response.read()
    conn.close()
    return res


def print_data(data, lookup="value"):
    """
    Provides pretty print of json data based on lookup parameter
    by default lookup=value. See couch db response for more info.
    """
    jsondict = json.loads(data)
    PM.print_blue("Total %s documents" % len(jsondict['rows']))
    maxl = 0
    padding = ""
    for row in jsondict['rows']:
        values = row[lookup]
        if isinstance(values, dict):
            if not padding:
                for key in values.keys():
                    if len(key) > maxl:
                        maxl = len(key)
            for key, val in values.items():
                padding = " " * (maxl - len(key))
                print("%s%s: %s" % (padding, PM.msg_blue(key), val))
            print()
        else:
            print(values)


def set_prompt(in1):
    """Define shell prompt"""
    if in1.find('|\#>') != -1:
        in1 = in1.replace('|\#>', '').strip()
    ip = __main__.__dict__['__IP']
    prompt = getattr(ip.outputcache, 'prompt1')
    prompt.p_template = in1 + " |\#> "
    prompt.set_p_str()


def couch_help(self, arg):
    """
    Provide simple help about available commands
    """
    pmgr = PrintManager()
    global magic_list
    msg = "\nAvailable commands:\n"
    for name, func in magic_list:
        msg += "%s\n%s\n" % (pmgr.msg_blue(name), pmgr.msg_green(func.__doc__))
    msg += "List of pre-defined variables to control your interactions "
    msg += "with CouchDB:\n"
    msg += pmgr.msg_green("    URI, DB, DESIGN, DEBUG\n")
    print(msg)


### MAGIC COMMANDS ###
def db_info():
    """
    Provide information about Couch DB. Use DB parameter to setup
    your couch DB name.
    """
    host = URI
    path = '/%s' % DB
    data = httplib_request(host, path, {}, 'GET', DEBUG)
    if not data:
        return data
    return json.loads(data)


def couch_views():
    """
    List registered views in couch db.
    """
    qqq = 'startkey=%22_design%2F%22&endkey=%22_design0%22'
    host = URI
    path = '/%s/_all_docs?%s' % (DB, qqq)
    results = httplib_request(host, path, {}, 'GET', DEBUG)
    designdocs = json.loads(results)
    results = {}
    for item in designdocs['rows']:
        doc = item['key']
        print(PM.msg_blue("design: ") + doc)
        path = '/%s/%s' % (DB, doc)
        res = httplib_request(host, path, {}, 'GET', DEBUG)
        rdict = json.loads(res)
        for view_name, view_dict in rdict['views'].items():
            print(PM.msg_blue("view name: ") + view_name)
            print(PM.msg_blue("map:"))
            print(PM.msg_green(view_dict['map']))
            if 'reduce' in view_dict:
                print(PM.msg_blue("reduce:"))
                print(PM.msg_green(view_dict['reduce']))


def create_view(view_dict):
    """
    Create couch db view. The db and design names are controlled via
    DB and DESIGN shell parameters, respectively.
    Parameters: <view_dict>
    Example of the view:
    {"view_name": {"map" : "function(doc) { if(doc.hash) {emit(1, doc.hash);}}" }}
    """
    # get existing views
    host = URI
    path = '/%s/_design/%s' % (DB, DESIGN)
    data = httplib_request(host, path, {}, 'GET', DEBUG)
    jsondict = json.loads(data)
    for view_name, view_def in view_dict.items():
        jsondict['views'][view_name] = view_def

    # update views
    encoder = JSONEncoder()
    params = encoder.encode(jsondict)
    request = 'PUT'
    debug = DEBUG
    data = httplib_request(host, path, params, request, debug)
    if not data:
        return data
    return json.loads(data)


def delete_view(view_name):
    """
    Delete couch db view. The db and design names are controlled via
    DB and DESIGN shell parameters, respectively.
    Parameters: <view_name>
    """
    # get existing views
    host = URI
    path = '/%s/_design/%s' % (DB, DESIGN)
    data = httplib_request(host, path, {}, 'GET', DEBUG)
    if not data:
        return data
    jsondict = json.loads(data)

    # delete requested view in view dict document
    try:
        del jsondict['views'][view_name]
        # update view dict document in a couch
        encoder = JSONEncoder()
        params = encoder.encode(jsondict)
        request = 'PUT'
        debug = DEBUG
        data = httplib_request(host, path, params, request, debug)
    except:
        traceback.print_exc()


def delete_all_views(design):
    """
    Delete all views in particular design document.
    The db and design names are controlled via
    DB and DESIGN shell parameters, respectively.
    Parameters: <design_name, e.g. dasadmin>
    """
    host = URI
    path = '/%s/_design/%s' % (DB, design)
    data = httplib_request(host, path, {}, 'DELETE', DEBUG)
    if not data:
        return data
    return json.loads(data)


def create_db(db_name):
    """
    Create a new DB in couch.
    Parameters: <db_name>
    """
    host = URI
    path = '/%s' % db_name
    data = httplib_request(host, path, {}, 'PUT', DEBUG)
    if not data:
        return data
    return json.loads(data)


def delete_db(db_name):
    """
    Delete DB in couch.
    Parameters: <db_name>
    """
    host = URI
    path = '/%s' % db_name
    data = httplib_request(host, path, {}, 'DELETE', DEBUG)
    if not data:
        return data
    return json.loads(data)


def get_all_docs(idx=0, limit=0, pretty_print=False):
    """
    Retrieve all documents from CouchDB.
    Parameters: <idx=0> <limit=0> <pretty_print=False>
    """
    host = URI
    path = '/%s/_all_docs' % DB
    kwds = {}
    if idx:
        kwds['skip'] = idx
    if limit:
        kwds['limit'] = limit
    data = httplib_request(host, path, kwds, 'GET', DEBUG)
    if not data:
        return data
    if pretty_print:
        print_data(data, lookup='id')
    else:
        return json.loads(data)


def get_doc(id):
    """
    Retrieve document with given id from CouchDB.
    Parameters: <id, e.g. 1323764f7a6af1b37b72119920cbaa08>
    """
    host = URI
    path = '/%s/%s' % (DB, id)
    kwds = {}
    data = httplib_request(host, path, kwds, 'GET', DEBUG)
    if not data:
        return data
    return json.loads(data)


def load_module(arg):
    """
    Load custom admin module. Name it as <module>_ipython.py and place in
    your PYTHONPATH. Implement a <module>_load() function to load your stuff.
    Parameters: <module>

    Example:
    def mycmd():
        pass # do something here
    def mymodule_load():
        ip = IPython.ipapi.get()
        ip.expose_magic('mycmd', mycmd)
    """
    # try to load custom modules
    stm = "from %s_ipython import %s_load\n" % (arg, arg)
    stm += "%s_load()" % arg
    obj = compile(str(stm), '<string>', 'exec')
    try:
        eval(obj)
        msg = "Loaded %s module. " % arg
        msg += "Use " + PM.msg_blue("%s_help" % arg) + \
               " for concrete module help if it's implemented"
        print(msg)
    except:
        traceback.print_exc()
        pass


# keep magic list as global since it's used in couch_help
magic_list = [
    ('db_info', db_info),
    ('couch_views', couch_views),
    ('create_view', create_view),
    ('delete_view', delete_view),
    ('delete_all_views', delete_all_views),
    ('get_all_docs', get_all_docs),
    ('get_doc', get_doc),
    ('create_db', create_db),
    ('delete_db', delete_db),
    ('load_module', load_module),
]


def main():
    """
    Main function which defint ipython behavior
    """
    pmgr = PrintManager()

    # global IP API
    ip = IPython.ipapi.get()

    o = ip.options
    ip.expose_magic('couch_help', couch_help)
    # load commands and expose them to the shell
    for m in magic_list:
        ip.ex(inspect.getsource(m[1]))

    # load configuration for couch-sh, supply a names of functions to
    # be loaded
    ip.ex(load_config([httplib_request, print_data]))

    # autocall to "full" mode (smart mode is default, I like full mode)
    o.autocall = 2

    # Set dbsh prompt
    o.prompt_in1 = 'couch-sh |\#> '
    o.prompt_in2 = 'couch-sh> '
    o.system_verbose = 0

    # define couch-sh banner
    pyver = sys.version.split('\n')[0]
    ipyver = Release.version
    msg = "Welcome to couch-sh \n[python %s, ipython %s]\n%s\n" \
          % (pyver, ipyver, os.uname()[3])
    msg += "For couch-sh help use "
    msg += pmgr.msg_blue("couch_help")
    msg += ", for python help use help commands\n"
    o.banner = msg
    o.prompts_pad_left = "1"
    # Remove all blank lines in between prompts, like a normal shell.
    o.separate_in = "0"
    o.separate_out = "0"
    o.separate_out2 = "0"


main()
