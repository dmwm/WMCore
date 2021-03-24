"""
Web tools.
Author: Valentin Kuznetsov <vkuznet {AT} gmail [DOT] com>
"""

from __future__ import print_function
from builtins import object, int

# system modules
import json
import logging
import os
import sys
import cgi
import types
from datetime import datetime, timedelta
from json import JSONEncoder
from time import mktime
from wsgiref.handlers import format_date_time

# cherrypy modules
import cherrypy
from cherrypy import log as cplog
from cherrypy import expose

try:
    from Cheetah.Template import Template
except:
    pass
try:
    import jinja2
except:
    pass


def quote(data):
    """
    Sanitize the data using cgi.escape.
    """
    if isinstance(data, (int, float, dict, list)):
        res = data
    else:
        try:
            if data:
                res = cgi.escape(data, quote=True)
            else:
                res = ""
        except Exception as exc:
            print("Unable to cgi.escape(%s, quote=True)" % data)
            print(exc)
            res = ""
    return res

class Page(object):
    """
    __Page__

    Page is a base class that holds a configuration
    """
    def __init__(self):
        self.name = "Page"

    def warning(self, msg):
        """Define warning log"""
        if  msg:
            self.log(msg, logging.WARNING)

    def exception(self, msg):
        """Define exception log"""
        if  msg:
            self.log(msg, logging.ERROR)

    def debug(self, msg):
        """Define debug log"""
        if  msg:
            self.log(msg, logging.DEBUG)

    def info(self, msg):
        """Define info log"""
        if  msg:
            self.log(msg, logging.INFO)

    def log(self, msg, severity):
        """Define log level"""
        if not isinstance(msg, str):
            msg = str(msg)
        if  msg:
            cplog(msg, context=self.name,
                    severity=severity, traceback=False)

class TemplatedPage(Page):
    """
    TemplatedPage is a class that provides simple Cheetah templating
    """
    def __init__(self, config):
        Page.__init__(self)
        tmpldir  = os.environ.get('RM_TMPLPATH', os.getcwd()+'/templates')
        self.templatedir = config.get('tmpldir', tmpldir)
        self.name = "TemplatedPage"
        self.base = config.get('base', '')
        self.jinja = True if 'jinja2' in sys.modules else False
        if  self.jinja:
            templates = 'JINJA'
        else:
            templates = 'Cheetah'
        self.log("### ReqMgr uses %s templates" % templates, logging.INFO)
        self.log("Templates are located in: %s" % self.templatedir, logging.INFO)

    def templatepage(self, ifile=None, *args, **kwargs):
        """Choose template page handler based on templates engine"""
        if  self.jinja and self.templatedir.find("jinja") != -1:
            return self.templatepage_jinja(ifile, *args, **kwargs)
        return self.templatepage_cheetah(ifile, *args, **kwargs)

    def templatepage_cheetah(self, ifile=None, *args, **kwargs):
        """
        Template page method.
        """
        search_list = [{'base':self.base}]
        if len(args) > 0:
            search_list.append(args)
        if len(kwargs) > 0:
            search_list.append(kwargs)
        templatefile = os.path.join(self.templatedir, ifile + '.tmpl')
        if os.path.exists(templatefile):
            # little workaround to fix '#include'
            search_list.append({'templatedir': self.templatedir})
            template = Template(file=templatefile, searchList=search_list)
            return template.respond()

        else:
            self.warning("%s not found at %s" % (ifile, self.templatedir))
            return "Template %s not known" % ifile

    def templatepage_jinja(self, ifile=None, *args, **kwargs):
        """
        Template page method.
        """
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(self.templatedir))
        for arg in args:
            kwargs.update(**arg)
        kwargs.update(**{"quote":quote})
        tmpl = os.path.join(self.templatedir, ifile + '.tmpl')
        if  os.path.exists(tmpl):
            template = env.get_template(ifile + '.tmpl')
            return template.render(kwargs)
        else:
            self.warning("%s not found at %s" % (ifile, self.templatedir))
            return "Template %s not known" % ifile

def exposetext (func):
    """CherryPy expose Text decorator"""
    @expose
    def wrapper (self, *args, **kwds):
        """Decorator wrapper"""
        data = func (self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "text/plain"
        return data
    return wrapper

def jsonstreamer(func):
    """JSON streamer decorator"""
    def wrapper (self, *args, **kwds):
        """Decorator wrapper"""
        cherrypy.response.headers['Content-Type'] = "application/json"
        func._cp_config = {'response.stream': True}
        head, data = func (self, *args, **kwds)
        yield json.dumps(head)[:-1] # do not yield }
        yield ', "data": ['
        if  isinstance(data, dict):
            for chunk in JSONEncoder().iterencode(data):
                yield chunk
        elif  isinstance(data, list) or isinstance(data, types.GeneratorType):
            sep = ''
            for rec in data:
                if  sep:
                    yield sep
                for chunk in JSONEncoder().iterencode(rec):
                    yield chunk
                if  not sep:
                    sep = ', '
        else:
            msg = 'jsonstreamer, improper data type %s' % type(data)
            raise Exception(msg)
        yield ']}'
    return wrapper

def exposejson (func):
    """CherryPy expose JSON decorator"""
    @expose
    def wrapper (self, *args, **kwds):
        """Decorator wrapper"""
        encoder = JSONEncoder()
        data = func (self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "text/json"
        try:
            jsondata = encoder.encode(data)
            return jsondata
        except Exception as exp:
            Exception("Fail to JSONtify obj '%s' type '%s', %s" \
                % (data, type(data), exp))
    return wrapper

def exposejs (func):
    """CherryPy expose JavaScript decorator"""
    @expose
    def wrapper (self, *args, **kwds):
        """Decorator wrapper"""
        data = func (self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "text/javascript"
        return data
    return wrapper

def exposecss (func):
    """CherryPy expose CSS decorator"""
    @expose
    def wrapper (self, *args, **kwds):
        """Decorator wrapper"""
        data = func (self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "text/css"
        return data
    return wrapper

def make_timestamp(seconds=0):
    """Create timestamp"""
    then = datetime.now() + timedelta(seconds=seconds)
    return mktime(then.timetuple())

def make_rfc_timestamp(seconds=0):
    """Create RFC timestamp"""
    return format_date_time(make_timestamp(seconds))
