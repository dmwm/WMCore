#!/usr/bin/env python
# -*- coding: ISO-8859-1 -*-

"""
web server.
__author__ = "Valentin Kuznetsov"
"""
from __future__ import print_function

from builtins import str as newstr, bytes, map
from future.utils import viewitems, viewvalues

# system modules
import collections
import json
import os
import pprint
import sys
import time
from copy import deepcopy
from datetime import datetime

# cherrypy modules
import cherrypy
from cherrypy import config as cherryconf
from cherrypy import expose, response, tools
from cherrypy.lib.static import serve_file

# import WMCore itself to determine path of modules
import WMCore
from Utils.CertTools import getKeyCertFromEnv
from WMCore.REST.Auth import get_user_info
# WMCore modules
from WMCore.ReqMgr.DataStructs.RequestStatus import ACTIVE_STATUS
from WMCore.ReqMgr.DataStructs.RequestStatus import REQUEST_STATE_TRANSITION, REQUEST_HUMAN_STATES
from WMCore.ReqMgr.DataStructs.RequestStatus import get_modifiable_properties, get_protected_properties
from WMCore.ReqMgr.Tools.cms import lfn_bases, lfn_unmerged_bases
from WMCore.ReqMgr.Tools.cms import releases, dashboardActivities
from WMCore.ReqMgr.Tools.cms import site_white_list, site_black_list
from WMCore.ReqMgr.Tools.cms import web_ui_names, SITE_CACHE, PNN_CACHE
from WMCore.ReqMgr.Utils.Validation import get_request_template_from_type
# ReqMgrSrv modules
from WMCore.ReqMgr.Web.tools import exposecss, exposejs, TemplatedPage
from WMCore.ReqMgr.Web.utils import gen_color
from WMCore.ReqMgr.Web.utils import json2table, json2form, genid, checkargs, tstamp, sort, reorder_list
from WMCore.Services.LogDB.LogDB import LogDB
# new reqmgr2 APIs
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.Services.pycurl_manager import RequestHandler
from WMCore.WMSpec.StdSpecs.StdBase import StdBase
from WMCore.Cache.GenericDataCache import MemoryCacheStruct


def getdata(url, params, headers=None):
    "Helper function to get data from the service"
    ckey, cert = getKeyCertFromEnv()
    mgr = RequestHandler()
    res = mgr.getdata(url, params=params, headers=headers, ckey=ckey, cert=cert)
    return json.loads(res)


def sort_bold(docs):
    "Return sorted list of bold items from provided doc list"
    return ', '.join(['<b>%s</b>' % m for m in sorted(docs)])


def set_headers(itype, size=0):
    """
    Set response header Content-type (itype) and Content-Length (size).
    """
    if size > 0:
        response.headers['Content-Length'] = size
    response.headers['Content-Type'] = itype
    response.headers['Expires'] = 'Sat, 14 Oct 2027 00:59:30 GMT'


def set_no_cache_flags():
    "Set cherrypy flags to prevent caching"
    cherrypy.response.headers['Cache-Control'] = 'no-cache'
    cherrypy.response.headers['Pragma'] = 'no-cache'
    cherrypy.response.headers['Expires'] = 'Sat, 01 Dec 2001 00:00:00 GMT'


def set_cache_flags():
    "Set cherrypy flags to prevent caching"
    headers = cherrypy.response.headers
    for key in ['Cache-Control', 'Pragma']:
        if key in headers:
            del headers[key]


def minify(content):
    """
    Remove whitespace in provided content.
    """
    content = content.replace('\n', ' ')
    content = content.replace('\t', ' ')
    content = content.replace('   ', ' ')
    content = content.replace('  ', ' ')
    return content


def menus():
    "Return dict of menus"
    items = ['home', 'create', 'approve', 'assign', 'batches']
    return items


def request_attr(doc, attrs=None):
    "Return request attributes/values in separate document"
    if not attrs:
        attrs = ['RequestName', 'Requestdate', 'Inputdataset', \
                 'Prepid', 'Group', 'Requestor', 'RequestDate', \
                 'RequestStatus']
    rdict = {}
    for key in attrs:
        if key in doc:
            if key == 'RequestDate':
                tval = doc[key]
                if isinstance(tval, list):
                    while len(tval) < 9:
                        tval.append(0)
                    # we do not know if dayling savings time was in effect or not
                    tval[-1] = -1
                    gmt = time.gmtime(time.mktime(tuple(tval)))
                    rdict[key] = time.strftime("%Y-%m-%d %H:%M:%S GMT", gmt)
                else:
                    rdict[key] = tval
            else:
                rdict[key] = doc[key]
    return rdict


def spec_list(root):
    "Return list of specs from given root directory"
    specs = []
    for fname in os.listdir(root):
        if not fname.endswith('.py') or fname == '__init__.py':
            continue
        sname = fname.split('.')[0]
        clsName = "%sWorkloadFactory" % sname
        with open(os.path.join(root, fname)) as fd:
            if clsName in fd.read():
                specs.append(sname)
    return specs


def user():
    """
    Return user name associated with this instance.
    """
    try:
        return get_user_info()['login']
    except:
        return 'testuser'


def user_dn():
    "Return user DN"
    try:
        return get_user_info()['dn']
    except:
        return '/CN/bla/foo'


def check_scripts(scripts, resource, path):
    """
    Check a script is known to the resource map
    and that the script actually exists
    """
    for script in scripts:
        if script not in resource:
            spath = os.path.normpath(os.path.join(path, script))
            if os.path.isfile(spath):
                resource.update({script: spath})
    return scripts


def _map_configcache_url(tConfigs, baseURL, configIDName, configID, taskName=""):
    if configIDName.endswith('ConfigCacheID') and configID is not None:
        url = "%s/reqmgr_config_cache/%s/configFile" % (baseURL, configID)
        prefix = "%s: " % taskName if taskName else ""
        task = "%s%s: %s " % (prefix, configIDName, configID)
        tConfigs.setdefault(task, url)
    return


def tasks_configs(docs, html=False):
    "Helper function to provide mapping between tasks and configs"
    if not isinstance(docs, list):
        docs = [docs]
    tConfigs = {}
    for doc in docs:
        name = doc.get('RequestName', '')
        if "TaskChain" in doc:
            chainTypeFlag = True
            ctype = "Task"
        elif "StepChain" in doc:
            chainTypeFlag = True
            ctype = "Step"
        else:
            chainTypeFlag = False
            ctype = None

        curl = doc.get('ConfigCacheUrl', 'https://cmsweb.cern.ch/couchdb')
        if curl == None or curl == "none":
            curl = 'https://cmsweb.cern.ch/couchdb'
        if not name:
            continue
        for key, val in viewitems(doc):
            _map_configcache_url(tConfigs, curl, key, val)
            if chainTypeFlag and key.startswith(ctype) and isinstance(val, dict):
                for kkk in val:
                    # append task/step number and name
                    keyStr = "%s: %s" % (key, val.get("%sName" % ctype, ''))
                    _map_configcache_url(tConfigs, curl, kkk, val[kkk], keyStr)
    if html:
        out = '<fieldset><legend>Config Cache List</legend><ul>'
        for task in sorted(tConfigs):
            out += '<li><a href="%s" target="config_page">%s</a></li>' % (tConfigs[task], task)
        out += '</ul></fieldset>'
        return out
    return tConfigs


def state_transition(docs):
    "Helper function to provide mapping between tasks and configs"
    if not isinstance(docs, list):
        docs = [docs]

    out = '<fieldset><legend>State Transition</legend><ul>'
    multiDocFlag = True if len(docs) > 1 else False
    for doc in docs:
        name = doc.get('RequestName', '')
        sTransition = doc.get('RequestTransition', '')

        if not name:
            continue
        if multiDocFlag:
            out += '%s<br />' % name
        for sInfo in sTransition:
            out += '<li><b>%s</b>: %s UTC <b>DN</b>: %s</li>' % (sInfo["Status"],
                                                                 datetime.utcfromtimestamp(
                                                                     sInfo["UpdateTime"]).strftime('%Y-%m-%d %H:%M:%S'),
                                                                 sInfo["DN"])
    out += '</ul></fieldset>'
    return out


def priority_transition(docs):
    "create html for priority transition format"
    if not isinstance(docs, list):
        docs = [docs]

    out = '<fieldset><legend>Priority Transition</legend><ul>'
    multiDocFlag = True if len(docs) > 1 else False
    for doc in docs:
        name = doc.get('RequestName', '')
        pTransition = doc.get('PriorityTransition', '')

        if not name:
            continue
        if multiDocFlag:
            out += '%s<br />' % name
        for pInfo in pTransition:
            out += '<li><b>%s</b>: %s UTC <b>DN</b>: %s</li>' % (pInfo["Priority"],
                                                                 datetime.utcfromtimestamp(
                                                                     pInfo["UpdateTime"]).strftime('%Y-%m-%d %H:%M:%S'),
                                                                 pInfo["DN"])
    out += '</ul></fieldset>'
    return out


# code taken from
# http://stackoverflow.com/questions/1254454/fastest-way-to-convert-a-dicts-keys-values-from-unicode-to-str
def toString(data):
    if isinstance(data, (newstr, bytes)):
        return str(data)
    elif isinstance(data, collections.Mapping):
        return dict(list(map(toString, viewitems(data))))
    elif isinstance(data, collections.Iterable):
        return type(data)(list(map(toString, data)))
    else:
        return data


def getPropValueMap():
    """
    Return all possible values for some assignment arguments
    """
    prop_value_map = {'CMSSWVersion': releases(),
                      'SiteWhitelist': SITE_CACHE.getData(),
                      'SiteBlacklist': SITE_CACHE.getData(),
                      'SubscriptionPriority': ['Low', 'Normal', 'High'],
                      'CustodialSites': PNN_CACHE.getData(),
                      'NonCustodialSites': PNN_CACHE.getData(),
                      'MergedLFNBase': lfn_bases(),
                      'UnmergedLFNBase': lfn_unmerged_bases(),
                      'TrustPUSitelists': [True, False],
                      'TrustSitelists': [True, False],
                      'Dashboard': dashboardActivities()}
    return prop_value_map


class ReqMgrService(TemplatedPage):
    """
    Request Manager web service class
    """

    def __init__(self, app, config, mount):
        self.base = config.base
        self.rootdir = '/'.join(WMCore.__file__.split('/')[:-1])
        if config and not isinstance(config, dict):
            web_config = config.dictionary_()
        if not config:
            web_config = {'base': self.base}
        TemplatedPage.__init__(self, web_config)
        imgdir = os.environ.get('RM_IMAGESPATH', os.getcwd() + '/images')
        self.imgdir = web_config.get('imgdir', imgdir)
        cssdir = os.environ.get('RM_CSSPATH', os.getcwd() + '/css')
        self.cssdir = web_config.get('cssdir', cssdir)
        jsdir = os.environ.get('RM_JSPATH', os.getcwd() + '/js')
        self.jsdir = web_config.get('jsdir', jsdir)
        spdir = os.environ.get('RM_SPECPATH', os.getcwd() + '/specs')
        self.spdir = web_config.get('spdir', spdir)
        # read scripts area and initialize data-ops scripts
        self.sdir = os.environ.get('RM_SCRIPTS', os.getcwd() + '/scripts')
        self.sdir = web_config.get('sdir', self.sdir)
        self.sdict_thr = web_config.get('sdict_thr', 600)  # put reasonable 10 min interval
        self.sdict = {'ts': time.time()}  # placeholder for data-ops scripts
        self.update_scripts(force=True)

        # To be filled at run time
        self.cssmap = {}
        self.jsmap = {}
        self.imgmap = {}
        self.yuimap = {}

        std_specs_dir = os.path.join(self.rootdir, 'WMSpec/StdSpecs')
        self.std_specs = spec_list(std_specs_dir)
        self.std_specs.sort()

        # Update CherryPy configuration
        mime_types = ['text/css']
        mime_types += ['application/javascript', 'text/javascript',
                       'application/x-javascript', 'text/x-javascript']
        cherryconf.update({'tools.encode.on': True,
                           'tools.gzip.on': True,
                           'tools.gzip.mime_types': mime_types,
                           })
        self._cache = {}

        # initialize access to reqmgr2 APIs
        self.reqmgr_url = config.reqmgr.reqmgr2_url
        self.reqmgr = ReqMgr(self.reqmgr_url)
        # only gets current view (This might cause to reponse time much longer,
        # If upto date view is not needed overwrite Fale)
        self.reqmgr._noStale = True

        # get fields which we'll use in templates
        cdict = config.reqmgr.dictionary_()
        self.couch_url = cdict.get('couch_host', '')
        self.couch_dbname = cdict.get('couch_reqmgr_db', '')
        self.couch_wdbname = cdict.get('couch_workload_summary_db', '')
        self.acdc_url = cdict.get('acdc_host', '')
        self.acdc_dbname = cdict.get('acdc_db', '')
        self.configcache_url = cdict.get('couch_config_cache_url', self.couch_url)
        self.dbs_url = cdict.get('dbs_url', '')
        self.dqm_url = cdict.get('dqm_url', '')
        self.sw_ver = cdict.get('default_sw_version', 'CMSSW_7_6_1')
        self.sw_arch = cdict.get('default_sw_scramarch', 'slc6_amd64_gcc493')

        # LogDB holder
        centralurl = cdict.get("central_logdb_url", "")
        identifier = cdict.get("log_reporter", "reqmgr2")
        self.logdb = LogDB(centralurl, identifier)

        # local team cache which will request data from wmstats
        base, uri = self.reqmgr_url.split('://')
        base_url = '%s://%s' % (base, uri.split('/')[0])
        self.wmstatsurl = cdict.get('wmstats_url', '%s/wmstatsserver' % base_url)
        if not self.wmstatsurl:
            raise Exception('ReqMgr2 configuration file does not provide wmstats url')
        # cache team information for 2 hours to limit wmstatsserver API calls
        self.TEAM_CACHE = MemoryCacheStruct(7200, self.refreshTeams)

        # fetch assignment arguments specification from StdBase
        self.assignArgs = StdBase().getWorkloadAssignArgs()
        self.assignArgs = {key: val['default'] for key, val in viewitems(self.assignArgs)}

    def getTeams(self):
        return self.TEAM_CACHE.getData()

    def refreshTeams(self):
        "Helper function to cache team info from wmstats"
        url = '%s/data/teams' % self.wmstatsurl
        params = {}
        headers = {'Accept': 'application/json'}
        try:
            data = getdata(url, params, headers)
            if 'error' in data:
                print("WARNING: fail to get teams from %s" % url)
                print(data)
            teams = data.get('result', [])
            return teams
        except Exception as exp:
            print("WARNING: fail to get teams from %s" % url)
            print(str(exp))

    def update_scripts(self, force=False):
        "Update scripts dict"
        if force or abs(time.time() - self.sdict['ts']) > self.sdict_thr:
            if os.path.isdir(self.sdir):
                for item in os.listdir(self.sdir):
                    with open(os.path.join(self.sdir, item), 'r') as istream:
                        self.sdict[item.split('.')[0]] = istream.read()
            self.sdict['ts'] = time.time()

    def abs_page(self, tmpl, content):
        """generate abstract page"""
        menu = self.templatepage('menu', menus=menus(), tmpl=tmpl)
        body = self.templatepage('generic', menu=menu, content=content)
        page = self.templatepage('main', content=body, user=user())
        return page

    def page(self, content):
        """
        Provide page wrapped with top/bottom templates.
        """
        return self.templatepage('main', content=content)

    def error(self, content):
        "Generate common error page"
        content = self.templatepage('error', content=content)
        return self.abs_page('error', content)

    @expose
    def index(self):
        """Main page"""
        content = self.templatepage('index', requests=ACTIVE_STATUS, rdict=REQUEST_STATE_TRANSITION)
        return self.abs_page('main', content)

    @expose
    def home(self, **kwds):
        """Main page"""
        return self.index(**kwds)

    ### Request actions ###

    @expose
    @checkargs(['status', 'sort'])
    def assign(self, **kwds):
        """assign page"""
        if not kwds:
            kwds = {}
        if 'status' not in kwds:
            kwds.update({'status': 'assignment-approved'})
        docs = []
        attrs = ['RequestName', 'RequestDate', 'Group', 'Requestor', 'RequestStatus']
        dataResult = self.reqmgr.getRequestByStatus(statusList=[kwds['status']])
        for data in dataResult:
            for val in viewvalues(data):
                docs.append(request_attr(val, attrs))
        sortby = kwds.get('sort', 'status')
        docs = [r for r in sort(docs, sortby)]
        assignDict = deepcopy(self.assignArgs)
        assignDict.update(getPropValueMap())
        assignDict['Team'] = self.getTeams()
        filter_sort = self.templatepage('filter_sort')
        content = self.templatepage('assign', sort=sortby,
                                    filter_sort_table=filter_sort,
                                    sites=SITE_CACHE.getData(),
                                    site_white_list=site_white_list(),
                                    site_black_list=site_black_list(),
                                    user=user(), user_dn=user_dn(), requests=toString(docs),
                                    misc_table=json2table(assignDict, web_ui_names(), "all_attributes"),
                                    misc_json=json2form(assignDict, indent=2, keep_first_value=True))
        return self.abs_page('assign', content)

    @expose
    @checkargs(['status', 'sort'])
    def approve(self, **kwds):
        """
        Approve page: get list of request associated with user DN.
        Fetch their status list from ReqMgr and display if requests
        were seen by data-ops.
        """
        if not kwds:
            kwds = {}
        if 'status' not in kwds:
            kwds.update({'status': 'new'})
        kwds.update({'_nostale': True})
        docs = []
        attrs = ['RequestName', 'RequestDate', 'Group', 'Requestor', 'RequestStatus', 'Campaign']
        dataResult = self.reqmgr.getRequestByStatus(statusList=[kwds['status']])
        for data in dataResult:
            for val in viewvalues(data):
                docs.append(request_attr(val, attrs))
        sortby = kwds.get('sort', 'status')
        docs = [r for r in sort(docs, sortby)]
        filter_sort = self.templatepage('filter_sort')
        content = self.templatepage('approve', requests=toString(docs), date=tstamp(),
                                    sort=sortby, filter_sort_table=filter_sort,
                                    gen_color=gen_color)
        return self.abs_page('approve', content)

    @expose
    def create(self, **kwds):
        """create page"""
        # get list of standard specs from WMCore and new ones from local area
        # loc_specs_dir = os.path.join(self.spdir, 'Specs') # local specs
        # loc_specs = spec_list(loc_specs_dir, 'Specs')
        # all_specs = list(set(self.std_specs + loc_specs))
        # all_specs.sort()
        all_specs = list(self.std_specs)
        spec = kwds.get('form', '')
        if not spec:
            spec = self.std_specs[0]
        # make spec first in all_specs list
        if spec in all_specs:
            all_specs.remove(spec)
        all_specs = [spec] + all_specs
        jsondata = get_request_template_from_type(spec)
        # create templatized page out of provided forms
        self.update_scripts()
        content = self.templatepage('create', table=json2table(jsondata, web_ui_names(), jsondata),
                                    jsondata=json2form(jsondata, indent=2, keep_first_value=True), name=spec,
                                    scripts=[s for s in self.sdict if s != 'ts'],
                                    specs=all_specs)
        return self.abs_page('create', content)

    def generate_objs(self, script, jsondict):
        """Generate objects from givem JSON template"""
        self.update_scripts()
        code = self.sdict.get(script, '')
        if code.find('def genobjs(jsondict)') == -1:
            return self.error(
                "Improper python snippet, your code should start with <b>def genobjs(jsondict)</b> function")
        exec (code)  # code snippet must starts with genobjs function
        return [r for r in genobjs(jsondict)]

    @expose
    def config(self, name):
        "Fetch config for given request name"
        result = self.reqmgr.getConfig(name)
        if len(result) == 1:
            result = result[0]
        else:
            result = 'Configuration not found for: %s' % name
        return result.replace('\n', '<br/>')

    @expose
    def fetch(self, rid):
        "Fetch document for given id"
        rid = rid.replace('request-', '')
        doc = self.reqmgr.getRequestByNames(rid)
        transitions = []
        tst = time.time()
        # get request tasks
        tasks = self.reqmgr.getRequestTasks(rid)
        if len(doc) == 1:
            try:
                doc = doc[0][rid]
            except:
                pass
            name = doc.get('RequestName', 'NA')
            title = 'Request %s' % name
            status = doc.get('RequestStatus', '')
            transitions = REQUEST_STATE_TRANSITION.get(status, [])
            if status in transitions:
                transitions.remove(status)
            visible_attrs = get_modifiable_properties(status)
            filterout_attrs = get_protected_properties()
            # extend filterout list with "RequestStatus" since it is passed separately
            filterout_attrs.append("RequestStatus")

            for key, val in viewitems(self.assignArgs):
                if not doc.get(key):
                    doc[key] = val

            if visible_attrs == "all_attributes":
                filteredDoc = doc
                for prop in filterout_attrs:
                    if prop in filteredDoc:
                        del filteredDoc[prop]
            else:
                filteredDoc = {}
                for prop in visible_attrs:
                    filteredDoc[prop] = doc.get(prop, "")

            propValueMap = getPropValueMap()
            propValueMap['Team'] = self.getTeams()

            selected = {}
            for prop in propValueMap:
                if prop in filteredDoc:
                    filteredDoc[prop], selected[prop] = reorder_list(propValueMap[prop], filteredDoc[prop])

            content = self.templatepage('doc', title=title, status=status, name=name, rid=rid,
                                        tasks=json2form(tasks, indent=2, keep_first_value=False),
                                        table=json2table(filteredDoc, web_ui_names(), visible_attrs, selected),
                                        jsondata=json2form(doc, indent=2, keep_first_value=False),
                                        doc=json.dumps(doc), time=time,
                                        tasksConfigs=tasks_configs(doc, html=True),
                                        sTransition=state_transition(doc),
                                        pTransition=priority_transition(doc),
                                        transitions=transitions, humanStates=REQUEST_HUMAN_STATES,
                                        ts=tst, user=user(), userdn=user_dn())
        elif len(doc) > 1:
            jsondata = [pprint.pformat(d) for d in doc]
            content = self.templatepage('doc', title='Series of docs: %s' % rid,
                                        table="", jsondata=jsondata, time=time,
                                        tasksConfigs=tasks_configs(doc, html=True),
                                        sTransition=state_transition(doc),
                                        pTransition=priority_transition(doc),
                                        transitions=transitions, humanStates=REQUEST_HUMAN_STATES,
                                        ts=tst, user=user(), userdn=user_dn())
        else:
            doc = 'No request found for name=%s' % rid
        return self.abs_page('request', content)

    @expose
    def record2logdb(self, **kwds):
        """LogDB submission page"""
        print(kwds)
        request = kwds['request']
        msg = kwds['message']
        self.logdb.post(request, msg)
        msg = '<h6>Confirmation</h6>Your request has been entered to LogDB.'
        return self.abs_page('generic', msg)

    @expose
    def requests(self, **kwds):
        """Page showing requests"""
        if not kwds:
            kwds = {}
        if 'status' not in kwds:
            kwds.update({'status': 'acquired'})
        dataResult = self.reqmgr.getRequestByStatus(kwds['status'])
        attrs = ['RequestName', 'RequestDate', 'Group', 'Requestor', 'RequestStatus', 'Campaign']
        docs = []
        for data in dataResult:
            for doc in viewvalues(data):
                docs.append(request_attr(doc, attrs))
        sortby = kwds.get('sort', 'status')
        docs = [r for r in sort(docs, sortby)]
        filter_sort = self.templatepage('filter_sort')
        content = self.templatepage('requests', requests=toString(docs), sort=sortby,
                                    status=kwds['status'], filter_sort_table=filter_sort)
        return self.abs_page('requests', content)

    @expose
    def request(self, **kwargs):
        "Get data example and expose it as json"
        dataset = kwargs.get('uinput', '')
        if not dataset:
            return {'error': 'no input dataset'}
        url = 'https://cmsweb.cern.ch/reqmgr2/data/request?outputdataset=%s' % dataset
        params = {}
        headers = {'Accept': 'application/json'}
        wdata = getdata(url, params, headers)
        wdict = dict(date=time.ctime(), team='Team-A', status='Running', ID=genid(wdata))
        winfo = self.templatepage('workflow', wdict=wdict,
                                  dataset=dataset, code=pprint.pformat(wdata))
        content = self.templatepage('search', content=winfo)
        return self.abs_page('request', content)

    @expose
    def batch(self, **kwds):
        """batch page"""
        # TODO: we need a template for batch attributes
        #       and read it from separate area, like DASMaps
        name = kwds.get('name', '')
        batch = {}
        if name:
            #            batch = self.reqmgr.getBatchesByName(name)
            batch = {'Name': 'Batch1', 'Description': 'Bla-bla', 'Creator': 'valya', 'Group': 'test',
                     'Workflows': ['workflow1', 'workflow2'],
                     'Attributes': {'HeavyIon': ['true', 'false']}}
        attributes = batch.get('Attributes', {})
        workflows = batch.get('Workflows', [])
        description = batch.get('Description', '')
        creator = batch.get('Creator', user_dn())
        content = self.templatepage('batch', name=name,
                                    attributes=json2table(attributes, web_ui_names()),
                                    workflows=workflows, creator=creator,
                                    description=description)
        return self.abs_page('batch', content)

    @expose
    def batches(self, **kwds):
        """Page showing batches"""
        if not kwds:
            kwds = {}
        if 'name' not in kwds:
            kwds.update({'name': ''})
        sortby = kwds.get('sort', 'name')
        #        results = self.reqmgr.getBatchesByName(kwds['name'])
        results = [
            {'Name': 'Batch1', 'Description': 'Bla-bla', 'Creator': 'valya', 'Group': 'test',
             'Workflows': ['workflow1', 'workflow2'],
             'Date': 'Fri Feb 13 10:36:41 EST 2015',
             'Attributes': {'HeavyIon': ['true', 'false']}},
            {'Name': 'Batch2', 'Description': 'lksdjflksjdf', 'Creator': 'valya', 'Group': 'test',
             'Workflows': ['workflow1', 'workflow2'],
             'Date': 'Fri Feb 10 10:36:41 EST 2015',
             'Attributes': {'HeavyIon': ['true', 'false']}},
        ]
        docs = [r for r in sort(results, sortby)]
        filter_sort = self.templatepage('filter_sort')
        content = self.templatepage('batches', batches=docs, sort=sortby,
                                    filter_sort_table=filter_sort)
        return self.abs_page('batches', content)

    ### Aux methods ###

    @expose
    def put_request(self, **kwds):
        "PUT request callback to reqmgr server, should be used in AJAX"
        reqname = kwds.get('RequestName', '')
        status = kwds.get('RequestStatus', '')
        if not reqname:
            msg = 'Unable to update request status, empty request name'
            raise cherrypy.HTTPError(406, msg)
        if not status:
            msg = 'Unable to update request status, empty status value'
            raise cherrypy.HTTPError(406, msg)
        return self.reqmgr.updateRequestStatus(reqname, status)

    @expose
    def images(self, *args):
        """
        Serve static images.
        """
        args = list(args)
        check_scripts(args, self.imgmap, self.imgdir)
        mime_types = ['*/*', 'image/gif', 'image/png',
                      'image/jpg', 'image/jpeg']
        accepts = cherrypy.request.headers.elements('Accept')
        for accept in accepts:
            if accept.value in mime_types and len(args) == 1 \
                    and args[0] in self.imgmap:
                image = self.imgmap[args[0]]
                # use image extension to pass correct content type
                ctype = 'image/%s' % image.split('.')[-1]
                cherrypy.response.headers['Content-type'] = ctype
                return serve_file(image, content_type=ctype)

    def serve(self, kwds, imap, idir, datatype='', minimize=False):
        "Serve files for high level APIs (yui/css/js)"
        args = []
        for key, val in viewitems(kwds):
            if key == 'f':  # we only look-up files from given kwds dict
                if isinstance(val, list):
                    args += val
                else:
                    args.append(val)
        scripts = check_scripts(args, imap, idir)
        return self.serve_files(args, scripts, imap, datatype, minimize)

    @exposecss
    @tools.gzip()
    def css(self, **kwargs):
        """
        Serve provided CSS files. They can be passed as
        f=file1.css&f=file2.css
        """
        resource = kwargs.get('resource', 'css')
        if resource == 'css':
            return self.serve(kwargs, self.cssmap, self.cssdir, 'css', True)

    @exposejs
    @tools.gzip()
    def js(self, **kwargs):
        """
        Serve provided JS scripts. They can be passed as
        f=file1.js&f=file2.js with optional resource parameter
        to speficy type of JS files, e.g. resource=yui.
        """
        resource = kwargs.get('resource', 'js')
        if resource == 'js':
            return self.serve(kwargs, self.jsmap, self.jsdir)

    def serve_files(self, args, scripts, resource, datatype='', minimize=False):
        """
        Return asked set of files for JS, YUI, CSS.
        """
        idx = "-".join(scripts)
        if idx not in self._cache:
            data = ''
            if datatype == 'css':
                data = '@CHARSET "UTF-8";'
            for script in args:
                path = os.path.join(sys.path[0], resource[script])
                path = os.path.normpath(path)
                with open(path) as ifile:
                    data = "\n".join([data, ifile.read().replace('@CHARSET "UTF-8";', '')])
            if datatype == 'css':
                set_headers("text/css")
            if minimize:
                self._cache[idx] = minify(data)
            else:
                self._cache[idx] = data
        return self._cache[idx]
