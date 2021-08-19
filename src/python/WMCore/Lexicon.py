#!/usr/bin/env python
"""
_Lexicon_

A set of tools and functios to load lexicon.json file and validate given values
against its regular expression patterns. Each function will raise an
exception if it fails to validate given candidate against function expression.

The data format used in lexicon.json is the following:

.. doctest::
    [
    {
      "name": str,
      "patterns": [ str, str, ...],
      "length": int.
      "functions": [
           {"name": str, "indexes": [ int, int, ..],
       ]
    },
    ...
    }

where `name` represents regexp attribute, like dataset, block, etc.
The patterns lists contains regular expressions. The functions list contains
dict of function name and corresponding indexes in patterns list.
"""

from __future__ import print_function, division

from future import standard_library
standard_library.install_aliases()
from builtins import str
from future.utils import viewvalues

import os
import io
import re
import json

from urllib.parse import urlparse, urlunparse

# this try block provides ability to load this module without WMCore dependency, e.g.
# when we need to regenerate lexicon dict
try:
    from WMCore.WMException import WMException, WMEXCEPTION_START_STR, WMEXCEPTION_END_STR
except:
    WMEXCEPTION_START_STR = str("<@========== WMException Start ==========@>")
    WMEXCEPTION_END_STR = str("<@---------- WMException End ----------@>")
    def WMException(msg):
        print(msg)

# condor log filtering lexicons
WMEXCEPTION_FILTER = "(?P<WMException>\%s(?!<@).*?\%s)" % (WMEXCEPTION_START_STR, WMEXCEPTION_END_STR)
WMEXCEPTION_FILTER += "|(?P<ERROR>(ERROR:root:.*?This is a CRITICAL error))"
WMEXCEPTION_REGEXP = re.compile(r"%s" % WMEXCEPTION_FILTER, re.DOTALL)

CONDOR_LOG_REASON_FILTER = '<a n="Reason"><s>(?P<Reason>(?!</s></a>).*?)</s></a>'
CONDOR_LOG_SITE_FILTER = '<a n="MachineAttrGLIDEIN_CMSSite0"><s>(?P<Site>(?!</s></a>).*?)</s></a>'

CONDOR_LOG_FILTER_REGEXP = re.compile(r"%s|%s" % (CONDOR_LOG_REASON_FILTER, CONDOR_LOG_SITE_FILTER),
                                      re.DOTALL)

def load_lexicon(fname):
    """
    Load regexp Lexicon file. It is reverse function to dump_regexp.
    It either return patterns Lexicon list, see dump_regexp data-format, or
    convert it to dictionaries. One dictionary contains keys and patterns,
    while another keys and lengths.
    """
    data = []
    with open(fname, 'r') as istream:
        data = json.load(istream)
    rdict = {} # rules patterns dict
    ldict = {} # length patterns dict
    fdict = {} # func patterns dict
    for item in data:
        patterns = item['patterns']
        rdict[item['name']] = patterns
        ldict[item['name']] = item['length']
        for func in item['functions']:
            fdict[func['name']] = [patterns[i] for i in func['indexes']]
    return rdict, ldict, fdict

# LEXICON defines lexicon.json dictionary
LEXICON = {}
fname = '{}/lexicon.json'.format(os.getcwd())
if hasattr(os.environ, 'LEXION_RULES'):
    fname = os.environ['LEXICON_RULES']
if not fname:
    raise Exception("Lexicon.json file is not found in local area and there is no LEXICON_RULES environment")
LEX_PATTERNS, LEX_LENGTHS, LEX_FUNCS = load_lexicon(fname)

def check_func_pattern(funcname, candidate):
    "Helper function to check given candidate against funcname patterns"
    errorMsg = "%s candidate: %s doesn't match any regular expression patterns:\n" % (funcmame, candidate)
    for pat in LEX_FUNC[funcname]:
        try:
            length = LEX_FUNC_LENGTH[funcname]
            if length > 0:
                return check(pat, candidate, length)
            else:
                return check(pat, candidate)
        except AssertionError:
            errorMsg += "  %s\n" % pat

def DBSUser(candidate):
    """
    create_by and last_modified_by in DBS are in several formats. The major ones are:
    1. DN that was mostly used in DBS2: example /DC=org/DC=doegrids/OU=People/CN=Lothar A.T. Bauerdick 301799;
    2. CERN HN account name that used in DBS3/CMSWEB if the HN is assocated with DN: example giffels ;
    3. username with host name: example cmsprod@vocms39.cern.ch;
    """
    if candidate == '' or not candidate:
        return candidate
    return check_func_pattern("DBSUser", candidate)

def searchblock(candidate):
    """
    A block name with a * wildcard one or more times in it.
    """
    return check_func_pattern("searchblock", candidate)

def searchdataset(candidate):
    """
    A dataset name with a * wildcard one or more times in it. Only the first '/' is mandatory to use.
    """
    return check_func_pattern("searchdataset", candidate)

def searchstr(candidate):
    """
    Used to check a DBS input that searches for names in dbs. Note block name, dataset name, file name have their own
    searching string.
    No white space found in names in DBS production and allowed to elimate input like "Drop table table1".
    letters, numbers, periods, dashes, underscores

    """
    if candidate == '':
        return candidate
    return check_func_pattern("searchstr", candidate)

def namestr(candidate):
    """
    Any input used in DBS and not defined here should pass namestr check.
    No white space found in names in DBS production and allowed to elimate input like "Drop table table1".
    letters, numbers, periods, dashes, underscores,#,/

    """
    if candidate == '' or not candidate:
        return candidate
    return check_func_pattern("namestr", candidate)

def sitetier(candidate):
    return check_func_pattern("sitetier", candidate)

def jobrange(candidate):
    """ Specifies a numbers/range of jobs separated by a comma.
        A range is composed by two numbers separated my minus
        For example valid candidates are either 1 or 1,2 or 3-6,5,7-8
        It is like when you specifies which pages to print in Word
    """
    return check_func_pattern("jobrange", candidate)

def cmsname(candidate):
    """
    Candidate must pass the CMS name pattern. Thus:
     * good candidates: T2_UK_SGrid or T2_UK_SGrid_Bristol
     * bad candidates: T2, T2_UK
    """
    candidate = candidate.rstrip('_')
    return check_func_pattern("cmsname", candidate)

def countrycode(candidate):
    # TODO: do properly with a look up table
    return check_func_pattern("countrycode", candidate)

def _blockStructCheck(candidate):
    """
    Basic block structure check
    /primary/process/tier#uuid
    """
    assert candidate.count('/') == 3, "need to have / between the 3 parts which construct block name"
    parts = candidate.split('/')
    assert parts[3].count('#') == 1, "need to have # in the last parts of block"
    # should be empty string for the first part
    check(r"", parts[0])
    return parts


def block(candidate):
    """assert if not a valid block name"""
    return check_func_pattern("block", candidate)

def identifier(candidate):
    """ letters, numbers, whitespace, periods, dashes, underscores """
    return check_func_pattern("identified", candidate)

def globalTag(candidate):
    """ Identifier plus colons """
    return check_func_pattern("globalTag", candidate)

def dataset(candidate):
    """ A slash followed by an identifier,x3 """
    return check_func_pattern("dataset", candidate)

def procdataset(candidate):
    """
    Check for processed dataset name.
    letters, numbers, dashes, underscores.
    """
    if not candidate or candidate.startswith('None'):
        raise AssertionError("ProcDataset cannot be empty or start with None.")
    return check_func_pattern("procdataset", candidate)

def publishdatasetname(candidate):
    if candidate == '' or not candidate:
        return candidate
    return check_func_pattern("publishdatasetname", candidate)

def userprocdataset(candidate):
    """
    Check for processed dataset name of users.
    letters, numbers, dashes, underscores.
    """
    if candidate == '' or not candidate:
        return candidate
    return check_func_pattern("userprocdataset", candidate)

def physicsgroup(candidate):
    """
    Check for Physics Group string which is added to StoreResults
    merged LFN base. Up to 30 letters, numbers, dashes, underscores.
    """
    return check_func_pattern("physicsgroup", candidate)

def procversion(candidate):
    """ Integers """
    if isinstance(candidate, dict):
        for candi in viewvalues(candidate):
            check(r'^[0-9]+$', str(candi))
        return True
    return check_func_pattern("procversion", candidate)

def procstring(candidate):
    """ Identifier """
    if not candidate:
        raise AssertionError("ProcStr cannot be empty or None.")
    if isinstance(candidate, dict):
        for candi in viewvalues(candidate):
            check(r'[a-zA-Z0-9_]{1,100}$', candi)
        return True
    return check_func_pattern("procstring", candidate)

def procstringT0(candidate):
    """
    ProcessingString validation function for T0 specs
    """
    if isinstance(candidate, dict):
        for candi in viewvalues(candidate):
            check(r'^$|[a-zA-Z0-9_]{1,100}$', candi)
        return True
    return check_func_pattern("procstringT0", candidate)

def acqname(candidate):
    """
    Check for acquisition name.
    letters, numbers, underscores.
    """
    if not candidate:
        raise AssertionError("AcqEra cannot be empty or None.")
    if isinstance(candidate, dict):
        for candi in viewvalues(candidate):
            check(r'[a-zA-Z][a-zA-Z0-9_]*$', candi)
        return True
    return check_func_pattern("acqname", candidate)

def campaign(candidate):
    """
    Check for Campaign name.
    letters, numbers, underscores and dashes are allowed, up to 60 chars.
    """
    if not candidate:
        return True
    return check_func_pattern("campaign", candidate)

def primdataset(candidate):
    """
    Check for primary dataset name.
    letters, numbers, dashes, underscores.
    """
    if candidate == '' or not candidate:
        return candidate
    return check_func_pattern("primdataset", candidate)

def taskStepName(candidate):
    """
    Validate the TaskName and/or StepName field.
    Letters, numbers, dashes and underscores are allowed.
    """
    return check_func_pattern("taskStepName", candidate)

def hnName(candidate):
    """
    Use lfn parts definitions to validate a simple HN name
    """
    validName = '([a-zA-Z0-9\.]+)'
    return check(validName, candidate)

def lfn(candidate):
    """
    Should be of the following form:
    /store/data/acquisition_era/primary-dataset/data_tier/processing_version/lfn_counter/filename.root
    See https://twiki.cern.ch/twiki/bin/viewauth/CMS/DMWMPG_Namespace for details

    NOTE:Because of the way we do lustre, we have to have two separate checks for this:
    /store/data
    /store/data/lustre

    Add for LHE files: /data/lhe/...
    """
    return check_func_pattern("lfn", candidate)

def lfnBase(candidate):
    """
    As lfn above, but for doing the lfnBase
    i.e., for use in spec generation and parsing
    """
    errorMsg = "LFN candidate: %s doesn't match any of the following regular expressions:\n" % candidate
    for pat in LEX_FUNC['lfn']:
        base = '/'.join(pat.split('/')[:-1])
        try:
            return check(base, candidate)
        except AssertionError:
            errorMsg += "  %s\n" % base

def userLfnBase(candidate):
    """
    As above but for the base part of the file
    """
    return check_func_pattern("userLfnBase", candidate)

def cmsswversion(candidate):
    return check_func_pattern("cmsswversion", candidate)

def couchurl(candidate):
    return check('https?://(([a-zA-Z0-9:@\.\-_]){0,100})([a-z0-9\.]+)(:\d+|/couchdb)', candidate)

def requestName(candidate):
    return check(r'[a-zA-Z0-9\.\-_]{1,150}$', candidate)

def validateUrl(candidate):
    """
    Basic input validation for http(s) urls
    """
    # regex taken from django https://github.com/django/django/blob/master/django/core/validators.py#L47
    # Copyright (c) Django Software Foundation and individual contributors
    protocol = r"^https?://"  # http:// or https://
    domain = r'?:(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,6}\.?'
    localhost = r'localhost'
    ipv4 = r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'
    ipv6 = r'\[?[a-fA-F0-9]*:[a-fA-F0-9:]+\]?'
    port = r'(?::\d+)?'  # optional port
    path = r'(?:/?|[/?]\S+)$'
    regex_url = r'%s(%s|%s|%s|%s)%s%s' % (protocol, domain, localhost, ipv4, ipv6, port, path)
    return check(regex_url, candidate)

def check(regexp, candidate, maxLength=None):
    if maxLength is not None:
        assert len(candidate) <= maxLength, \
            "%s is longer than max length (%s) allowed" % (candidate, maxLength)
    assert re.compile(regexp).match(candidate) is not None, \
        "'%s' does not match regular expression %s" % (candidate, regexp)
    return True


def parseLFN(candidate):
    """
    _parseLFN_

    Take an LFN, return the component parts
    """
    separator = "/"

    # First, make sure what we've gotten is a real LFN
    lfn(candidate)

    parts = candidate.split('/')
    final = {}

    if parts[0] == '':
        parts.remove('')
    if 'user' in parts[1:3] or 'group' in parts[1:3]:
        if parts[1] in ['user', 'group']:
            final['baseLocation'] = '/%s' % separator.join(parts[:2])
            parts = parts[2:]
        else:
            final['baseLocation'] = '/%s' % separator.join(parts[:3])
            parts = parts[3:]

        final['hnName'] = parts[0]
        final['primaryDataset'] = parts[1]
        final['secondaryDataset'] = parts[2]
        final['processingVersion'] = parts[3]
        final['lfnCounter'] = parts[4]
        final['filename'] = parts[5]

        return final

    if len(parts) == 8:
        # Then we have only two locations
        final['baseLocation'] = '/%s' % separator.join(parts[:2])
        parts = parts[2:]
    elif len(parts) == 9:
        final['baseLocation'] = '/%s' % separator.join(parts[:3])
        parts = parts[3:]
    else:
        # How did we end up here?
        # Something just went wrong
        msg = """CRITICAL!  This machine has experienced a complete logic failure while parsing LFNs.\n
        If you are a developer this indicates that you have changed the Lexicon LFN regexp functions without changing the parsing.\n
        If you are an operator, this indicates that this machine is likely unstable.\n
        All data should be backed up and the machine removed from production for examination.\n"""
        msg += "Candidate: %s" % candidate
        raise WMException(msg)

    final['acquisitionEra'] = parts[0]
    final['primaryDataset'] = parts[1]
    final['dataTier'] = parts[2]
    final['processingVersion'] = parts[3]
    final['lfnCounter'] = parts[4]
    final['filename'] = parts[5]

    return final


def parseLFNBase(candidate):
    """
    _parseLFNBase_

    Return a meaningful dictionary with info from an LFNBase
    """
    separator = "/"

    # First, make sure what we've gotten is a real LFNBase
    lfnBase(candidate)

    parts = candidate.split('/')
    final = {}

    if parts[0] == '':
        parts.remove('')

    if 'user' in parts[1:3] or 'group' in parts[1:3]:
        if parts[1] in ['user', 'group']:
            final['baseLocation'] = '/%s' % separator.join(parts[:2])
            parts = parts[2:]
        else:
            final['baseLocation'] = '/%s' % separator.join(parts[:3])
            parts = parts[3:]

        final['hnName'] = parts[0]
        final['primaryDataset'] = parts[1]
        final['secondaryDataset'] = parts[2]
        final['processingVersion'] = parts[3]

        return final

    if len(parts) == 6:
        # Then we have only two locations
        final['baseLocation'] = '/%s' % separator.join(parts[:2])
        parts = parts[2:]
    elif len(parts) == 7:
        final['baseLocation'] = '/%s' % separator.join(parts[:3])
        parts = parts[3:]
    else:
        # How did we end up here?
        # Something just went wrong
        msg = """CRITICAL!  This machine has experienced a complete logic failure while parsing LFNs.\n
        If you are a developer this indicates that you have changed the Lexicon LFN regexp functions without changing the parsing.\n
        If you are an operator, this indicates that this machine is likely unstable.\n
        All data should be backed up and the machine removed from production for examination.\n"""
        msg += "Candidate: %s" % candidate
        raise WMException(msg)

    final['acquisitionEra'] = parts[0]
    final['primaryDataset'] = parts[1]
    final['dataTier'] = parts[2]
    final['processingVersion'] = parts[3]

    return final


def sanitizeURL(url):
    """Take the url with/without username and password and return sanitized url,
       username and password in dict format
       WANNING: This doesn't check the correctness of url format.
       Don't use ':' in username or password.
    """
    endpoint_components = urlparse(url)
    # Cleanly pull out the user/password from the url
    if endpoint_components.port:
        netloc = '%s:%s' % (endpoint_components.hostname,
                            endpoint_components.port)
    else:
        netloc = endpoint_components.hostname

    # Build a URL without the username/password information
    url = urlunparse(
        [endpoint_components.scheme,
         netloc,
         endpoint_components.path,
         endpoint_components.params,
         endpoint_components.query,
         endpoint_components.fragment])

    return {'url': url, 'username': endpoint_components.username,
            'password': endpoint_components.password}


def replaceToSantizeURL(url_str):
    """
    Take arbitrary string and search for urls with user and password and
    replace it with sanitized url.
    """

    def _repUrl(matchObj):
        return matchObj.group(1) + matchObj.group(4)

    # TODO: won't catch every case (But is it good enough (trade off to performance)?)
    urlRegExpr = r'\b(((?i)http|https|ftp|mysql|oracle|sqlite)+://)([^:]+:[^@]+@)(\S+)\b'
    return re.sub(urlRegExpr, _repUrl, url_str)


def splitCouchServiceURL(serviceURL):
    """
    split service URL to couchURL and couchdb name
    serviceURL should be couchURL/dbname format.
    """

    splitedURL = serviceURL.rstrip('/').rsplit('/', 1)
    return splitedURL[0], splitedURL[1]


def primaryDatasetType(candidate):
    if candidate in pDatasetTypes:
        return True
    return check_func_pattern("primaryDatasetType", candidate)

def activity(candidate):
    dashboardActivities = ['reprocessing', 'production', 'relval', 'tier0', 't0',
                           'harvesting', 'storeresults', 'integration',
                           'test', 'analysis']
    if candidate in dashboardActivities:
        return True
    raise AssertionError("Invalid dashboard activity: %s should 'test'" % candidate)


def getStringsBetween(start, end, source):
    """
    get the string between start string and end string for given source string
    source string is one line string (no new line charactor in it)
    """
    regex = r"\%s(.*?)\%s" % (start, end)
    result = re.match(regex, source)
    if result:
        return result.group(1).strip()
    else:
        return None


def getIterMatchObjectOnRegexp(filePath, regexp):
    with io.open(filePath, 'r', encoding='utf8', errors='ignore') as f:
        for m in re.finditer(regexp, f.read()):
            yield m
