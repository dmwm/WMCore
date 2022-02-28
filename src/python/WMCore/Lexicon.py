#!/usr/bin/env python
"""
_Lexicon_

A set of regular expressions  and other tests that we can use to validate input
to other classes. If a test fails an AssertionError should be raised, and
handled appropriately by the client methods, on success returns True.
"""
from __future__ import print_function, division

from future import standard_library
standard_library.install_aliases()
from builtins import str, bytes
from future.utils import viewvalues, viewkeys

import io
import re
import json

from urllib.parse import urlparse, urlunparse

from WMCore.WMException import WMException, WMEXCEPTION_START_STR, WMEXCEPTION_END_STR

# restriction enforced by DBS. for different types blocks.
# It could have a strict restriction
# i.e production should end with v[number]
PRIMARY_DS = {'re': '^[a-zA-Z][a-zA-Z0-9\-_]*$', 'maxLength': 99}
PROCESSED_DS = {'re': '[a-zA-Z0-9\.\-_]+', 'maxLength': 199}
TIER = {'re': '[A-Z\-_]+', 'maxLength': 99}
BLOCK_STR = {'re': '#[a-zA-Z0-9\.\-_]+', 'maxLength': 100}

lfnParts = {
    'era': '([a-zA-Z0-9\-_]+)',
    'primDS': '([a-zA-Z][a-zA-Z0-9\-_]*)',
    'tier': '(%(re)s)' % TIER,
    'version': '([a-zA-Z0-9\-_]+)',
    'procDS': '([a-zA-Z0-9\-_]+)',  # Processed dataset = Processing string + Processing version
    'counter': '([0-9]+)',
    'root': '([a-zA-Z0-9\-_]+).root',
    'hnName': '([a-zA-Z0-9\.]+)',
    'subdir': '([a-zA-Z0-9\-_]+)',
    'file': '([a-zA-Z0-9\-\._]+)',
    'workflow': '([a-zA-Z0-9\-_]+)',
    'physics_group': '([a-zA-Z0-9\-_]+)'
}

userProcDSParts = {
    'groupuser': '([a-zA-Z0-9\.\-_])+',
    'publishdataname': '([a-zA-Z0-9\-_])+',
    'psethash': '([a-f0-9]){32}'
}

STORE_RESULTS_LFN = '/store/results/%(physics_group)s/%(era)s/%(primDS)s/%(tier)s/%(procDS)s' % lfnParts

# condor log filtering lexicons
WMEXCEPTION_FILTER = "(?P<WMException>\%s(?!<@).*?\%s)" % (WMEXCEPTION_START_STR, WMEXCEPTION_END_STR)
WMEXCEPTION_FILTER += "|(?P<ERROR>(ERROR:root:.*?This is a CRITICAL error))"
WMEXCEPTION_REGEXP = re.compile(r"%s" % WMEXCEPTION_FILTER, re.DOTALL)

CONDOR_LOG_REASON_FILTER = '<a n="Reason"><s>(?P<Reason>(?!</s></a>).*?)</s></a>'
CONDOR_LOG_SITE_FILTER = '<a n="MachineAttrGLIDEIN_CMSSite0"><s>(?P<Site>(?!</s></a>).*?)</s></a>'

CONDOR_LOG_FILTER_REGEXP = re.compile(r"%s|%s" % (CONDOR_LOG_REASON_FILTER, CONDOR_LOG_SITE_FILTER),
                                      re.DOTALL)


def DBSUser(candidate):
    """
    create_by and last_modified_by in DBS are in several formats. The major ones are:
    1. DN that was mostly used in DBS2: example /DC=org/DC=doegrids/OU=People/CN=Lothar A.T. Bauerdick 301799;
    2. CERN HN account name that used in DBS3/CMSWEB if the HN is assocated with DN: example giffels ;
    3. username with host name: example cmsprod@vocms39.cern.ch;
    """
    if candidate == '' or not candidate:
        return candidate
    r1 = r'^/[a-zA-Z][a-zA-Z0-9/\=\s()\']*\=[a-zA-Z0-9/\=\.\-_/#:\s\']*$'
    r2 = r'^[a-zA-Z0-9/][a-zA-Z0-9/\.\-_\']*$'
    r3 = r'^[a-zA-Z0-9/][a-zA-Z0-9/\.\-_]*@[a-zA-Z0-9/][a-zA-Z0-9/\.\-_]*$'

    errorMsg = "DBSUser candidate: %s doesn't match any of the following regular expressions:\n" % candidate
    try:
        return check(r1, candidate)
    except AssertionError:
        errorMsg += "  %s\n" % r1

    try:
        return check(r2, candidate)
    except AssertionError:
        errorMsg += "  %s\n" % r2

    try:
        return check(r3, candidate)
    except AssertionError:
        errorMsg += "  %s\n" % r3
        raise AssertionError(errorMsg)


def searchblock(candidate):
    """
    A block name with a * wildcard one or more times in it.
    """
    regexp = r"^/(\*|[a-zA-Z\*][a-zA-Z0-9_\*]{0,100})(/(\*|[a-zA-Z0-9_\.\-\*]{1,199})){0,1}(/(\*|[A-Z\-\*]{1,99})(#(\*|[a-zA-Z0-9\.\-_\*]){0,100}){0,1}){0,1}$"
    return check(regexp, candidate)


SEARCHDATASET_RE = r'^/(\*|[a-zA-Z\*][a-zA-Z0-9_\*\-]{0,100})(/(\*|[a-zA-Z0-9_\.\-\*]{1,199})){0,1}(/(\*|[A-Z\-\*]{1,50})){0,1}$'


def searchdataset(candidate):
    """
    A dataset name with a * wildcard one or more times in it. Only the first '/' is mandatory to use.
    """
    return check(SEARCHDATASET_RE, candidate)


def searchstr(candidate):
    """
    Used to check a DBS input that searches for names in dbs. Note block name, dataset name, file name have their own
    searching string.
    No white space found in names in DBS production and allowed to elimate input like "Drop table table1".
    letters, numbers, periods, dashes, underscores

    """
    if candidate == '':
        return candidate
    return check(r'^[a-zA-Z0-9/%*][a-zA-Z0-9/\.\-_%*/#]*$', candidate)


def namestr(candidate):
    """
    Any input used in DBS and not defined here should pass namestr check.
    No white space found in names in DBS production and allowed to elimate input like "Drop table table1".
    letters, numbers, periods, dashes, underscores,#,/

    """
    if candidate == '' or not candidate:
        return candidate
    return check(r'^[a-zA-Z0-9/][a-zA-Z0-9/\.\-_/#]*$', candidate)


def sitetier(candidate):
    return check("^T[0-3]", candidate)


def jobrange(candidate):
    """ Specifies a numbers/range of jobs separated by a comma.
        A range is composed by two numbers separated my minus
        For example valid candidates are either 1 or 1,2 or 3-6,5,7-8
        It is like when you specifies which pages to print in Word
    """
    return check("^\d+(-\d+)?(,\d+(-\d+)?)*$", candidate)


def cmsname(candidate):
    """
    Candidate must pass the CMS name pattern. Thus:
     * good candidates: T2_UK_SGrid or T2_UK_SGrid_Bristol
     * bad candidates: T2, T2_UK
    """
    candidate = candidate.rstrip('_')
    return check("^T[0-3]_[A-Z]{2}((_[A-Za-z0-9]+)+)$", candidate)


def countrycode(candidate):
    # TODO: do properly with a look up table
    return check("^[A-Z]{2}$", candidate)


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

    parts = _blockStructCheck(candidate)

    primDSCheck = check(r"%s" % PRIMARY_DS['re'], parts[1], PRIMARY_DS['maxLength'])
    procDSCheck = check(r"%s" % PROCESSED_DS['re'], parts[2], PROCESSED_DS['maxLength'])
    lastParts = parts[3].split("#")
    tierCheck = check(r"%s" % TIER['re'], lastParts[0], TIER['maxLength'])
    blockCheck = check(r"%s" % BLOCK_STR['re'], "#%s" % lastParts[1], BLOCK_STR['maxLength'])
    return (primDSCheck and procDSCheck and tierCheck and blockCheck)


def identifier(candidate):
    """ letters, numbers, whitespace, periods, dashes, underscores """
    return check(r'[a-zA-Z0-9\s\.\-_]{1,100}$', candidate)


def globalTag(candidate):
    """ Identifier plus colons """
    return check(r'[a-zA-Z0-9\s\.\-_:]{1,100}$', candidate)


DATASET_RE = r'^/[a-zA-Z0-9\-_]{1,99}/[a-zA-Z0-9\.\-_]{1,199}/[A-Z\-]{1,50}$'


def dataset(candidate):
    """ A slash followed by an identifier,x3 """
    return check(DATASET_RE, candidate)


PROCDATASET_RE = r'[a-zA-Z][a-zA-Z0-9_]*(\-[a-zA-Z0-9_]+){0,2}-v[0-9]*$'


def procdataset(candidate):
    """
    Check for processed dataset name.
    letters, numbers, dashes, underscores.
    """
    if not candidate or candidate.startswith('None'):
        raise AssertionError("ProcDataset cannot be empty or start with None.")

    commonCheck = check(r"%s" % PROCESSED_DS['re'], candidate, PROCESSED_DS['maxLength'])
    prodCheck = check(PROCDATASET_RE, candidate)
    return (commonCheck and prodCheck)


def publishdatasetname(candidate):
    if candidate == '' or not candidate:
        return candidate
    return check(r'%(publishdataname)s$' % userProcDSParts, candidate, 100)


USERPROCDATASET_RE = r'%(groupuser)s-%(publishdataname)s-%(psethash)s$' % userProcDSParts


def userprocdataset(candidate):
    """
    Check for processed dataset name of users.
    letters, numbers, dashes, underscores.
    """
    if candidate == '' or not candidate:
        return candidate

    commonCheck = check(r"%s" % PROCESSED_DS['re'], candidate, PROCESSED_DS['maxLength'])
    anlaysisCheck = check(USERPROCDATASET_RE, candidate)
    return (commonCheck and anlaysisCheck)


def physicsgroup(candidate):
    """
    Check for Physics Group string which is added to StoreResults
    merged LFN base. Up to 30 letters, numbers, dashes, underscores.
    """
    return check(r'%(physics_group)s$' % lfnParts, candidate, 30)


def procversion(candidate):
    """ Integers """
    if isinstance(candidate, dict):
        for candi in viewvalues(candidate):
            check(r'^[0-9]+$', str(candi))
        return True
    else:
        return check(r'^[0-9]+$', str(candidate))


def procstring(candidate):
    """ Identifier """
    if not candidate:
        raise AssertionError("ProcStr cannot be empty or None.")
    if isinstance(candidate, dict):
        for candi in viewvalues(candidate):
            check(r'[a-zA-Z0-9_]{1,100}$', candi)
        return True
    else:
        return check(r'[a-zA-Z0-9_]{1,100}$', candidate)


def procstringT0(candidate):
    """
    ProcessingString validation function for T0 specs
    """
    if isinstance(candidate, dict):
        for candi in viewvalues(candidate):
            check(r'^$|[a-zA-Z0-9_]{1,100}$', candi)
        return True
    else:
        return check(r'^$|[a-zA-Z0-9_]{1,100}$', candidate)


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
    else:
        return check(r'[a-zA-Z][a-zA-Z0-9_]*$', candidate)


def campaign(candidate):
    """
    Check for Campaign name.
    letters, numbers, underscores and dashes are allowed, up to 60 chars.
    """
    if not candidate:
        return True
    return check(r'^[a-zA-Z0-9-_]{1,80}$', candidate)


def primdataset(candidate):
    """
    Check for primary dataset name.
    letters, numbers, dashes, underscores.
    """
    if candidate == '' or not candidate:
        return candidate
    return check(r"%s" % PRIMARY_DS['re'], candidate, PRIMARY_DS['maxLength'])


TASK_STEP_NAME = {'re': '^[a-zA-Z][a-zA-Z0-9\-_]*$', 'maxLength': 50}
def taskStepName(candidate):
    """
    Validate the TaskName and/or StepName field.
    Letters, numbers, dashes and underscores are allowed.
    """
    return check(r"%s" % TASK_STEP_NAME['re'], candidate, TASK_STEP_NAME['maxLength'])

def hnName(candidate):
    """
    Use lfn parts definitions to validate a simple HN name
    """

    validName = '^%(hnName)s$' % lfnParts
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
    regexp1 = '/([a-z]+)/([a-z0-9]+)/(%(era)s)/([a-zA-Z0-9\-_]+)/([A-Z\-_]+)/([a-zA-Z0-9\-_]+)((/[0-9]+){3}){0,1}/([0-9]+)/([a-zA-Z0-9\-_]+).root' % lfnParts
    regexp2 = '/([a-z]+)/([a-z0-9]+)/([a-z0-9]+)/([a-zA-Z0-9\-_]+)/([a-zA-Z0-9\-_]+)/([A-Z\-_]+)/([a-zA-Z0-9\-_]+)((/[0-9]+){3}){0,1}/([0-9]+)/([a-zA-Z0-9\-_]+).root'
    regexp3 = '/store/(temp/)*(user|group)/(%(hnName)s|%(physics_group)s)/%(primDS)s/%(procDS)s/%(version)s/%(counter)s/%(root)s' % lfnParts
    regexp4 = '/store/(temp/)*(user|group)/(%(hnName)s|%(physics_group)s)/%(primDS)s/(%(subdir)s/)+%(root)s' % lfnParts

    oldStyleTier0LFN = '/store/data/%(era)s/%(primDS)s/%(tier)s/%(version)s/%(counter)s/%(counter)s/%(counter)s/%(root)s' % lfnParts
    tier0LFN = '/store/(backfill/[0-9]/){0,1}(t0temp/|unmerged/){0,1}(data|express|hidata)/%(era)s/%(primDS)s/%(tier)s/%(version)s/%(counter)s/%(counter)s/%(counter)s(/%(counter)s)?/%(root)s' % lfnParts

    storeMcLFN = '/store/mc/(%(era)s)/([a-zA-Z0-9\-_]+)/([a-zA-Z0-9\-_]+)/([a-zA-Z0-9\-_]+)(/([a-zA-Z0-9\-_]+))*/([a-zA-Z0-9\-_]+).root' % lfnParts

    storeResults2LFN = '/store/results/%(physics_group)s/%(primDS)s/%(procDS)s/%(primDS)s/%(tier)s/%(procDS)s/%(counter)s/%(root)s' % lfnParts

    storeResultRootPart = '%(counter)s/%(root)s' % lfnParts
    storeResultsLFN = "%s/%s" % (STORE_RESULTS_LFN, storeResultRootPart)

    lheLFN1 = '/store/lhe/([0-9]+)/([a-zA-Z0-9\-_]+).lhe(.xz){0,1}'
    # This is for future lhe LFN structure. Need to be tested.
    lheLFN2 = '/store/lhe/%(era)s/%(primDS)s/([0-9]+)/([a-zA-Z0-9\-_]+).lhe(.xz){0,1}' % lfnParts

    errorMsg = "LFN candidate: %s doesn't match any of the following regular expressions:\n" % candidate

    try:
        return check(regexp1, candidate)
    except AssertionError:
        errorMsg += "  %s\n" % regexp1

    try:
        return check(regexp2, candidate)
    except AssertionError:
        errorMsg += "  %s\n" % regexp2

    try:
        return check(regexp3, candidate)
    except AssertionError:
        errorMsg += "  %s\n" % regexp3

    try:
        return check(regexp4, candidate)
    except AssertionError:
        errorMsg += "  %s\n" % regexp4

    try:
        return check(tier0LFN, candidate)
    except AssertionError:
        errorMsg += "  %s\n" % tier0LFN

    try:
        return check(oldStyleTier0LFN, candidate)
    except AssertionError:
        errorMsg += "  %s\n" % oldStyleTier0LFN

    try:
        return check(storeMcLFN, candidate)
    except AssertionError:
        errorMsg += "  %s\n" % storeMcLFN

    try:
        return check(lheLFN1, candidate)
    except AssertionError:
        errorMsg += "  %s\n" % lheLFN1

    try:
        return check(lheLFN2, candidate)
    except AssertionError:
        errorMsg += "  %s\n" % lheLFN2

    try:
        return check(storeResults2LFN, candidate)
    except AssertionError:
        errorMsg += "  %s\n" % storeResults2LFN

    try:
        return check(storeResultsLFN, candidate)
    except AssertionError:
        errorMsg += "  %s\n" % storeResultsLFN
        raise AssertionError(errorMsg)


def lfnBase(candidate):
    """
    As lfn above, but for doing the lfnBase
    i.e., for use in spec generation and parsing
    """
    regexp1 = '/([a-z]+)/([a-z0-9]+)/([a-zA-Z0-9\-_]+)/([a-zA-Z0-9\-_]+)/([A-Z\-_]+)/([a-zA-Z0-9\-_]+)'
    regexp2 = '/([a-z]+)/([a-z0-9]+)/([a-z0-9]+)/([a-zA-Z0-9\-_]+)/([a-zA-Z0-9\-_]+)/([A-Z\-_]+)/([a-zA-Z0-9\-_]+)((/[0-9]+){3}){0,1}'
    regexp3 = '/(store)/(temp/)*(user|group)/(%(hnName)s|%(physics_group)s)/%(primDS)s/%(procDS)s/%(version)s' % lfnParts

    tier0LFN = '/store/(backfill/[0-9]/){0,1}(t0temp/|unmerged/){0,1}(data|express|hidata)/%(era)s/%(primDS)s/%(tier)s/%(version)s/%(counter)s/%(counter)s/%(counter)s' % lfnParts

    errorMsg = "LFN candidate: %s doesn't match any of the following regular expressions:\n" % candidate

    try:
        return check(regexp1, candidate)
    except AssertionError:
        errorMsg += "  %s\n" % regexp1

    try:
        return check(regexp2, candidate)
    except AssertionError:
        errorMsg += "  %s\n" % regexp2

    try:
        return check(regexp3, candidate)
    except AssertionError:
        errorMsg += "  %s\n" % regexp3

    try:
        return check(tier0LFN, candidate)
    except AssertionError:
        errorMsg += "  %s\n" % tier0LFN

    try:
        return check(STORE_RESULTS_LFN, candidate)
    except AssertionError:
        errorMsg += "  %s\n" % STORE_RESULTS_LFN
        raise AssertionError(errorMsg)


def userLfn(candidate):
    """
    Check LFNs in /store/{temp}/user that are not EDM data
    """
    regexp = '/store/(temp/)*(user|group)/(%(hnName)s|%(physics_group)s)/%(subdir)s/%(workflow)s/%(subdir)s/%(file)s' % lfnParts
    return check(regexp, candidate)


def userLfnBase(candidate):
    """
    As above but for the base part of the file
    """
    regexp = '/store/(temp/)*(user|group)/(%(hnName)s|%(physics_group)s)/%(subdir)s/%(workflow)s/%(subdir)s' % lfnParts
    return check(regexp, candidate)


def cmsswversion(candidate):
    return check('CMSSW(_\d+){3}(_[a-zA-Z0-9_]+)?$', candidate)


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
    pDatasetTypes = ["mc", "data", "cosmic", "test"]
    if candidate in pDatasetTypes:
        return True
    # to sync with the check() exception when it doesn't match
    raise AssertionError("Invalid primary dataset type : %s should be 'mc' or 'data' or 'test'" % candidate)


def subRequestType(candidate):
    subTypes = ["MC", "ReDigi", "Pilot", "RelVal", "HIRelVal", "ReReco", ""]
    if candidate in subTypes:
        return True
    # to sync with the check() exception when it doesn't match
    msg = "Invalid SubRequestType value: '{}'. Allowed values are: {}".format(candidate, subTypes)
    raise AssertionError(msg)


def activity(candidate):
    dashboardActivities = ['reprocessing', 'production', 'relval', 'tier0', 't0',
                           'harvesting', 'storeresults', 'integration',
                           'test', 'analysis']
    if candidate in dashboardActivities:
        return True
    raise AssertionError("Invalid dashboard activity: %s should 'test'" % candidate)


def gpuParameters(candidate):
    """
    Validate the spec "GPUParams" argument, which is a JSON encoded object, thus:
    * an encoded None object (like 'null')
    * an encoded dictionary with the following parameters:
      * mandatory: GPUMemoryMB (int), CUDARuntime (str), CUDACapabilities (list of str)
      * optional: GPUName (str), CUDADriverVersion (str), CUDARuntimeVersion (str)
    :param candidate: a JSON encoded data to be validated
    :return: True if validation succeeded, False or exception otherwise
    """
    mandatoryArgs = set(["GPUMemoryMB", "CUDARuntime", "CUDACapabilities"])
    optionalArgs = set(["GPUName", "CUDADriverVersion", "CUDARuntimeVersion"])
    try:
        data = json.loads(candidate)
    except Exception:
        raise AssertionError("GPUParams is not a valid JSON object")
        # once python2 code is deprecated, this is the way to raise only the last exception
        # raise AssertionError("GPUParams is not a valid JSON object") from None
    if data is None:
        return True
    if not isinstance(data, dict):
        raise AssertionError("GPUParams is not a dictionary encoded as JSON object")

    paramSet = set(viewkeys(data))
    # is every mandatory argument also in the provided args?
    if not mandatoryArgs <= paramSet:
        msg = "GPUParams does not contain all the mandatory arguments. "
        msg +="Mandatory args: {}, while args provided are: {}".format(mandatoryArgs, paramSet)
        raise AssertionError(msg)
    # are there unknown arguments in the data provided?
    unknownArgs = paramSet - mandatoryArgs - optionalArgs
    if unknownArgs:
        msg = "GPUParams contains arguments that are not supported. Args provided: {}, ".format(paramSet)
        msg +="while mandatory args are: {} and optional args are: {}".format(mandatoryArgs, optionalArgs)
        raise AssertionError(msg)
    return _gpuInternalParameters(data)


CUDA_VERSION_REGEX = {"re": r"^\d+\.\d+(\.\d+)?$", "maxLength": 100}
def _gpuInternalParameters(candidate):
    """
    NOTE: this function is supposed to be called only from gpuParameters, which already
    does the high level validation.
    List of **required** parameters is:
      * `GPUMemoryMB`: integer with the amount of memory, in Megabytes (MB). Validate as `> 0`. E.g.: 8000
      * `CUDACapabilities`: a list of short strings (<= 100 chars). Validation should ensure at least one item
        in the list and matching this regex: `r"^\d+.\d$"`. E.g.: ["7.5", "8.0"]
      * `CUDARuntime`: a short string (<=100 chars) with the runtime version.
        Validated against this regex: `r"^\d+.\d+$"`. E.g.: "11.2"
    List of **optional** parameters is:
      * `GPUName`: a string with the GPU name. Validate against `<= 100 chars`. E.g. "Tesla T4", "Quadro RTX 6000";
      * `CUDADriverVersion`: a string with the CUDA driver version.
        Validated against this regex: `r"^\d+.\d+\d+$"`E.g. "460.32.03"
      * `CUDARuntimeVersion`: a string with the CUDA runtime version.
        Validated against this regex: `r"^\d+.\d+\d+$"`E.g. "11.2.152"

    This function validates all the internal key/value pairs provided for the GPUParams
    argument, mostly against their own regular expressions.
    :param candidate: the JSON object already decoded (thus, str or dict)
    :return: True if validation succeeded, False or exception otherwise
    """
    # Generic regular expression for CUDA runtime/driver version
    # It matches either something like "11.2", or "11.2.231"
    # GPUMemoryMB validation
    if not isinstance(candidate["GPUMemoryMB"], int) or not candidate["GPUMemoryMB"] > 0:
        raise AssertionError("Mandatory GPUParams.GPUMemoryMB must be an integer and greater than 0")
    # CUDACapabilities validation
    if not isinstance(candidate["CUDACapabilities"], (list, set)) or not candidate["CUDACapabilities"]:
        raise AssertionError("Mandatory GPUParams.CUDACapabilities must be a non-empty list")
    for cudaCapabItem in candidate["CUDACapabilities"]:
        if not isinstance(cudaCapabItem, (str, bytes)):
            raise AssertionError("Mandatory GPUParams.CUDACapabilities must be a list of strings")
        check(CUDA_VERSION_REGEX["re"], cudaCapabItem, CUDA_VERSION_REGEX["maxLength"])
    # CUDARuntime validation
    if not isinstance(candidate["CUDARuntime"], (str, bytes)) or\
            not check(CUDA_VERSION_REGEX["re"], candidate["CUDARuntime"], CUDA_VERSION_REGEX["maxLength"]):
        raise AssertionError("Mandatory GPUParams.CUDARuntime must be a string and shorter than 100 chars")

    ### And now, validate the optional arguments
    # GPUName validation
    if "GPUName" in candidate:
        if not isinstance(candidate["GPUName"], (str, bytes)):
            raise AssertionError("Optional GPUParams.GPUName must be a string")
        check(r".*", candidate["GPUName"], 100)
    # CUDADriverVersion validation
    if "CUDADriverVersion" in candidate:
        if not isinstance(candidate["CUDADriverVersion"], (str, bytes)):
            raise AssertionError("Optional GPUParams.CUDADriverVersion must be a string")
        check(CUDA_VERSION_REGEX["re"], candidate["CUDADriverVersion"], CUDA_VERSION_REGEX["maxLength"])
    # CUDARuntimeVersion validation
    if "CUDARuntimeVersion" in candidate:
        if not isinstance(candidate["CUDARuntimeVersion"], (str, bytes)):
            raise AssertionError("Optional GPUParams.CUDARuntimeVersion must be a string")
        check(CUDA_VERSION_REGEX["re"], candidate["CUDARuntimeVersion"], CUDA_VERSION_REGEX["maxLength"])
    return True


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
