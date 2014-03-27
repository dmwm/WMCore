#!/usr/bin/python
"""
_Lexicon_

A set of regular expressions  and other tests that we can use to validate input
to other classes. If a test fails an AssertionError should be raised, and
handled appropriately by the client methods, on success returns True.
"""

import re
import string
import urlparse

from WMCore.WMException import WMException

#restriction enforced by DBS. for different types blocks. 
#It could have a strict restriction
# i.e production should end with v[number]
PRIMARY_DS = {'re': '[a-zA-Z0-9\.\-_]+', 'maxLength': 99}
PROCESSED_DS = {'re': '[a-zA-Z0-9\.\-_]+', 'maxLength': 199}
TIER = {'re': '[A-Z\-_]+', 'maxLength': 99}
BLOCK_STR = {'re': '#[a-zA-Z0-9\.\-_]+', 'maxLength': 100}

lfnParts = {
    'era': '([a-zA-Z0-9\-_]+)',
    'primDS': '(%(re)s)' % PRIMARY_DS,
    'tier': '(%(re)s)' % TIER,
    'version': '([a-zA-Z0-9\-_]+)',
    'secondary': '([a-zA-Z0-9\-_]+)',
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

STORE_RESULTS_LFN = '/store/results/%(physics_group)s/%(era)s/%(primDS)s/%(tier)s/%(secondary)s' % lfnParts

def DBSUser(candidate):
    """
    create_by and last_modified_by in DBS are in several formats. The major ones are: 
    1. DN that is mostly used in DBS2: example /DC=org/DC=doegrids/OU=People/CN=Lothar A.T. Bauerdick 301799; 
    2. CERN HN account name that used in DBS3/CMSWEB if the HN is assocated with DN: example giffels ;
    3. username with host name: example cmsprod@vocms39.cern.ch;
    """
    if candidate =='' or not candidate :
        return candidate
    r1 = r'^/[a-zA-Z][a-zA-Z0-9/\=\s()]*\=[a-zA-Z0-9/\.\-_/#:\s]*$'
    r2 = r'^[a-zA-Z0-9/][a-zA-Z0-9/\.\-_]*$'
    r3 = r'^[a-zA-Z0-9/][a-zA-Z0-9/\.\-_]*@[a-zA-Z0-9/][a-zA-Z0-9/\.\-_]*$'

    try:
        return check(r1, candidate)
    except AssertionError:
        pass

    try:
        return check(r2, candidate)
    except AssertionError:
        return check(r3, candidate)


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
    if candidate =='' or not candidate :
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
    Check candidate as a (partial) CMS name. Should pass:
        T2
        T2_UK
        T2_UK_SGrid
        T2_UK_SGrid_Bristol
    """
    #remove any trailing _'s
    candidate = candidate.rstrip('_')
    return check("^T[0-3%]((_[A-Z]{2}(_[A-Za-z0-9]+)*)?)$", candidate)

def countrycode(candidate):
    #TODO: do properly with a look up table
    return check("^[A-Z]{2}$", candidate)

def _blockStructCheck(candidate):
    """
    Basic block structure check 
    /primary/process/tier#uuid
    """
    assert candidate.count('/') == 3, "need to have / between the 3 parts which construct block name"
    parts = candidate.split('/')
    assert parts[3].count('#') == 1, "need to have # in the last parts of block"
    #should be empty string for the first part
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

DATASET_RE = r'^/[a-zA-Z0-9\.\-_]{1,99}/[a-zA-Z0-9\.\-_]{1,199}/[a-zA-Z0-9\.\-_]{1,99}$'
def dataset(candidate):
    """ A slash followed by an identifier,x3 """
    return check(DATASET_RE, candidate)

PROCDATASET_RE = r'[a-zA-Z][a-zA-Z0-9_]*(\-[a-zA-Z0-9_]+){0,2}-v[0-9]*$'
def procdataset(candidate):
    """
    Check for processed dataset name.
    letters, numbers, dashes, underscores.
    """
    if candidate == '' or not candidate:
        return candidate

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

def procversion(candidate):
    """ Integers """
    return check(r'^[0-9]+$', candidate)

def procstring(candidate):
    """ Identifier """
    return check(r'[a-zA-Z0-9_]{1,100}$', candidate)

def acqname(candidate):
    """
    Check for acquisition name.
    letters, numbers, underscores.
    """
    if candidate == '' or not candidate:
        return candidate
    return check(r'[a-zA-Z][a-zA-Z0-9_]*$', candidate)

def primdataset(candidate):
    """
    Check for primary dataset name.
    letters, numbers, dashes, underscores.
    """
    if candidate =='' or not candidate :
        return candidate
    return (check(r"%s" % PRIMARY_DS['re'], candidate, PRIMARY_DS['maxLength']) and
            check(r'^[a-zA-Z][a-zA-Z0-9\-_]*$', candidate))


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
    """
    regexp1 = '/([a-z]+)/([a-z0-9]+)/([a-zA-Z0-9\-_]+)/([a-zA-Z0-9\-_]+)/([A-Z\-_]+)/([a-zA-Z0-9\-_]+)((/[0-9]+){3}){0,1}/([0-9]+)/([a-zA-Z0-9\-_]+).root'
    regexp2 = '/([a-z]+)/([a-z0-9]+)/([a-z0-9]+)/([a-zA-Z0-9\-_]+)/([a-zA-Z0-9\-_]+)/([A-Z\-_]+)/([a-zA-Z0-9\-_]+)((/[0-9]+){3}){0,1}/([0-9]+)/([a-zA-Z0-9\-_]+).root'
    regexp3 = '/store/(temp/)*(user|group)/(%(hnName)s|%(physics_group)s)/%(primDS)s/%(secondary)s/%(version)s/%(counter)s/%(root)s' % lfnParts
    regexp4 = '/store/(temp/)*(user|group)/(%(hnName)s|%(physics_group)s)/%(primDS)s/(%(subdir)s/)+%(root)s' % lfnParts

    oldStyleTier0LFN = '/store/data/%(era)s/%(primDS)s/%(tier)s/%(version)s/%(counter)s/%(counter)s/%(counter)s/%(root)s' % lfnParts
    tier0LFN = '/store/(backfill/[0-9]/){0,1}(t0temp/|unmerged/){0,1}(data|express|hidata)/%(era)s/%(primDS)s/%(tier)s/%(version)s/%(counter)s/%(counter)s/%(counter)s(/%(counter)s)?/%(root)s' % lfnParts

    storeMcLFN = '/store/mc/([a-zA-Z0-9\-_]+)/([a-zA-Z0-9\-_]+)/([a-zA-Z0-9\-_]+)/([a-zA-Z0-9\-_]+)(/([a-zA-Z0-9\-_]+))*/([a-zA-Z0-9\-_]+).root'

    storeResults2LFN = '/store/results/%(physics_group)s/%(primDS)s/%(secondary)s/%(primDS)s/%(tier)s/%(secondary)s/%(counter)s/%(root)s' % lfnParts

    storeResultRootPart = '%(counter)s/%(root)s' % lfnParts
    storeResultsLFN = "%s/%s" % (STORE_RESULTS_LFN, storeResultRootPart)
    
    try:
        return check(regexp1, candidate)
    except AssertionError:
        pass

    try:
        return check(regexp2, candidate)
    except AssertionError:
        pass

    try:
        return check(regexp3, candidate)
    except AssertionError:
        pass

    try:
        return check(regexp4, candidate)
    except AssertionError:
        pass

    try:
        return check(tier0LFN, candidate)
    except AssertionError:
        pass

    try:
        return check(oldStyleTier0LFN, candidate)
    except AssertionError:
        pass

    try:
        return check(storeMcLFN, candidate)
    except AssertionError:
        pass
    
    try:
        return check(storeResults2LFN, candidate)
    except AssertionError:
        return check(storeResultsLFN, candidate)

def lfnBase(candidate):
    """
    As lfn above, but for doing the lfnBase
    i.e., for use in spec generation and parsing
    """
    regexp1 = '/([a-z]+)/([a-z0-9]+)/([a-zA-Z0-9\-_]+)/([a-zA-Z0-9\-_]+)/([A-Z\-_]+)/([a-zA-Z0-9\-_]+)'
    regexp2 = '/([a-z]+)/([a-z0-9]+)/([a-z0-9]+)/([a-zA-Z0-9\-_]+)/([a-zA-Z0-9\-_]+)/([A-Z\-_]+)/([a-zA-Z0-9\-_]+)((/[0-9]+){3}){0,1}'
    regexp3 = '/(store)/(temp/)*(user|group)/(%(hnName)s|%(physics_group)s)/%(primDS)s/%(secondary)s/%(version)s' % lfnParts

    tier0LFN = '/store/(backfill/[0-9]/){0,1}(t0temp/|unmerged/){0,1}(data|express|hidata)/%(era)s/%(primDS)s/%(tier)s/%(version)s/%(counter)s/%(counter)s/%(counter)s' % lfnParts

    try:
        return check(regexp1, candidate)
    except AssertionError:
        pass

    try:
        return check(regexp2, candidate)
    except AssertionError:
        pass

    try:
        return check(regexp3, candidate)
    except AssertionError:
        pass
    
    try:
        return check(tier0LFN, candidate)
    except AssertionError:
        return check(STORE_RESULTS_LFN, candidate)

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
    #regex taken from django https://github.com/django/django/blob/master/django/core/validators.py#L47
    #Copyright (c) Django Software Foundation and individual contributors
    protocol = r"^https?://"  # http:// or https://
    domain = r'?:(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,6}\.?'
    localhost = r'localhost'
    ipv4 = r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'
    ipv6 = r'\[?[a-fA-F0-9]*:[a-fA-F0-9:]+\]?'
    port = r'(?::\d+)?'  # optional port
    path = r'(?:/?|[/?]\S+)$'
    regex_url = r'%s(%s|%s|%s|%s)%s%s' % (protocol, domain, localhost, ipv4, ipv6, port, path)
    return check(regex_url, candidate)

def check(regexp, candidate, maxLength = None):
    if maxLength != None:
        assert len(candidate) <= maxLength, \
            "%s is longer then max length (%s) allowed" % (candidate, maxLength)
    assert re.compile(regexp).match(candidate) != None , \
              "'%s' does not match regular expression %s" % (candidate, regexp)
    return True


def parseLFN(candidate):
    """
    _parseLFN_

    Take an LFN, return the component parts
    """

    # First, make sure what we've gotten is a real LFN
    lfn(candidate)

    parts = candidate.split('/')
    final = {}

    if parts[0] == '':
        parts.remove('')
    if 'user' in parts[1:3] or 'group' in parts[1:3]:
        if parts[1] in ['user', 'group']:
            final['baseLocation'] = '/%s' % string.join(parts[:2], '/')
            parts = parts[2:]
        else:
            final['baseLocation'] = '/%s' % string.join(parts[:3], '/')
            parts = parts[3:]

        final['hnName']            = parts[0]
        final['primaryDataset']    = parts[1]
        final['secondaryDataset']  = parts[2]
        final['processingVersion'] = parts[3]
        final['lfnCounter']        = parts[4]
        final['filename']          = parts[5]

        return final


    if len(parts) == 8:
        # Then we have only two locations
        final['baseLocation'] = '/%s' % string.join(parts[:2], '/')
        parts = parts[2:]
    elif len(parts) == 9:
        final['baseLocation'] = '/%s' % string.join(parts[:3], '/')
        parts = parts[3:]
    else:
        # How did we end up here?
        # Something just went wrong
        msg =  """CRITICAL!  This machine has experienced a complete logic failure while parsing LFNs.\n
        If you are a developer this indicates that you have changed the Lexicon LFN regexp functions without changing the parsing.\n
        If you are an operator, this indicates that this machine is likely unstable.\n
        All data should be backed up and the machine removed from production for examination.\n"""
        msg += "Candidate: %s" % candidate
        raise WMException(msg)


    final['acquisitionEra']    = parts[0]
    final['primaryDataset']    = parts[1]
    final['dataTier']          = parts[2]
    final['processingVersion'] = parts[3]
    final['lfnCounter']        = parts[4]
    final['filename']          = parts[5]

    return final


def parseLFNBase(candidate):
    """
    _parseLFNBase_

    Return a meaningful dictionary with info from an LFNBase
    """

    # First, make sure what we've gotten is a real LFNBase
    lfnBase(candidate)

    parts = candidate.split('/')
    final = {}

    if parts[0] == '':
        parts.remove('')

    if 'user' in parts[1:3] or 'group' in parts[1:3]:
        if parts[1] in ['user', 'group']:
            final['baseLocation'] = '/%s' % string.join(parts[:2], '/')
            parts = parts[2:]
        else:
            final['baseLocation'] = '/%s' % string.join(parts[:3], '/')
            parts = parts[3:]

        final['hnName']            = parts[0]
        final['primaryDataset']    = parts[1]
        final['secondaryDataset']  = parts[2]
        final['processingVersion'] = parts[3]

        return final

    if len(parts) == 6:
        # Then we have only two locations
        final['baseLocation'] = '/%s' % string.join(parts[:2], '/')
        parts = parts[2:]
    elif len(parts) == 7:
        final['baseLocation'] = '/%s' % string.join(parts[:3], '/')
        parts = parts[3:]
    else:
        # How did we end up here?
        # Something just went wrong
        msg =  """CRITICAL!  This machine has experienced a complete logic failure while parsing LFNs.\n
        If you are a developer this indicates that you have changed the Lexicon LFN regexp functions without changing the parsing.\n
        If you are an operator, this indicates that this machine is likely unstable.\n
        All data should be backed up and the machine removed from production for examination.\n"""
        msg += "Candidate: %s" % candidate
        raise WMException(msg)

    final['acquisitionEra']    = parts[0]
    final['primaryDataset']    = parts[1]
    final['dataTier']          = parts[2]
    final['processingVersion'] = parts[3]

    return final

def sanitizeURL(url):
    """Take the url with/without username and password and return sanitized url,
       username and password in dict format
       WANNING: This doesn't check the correctness of url format.
       Don't use ':' in username or password.
    """
    endpoint_components = urlparse.urlparse(url)
    # Cleanly pull out the user/password from the url
    if endpoint_components.port:
        netloc = '%s:%s' % (endpoint_components.hostname,
                    endpoint_components.port)
    else:
        netloc = endpoint_components.hostname

    #Build a URL without the username/password information
    url = urlparse.urlunparse(
            [endpoint_components.scheme,
             netloc,
             endpoint_components.path,
             endpoint_components.params,
             endpoint_components.query,
             endpoint_components.fragment])

    return {'url': url , 'username': endpoint_components.username,
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
