""" Functions to interpret lists that get sent in as text"""
import urllib
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper


def parseRunList(l):
    """ Changes a string into a list of integers """
    toks = l.lstrip(' [').rstrip(' ]').split(',')
    if toks == ['']:
        return []
    return [int(tok) for tok in toks]

def parseBlockList(l):
    """ Changes a string into a list of strings """
    toks = l.lstrip(' [').rstrip(' ]').split(',')
    if toks == ['']:
        return []
    # only one set of quotes
    return [str(tok.strip(' \'"')) for tok in toks]

def parseSite(kw, name):
    """ puts site whitelist & blacklists into nice format"""
    value = kw.get(name, [])
    if value == None:
        value = []
    if not isinstance(value, list):
        value = [value]
    return value

def allSoftwareVersions():
    result = []
    f = urllib.urlopen("https://cmstags.cern.ch/cgi-bin/CmsTC/ReleasesXML")
    for line in f:
        for tok in line.split():
            if tok.startswith("label="):
                release = tok.split("=")[1].strip('"')
                result.append(release)
    return result

def saveWorkload(helper, workload):
    """ Saves the changes to this workload """
    if workload.startswith('http://'):
        helper.saveCouchUrl(workload)
    else:
        helper.save(workload)

def removePasswordFromUrl(url):
    # where the @ symbol is at.
    result = url
    atat = url.find('@')
    slashslashat = url.find('//')
    if atat != -1 and slashslashat != -1 and slashslashat < atat:
       result = url[:slashslashat+2] + url[atat+1:]
    return result

