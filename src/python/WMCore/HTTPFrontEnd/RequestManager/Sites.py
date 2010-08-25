import WMCore.Wrappers.JsonWrapper as JsonWrapper
import urllib

def sites():
    # download all the sites from siteDB
    url = 'https://cmsweb.cern.ch/sitedb/json/index/CEtoCMSName?name'
    data = JsonWrapper.loads(urllib.urlopen(url).read().replace("'", '"'))
    # kill duplicates, then put in alphabetical order
    siteset = set([d['name'] for d in data.values()])
    # warning: alliteration
    sitelist = list(siteset)
    sitelist.sort()
    return sitelist


