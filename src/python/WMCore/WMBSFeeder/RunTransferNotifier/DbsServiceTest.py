#!/usr/bin/env python

from DbsCli import sendMessage as callDbsWithOptions
import sys
from xml.dom.minidom import parseString

def queryDbs(query):
    """
    Queries DBS and returns XML of interest
    """
    res = callDbsWithOptions("cmsweb.cern.ch/dbs_discovery/", 443, "cms_dbs_prod_global", query,
                       0, -1, 1)
    print res
    return res

def getText(nodelist):
    rc = ""
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc = rc + node.data
    return rc

def getXmlNodes(xml, nodelist):
    if len(nodelist) == 1:
        nodes = xml.getElementsByTagName(nodelist[0])
        retList = []
        for node in nodes:
            retList.append(getText(node.childNodes))
        return retList
    else:
        node = xml.getElementsByTagName(nodelist[0])[0]
        return getXmlNodes(node, nodelist[1:])

xmlStr = queryDbs(sys.argv[1])
print "----------------------------------"

xml = None
try:
    xml = parseString(xmlStr)
    print xml.toprettyxml()
except:
    print "Error parsing XML"

print getXmlNodes(xml, ["ddresponse","output","name"])