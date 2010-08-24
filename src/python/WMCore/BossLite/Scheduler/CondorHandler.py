#!/usr/bin/env python
"""
_CondorHandler_
Parser for XML output of condor_q
Original version by Brian Bockelman
"""




from xml.sax.handler import ContentHandler, feature_external_ges
from xml.sax import make_parser
import sys

class CondorHandler(ContentHandler):

    def __init__(self, idx, attrlist=[]):
        self.attrlist = attrlist
        self.idxAttr = idx

    def startDocument(self):
        self.attrInfo = ''
        self.jobInfo = {}

    def startElement(self, name, attrs):
        if name == 'c':
            self.curJobInfo = {}
        elif name == 'a':
            self.attrName = str(attrs.get('n', 'Unknown'))
            self.attrInfo = ''
        else:
            pass

    def endElement(self, name):
        if name == 'c':
            idx = self.curJobInfo.get(self.idxAttr, None)
            if idx:
                self.jobInfo[idx] = self.curJobInfo
        elif name == 'a':
            if self.attrName in self.attrlist or len(self.attrlist) == 0:
                self.curJobInfo[self.attrName] = self.attrInfo
        else:
            pass

    def characters(self, ch):
        self.attrInfo += str(ch)

    def getJobInfo(self):
        return self.jobInfo

if __name__ == '__main__':
    handler = CondorHandler('GlobalJobId', ['JobStatus', 'GridJobId', \
        'MATCH_GLIDEIN_Gatekeeper', 'GlobalJobId'])
    parser = make_parser()
    parser.setContentHandler(handler)
    parser.setFeature(feature_external_ges, False)
    parser.parse(open('test.xml', 'r'))
    print >> sys.stderr, handler.getJobInfo()

