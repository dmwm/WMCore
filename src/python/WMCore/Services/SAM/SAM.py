#!/usr/bin/env python
"""
_SAM_

Talk to the SAM Service to get site status from the results of SAM tests.
"""

__revision__ = "$Id: SAM.py,v 1.6 2010/03/08 23:16:51 sryu Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.Services.Service import Service

class SAM(Service):
    def getCMSSWInstalls(self, ce = None):
        """
        Contact the SAM test page and get the tested, installed software on a CE.
        
        Can't use an XML parser because it's not compliant XML...
        """
        inputdata = {"funct":"TestResultLatest",
                    "nodename":ce,
                    "vo":"cms",
                    "testname":"CE-cms-swinst"}
        file = 'sam_cmssw_inst_%s.html' % ce
        
        f = self.refreshCache(file, url = '/sam/sam.py', inputdata = inputdata, verb = 'GET')
        lines = f.readlines()
        f.close()
        
        start = []
        end = []
        for i in lines:
            if i.startswith('<ol'):
                idx = lines.index(i)
                start.append(idx)
            elif i.endswith('</ol>\n'):
                idx = lines.index(i)
                end.append(idx)
                # Change lines[idx] so we don't find it next time...
                lines[idx] = '----'
        
        # Here I overwrite the cached file with an abridged version, to speed 
        # things up next access, so long as the file hasn't expired.
        f = open(file, 'w')
        releases = {}
        assert len(start) == len(end)
        for s,e in zip(start, end):
            arch = ""
            try:
                #sometimes the class is quoted...
                arch = lines[s].split('"')[3]
            except:
                #and sometimes its not!
                arch = lines[s].split('"')[1]
            f.write(lines[s])
            releases[arch] = []
            for rel in lines[s+1: e]:
                releases[arch].append(rel[5:-6])
            f.write(rel)
            f.write('</ol>\n')
        f.close()

        return releases

if __name__ == '__main__':
    import logging
    logging.basicConfig(level = logging.DEBUG,
                    format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt = '%m-%d %H:%M',
                    filename = '/tmp/samstatus.log',
                    filemode = 'w')

    dict = {}
    dict['endpoint'] = 'https://lcg-sam.cern.ch:8443'
    dict['cachepath'] = '/tmp/'
    dict['type'] = 'text/json'
    dict['logger'] = logging.getLogger('SAMtest')

    sam = SAM(dict = dict)
    print sam.getCMSSWInstalls(ce = 'lcgce01.phy.bris.ac.uk')
