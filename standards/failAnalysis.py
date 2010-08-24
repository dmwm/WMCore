#!/usr/bin/python
import os, sys
import xml.dom.minidom
import time
import subprocess
import urllib
from WMCore.Database.CMSCouch import CouchServer
try:
    import json
except:
    import simplejson as json
# the buildconfig file will end up being in the buildslave's main path
sys.path.append( os.path.realpath(os.path.join(os.getcwd(), "..",".." )) )
import buildslaveconfig as buildslave

couch = CouchServer(buildslave.conf['failTarget'])
# Don't have permission to check list of databases, so can't create
database = couch.connectDatabase('buildbot-couch', create = False)

# what the slave does to us
#                f.addStep(ShellCommand(command=['python','standards/wrapEnv.py','python26',
#                                                'sqlite','python2.6','standards/failAnalysis.py',
#                                                WithProperties("nosetests-" + x['db'] + "-%s.xml", 'got_revision'),
#                                                WithProperties("%s", 'buildername'),
#                                                WithProperties("%s", 'buildnumber'),
#                                                WithProperties("%s", 'got_revision'),
#                                                x['db']],
#                                                description="Analyzing fail", descriptionDone="Fail Being Analyzed"))
print "confused .. %s" % sys.argv
myname, xmlfile, buildername, buildnumber, revision, db = sys.argv[:6]
timestamp = time.time()
#(myname, xmlfile, buildername, buildnumber, revision, db ) = ('dum', '../nosetests.xml',1,2,3,4)
# what metson wants
#{
#    "test_name": test_name, 
#    "builder": builder, 
#    "timestamp": timestamp, 
#    "bld_id": build_id, 
#    "step": step,
#    "reason": reason
#}

handle = open(xmlfile,'r')
xunit  = xml.dom.minidom.parse( handle )
handle.close()

if len(xunit.getElementsByTagName('testsuite')) > 1:
    raise RuntimeError, "More than one test suite? need to handle this"
longRunning = []

for case in xunit.getElementsByTagName("testsuite")[0].getElementsByTagName('testcase'):
    if case.hasAttribute('time'):
        longRunning.append([int(case.getAttribute('time')), case.getAttribute('classname'), case.getAttribute('name')])
        longRunning.sort()

        longRunning = longRunning[-20:]
        
    if len(case.childNodes) > 0:
        if len(case.childNodes) > 1:
            raise RuntimeError, "Shouldn't be more than one error in a testcase"
        # this isn't a win.
        message  = "++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n"
        message += "Exception type: %s \n" % case.firstChild.getAttribute('type')
        message += "Error message: %s \n" % case.firstChild.getAttribute('message')
        case.firstChild.normalize()
        traceback = ""
        if case.firstChild.hasChildNodes():
            for child in case.firstChild.childNodes:
                traceback += child.nodeValue
        else:
            traceback = child.firstChild.nodeValue
        message += "Traceback: %s \n" % traceback
        message += "========================================================"
        myData = {
                "test_name": (case.getAttribute('classname') + '.' + case.getAttribute('name')), 
                "builder": buildername, 
                "timestamp": timestamp, 
                "bld_id": buildnumber, 
                "step": "notsure",
                "reason": traceback
            }
        database.queue(myData)
        
database.commit()

longRunning.reverse()
print "These are the longest-running tests:"
tmp = 1
for longTest in longRunning:
    print "%s) %s seconds - %s.%s" % (tmp, longTest[0],longTest[1], longTest[2])
    tmp += 1
    

# testcase win:
#<testcase classname="WMCore_t.FwkJobReport_t.Report_t.ReportTest" name="testBadXMLParsing" time="0" />

# regular error
#<testcase classname="WMQuality_t.TestInit_t.TestInitTest" name="testDeletion" time="0">
#        <error message="You must set the DATABASE environment variable to run tests" type="exceptions.RuntimeError">
#<![CDATA[Traceback (most recent call last):couchURL = 'http://andrewmelo:blumpkin@drsm79.cloudant.com/buildbot-couch'
#  File "/opt/local/Library/Frameworks/Python.framework/Versions/2.6/lib/python2.6/unittest.py", line 270, in run
#    self.setUp()
#  File "/Users/meloam/Documents/workspace/WMCORE/test/python/WMQuality_t/TestInit_t.py", line 16, in setUp
#    self.temptestInit.setDatabaseConnection()
#  File "/Users/meloam/Documents/workspace/WMCORE/src/python/WMQuality/TestInit.py", line 202, in setDatabaseConnection
#    config = self.getConfiguration(connectUrl=connectUrl, socket=socket)
#  File "/Users/meloam/Documents/workspace/WMCORE/src/python/WMQuality/TestInit.py", line 295, in getConfiguration
#    "You must set the DATABASE environment variable to run tests"
#RuntimeError: You must set the DATABASE environment variable to run tests
#]]>        </error>
#    </testcase>

# import error

#<testcase classname="nose.failure.Failure" name="runTest" time="0">
#        <error message="No module named cherrypy" type="exceptions.ImportError">
#<![CDATA[Traceback (most recent call last):
#  File "/opt/local/Library/Frameworks/Python.framework/Versions/2.6/lib/python2.6/unittest.py", line 279, in run
#    testMethod()
#  File "/opt/local/Library/Frameworks/Python.framework/Versions/2.6/lib/python2.6/site-packages/nose-0.11.4-py2.6.egg/nose/loader.py", line 382, in loadTestsFromName
#    addr.filename, addr.module)
#  File "/opt/local/Library/Frameworks/Python.framework/Versions/2.6/lib/python2.6/site-packages/nose-0.11.4-py2.6.egg/nose/importer.py", line 39, in importFromPath
#    return self.importFromDir(dir_path, fqname)
#  File "/opt/local/Library/Frameworks/Python.framework/Versions/2.6/lib/python2.6/site-packages/nose-0.11.4-py2.6.egg/nose/importer.py", line 86, in importFromDir
#    mod = load_module(part_fqname, fh, filename, desc)
#  File "/Users/meloam/Documents/workspace/WMCORE/test/python/WMCore_t/WebTools_t/NestedModel_t.py", line 2, in <module>
#    from cherrypy import HTTPError
#ImportError: No module named cherrypy
#]]>        </error>
#    </testcase>

