#!/usr/bin/env python
"""
_couch_archiver_

Replicate an agent's jobdump to another couch machine.
"""
from __future__ import print_function

import os
import sys

from WMCore.Configuration import loadConfigurationFile
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Lexicon import sanitizeURL

if len(sys.argv) != 3:
    print("Usage: ")
    print("  archiver.py [Destination Couch URL] [Destination DB Name]")
    print("")
    print("Example:")
    print("  archiver.py http://username:password@cms-xen41.fnal.gov:5984 cmssrv98_070_jobdump")
    sys.exit(0)

wmagentConfig = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])
srcCouchHost = wmagentConfig.JobStateMachine.couchurl
srcDbBase = wmagentConfig.JobStateMachine.couchDBName
destCouchHost = sys.argv[1]
destDbBase = sys.argv[2]

destCouchServer = CouchServer(dburl = destCouchHost)
srcCouchServer = CouchServer(dburl = srcCouchHost)

srcJobsDb = srcCouchHost + "/" + srcDbBase + "%2Fjobs"
destJobsDb = destCouchHost + "/" + destDbBase + "%2Fjobs"
srcFwjrsDb = srcCouchHost + "/" + srcDbBase + "%2Ffwjrs"
destFwjrsDb = destCouchHost + "/" + destDbBase + "%2Ffwjrs"

print("Archiving %s/%s to %s/%s..." % (srcCouchHost, srcDbBase, destCouchHost, destDbBase))

# Replicate the FWJR and Jobs databases...
print("  Replicating jobs database...")
destCouchServer.replicate(srcJobsDb, destJobsDb, create_target = True)
print("  Replication fwjrs database...")
destCouchServer.replicate(srcFwjrsDb, destFwjrsDb, create_target = True)

# Generate views for the various databases
destJobsDb = destCouchServer.connectDatabase(destDbBase + "/jobs")
destFwjrsDb = destCouchServer.connectDatabase(destDbBase + "/fwjrs")
print("  Triggering view generation for jobs database...")
destJobsDb.loadView("JobDump", "statusByWorkflowName", options = {"limit": 1})
print("  Triggering view generation for fwjrs database...")
destFwjrsDb.loadView("FWJRDump", "outputByWorkflowName", options = {"limit": 1})

print("")
# Query destination DB for list of workflows
summaryBase = "%s/%s%%2Ffwjrs/_design/FWJRDump/_show/workflowSummary/%s" # dest host, dest db base, workflow name
successBase = "%s/%s%%2Fjobs/_design/JobDump/_list/successJobs/statusByWorkflowName?startkey=%%5B%%22%s%%22%%5D&endkey=%%5B%%22%s%%22%%2C%%7B%%7D%%5D&reduce=false" # dest host, dest db base, workflow, workflow
failedBase = "%s/%s%%2Fjobs/_design/JobDump/_list/failedJobs/statusByWorkflowName?startkey=%%5B%%22%s%%22%%5D&endkey=%%5B%%22%s%%22%%2C%%7B%%7D%%5D&reduce=false" # dest host, dest db base, workflow, workflow

srcJobsDb = srcCouchServer.connectDatabase(srcDbBase + "/jobs")
statusResult = srcJobsDb.loadView("JobDump", "statusByWorkflowName", options = {"group_level": 1})

fileHandle = open("archived.html", "w")
fileHandle.write("<html><head><title>Archived Workflows</title></head>\n")
fileHandle.write("<body>\n")

workflowNames = []
for statusRow in statusResult["rows"]:
    wfName = statusRow["key"][0]
    summaryUrl = summaryBase % (destCouchHost, destDbBase, wfName)
    successUrl = successBase % (destCouchHost, destDbBase, wfName, wfName)
    failedUrl = successBase % (destCouchHost, destDbBase, wfName, wfName)
    fileHandle.write("%s " % wfName)
    fileHandle.write("<a href=%s>(summary)</a>" % sanitizeURL(summaryUrl)["url"])
    fileHandle.write(" <a href=%s>(success)</a>" % sanitizeURL(successUrl)["url"])
    fileHandle.write(" <a href=%s>(failure)</a><br>\n" % sanitizeURL(failedUrl)["url"])

fileHandle.write("</body></html>\n")
fileHandle.close()
