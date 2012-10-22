"""
OpsClipboard auxiliary development script.

Load OpsClipboard couchapp and inject some example requests.

CouchDB URL as needed as CLI argument could be
    $COUCHURL env.variable
    https://localhost:2000/couchdb/ [tunnel to the VM development instance]
    https://maxadmwm.cern.ch/couchdb/" [VM development instance, via frontend]

    actually, it's not possible (envisaged) to acquire CouchDB admin rights
    when running CouchDB behind frontend. This would need fiddling with
    local.ini settings on dev-vm CouchDB instance, otherwise admin operations
    such as deleting a database can be done locally,i.e. going through localhost:5984
    This can be tested manually with curl, on localhost dropping a database:
        {"error":"unauthorized","reason":"You are not a server admin."}
    If these admin rights would be sorted, the TestInitCouchApp would
    need a little modification as well since CouchAppTestHarness always
    reads COUCHURL from env. variable rather than taking into account
    its input argument - the couchUrl as specified on CLI would need
    to be used there.

"""

import os
import sys
import logging
from optparse import OptionParser, TitledHelpFormatter

from WMCore.Database.CMSCouch import CouchServer, Database
import WMCore.RequestManager.OpsClipboard.Inject as OpsClipboard
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore_t.RequestManager_t.OpsClipboard_t import getTestRequests




class CouchAppTester(object):
    def __init__(self, couchUrl, dbName):
        self.couchUrl = couchUrl
        self.dbName = dbName
        self.couch = Database(dbName, couchUrl)


    def queryAll(self):
        print "Quering all docs in the database '%s'..." % self.dbName
        r = self.couch.allDocs()
        print "total_rows: %s" % r[u"total_rows"]
        for row in r[u"rows"]:
            print row


    def couchapp(self, couchappName):
        """
        Drop database in CouchDB.
        Reload (push) the couchapp couchappName (likely to be "OpsClipboard").

        """
        print "Pushing couchapp '%s' ..." % couchappName
        # here probably need to specify the couchUrl, otherwise default env COUCHURL
        # will be taken (as described above)
        testInit = TestInitCouchApp(__file__, dropExistingDb=True)
        # should now drop the CouchDB db, create a new one and push the couchapp
        testInit.setupCouch(self.dbName, couchappName)


    def createRequests(self, numRequests):
        """
        Create numRequests in CouchDB.

        """
        print "Creating %s requests ..." % numRequests
        # attempt to do this on CouchDB behind frontend - as described above
        requests, campaignIds, requestIds = getTestRequests(numRequests)
        print "Request names: %s" % requestIds
        OpsClipboard.inject(self.couchUrl, self.dbName, *requests)
        print "OpsClipboard.inject() will inject only new request names ..."
        # in order to load a couchapp view - need to have couchapp name, e.g.:
        #results = self.couch.loadView("OpsClipboard", "all")
        #print results



def _processCmdLineArgs(args):
    usage = \
"""usage: %prog options"""
    form = TitledHelpFormatter(width = 78)
    parser = OptionParser(usage = usage, formatter = form, add_help_option = None)
    _defineCmdLineOptions(parser)
    # opts - new processed options
    # args - remainder of the input array
    opts, args = parser.parse_args(args = args)
    for mandatory in ("couchUrl", "database"):
        if not getattr(opts, mandatory, None):
            print "Missing mandatory option ('%s')." % mandatory
            parser.print_help()
            sys.exit(1)
    return opts



def _defineCmdLineOptions(parser):
    help = "Display this help"
    parser.add_option("-h", "--help", help=help, action='help')
    help = "CouchDB instance URL."
    parser.add_option("-u", "--couchUrl", help=help)
    help = "CouchDB database name (no other options to query all docs)."
    parser.add_option("-d", "--database", help=help)
    help = "Name of couchapp to push into CouchDB/database (source from src/couchapps)."
    parser.add_option("-a", "--couchapp", help=help)
    help = "Create specified number of new requests."
    parser.add_option("-c", "--createRequests", help=help)



def main():
    # couchapp manipulation produces a lot more debug output
    logging.basicConfig(level = logging.DEBUG)
    opts = _processCmdLineArgs(sys.argv)
    tester = CouchAppTester(opts.couchUrl, opts.database)
    if opts.couchapp:
        tester.couchapp(opts.couchapp)
    if opts.createRequests:
        tester.createRequests(int(opts.createRequests))
    tester.queryAll()

    print "Finished."



if __name__ == "__main__":
    main()
