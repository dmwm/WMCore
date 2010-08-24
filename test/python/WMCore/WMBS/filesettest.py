from WMCore.WMBS.Factory import SQLFactory 
from sqlalchemy import create_engine
from sqlalchemy import __version__ as sqlalchemy_version
import sqlalchemy.pool as pool
import logging
from WMCore.Database.DBCore import DBInterface
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.File import File
from WMCore.WMBS.Workflow import Workflow

database = 'sqlite://'
#database = 'sqlite:///filesettest'

engine = create_engine(database)
"make a logger instance"
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='%s.log' % __file__,
                    filemode='w')

logger = logging.getLogger('wmbs_sql')
logger.info('test')
logger.debug("Using SQLAlchemy v.%s" % sqlalchemy_version)

wmbs = SQLFactory(logger).connect(engine)
try:
    wmbs.createWMBS()
except Exception, e:
    print 'already a WMBS instance in %s' % database
    print e

fs = Fileset('Simons Data', wmbs)
if fs.exists():
    print "fileset exists - which it shouldn't"
else:
    try:
        print "fileset didn't exist, creating now"
        fs.create()
    except:
        print "this exception should not be thrown - the fileset should not exist yet"
if fs.exists():
    print 'fileset exists - which it should, and an exception should be thrown next'
    try:
        fs.create()
    except:
        print "this exception should be thrown - the fileset should exist by now"
    fs.populate()

file1 = File('/store/user/metson/file1', 123, 234, 345, 456)
file2 = File('/store/user/metson/file2', 123, 234, 345, 456)
print "list of files, should be empty:\n\t %s" % fs.listFiles()
fs.addFile(file1)
print "list of files, should have one file:\n\t %s" % fs.listFiles()
print "list of new files, should have one file:\n\t %s" % fs.listNewFiles()
fs.commit()
print "list of files, should have one file:\n\t %s" % fs.listFiles()
print "list of files, should be empty:\n\t %s" % fs.listNewFiles()
fs.addFile(file2)
print "list of files, should have two files:\n\t %s" % fs.listFiles()
print "list of new files, should have one file:\n\t %s" % fs.listNewFiles()
fs.commit()

fs2 = Fileset('Simons Data', wmbs)
print 'fileset exists? %s' % fs2.exists()
fs2.populate()
print "list of files, should have two files:\n\t %s" % fs2.listFiles()
print "list of files, should be empty:\n\t %s" % fs2.listNewFiles()

print " ### Look for subscriptions, should be none"

fs.subscriptions()
fs.subscriptions('merge')
print "check for subscriptions of type 'blah' - should throw exception"
try:
    fs.subscriptions('blah')
except:
    print "\t Exception - This should be thrown"

wf = Workflow('~/myspec.xml', 'metson', wmbs)
print 'Workflow %s exists?: %s' % (wf.spec, wf.exists())
wf.create()
print 'Workflow %s exists?: %s' % (wf.spec, wf.exists())

fs.createSubscription(wf)

print " ### Look for subscriptions, should be one"

fs.subscriptions()
fs.subscriptions('merge')
print "check for subscriptions of type 'blah' - should throw exception"
try:
    fs.subscriptions('blah')
except:
    print "\t Exception - This should be thrown"




































