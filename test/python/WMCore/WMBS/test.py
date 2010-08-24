from WMCore.WMBS.Factory import SQLFactory 
from sqlalchemy import create_engine
from sqlalchemy import __version__ as sqlalchemy_version
import sqlalchemy.pool as pool
import logging

database = "sqlite:///database.lite"
# mysql
database = 'mysql://metson@localhost/wmbs'
"make a connection to a database"
"for some weird reason my sql wants pool_size > 1"
"something to do with the number of sub queries?" 
engine = create_engine(database, convert_unicode=True, encoding='utf-8', pool_size=10, pool_recycle=30)

"make a logger instance"
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='%s.log' % __file__,
                    filemode='w')

logger = logging.getLogger('wmbs_sql')
logger.info('test')
logger.debug("Using SQLAlchemy v.%s" % sqlalchemy_version)

factory = SQLFactory(logger)

wmbs = factory.connect(engine)
# TODO: Drop tables first!

try: wmbs.createFilesetTable()
except:
    print "Couldn't create Fileset table - probably already exists, check the log" 
try: wmbs.createFileTable()
except:
    print "Couldn't create File table - probably already exists, check the log"
try:
    wmbs.createFileParentTable()
except:
    print "Couldn't create File Parent table - probably already exists, check the log"
try: wmbs.createFileDetailsTable()
except:
    print "Couldn't create File Details table - probably already exists, check the log"
try: wmbs.createLocationTable()
except:
    print "Couldn't create Location table - probably already exists, check the log"
try: wmbs.createFileLocationsTable()
except:
    print "Couldn't create File Locations table - probably already exists, check the log"
try: wmbs.createSubscriptionsTable()
except:
    print "Couldn't create Subscriptions table - probably already exists, check the log"
try: wmbs.createSubscriptionAcquiredFilesTable()
except:
    print "Couldn't create Acquired Files table - probably already exists, check the log"
try: wmbs.createSubscriptionFailedFilesTable()
except:
    print "Couldn't create Failed Files table - probably already exists, check the log"
try: wmbs.createSubscriptionCompletedFilesTable()
except:
    print "Couldn't create Complete Files table - probably already exists, check the log"
try: wmbs.createJobTable()
except:
    print "Couldn't create Job table - probably already exists, check the log"
try: wmbs.createJobAssociationTable()
except:
    print "Couldn't create Job Association table - probably already exists, check the log"

print "Created a WMBS instance in %s" % database

wmbs.insertFileset('/Higgs/SimonsCoolData/RECO')
wmbs.insertFileset(['dataset1', 'dataset2'])

fs = wmbs.showAllFilesets()
print 'All filesets'
for f in fs:
    for i in f.fetchall():
        print i

wmbs.insertFilesForFileset(files='file1', fileset='/Higgs/SimonsCoolData/RECO')
fs = wmbs.showFilesInFileset(fileset='/Higgs/SimonsCoolData/RECO')
print 'files in fileset /Higgs/SimonsCoolData/RECO'
for f in fs:
    for i in f.fetchall():
        print i
    
filelist=[]
# files is a list of tuples containing lfn, size, events, run and lumi
for x in range(500):
    f = 'lfn%s' % x,  5 *x,  10* x, x, x+3
    filelist.append(f)

wmbs.insertFilesForFileset(files=filelist, fileset='dataset1')
print "inserted 5 files:"
fs = wmbs.showFilesInFileset(fileset='dataset1')
for f in fs:
    for i in f.fetchall():
        print i

wmbs.newSubscription(fileset='/Higgs/SimonsCoolData/RECO')
wmbs.newSubscription(fileset='/Higgs/SimonsCoolData/RECO', subtype='merge')
wmbs.newSubscription(fileset=['dataset1', 'dataset2'])

fs = wmbs.subscriptionsForFileset(fileset='/Higgs/SimonsCoolData/RECO')
print 'all subscriptions on /Higgs/SimonsCoolData/RECO - should be a processing and a merge'
for f in fs:
    for i in f.fetchall():
        print i
    
fs = wmbs.subscriptionsForFileset(fileset='/Higgs/SimonsCoolData/RECO123')
print 'all subscriptions on /Higgs/SimonsCoolData/RECO123 - should be none'
for f in fs:
    for i in f.fetchall():
        print i
    
fs = wmbs.subscriptionsForFileset(fileset='/Higgs/SimonsCoolData/RECO', subtype='merge')
print 'merge subscriptions on /Higgs/SimonsCoolData/RECO - should be one'
for f in fs:
    for i in f.fetchall():
        print i  

fs = wmbs.listSubscriptionsOfType('processing')
print 'processing subscriptions - should be 3'
for f in fs:
    for i in f.fetchall():
        print i   

print "files in subscription 1"
fs = wmbs.filesForSubscription(1)
for f in fs:
    for i in f.fetchall():
        print i

print "files in subscription 2"
fs = wmbs.filesForSubscription(3)
for f in fs:
    for i in f.fetchall():
        print i

print 'add a location'
fs = wmbs.addNewLocation('se.place.uk')
for f in fs:
    for i in f.fetchall():
        print i

print 'add a list of locations'
fs = wmbs.addNewLocation(['se1.place.uk', 'se2.place.uk', 'se3.place.uk'])
for f in fs:
    for i in f.fetchall():
        print i        

fs = wmbs.listAllLocations()
print 'list all locations'
for f in fs:
    for i in f.fetchall():
        print i

print 'put file1 at a site'        
fs = wmbs.putFileAtLocation('file1', 'se.place.uk')
fs = wmbs.putFileAtLocation('file1', 'se3.place.uk')
print 'find sites with file1'
fs = wmbs.locateFile('file1')
for f in fs:
    for i in f.fetchall():
        print i 
               
print 'adding a new file to a location'
fs = wmbs.addNewFileToLocation(file='file2',fileset='/Higgs/SimonsCoolData/RECO', sename='se.place.uk')

fs = wmbs.listFilesAtLocation('se.place.uk')
for f in fs:
    for i in f.fetchall():
        print i 

print "list available files for subscription 3"
fs = wmbs.listAvailableFiles(3)
for f in fs:
    for i in f.fetchall():
        print i 
        
acqsize = 50
for i in range(5):
    print "acquire some files for subscription 3"
    fs = wmbs.listAvailableFiles(3)
    filelist = fs[0].fetchall()
    print 'number of available files: %s' % len( filelist )
    print "acquiring %s " % filelist[:acqsize]
    filelist = [ x[0] for x in filelist[:acqsize] ]
    fs = wmbs.acquireNewFiles(3, filelist[:acqsize])
    fs = wmbs.listAcquiredFiles(3)
    print 'number of acquired files: %s' % len( fs[0].fetchall() )
    if i % 2 == 0 :
        fs = wmbs.failFiles(3, filelist[:acqsize])
    else:
        fs = wmbs.completeFiles(3, filelist[:acqsize])
    fs = wmbs.listCompletedFiles(3)
    print 'number of completed files: %s' % len( fs[0].fetchall() )
    fs = wmbs.listFailedFiles(3)
    print 'number of failed files: %s' % len( fs[0].fetchall() )












