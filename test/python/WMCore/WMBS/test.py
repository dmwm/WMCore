from WMCore.WMBS.Factory import SQLFactory 
from sqlalchemy import create_engine
from sqlalchemy import __version__ as sqlalchemy_version
import sqlalchemy.pool as pool
import logging

database = "sqlite:///database.lite"
# mysql
#database = 'mysql://metson@localhost/wmbs'
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

try: 
    wmbs.createWMBS()
    print "Created a WMBS instance in %s" % database
except:
    print "Couldn't create WMBS tables - probably already exists, check the log" 

wmbs.insertFileset('/Higgs/SimonsCoolData/RECO')
#wmbs.insertFileset(['dataset1', 'dataset2'])
wmbs.insertFileset('dataset1')
wmbs.insertFileset('dataset2')
fs = wmbs.showAllFilesets()
print 'All filesets'
print fs

wmbs.insertFilesForFileset(files='file1', fileset='/Higgs/SimonsCoolData/RECO')
fs = wmbs.showFilesInFileset(fileset='/Higgs/SimonsCoolData/RECO')
print 'files in fileset /Higgs/SimonsCoolData/RECO'
print fs
    
filelist=[]
# files is a list of tuples containing lfn, size, events, run and lumi
size = 5
for x in range(size):
    f = 'lfn%s' % x,  5 *x,  10* x, x, x+3
    filelist.append(f)

wmbs.insertFilesForFileset(files=filelist, fileset='dataset1')
print "inserted %s files:" % size
fs = wmbs.showFilesInFileset(fileset='dataset1')
for f in fs:
    print f

wmbs.newWorkflow('file.xml', 'metson')
wmbs.newWorkflow('file2.xml', 'metson')

wmbs.newSubscription(fileset='/Higgs/SimonsCoolData/RECO', spec='file.xml', owner = 'metson')
wmbs.newSubscription(fileset='/Higgs/SimonsCoolData/RECO', spec='file.xml', owner = 'metson', subtype='Merge')
wmbs.newSubscription(fileset=['dataset1', 'dataset2'], spec='file2.xml', owner = 'metson')

print "### ID number for the four subscriptions:"
print wmbs.subscriptionID(fileset='/Higgs/SimonsCoolData/RECO', spec='file.xml', owner = 'metson')
print wmbs.subscriptionID(fileset='/Higgs/SimonsCoolData/RECO', spec='file.xml', owner = 'metson', subtype='Merge')
print wmbs.subscriptionID(fileset='dataset1', spec='file2.xml', owner = 'metson')
print wmbs.subscriptionID(fileset='dataset2', spec='file2.xml', owner = 'metson')
print

fs = wmbs.subscriptionsForFileset(fileset='/Higgs/SimonsCoolData/RECO')
print 'all subscriptions on /Higgs/SimonsCoolData/RECO - should be a processing and a merge'
print fs
    
fs = wmbs.subscriptionsForFileset(fileset='/Higgs/SimonsCoolData/RECO123')
print 'all subscriptions on /Higgs/SimonsCoolData/RECO123 - should be none'
print fs
    
fs = wmbs.subscriptionsForFileset(fileset='/Higgs/SimonsCoolData/RECO', subtype='Merge')
print 'merge subscriptions on /Higgs/SimonsCoolData/RECO - should be one'
print fs  

fs = wmbs.listSubscriptionsOfType('Processing')
print 'processing subscriptions - should be 3'
print fs

print "files in subscription 1"
fs = wmbs.filesForSubscription(1)
print fs

print "files in subscription 2"
fs = wmbs.filesForSubscription(3)
print fs

print 'add a location'
fs = wmbs.addNewLocation('se.place.uk')
print fs

print 'add a list of locations'
fs = wmbs.addNewLocation(['se1.place.uk', 'se2.place.uk', 'se3.place.uk'])
print fs    

fs = wmbs.listAllLocations()
print 'list all locations'
print fs

print 'put file1 at 2 sites'        
fs = wmbs.putFileAtLocation('file1', 'se.place.uk')
fs = wmbs.putFileAtLocation('file1', 'se3.place.uk')
print 'find sites with file1 - should be 2'
fs = wmbs.locateFile('file1')
print fs
               
print 'adding a new file to a location'
fs = wmbs.addNewFileToLocation(file='file2',fileset='/Higgs/SimonsCoolData/RECO', sename='se.place.uk')

fs = wmbs.listFilesAtLocation('se.place.uk')
print fs

print "list available files for subscription 3"
fs = wmbs.listAvailableFiles(3)
for file in fs:
    print file
        
acqsize = 50
for i in range(5):
    print "acquire some files for subscription 3"
    fs = wmbs.listAvailableFiles(3)
    filelist = fs
    #filelist = [ file[0] for file in fs]
    print 'number of available files: %s' % len( filelist )
    print fs
    print "acquiring %s " % filelist[:acqsize]
    filelist = [ x[0] for x in filelist[:acqsize] ]
    fs = wmbs.acquireNewFiles(3, filelist[:acqsize])
    fs = wmbs.listAcquiredFiles(3)
    print 'number of acquired files: %s' % len( fs )
    if i % 2 == 0 :
        fs = wmbs.failFiles(3, filelist[:acqsize])
    else:
        fs = wmbs.completeFiles(3, filelist[:acqsize])
    fs = wmbs.listCompletedFiles(3)
    print 'number of completed files: %s' % len( fs )
    fs = wmbs.listFailedFiles(3)
    print 'number of failed files: %s' % len( fs )


fs = wmbs.newFilesSinceDate('dataset1', 1208137347)
print len( fs )
    
fs = wmbs.filesInDateRange('dataset1', 1208137347, 1208147347)
print len( fs )
    
print 'test ended'






