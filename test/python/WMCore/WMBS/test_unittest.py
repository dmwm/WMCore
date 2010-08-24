#!/usr/bin/env python
"""
Test case for wmbs
"""

from WMCore.WMBS.Factory import SQLFactory 
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Fileset import Subscription
from WMCore.WMBS.File import File
from WMCore.WMBS.Workflow import Workflow
from sqlalchemy import __version__ as sqlalchemy_version
from sqlalchemy import create_engine
import logging
import sqlalchemy.pool as pool
import unittest
from sqlalchemy.exceptions import IntegrityError

#database = "sqlite:///database.lite"
database = 'sqlite:///:memory:'
# mysql
#database = 'mysql://metson@localhost/wmbs'



logging.basicConfig(level=logging.DEBUG,
format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
datefmt='%m-%d %H:%M',
filename='%s.log' % __file__, filemode='w')
logger = logging.getLogger('wmbs_sql')

"make a connection to a database"
"for some weird reason my sql wants pool_size > 1"
"something to do with the number of sub queries?" 
engine = create_engine(database, convert_unicode=True, encoding='utf-8', pool_size=10, pool_recycle=30)
factory = SQLFactory(logger)
wmbs = factory.connect(engine)
wmbs.createWMBS()
fs1, dataset1, dataset2 = None, None, None

class WMBSTester(unittest.TestCase):
    """
    WMBS test case's
    """
    
    def setUp(self):
        self.wmbs = wmbs
        try:
            # variables used in multiple tests
            self.fs1 = Fileset('/Higgs/SimonsCoolData/RECO', self.wmbs).populate()
            self.dataset1 = Fileset('dataset1', self.wmbs).populate()
            self.dataset2 = Fileset('dataset2', self.wmbs).populate()
            self.workflow1 = Workflow('myworkflow1', 'testsystem', self.wmbs).create()
            self.workflow2 = Workflow('myworkflow2', 'testsystem', self.wmbs).create()
            self.workflow3 = Workflow('myworkflow3', 'testsystem', self.wmbs).create()
        except:
            pass
        
    def test0CreateDB(self):
        """
        setup db
        """
        
        "make a connection to a database"
        "for some weird reason my sql wants pool_size > 1"
        "something to do with the number of sub queries?" 
#        engine = create_engine(database, convert_unicode=True, encoding='utf-8', pool_size=10, pool_recycle=30)
#        factory = SQLFactory(logger)
#        self.wmbs = factory.connect(engine)
        
        
    def tearDown(self):
        """
        drop statements would go here
        """
        pass
    
    
    def setUpDb(self):
        """
        set up the db connection
        """
        pass

    def test100InsertFileset(self):
        """
        FileSet insertions
        """
        datasets = (Fileset('/Higgs/SimonsCoolData/RECO', self.wmbs),
                    Fileset('dataset1', self.wmbs, parents=['dataset2'], parents_open=False))
        for fs in datasets:
            self.assertFalse(fs.exists())
            fs.create()
            self.assertTrue(fs.exists())
            #self.assertRaises(IntegrityError, fs.create)
            # allow multiple invocations
            fs.create
        self.assertTrue(Fileset('dataset2', self.wmbs).exists())
        
        fs = self.wmbs.showAllFilesets()
        self.assertEqual(3, len(fs))
        for dataset in fs:
            self.assert_(dataset[1] in ('/Higgs/SimonsCoolData/RECO',
                                        'dataset1', 'dataset2'))
        
        fs = self.wmbs.listFileSets(only_open = True)
        self.assertEqual(2, len(fs))
        for fileset in fs:
            self.assert_(fileset['fileset'] in ('/Higgs/SimonsCoolData/RECO', 
                                                'dataset1'))
            self.assertEqual(True, fileset['is_open'])
        
        self.fs1 = Fileset('/Higgs/SimonsCoolData/RECO', self.wmbs).populate()
        self.dataset1 = Fileset('dataset1', self.wmbs).populate()
        self.dataset2 = Fileset('dataset2', self.wmbs).populate()
        
            
    def test110InsertFilesForFileset(self):
        """
        File insertions
        """
        
#        self.fs1 = Fileset('/Higgs/SimonsCoolData/RECO', self.wmbs).populate()
#        self.dataset1 = Fileset('dataset1', self.wmbs).populate()
#        self.dataset2 = Fileset('dataset2', self.wmbs).populate()
        filelist=[]
        # files is a list of dicts containing lfn, size, events, run and lumi
        for x in range(500):
            filelist.append(File('lfn%s' % x,
                         size = 5 * x,
                         events = 10 * x,
                         run = x,
                         lumi = x + 3)
                        )
        
        #fs = self.wmbs.showAllFilesets()
        #print "filesets = %s" % fs
        # test just lfn insertion
        #self.wmbs.insertFilesForFileset(files=filelist[0]['lfn'], fileset='/Higgs/SimonsCoolData/RECO')
        self.fs1.addFile(filelist[0])
        self.fs1.commit()
        #self.wmbs.insertFilesForFileset(files=filelist[1:], fileset='/Higgs/SimonsCoolData/RECO')
        for f in filelist[1:]:
            self.fs1.addFile(f)
        self.fs1.commit()
        #fs = self.wmbs.showFilesInFileset(fileset='/Higgs/SimonsCoolData/RECO')
        fs = Fileset('/Higgs/SimonsCoolData/RECO', self.wmbs).populate()
        print 'files in fileset /Higgs/SimonsCoolData/RECO'
        print fs
        self.assertEqual(len(filelist), len(fs.listFiles()))
        lfns = [ x.lfn for x in filelist ]
        for file in fs.listFiles():
            self.assert_(file.lfn in lfns)


#    def test110InsertFilesForFileset2(self):
#        """
#        file - with details - insertions
#        """
#        filelist=[]
#        # files is a list of tuples containing lfn, size, events, run and lumi
#        for x in range(500):
#            f = 'lfn%s' % x,  5 *x,  10* x, x, x+3
#            filelist.append(f)
#
#        self.wmbs.insertFilesForFileset(files=filelist, fileset='dataset1')
#        print "inserted 5 files:"
#        fs = self.wmbs.showFilesInFileset(fileset='dataset1')
#        for f in fs:
#            for i in f.fetchall():
#                print i
                
    def test120NewSubscription(self):
        """
        Create subscriptions
        """
        
#        workflow1 = Workflow('myworkflow1', 'testsystem', self.wmbs).create()
#        workflow2 = Workflow('myworkflow2', 'testsystem', self.wmbs).create()
#        workflow3 = Workflow('myworkflow3', 'testsystem', self.wmbs).create()
        sub1 = self.fs1.createSubscription(self.workflow1)
        sub2 = Subscription(self.fs1, self.workflow2, type='Merge',
                            wmbs=self.wmbs).create()
        sub3 = Subscription(self.dataset1, self.workflow3, type='Processing',
                            parentage=1, wmbs=self.wmbs).create()

        fs = self.fs1.subscriptions()
        #print 'all subscriptions on /Higgs/SimonsCoolData/RECO - should be a processing and a merge'
        self.assertEqual(len(fs), 2)
        
        fs = self.wmbs.subscriptionsForFileset(fileset='/Higgs/SimonsCoolData/RECO123')
        self.assertEqual(0, len(fs))
        
        fs = self.fs1.subscriptions('Merge')
        self.assertEqual(1, len(fs))

        fs = self.wmbs.listSubscriptionsOfType('Processing')
        self.assertEqual(len(fs), 2)


    def test130FilesForSubscription(self):
        """
        Get files for a subscription
        """
        sub = Subscription(self.fs1, self.workflow1, type='Processing', wmbs=self.wmbs).load()
        fs = sub.availableFiles()
        self.assertEqual(len(fs), 500)


    def test140AddNewLocation(self):
        """
        Add a new location
        """
        locations = ['se.place.uk', 'se1.place.uk', 'se2.place.uk', 'se3.place.uk']
        fs = self.wmbs.addNewLocation(locations[0])
        fs = self.wmbs.addNewLocation(locations[1:])
        places = self.wmbs.listAllLocations()
        self.assertEqual(len(places), len(locations))
        for place in places:
            self.assert_(place[1] in locations)
  
    
    def test150PutFileAtLocation(self):
        """
        put a known file at a site
        """
        locations = [ 'se.place.uk', 'se3.place.uk' ]
        fs = self.wmbs.putFileAtLocation('lfn1', locations[0])
        fs = self.wmbs.putFileAtLocation('lfn1', locations[1])
        file = File('lfn1', wmbs=self.wmbs).load()
        fs = self.wmbs.locateFile('lfn1')
        self.assertEqual(len(fs), len(locations))
        self.assertEqual(len(file.locations), len(locations))
        self.assertEqual(file.locations, locations)
           
           
    def test160AddNewFileToLocation(self):
        """
        Add a new file to a location
        """
        #fs = self.wmbs.addNewFileToLocation(file='file2', \
        #        fileset='/Higgs/SimonsCoolData/RECO', sename='se2.place.uk')
        self.fs1.addFile(File(lfn='file2',
                              locations=['se2.place.uk'], wmbs=self.wmbs))
        self.fs1.commit()
        fs = self.wmbs.listFilesAtLocation('se2.place.uk')
        self.assertEqual('file2', fs[0][1])
        fs = self.wmbs.locateFile('file2')
        self.assertEqual('se2.place.uk', fs[0][0])
                

    def test170AcquireFilesForSubscription(self):
        """
        Acquire files for a subscription
        """
        #get total files in subscription
        #num_files = len(self.wmbs.filesForSubscription(1))
        sub = Subscription(self.fs1, self.workflow1, type='Processing', wmbs=self.wmbs).load()
        num_files = len(sub.availableFiles())
        print "total files %s " % num_files
        acqsize = 50
        acquired_files = []
        completed_files = []
        failed_files = []
        for i in range(5):
            fs = sub.availableFiles()
            #files_to_acquire = [ x['id'] for x in fs[:acqsize] ]
            files_to_acquire = fs[:acqsize]
            acquired_files.extend(files_to_acquire)
            sub.acquireFiles(files_to_acquire)
            #fs = self.wmbs.acquireNewFiles(1, files_to_acquire)
            fs = sub.acquiredFiles()
            # only currently acquired files listed - not failed or complete
            self.assertEqual(len(files_to_acquire), len(fs))
            for file in fs:
                self.assert_(file in files_to_acquire)
            # set files to either completed or failed
            if i % 2 == 0 :
                #fs = self.wmbs.failFiles(1, files_to_acquire)
                sub.failFiles(files_to_acquire)
                failed_files.extend(files_to_acquire)
            else:
                #fs = self.wmbs.completeFiles(1, files_to_acquire)
                sub.completeFiles(files_to_acquire)
                completed_files.extend(files_to_acquire)
            
        # reached end - do final count
        fs = sub.availableFiles()
        self.assertEqual(len(fs), 251)
        self.assertEqual(len(acquired_files), 250)
        fs = sub.completedFiles()
        self.assertEqual(len(fs), len(completed_files))
        for file in fs:
            self.assert_(file in completed_files)
        fs = sub.failedFiles()
        self.assertEqual(len(fs), len(failed_files))
        for file in fs:
            self.assert_(file in failed_files)
        

#    def test190AddNewFileToNewLocation(self):
#        """
#        Add a new file to a new location
#        """
#        sub = Subscription(self.fs1, self.workflow1, type='Processing', wmbs=self.wmbs).load()
#        self.fs1
#        fs = self.wmbs.addNewFileToNewLocation('lfnNotSeenBefore', 
#                                               '/Higgs/SimonsCoolData/RECO',
#                                               'se-not-seen-before.example.com')
#        fs = self.wmbs.showFilesInFileset(fileset='/Higgs/SimonsCoolData/RECO')
#        self.assert_('lfnNotSeenBefore' in [ x[1] for x in fs ] )
#        fs = self.wmbs.locateFile('lfnNotSeenBefore')
#        self.assertEqual([('se-not-seen-before.example.com',)], fs)
        
        
    def test200AddSubscriptionWithParentage(self):
        """
        Add a subscription which requires a parentage level,
        ensure all info is passed down i.e. file parents to
        desired limit
        """
        parentDataset = Fileset('parentDataset', wmbs=self.wmbs).create()
        childDataset = Fileset('childDataset', parents=[parentDataset],
                                                 wmbs=self.wmbs).create()
        workflow = Workflow('WorkflowWithParentage', 'testsystem', self.wmbs).create()
        sub = Subscription(childDataset, workflow, type='Processing',
                           parentage=1, wmbs=self.wmbs).create()
        # should do nothing
        sub = Subscription(childDataset, workflow, type='Processing',
                           parentage=1, wmbs=self.wmbs).create()
        parentFile = File('parentFile1', locations=['se2.place.uk'])
        childFile = File('childFile1', parents=[parentFile.lfn], locations=['se2.place.uk'])
        parentDataset.addFile(parentFile)
        parentDataset.commit()
        childDataset.addFile(childFile)
        childDataset.commit()
        
        #try get files for sub - should have location and parentage
        fs = sub.availableFiles()
        self.assertEqual(1, len(fs))
        self.assertEqual([childFile], fs)
        # make sure parentage is listed
        self.assertEqual([parentFile], fs[0].getInfo()[7])
          
    
        
    def test50ErrorHandling(self):
        """
        Ensure error handling works as expected
        i.e. multiple calls may be made within a transaction
        and descarded if required, also test connection cna be closed
        and restarted without losing sql statements
        """
        pass

#    def testNewFilesSince(self):
#        """
#        Get files inserted after a given time
#        """
#        fs = self.wmbs.newFilesSinceDate('dataset1', 1208137347)
#        for f in fs:
#            print len( f.fetchall() )
#
#
#    def testNewFilesInDateRange(self):
#        """
#        Get files inserted in date range
#        """
#        fs = self.wmbs.filesInDateRange('dataset1', 1208137347, 1208147347)
#        for f in fs:
#            print len( f.fetchall() )

        
if __name__ == '__main__':
    #suite = unittest.makeSuite(WMBSTester,'test')
    #runner = unittest.TextTestRunner()
    #runner.run(WMBSTester)
    unittest.main()



