import unittest
import nose
from WMCore.Database.DBFactory import DBFactory
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit
import WMQuality.TestInit

class TestInitTest(unittest.TestCase):
    def setUp(self):
        WMQuality.TestInit.trashDatabases = True
        if not WMQuality.TestInit.trashDatabases:
            raise nose.SkipTest, "This test makes no sense if you're not going to delete the database" 
        "make a logger instance and create tables"
        self.temptestInit = TestInit(__file__)
        self.temptestInit.setLogging()
        self.temptestInit.setDatabaseConnection()
        self.temptestInit.setSchema()
        
        
    def testDeletion(self):
        if not WMQuality.TestInit.trashDatabases:
            raise nose.SkipTest, "This test makes no sense if you're not going to delete the database" 
        
        deleteThings= TestInit("DELETION TEST")
        deleteThings.setLogging()
        deleteThings.setDatabaseConnection()
        
        self.assertEqual( 0, self.temptestInit.getDBInterface() )

    def tearDown(self):
        """
        Deletion of the databases 
        """
        self.temptestInit.clearDatabase()


if __name__ == "__main__":
    unittest.main()