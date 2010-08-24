from WMCore.WMBS.Factory import SQLFactory
from WMCore.WMBS.File import File

class Fileset(object):
    def __init__(self, name, wmbs):
        self.name = name
        self.files = []
        self.newfiles = []
        self.wmbs = wmbs
    
    def exists(self):
        """
        Does a fileset exist with this name
        """
        result = -1
        for f in self.wmbs.filesetExists(self.name):
            for i in f.fetchall():
                result = i[0]
        if result > 0:
            return True
        else:
            return False
        
    def create(self):
        """
        Add the new fileset to WMBS
        """
        try:
            self.wmbs.insertFileset(self.name)
        except Exception, e:
            self.wmbs.logger.exception('Fileset %s exists' % self.name)
            raise e
        
    def populate(self):
        """
        Load up the files in the file set from the database
        """
        for f in self.wmbs.showFilesInFileset(self.name):
            for i in f.fetchall():
                id, lfn, size, events, run, lumi = i
                file = File(lfn, size, events, run, lumi)
                self.files.append(file)
                
    def addFile(self, file):
        """
        Add a file to the fileset
        """
        self.newfiles.append(file)
    
    def listFiles(self):
        list = []
        list.extend(self.files)
        list.extend(self.newfiles)
        return list
    
    def listNewFiles(self):         
        return self.newfiles
    
    def commit(self):
        """
        Commit changes to the fileset
        """ 
        comfiles = []
        for f in self.newfiles:
            comfiles.append(f.getInfo())
        self.wmbs.logger.debug ( "commiting : %s" % comfiles )    
        self.wmbs.insertFilesForFileset(files=comfiles, fileset=self.name)
        self.files.extend(self.newfiles)
        self.newfiles = []
    
    def createSubscription(self, workflow=None, type='processing'):
        """
        Create a subscription for the fileset using the given workflow 
        """
        self.wmbs.newSubscription(self.name, workflow.spec, 
                                  workflow.owner, type)
        
    def subscriptions(self, type="processing"):
        type = type.lower()
        #TODO: types should come from DB
        if type in ("merge", "processing"):
            subscriptions = self.wmbs.subscriptionsForFileset(self.name, type)
            for i in subscriptions:
                print i.fetchall()
        else:
            self.wmbs.logger.exception('%s is an unknown subscription type' % type)
            raise TypeError
        