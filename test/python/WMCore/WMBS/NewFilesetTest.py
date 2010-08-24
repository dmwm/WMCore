import logging
from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.WMBS.Actions.NewFileset import NewFilesetAction 
from WMCore.WMBS.Actions.ListFileset import ListFilesetAction
from WMCore.WMBS.Actions.LoadFileset import LoadFilesetAction
from WMCore.WMBS.Actions.CreateWMBS import CreateWMBSAction
from WMCore.WMBS.Actions.AddFile import AddFileAction
from WMCore.WMBS.Actions.AddFileToFileset import AddFileToFilesetAction
from WMCore.WMBS.Actions.SetFileLocation import SetFileLocationAction
from WMCore.WMBS.Actions.AddLocation import AddLocationAction

"make a logger instance"
logging.basicConfig(level=logging.DEBUG,
            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
            datefmt='%m-%d %H:%M',
            filename='%s.log' % __file__,
            filemode='w')

logger = logging.getLogger('wmbs_sql')
logger.info('test')

database = 'sqlite:///filesettest.lite'
#database = 'mysql://metson@localhost/wmbs'
dbfactory = DBFactory(logger, database)

theCreator = CreateWMBSAction(logger)
theCreator.printschema(dbinterface=dbfactory.connect(), table='wmbs_workflow1231')
theCreator.printschema(dbinterface=dbfactory.connect(), table='wmbs_workflow')

createworked = theCreator.execute(dbinterface=dbfactory.connect())
print " made a WMBS instace? %s" % createworked
        
if createworked:
    print "add a fileset"
    action = NewFilesetAction(logger)
    
    print "action created" 
    myfs = '/Higgs/SimonsCoolData/RECO'
    print "fileset added : %s" % action.execute(fileset=myfs, 
                   dbinterface=dbfactory.connect())
    
    print "list fileset \n\t %s" % \
            ListFilesetAction(logger).execute(dbinterface=dbfactory.connect())
            
    print "Add some files"
    
    file1 = '/store/user/metson/file1', 123, 234, 345, 456
    file2 = '/store/user/metson/file2', 123, 234, 345, 456
    filelist = [file1, file2]
    action = AddFileAction(logger).execute(filelist, dbinterface=dbfactory.connect())
    
    filelist = [file1[0], file2[0]]
    se = "lcgse01.phy.bris.ac.uk"
    action = AddLocationAction(logger)
    action.execute(se, dbinterface=dbfactory.connect())
    
    action = SetFileLocationAction(logger)
    action.execute(filelist, se, dbinterface=dbfactory.connect())
    
    action = AddFileToFilesetAction(logger)
    action.execute(filelist, myfs, dbinterface=dbfactory.connect())
    
    action = LoadFilesetAction(logger)
    print action.execute(name=myfs, 
                   dbinterface=dbfactory.connect())