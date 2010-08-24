import logging
from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.WMBS.Actions.NewFileset import NewFilesetAction 
from WMCore.WMBS.Actions.ListFileset import ListFilesetAction
from WMCore.WMBS.Actions.CreateWMBS import CreateWMBSAction

"make a logger instance"
logging.basicConfig(level=logging.DEBUG,
            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
            datefmt='%m-%d %H:%M',
            filename='%s.log' % __file__,
            filemode='w')

logger = logging.getLogger('wmbs_sql')
logger.info('test')

database = 'sqlite:///filesettest.lite'
database = 'mysql://metson@localhost/wmbs'
dbfactory = DBFactory(logger, database)
print " made a WMBS instace? %s" % \
        CreateWMBSAction(logger).execute(dbinterface=dbfactory.connect())

print "add a fileset"
action = NewFilesetAction(logger)

print "action created" 

print "fileset added : %s" % action.execute(fileset='/Higgs/SimonsCoolData/RECO', 
               dbinterface=dbfactory.connect())

print "list fileset \n\t %s" % \
        ListFilesetAction(logger).execute(dbinterface=dbfactory.connect())
