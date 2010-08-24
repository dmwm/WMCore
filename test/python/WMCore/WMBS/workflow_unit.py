#!/usr/bin/env python
"""
_WorkflowExistsTestCase_

Unit tests for Workflow creation and exists, including checks to see that calls 
are database dialect neutral

"""

__revision__ = "$Id: workflow_unit.py,v 1.3 2008/06/10 11:08:11 metson Exp $"
__version__ = "$Revision: 1.3 $"

import unittest, logging, os, commands
from pylint import lint
from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.WMBS.Actions.CreateWMBS import CreateWMBSAction
#pylint --rcfile=../../../../standards/.pylintrc  ../../../../src/python/WMCore/WMBS/Fileset.py

class BaseWorkflowTestCase(unittest.TestCase):
    def setUp(self):
        "make a logger instance"
        #level=logging.ERROR
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='%s.log' % __file__,
                    filemode='w')
        
        self.mysqllogger = logging.getLogger('wmbs_mysql')
        self.sqlitelogger = logging.getLogger('wmbs_sqlite')
        self.testlogger = logging.getLogger('unit_test')
        
        self.dbf1 = DBFactory(self.mysqllogger, 'mysql://metson@localhost/wmbs')
        self.dbf2 = DBFactory(self.sqlitelogger, 'sqlite:///filesettest.lite')
        
        theMySQLCreator = CreateWMBSAction(self.mysqllogger)
        createworked = theMySQLCreator.execute(dbinterface=self.dbf1.connect())
        if createworked:
            self.testlogger.debug("WMBS MySQL database created")
        else:
            self.testlogger.debug("WMBS MySQL database could not be created, already exists?")
            
        theSQLiteCreator = CreateWMBSAction(self.sqlitelogger)    
        createworked = theSQLiteCreator.execute(dbinterface=self.dbf2.connect())
        if createworked:
            self.testlogger.debug("WMBS SQLite database created")
        else:
            self.testlogger.debug("WMBS SQLite database could not be created, already exists?")
                       
    def tearDown(self):
        """
        Delete the databases
        """
        self.testlogger.debug(commands.getstatusoutput('echo yes | mysqladmin -u root drop wmbs'))
        self.testlogger.debug(commands.getstatusoutput('mysqladmin -u root create wmbs'))
        self.testlogger.debug("WMBS MySQL database deleted")
        self.testlogger.debug(os.remove('filesettest.lite'))
        self.testlogger.debug("WMBS SQLite database deleted")

class WorkflowExistsTestCase(BaseWorkflowTestCase):
    def setUp(self):
        BaseWorkflowTestCase.setUp(self)
        
        from WMCore.WMBS.Actions.Workflow.Exists import WorkflowExistsAction
        from WMCore.WMBS.Actions.Workflow.New import NewWorkflowAction
        from WMCore.WMBS.Actions.Workflow.Delete import DeleteWorkflowAction
        
        self.action1 = WorkflowExistsAction(self.testlogger)
        self.action2 = NewWorkflowAction(self.testlogger) 
        self.action3 = DeleteWorkflowAction(self.testlogger)        
                
    def testCreateExistsMySQL(self):
        assert self.action1.execute(spec='spec.xml', owner='Simon', name='wf001',
                               dbinterface=self.dbf1.connect()) == False, \
                               'workflow exists before it has been created'

        assert self.action2.execute(spec='spec.xml', owner='Simon', name='wf001',
                               dbinterface=self.dbf1.connect()) == True, \
                               'workflow cannot be created'

        assert self.action1.execute(spec='spec.xml', owner='Simon', name='wf001',
                               dbinterface=self.dbf1.connect()) == True, \
                               'workflow does not exist after it has been created'   
                               
        assert self.action3.execute(spec='spec.xml', owner='Simon', name='wf001',
                               dbinterface=self.dbf1.connect()) == True, \
                               'workflow cannot be deleted'
                               
        assert self.action1.execute(spec='spec.xml', owner='Simon', name='wf001',
                               dbinterface=self.dbf1.connect()) == False, \
                               'workflow exists after it has been deleted'
                                            
    def testCreateExistsSQLite(self):
        assert self.action1.execute(spec='spec.xml', owner='Simon', name='wf001',
                               dbinterface=self.dbf2.connect()) == False, \
                               'workflow exists before it has been created'

        assert self.action2.execute(spec='spec.xml', owner='Simon', name='wf001',
                               dbinterface=self.dbf2.connect()) == True, \
                               'workflow cannot be created'

        assert self.action1.execute(spec='spec.xml', owner='Simon', name='wf001',
                               dbinterface=self.dbf2.connect()) == True, \
                               'workflow does not exist after it has been created' 
                               
        assert self.action3.execute(spec='spec.xml', owner='Simon', name='wf001',
                               dbinterface=self.dbf2.connect()) == True, \
                               'workflow cannot be deleted'
                               
        assert self.action1.execute(spec='spec.xml', owner='Simon', name='wf001',
                               dbinterface=self.dbf2.connect()) == False, \
                               'workflow exists after it has been deleted'
        
        #TODO: Add the following back in when deletes have some checking                               
        #assert self.action3.execute(spec='spec.xml', owner='Simon', name='wf001',
        #                       dbinterface=self.dbf2.connect()) == False, \
        #                       'workflow can be deleted but does not exist'
                               
    def testNotExistsDialectNeutral(self):        
        mysql = self.action1.execute(spec='spec.xml', owner='Simon', name='wf001',
                               dbinterface=self.dbf1.connect())
        sqlite = self.action1.execute(spec='spec.xml', owner='Simon', name='wf001',
                               dbinterface=self.dbf2.connect())
        
        assert mysql == sqlite, 'dialect difference mysql: %s, sqlite: %s' % (mysql, sqlite)

    def testCreateDialectNeutral(self):
        mysql = self.action2.execute(spec='spec.xml', owner='Simon', name='wf001',
                               dbinterface=self.dbf1.connect())
        sqlite = self.action2.execute(spec='spec.xml', owner='Simon', name='wf001',
                               dbinterface=self.dbf2.connect())
        
        assert mysql == sqlite, 'dialect difference mysql: %s, sqlite: %s' % (mysql, sqlite)
    
    def testExistsDialectNeutral(self):
        mysql = self.action1.execute(spec='spec.xml', owner='Simon', name='wf001',
                               dbinterface=self.dbf1.connect())
        sqlite = self.action1.execute(spec='spec.xml', owner='Simon', name='wf001',
                               dbinterface=self.dbf2.connect())
        
        assert mysql == sqlite, 'dialect difference mysql: %s, sqlite: %s' % (mysql, sqlite)
    
    def testDeleteDialectNeutral(self):
        mysql = self.action3.execute(spec='spec.xml', owner='Simon', name='wf001',
                               dbinterface=self.dbf1.connect())
        sqlite = self.action3.execute(spec='spec.xml', owner='Simon', name='wf001',
                               dbinterface=self.dbf2.connect())
        assert mysql == sqlite, 'dialect difference mysql: %s, sqlite: %s' % (mysql, sqlite)
    

if __name__ == "__main__":
    unittest.main()