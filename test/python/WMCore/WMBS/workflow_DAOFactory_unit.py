#!/usr/bin/env python
"""
_WorkflowExistsTestCase_

Unit tests for Workflow creation and exists, including checks to see that calls 
are database dialect neutral

"""

__revision__ = "$Id: workflow_DAOFactory_unit.py,v 1.2 2008/06/12 11:05:07 metson Exp $"
__version__ = "$Revision: 1.2 $"

import unittest, logging, os, commands

from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.Workflow import Workflow
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
        
        self.tearDown()
        
        self.dbf1 = DBFactory(self.mysqllogger, 'mysql://metson@localhost/wmbs')
        self.dbf2 = DBFactory(self.sqlitelogger, 'sqlite:///filesettest.lite')
        
        self.daofactory1 = DAOFactory(package='WMCore.WMBS', logger=self.mysqllogger, dbinterface=self.dbf1.connect())
        self.daofactory2 = DAOFactory(package='WMCore.WMBS', logger=self.sqlitelogger, dbinterface=self.dbf2.connect())
        
        theMySQLCreator = self.daofactory1(classname='CreateWMBS')
        createworked = theMySQLCreator.execute()
        
        if createworked:
            self.testlogger.debug("WMBS MySQL database created")
        else:
            self.testlogger.debug("WMBS MySQL database could not be created, already exists?")
            
        theSQLiteCreator = self.daofactory2(classname='CreateWMBS')
        createworked = theSQLiteCreator.execute()
        
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
        try:
            self.testlogger.debug(os.remove('filesettest.lite'))
        except OSError:
            #Don't care if the file doesn't exist
            pass
        self.testlogger.debug("WMBS SQLite database deleted")
        
class WorkflowDAOObjectTestCase(BaseWorkflowTestCase):
    def setUp(self):
        BaseWorkflowTestCase.setUp(self)
        
        i = 0
        self.action1 = []
        self.action2 = []
        self.action3 = []
        for daofactory in self.daofactory1, self.daofactory2:
            self.action1.append(daofactory(classname='Workflow.Exists'))
            self.action2.append(daofactory(classname='Workflow.New'))
            self.action3.append(daofactory(classname='Workflow.Delete'))        
                
    def testCreateExistsMySQL(self):
        assert self.action1[0].execute(spec='spec.xml', owner='Simon', name='wf001') == False, \
                               'workflow exists before it has been created'

        assert self.action2[0].execute(spec='spec.xml', owner='Simon', name='wf001') == True, \
                               'workflow cannot be created'

        assert self.action1[0].execute(spec='spec.xml', owner='Simon', name='wf001') == True, \
                               'workflow does not exist after it has been created'   
                               
        assert self.action3[0].execute(spec='spec.xml', owner='Simon', name='wf001') == True, \
                               'workflow cannot be deleted'
                               
        assert self.action1[0].execute(spec='spec.xml', owner='Simon', name='wf001') == False, \
                               'workflow exists after it has been deleted'
                                            
    def testCreateExistsSQLite(self):
        assert self.action1[1].execute(spec='spec.xml', owner='Simon', name='wf001') == False, \
                               'workflow exists before it has been created'

        assert self.action2[1].execute(spec='spec.xml', owner='Simon', name='wf001') == True, \
                               'workflow cannot be created'

        assert self.action1[1].execute(spec='spec.xml', owner='Simon', name='wf001') == True, \
                               'workflow does not exist after it has been created' 
                               
        assert self.action3[1].execute(spec='spec.xml', owner='Simon', name='wf001') == True, \
                               'workflow cannot be deleted'
                               
        assert self.action1[1].execute(spec='spec.xml', owner='Simon', name='wf001') == False, \
                               'workflow exists after it has been deleted'
        
        #TODO: Add the following back in when deletes have some checking                               
        #assert self.action3.execute(spec='spec.xml', owner='Simon', name='wf001',
        #                       dbinterface=self.dbf2.connect()) == False, \
        #                       'workflow can be deleted but does not exist'
                               
    def testNotExistsDialectNeutral(self):        
        mysql = self.action1[0].execute(spec='spec.xml', owner='Simon', name='wf001')
        sqlite = self.action1[1].execute(spec='spec.xml', owner='Simon', name='wf001')
        
        assert mysql == sqlite, 'dialect difference mysql: %s, sqlite: %s' % (mysql, sqlite)

    def testCreateDialectNeutral(self):
        mysql = self.action2[0].execute(spec='spec.xml', owner='Simon', name='wf001')
        sqlite = self.action2[1].execute(spec='spec.xml', owner='Simon', name='wf001')
        
        assert mysql == sqlite, 'dialect difference mysql: %s, sqlite: %s' % (mysql, sqlite)
    
    def testExistsDialectNeutral(self):
        mysql = self.action1[0].execute(spec='spec.xml', owner='Simon', name='wf001')
        sqlite = self.action1[1].execute(spec='spec.xml', owner='Simon', name='wf001')
        
        assert mysql == sqlite, 'dialect difference mysql: %s, sqlite: %s' % (mysql, sqlite)
    
    def testDeleteDialectNeutral(self):
        mysql = self.action3[0].execute(spec='spec.xml', owner='Simon', name='wf001')
        sqlite = self.action3[1].execute(spec='spec.xml', owner='Simon', name='wf001')
        assert mysql == sqlite, 'dialect difference mysql: %s, sqlite: %s' % (mysql, sqlite)
    
class WorkflowBusinessObjectTestCase(BaseWorkflowTestCase):
    
    
    def setUp(self):
        BaseWorkflowTestCase.setUp(self)
        
        self.workflow = Workflow(spec='/home/metson/workflow.xml', 
                                 owner='metson', 
                                 name='My Analysis', 
                                 logger=self.testlogger, 
                                 dbfactory=self.dbf1)

    def testExists(self):
        assert self.workflow.exists() == False, \
            'Workflow exists before creating it'
            
    def testCreate(self):
        self.workflow.create()
        assert self.workflow.exists() == True, \
            'Workflow does not exist after creating it'
            
    def testDelete(self):
        assert self.workflow.exists() == False, \
            'Workflow exists before creating it'
        self.workflow.create()
        assert self.workflow.exists() == True, \
            'Workflow does not exist after creating it'
        self.workflow.delete()
        
if __name__ == "__main__":
    unittest.main()