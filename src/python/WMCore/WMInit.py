#!/usr/bin/env python
"""
_WMInit

Init class that can be used by external projects
that only use part of the libraries
"""
from __future__ import print_function

import logging
import os
import os.path
import sys
import threading
import traceback
import warnings
import wmcoredb

from WMCore.Configuration import loadConfigurationFile
from WMCore.DAOFactory import DAOFactory
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMBase import getWMBASE
from WMCore.WMException import WMException


class WMInitException(WMException):
    """
    WMInitException

    You should never, ever see one of these.
    I'm not optimistic that this will be the case.
    """


def connectToDB():
    """
    _connectToDB_

    Connect to the database specified in the WMAgent config.
    """
    if "WMAGENT_CONFIG" not in os.environ:
        print("Please set WMAGENT_CONFIG to point at your WMAgent configuration.")
        sys.exit(1)

    if not os.path.exists(os.environ["WMAGENT_CONFIG"]):
        print("Can't find config: %s" % os.environ["WMAGENT_CONFIG"])
        sys.exit(1)

    wmAgentConfig = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])

    if not hasattr(wmAgentConfig, "CoreDatabase"):
        print("Your config is missing the CoreDatabase section.")
        sys.exit(1)

    socketLoc = getattr(wmAgentConfig.CoreDatabase, "socket", None)
    connectUrl = getattr(wmAgentConfig.CoreDatabase, "connectUrl", None)
    (dialect, _) = connectUrl.split(":", 1)

    myWMInit = WMInit()
    myWMInit.setDatabaseConnection(dbConfig=connectUrl, dialect=dialect,
                                   socketLoc=socketLoc)

    return


class WMInit(object):
    def __init__(self):
        return

    def getWMBASE(self):
        """ for those that don't want to use the static version"""
        return getWMBASE()

    def setLogging(self, logFile=None, logName=None, logLevel=logging.INFO, logExists=True):
        """
        Sets logging parameters, depending on the settings,
        this method will create a logging file.
        """
        # use logName as name for file is no log file is given
        if not logExists:
            logging.basicConfig(level=logLevel, \
                                format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', \
                                datefmt='%m-%d %H:%M', \
                                filename='%s.log' % logFile, \
                                filemode='w')
            logging.debug("Log file ready")

        myThread = threading.currentThread()
        if logName != None:
            myThread.logger = logging.getLogger(logName)
        else:
            myThread.logger = logging.getLogger()

    def setDatabaseConnection(self, dbConfig, dialect, socketLoc=None):
        """
        Sets the default connection parameters, without having to worry
        much on what attributes need to be set. This is esepcially
        advantagous for developers of third party projects that want
        to use only parts of the WMCore lib.

        The class differentiates between different formats used by external
        projects. External project formats that are supported can activated
        it by setting the flavor flag.
        """
        myThread = threading.currentThread()
        if getattr(myThread, "dialect", None) != None:
            # Database is already initialized, we'll create a new
            # transaction and move on.
            if hasattr(myThread, "transaction"):
                if myThread.transaction != None:
                    myThread.transaction.commit()

            myThread.transaction = Transaction(myThread.dbi)
            return

        options = {}
        if dialect.lower() in ['mysql', 'mariadb']:
            dialect = 'mariadb'  # Both MySQL and MariaDB use the mariadb directory
            if socketLoc != None:
                options['unix_socket'] = socketLoc
        elif dialect.lower() == 'oracle':
            dialect = 'oracle'  # Keep lowercase for consistency
        elif dialect.lower() == 'http':
            dialect = 'CouchDB'
        else:
            msg = "Unsupported dialect %s !" % dialect
            logging.error(msg)
            raise WMInitException(msg)

        myThread.dialect = dialect
        myThread.logger = logging
        myThread.dbFactory = DBFactory(logging, dbConfig, options)
        myThread.dbi = myThread.dbFactory.connect()

        # The transaction object will begin a transaction as soon as it is
        # initialized.  I'd rather have the user handle that, so we'll commit
        # it here.
        myThread.transaction = Transaction(myThread.dbi)
        myThread.transaction.commit()

        return

    def setSchema(self, modules=None, params=None):
        """
        Creates the schema in the database based on the modules
        input.

        This method needs to have been preceded by the
        setDatabaseConnection.

        @deprecated: Use setSchemaFromModules instead
        """
        warnings.warn("setSchema is deprecated. Use setSchemaFromModules instead.", DeprecationWarning)

        # create a map of old to new SQL module names
        moduleMap = {
            'WMCore.WMBS': 'wmbs',
            'WMCore.ResourceControl': 'resourcecontrol',
            'WMCore.BossAir': 'bossair',
            'WMCore.Agent.Database': 'agent',
            'WMComponent.DBS3Buffer': 'dbs3buffer',
            'T0.WMBS': 'tier0',
            'WMQuality.TestDB': 'testdb'
        }

        # convert old module names to new format
        if modules:
            modules = [moduleMap.get(module, module) for module in modules]

        self.setSchemaFromModules(modules)

    def setSchemaFromModules(self, sqlModules):
        """
        Initialize database schema for one or more SQL packages.
        It finds out which dialect is being used and then looks for the
        appropriate SQL files in the sql directory.

        :param sqlModules: List of SQL database modules to be initialized.
            Current supported values are (default is 'wmbs'):
            - 'wmbs'
            - 'resourcecontrol'
            - 'bossair'
            - 'agent'
            - 'dbs3buffer'
            - 'tier0'
        """
        myThread = threading.currentThread()
        if not hasattr(myThread, 'dbi'):
            raise WMInitException("Database connection not initialized. Call setDatabaseConnection first.")

        # Get the database dialect
        dialect = myThread.dialect.lower()
        if dialect not in ['mariadb', 'oracle']:
            raise WMInitException(f"Unsupported database dialect: {dialect}")

        # Get the base directory (WMCore root)
        if os.environ.get('WMCORE_ROOT'):
            baseDir = os.environ['WMCORE_ROOT']
            logging.info("SQL base directory based on WMCORE_ROOT: %s", baseDir)
        else:
            baseDir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
            logging.info("SQL base directory based on WMInit-relative path: %s", baseDir)

        # Define the SQL files needed for each module and their dependencies
        # The order matters - modules that depend on others should come later
        moduleSQLFiles = {
            'wmbs': {
                'files': ['create_wmbs_tables.sql', 'create_wmbs_indexes.sql', 'initial_wmbs_data.sql'],
                'dependencies': []
            },
            'resourcecontrol': {
                'files': ['create_resourcecontrol.sql'],
                'dependencies': ['wmbs']
            },
            'bossair': {
                'files': ['create_bossair.sql'],
                'dependencies': ['wmbs']
            },
            'agent': {
                'files': ['create_agent.sql'],
                'dependencies': []
            },
            'dbs3buffer': {
                'files': ['create_dbs3buffer.sql'],
                'dependencies': ['wmbs']
            },
            'tier0': {
                'files': ['create_tier0_tables.sql', 'create_tier0_indexes.sql', 'create_tier0_functions.sql', 'initial_tier0_data.sql'],
                'dependencies': ['wmbs', 'dbs3buffer']
            },
            'testdb': {
                'files': ['create_testdb.sql'],
                'dependencies': []
            },
        }

        # Validate all requested modules exist
        for module in sqlModules:
            if module not in moduleSQLFiles:
                raise WMInitException(f"Unknown module: {module}")

        # Sort modules based on dependencies
        sorted_modules = []
        remaining_modules = set(sqlModules)

        while remaining_modules:
            # Find modules with no remaining dependencies
            ready_modules = [
                mod for mod in remaining_modules
                if all(dep in sorted_modules for dep in moduleSQLFiles[mod]['dependencies'])
            ]

            if not ready_modules:
                # Circular dependency detected
                raise WMInitException("Circular dependency detected in module dependencies")

            sorted_modules.extend(ready_modules)
            remaining_modules -= set(ready_modules)

        # Execute SQL files in dependency order
        for module in sorted_modules:
            logging.info("Executing SQL files for: %s", module)
            for sql_file in moduleSQLFiles[module]['files']:
                dialect_sql_file = wmcoredb.get_sql_file(module_name=module, file_name=sql_file, backend=dialect)

                # now execute each SQL statement
                for stmt in self._getSQLStatements(dialect_sql_file, dialect):
                    try:
                        myThread.dbi.processData(stmt)
                    except Exception as ex:
                        msg = f"Error executing SQL file {dialect_sql_file}. "
                        msg += f"Statement: {stmt}"
                        msg += str(ex)
                        raise WMInitException(msg)

        return

    def _getSQLStatements(self, sqlFile, dialect):
        """
        Return the SQL statements from the file.
        For MariaDB, it accepts the whole SQL file content in a single statement.
        For Oracle, it splits the SQL file content into statements using the slash (/) terminator
        when it appears as the first character in a line.
        """
        if not os.path.exists(sqlFile):
            raise WMInitException(f"SQL file not found: {sqlFile}")

        with open(sqlFile, 'r', encoding='utf-8') as f:
            sql = f.read()

        if dialect == 'mariadb':
            return [sql]
        elif dialect == 'oracle':
            statements = []
            current_statement = []

            for line in sql.split('\n'):
                line = line.strip()
                # Skip empty lines and comments
                if not line or line.startswith('--'):
                    continue

                # If we find a slash terminator at the start of a line
                if line == '/':
                    # Join all lines collected so far into a statement
                    stmt = '\n'.join(current_statement)
                    if stmt.strip():
                        statements.append(stmt)
                    current_statement = []
                else:
                    current_statement.append(line)

            # Add any remaining statement
            if current_statement:
                stmt = '\n'.join(current_statement)
                if stmt.strip():
                    statements.append(stmt)

            return statements
        else:
            raise WMInitException(f"Unsupported database dialect: {dialect}")

    def clearDatabase(self, modules=None):
        """
        Database deletion. Global, ignore modules.
        """
        myThread = threading.currentThread()
        if hasattr(myThread, 'transaction') and getattr(myThread.transaction, 'transaction', None):
            # Then we have an open transaction
            # We should try and close it first
            try:
                myThread.transaction.commit()
            except:
                try:
                    myThread.transaction.rollback()
                except:
                    pass

        # Setup the DAO
        daoFactory = DAOFactory(package="WMCore.Database",
                                logger=myThread.logger,
                                dbinterface=myThread.dbi)
        destroyDAO = daoFactory(classname="Destroy")

        # Actually run a transaction and delete the DB
        try:
            destroyDAO.execute()
        except Exception as ex:
            msg = "Critical error while attempting to delete entire DB!\n"
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
            raise WMInitException(msg)

        return

    def checkDatabaseContents(self):
        """
        _checkDatabaseContents_

        Check and see if anything is in the database.
        This should be called by methods about to build the schema to make sure
        that the DB itself is empty.
        """

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.Database",
                                logger=myThread.logger,
                                dbinterface=myThread.dbi)

        testDAO = daoFactory(classname="ListUserContent")

        result = testDAO.execute()
        myThread.dbi.engine.dispose()

        return result
