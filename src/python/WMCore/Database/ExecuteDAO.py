#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script to create a dedicated DAOFactory and execute a single DAO.

This script is intended to be used by developers and experts only.
The execution environment should be under any of the WMAgents we have. And the full set
of agent management and initialisation scripts need to be sourced in advance i.e.:

Usage:

      For production:
           source  /data/admin/wmagent/env.sh
           source  /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh
           python3 ExecuteDAO.py [--options] -- [SQL Query Arguments]

      For tier0:
           source  /data/tier0/admin/env.sh
           source  /data/tier0/srv/wmagent/current/apps/t0/etc/profile.d/init.sh
           python3 ExecuteDAO.py [--options] -- [SQL Query Arguments]

"""

import sys
import os
import re

import threading
import logging
import argparse
import pickle
from pprint import pformat

from WMCore.DAOFactory import DAOFactory
from WMCore.WMInit import WMInit
from WMCore.Agent.Configuration import Configuration, loadConfigurationFile
from Utils.FileTools import loadEnvFile

def parseArgs():
    """
    Generic Argument Parser function
    """
    parser = argparse.ArgumentParser(
        prog='ExecuteDAO',
        formatter_class=argparse.RawTextHelpFormatter,
        description=__doc__)

    parser.add_argument('-c', '--config', required=False,
                        default=os.environ.get("WMA_CONFIG_FILE", None),
                        help="""\
                        The WMAgent config file to be used for the this execution. Default is taken from
                        the current's shell environment variable $WMA_CONFIG_FILE
                        """)
    parser.add_argument('-e', '--envFile', required=False,
                        default=os.environ.get("WMA_ENV_FILE", None),
                        help="""
                        The WMAgent environment file to be used for the this execution. Default is taken from
                        the current's shell environment variable $WMA_ENV_FILE
                        """)
    parser.add_argument('-p', '--package', required=True,
                        help="""\
                        The package from which the DAO factory to be created for this execution, e.g. WMCore.WMBS or WMComponent.DBS3Buffer""")
    parser.add_argument('-m', '--module', required=True,
                        help="""\
                        The DAO Module to be executed, e.g. Workflow.GetDeletableWorkflows or CountUndeletedBlocksByWorkflow""")
    parser.add_argument('-d', '--debug', action='store_true', default=False,
                        help="""\
                        Set logging to debug mode.""")
    parser.add_argument('--dryRun', action='store_true', default=False,
                        help="""\
                        Simulation mode only""")
    parser.add_argument('-s', '--sqlKwArgs', default={},
                        help="""\
                        Named paramaters to be forwarded to the DAO execute method and used as SQL arguments in the query.
                        Should be formatted as a dictionary e.g.:
                        -s "{'workflowName': name, injected: True}"
                        """)
    parser.add_argument('sqlArgs', nargs=argparse.REMAINDER, default=(),
                        help="""\
                        -- Positional parameters to be forwarded to the DAO execute method and used as SQL arguments in the query.""")
    parser.add_argument('-f', '--pklFile', default=None,
                        help="""\
                        An extra *.pkl file containing any additional python objects needed for the given dao
                        e.g. WMCore.WMBS.Files.AddRunLumi.
                        The object is always loaded under the name `pklFile`. One can access the contents of the so loaded pkl file
                        during the dao execution trough the -s arguent e.g.:
                        ExecuteDAO.py -p  WMCore.WMBS -m Files.AddRunLumi -c $WMA_CONFIG_FILE -f runLumiBinds_2035-4016.pkl -s "{'file': pklFile['data']}
                        """)
    currArgs = parser.parse_args()

    return currArgs


def loggerSetup(logLevel=logging.INFO):
    """
    Return a logger which writes everything to stdout.
    """
    currLogger = logging.getLogger()
    outHandler = logging.StreamHandler(sys.stdout)
    outHandler.setFormatter(logging.Formatter("%(asctime)s:%(levelname)s:%(module)s: %(message)s"))
    outHandler.setLevel(logLevel)
    if currLogger.handlers:
        currLogger.handlers.clear()
    currLogger.addHandler(outHandler)
    currLogger.setLevel(logLevel)
    return currLogger


def getBackendFromDbURL(dburl):
    """
    Auxiliary function for determining sql dialect from a connection Url
    :param dbUrl: The connection Url to be parsed.
    :return: A string pointing to the correct dialect.
    """
    dialectPart = dburl.split(":")[0]
    if dialectPart == 'mysql':
        return 'MySQL'
    elif dialectPart == 'oracle':
        return 'Oracle'
    else:
        raise RuntimeError("Unrecognized dialect %s" % dialectPart)


class ExecuteDAO():
    """
    A generic class to create the DAO Factory and execute the DAO module.
    """
    def __init__(self, connectUrl=None, socket=None, configFile=None,
                 package=None, daoModule=None, logger=None):
        """
        __init__
        The ExecuteDAO constructor method.
        :param package: The Package from which the DAO factory to be initialised.
        :param connectUrl: Database connection URL (overwrites the connectUrl param from configFile if both present)
        :param socket: Database connection URL (overwrites the socket param from configFile if both present)
        :param module: The DAO module to be executed.
        """
        # Get the current thread:
        myThread = threading.currentThread()

        # Create default WMCore Init thread and configs:
        self.init = WMInit()

        if logger is None:
            self.init.setLogging()
            self.logger = logging.getLogger()
        else:
            self.logger = logger

        if configFile is not None:
            self.logger.info("Loading configFile: %s", configFile)
            config = loadConfigurationFile(configFile)
        else:
            config = Configuration()

        # Overwrite database config parameters from configFile if present as init arguments:
        config.section_("CoreDatabase")
        if connectUrl is not None:
            config.CoreDatabase.connectUrl = connectUrl

        if socket is not None:
            config.CoreDatabase.socket = socket

        # If still no proper database connection parameters provided,
        # last resort - try fetching them from the environment:
        if getattr(config.CoreDatabase, "connectUrl", None) is None and os.getenv('DATABASE', None):
            config.CoreDatabase.connectUrl = os.getenv('DATABASE')
            config.CoreDatabase.dialect = getBackendFromDbURL(os.getenv("DATABASE"))
            config.CoreDatabase.socket = os.getenv("DBSOCK", None)

        # always try to determine the dialect from the URL
        if getattr(config.CoreDatabase, "connectUrl", None):
            config.CoreDatabase.dialect = getBackendFromDbURL(config.CoreDatabase.connectUrl)

        # finally if no socket is provided, set it to None and let WMInit to create it.
        config.CoreDatabase.socket = getattr(config.CoreDatabase, "socket", None)

        # check if all database connection parameters are provided:
        if not all([getattr(config.CoreDatabase, "connectUrl", None),
                    getattr(config.CoreDatabase, "dialect", None)]):
            raise RuntimeError("You must set proper DATABASE parameters: connectUrl, dialect, socket!")

        # Connecting to database:
        self.init.setDatabaseConnection(config.CoreDatabase.connectUrl,
                                        config.CoreDatabase.dialect,
                                        socketLoc=config.CoreDatabase.socket)

        self.dbi = myThread.dbi
        self.package = package
        self.daoModule = daoModule

        # Avoid any name that starts with _
        self.sqlRegEx = re.compile("^(?!_.*)", re.IGNORECASE)

        self.daoFactory = DAOFactory(package=package,
                                     logger=self.logger,
                                     dbinterface=self.dbi)
        self.logger.info("DAO Factory initialised from package: %s", self.package)

        self.dao = self.daoFactory(classname=daoModule)
        self.logger.info("DAO Module initialised as: %s", self.daoModule)

    def __call__(self, *sqlArgs, dryRun=False, daoHelp=False, **sqlKwArgs):
        """
        __call__
        The ExecuteDAO call method. This is the method to forward all provided
        arguments to the execute method of the DAO and return the result from the query
        :param dryRun:      Bool flag to indicate dryrun method
        :param *sqlArgs:    All positional arguments to be forwarded to the DAO's execute method.
        :param **sqlKwArgs: All named arguments to be forwarded to the DAO's execute method.
        :return:            The result from the DAO. Depending on the DAO itself it Could be one of:
                            * A dictionary
                            * A list
                            * A generator
        """
        if dryRun:
            results = []
            if daoHelp:
                self.getHelp()
            self.logger.info("DAO SQL queries to be executed:")
            sqlQueries = self.getSqlQuery()
            for sqlName, sqlStr in sqlQueries.items():
                msg = "\n----------------------------------------------------------------------\n"
                msg += "%s: %s"
                msg += "\n----------------------------------------------------------------------\n"
                self.logger.info(msg, sqlName, sqlStr)
            self.logger.info("DAO SQL arguments provided:\n%s, %s", pformat(sqlArgs), pformat(sqlKwArgs))
        else:
            results = self.dao.execute(*sqlArgs, **sqlKwArgs)
            # self.logger.info("DAO Results:\n%s", pformat(results if isinstance(results, dict) else list(results)))
            if isinstance(results, dict):
                self.logger.info("DAO Results:\n%s", pformat(results))
            elif isinstance(results, bool):
                self.logger.info("DAO Results:\n%s", results)
            else:
                self.logger.info("DAO Results:\n%s", list(results))
        return results

    def getSqlQuery(self):
        """
        A simple method to inspect all DAO object attributes and accumulate any sql query it finds in a simple list
        :return: A list of all sql queries it finds in the object.
        """
        # NOTE: Use this method with caution because it may also return an object which is not an sql query.
        #       This may happen if there is a DAO attribute which is of type string and satisfies self.sqlRegEx
        sqlQueries = {}
        for attr in dir(self.dao):
            if self.sqlRegEx.match(attr) and isinstance(getattr(self.dao, attr), (str, bytes)):
                sqlQueries[attr] = getattr(self.dao, attr)
        return sqlQueries

    def getHelp(self):
        """
        A simple method to generate interactive DAO help message from the DAO source.
        """
        help(self.dao)


def strToDict(dString, logger=None):
    """
    A simple Function to parse a string and produce a dictionary out of it.
    :param dString: The dictionary string to be parsed. Possible formats are either a string
                    of multiple space separated named values of the form 'name=value':
                    or a srting fully defining the dictionary itself.
    :return:        The constructed dictionary
    """
    if not logger:
        logger = logging.getLogger()
    # result = ast.literal_eval(dString)
    result = eval(dString)
    if not isinstance(result, dict):
        logger.error("The Query named arguments need to be provided as a dictionary. WRONG option: %s", pformat(dString))
        raise TypeError(pformat(dString))
    return result


if __name__ == '__main__':
    args = parseArgs()

    if args.debug:
        logger = loggerSetup(logging.DEBUG)
    else:
        logger = loggerSetup()

    # Create an instance of the *.pkl file provided with the dao call, if any.
    if args.pklFile:
        pklFilePath = os.path.normpath(args.pklFile)
        if not os.path.exists(pklFilePath):
            logger.error("Cannot find the pkl file: %s. Exit!", pklFilePath)
            sys.exit(1)
        with open(pklFilePath, 'rb') as fd:
            pklFile = pickle.load(fd)
            logger.info('PklFile: %s loaded as: `pklFile`. You can refer to its content through the -s argument.', pklFilePath)
    # logger.info(pformat(pklFile))

    # Remove leading double slash if present:
    if args.sqlArgs and args.sqlArgs[0] == '--':
        args.sqlArgs = args.sqlArgs[1:]

    # Convert the positional arguments to a tuple:
    if not isinstance(args.sqlArgs, tuple):
        args.sqlArgs = tuple(args.sqlArgs)

    # Parse named arguments to a proper dictionary:
    if not isinstance(args.sqlKwArgs, dict):
        args.sqlKwArgs = strToDict(args.sqlKwArgs)

    # Trying to load WMA_ENV_FILE
    if not args.envFile or not os.path.exists(args.envFile):
        logger.warning("Missing WMAgent environment file! One may expect DAO misbehavior!")
    else:
        logger.info("Trying to source explicitely the WMAgent environment file: %s", args.envFile)
        try:
            loadEnvFile(args.envFile)
        except Exception as ex:
            logger.error("Failed to load wmaEnvFile: %s", args.envFile)
            raise

    if not args.config or not os.path.exists(args.config):
        logger.warning("Missing WMAgent config file! One may expect DAO failure")
    else:
        # resetting the configuration file in the env (if the default is overwritten through args)
        os.environ['WMAGENT_CONFIG'] = args.config
        os.environ['WMA_CONFIG_FILE'] = args.config

    daoObject = ExecuteDAO(package=args.package, daoModule=args.module, configFile=args.config)
    daoObject(*args.sqlArgs, dryRun=args.dryRun, daoHelp=True, **args.sqlKwArgs)
