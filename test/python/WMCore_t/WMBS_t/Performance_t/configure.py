#!/usr/bin/env python
"""
A simple script to write an ini file for tests to use. Currently MySQL 
orientated
"""

from ConfigParser import ConfigParser
from optparse import OptionParser

def input():
    parser = OptionParser()
    parser.add_option("-d", "--dialect", dest="dbtype", default='mysql',
                      help="Create ini for database of TYPE", metavar="TYPE")
    parser.add_option("-i", "--instance", dest="inst", default='wmbs',
                      help="User INSTANCE", metavar="INSTANCE")
    #MySQL
    parser.add_option("--mysqluser", dest="mysqluser", metavar="MYSQLUSER",
                      help="Connect to MySQL database as USER", default='')
    parser.add_option("--mysqlhost", dest="mysqlhost", metavar="MYSQLHOST", 
                      help="MySQL Database is running on HOST", default='')
    parser.add_option("--mysqlpass", dest="mysqlpass", metavar="MYSQLPASS", 
                      help="Password to access MySQL database", default='')
    #SQLite
    parser.add_option("--sqliteuser", dest="sqliteuser", metavar="SQLITEUSER",
                      help="Connect to SQLite database as USER", default='')
    parser.add_option("--sqlitehost", dest="sqlitehost", metavar="SQLITEHOST", 
                      help="SQLite Database is running on HOST", default='')
    parser.add_option("--sqlitepass", dest="sqlitepass", metavar="SQLITEPASS", 
                      help="Password to access SQLite database", default='')
    #Oracle
    parser.add_option("--oracleuser", dest="oracleuser", metavar="ORACLEUSER",
                      help="Connect to Oracle database as USER", default='')
    parser.add_option("--oraclehost", dest="oraclehost", metavar="ORACLEHOST", 
                      help="Oracle Database is running on HOST", default='')
    parser.add_option("--oraclepass", dest="oraclepass", metavar="ORACLEPASS", 
                      help="Password to access Oracle database", default='')

    parser.add_option("-r", "--threshold", dest="threshold", metavar="THRESHOLD", 
                      help="Threshold time for a single DAO operation")
    parser.add_option("-e", "--totalthreshold", dest="total_threshold", metavar="TOTALTHRESHOLD", 
                      help="Threshold time for cumulative DAO operations")
    parser.add_option("-t", "--times", dest="times", metavar="TIMES", 
                      help="Number of times each test should be executed")
    parser.add_option("-v", "--verbose", dest="verbose", default=False,
                      help="Set query verbose mode", action="store_true")
 
    return parser.parse_args()

def config(options):
    config = ConfigParser ()

    config.add_section("output")
    config.add_section("settings")
    
    config.add_section("mysql")
    config.add_section("sqlite")
    config.add_section("oracle")

    if options.threshold == None:
        print "You must set the threshold in test.ini to a real value to run"
    if options.total_threshold == None:
        print "You must set the total_threshold in test.ini to a real value to run"
    if options.times == None:
        print "You must set the times in test.ini to a real value to run"

    #Verbose Mode ON/OFF
    config.set("output", "verbose", options.verbose)

    #TODO - Security must be enhanced for plain text passwords
    #Testcases specific settings
    config.set("settings", "threshold", options.threshold)
    config.set("settings", "total_threshold", options.total_threshold)
    config.set("settings", "times", options.times)

    #MySQL
    config.set("mysql", "user", options.mysqluser)
    config.set("mysql", "host", options.mysqlhost)
    config.set("mysql", "pass", options.mysqlpass)
    config.set("mysql", "instance", options.inst)

    #SQLite
    config.set("sqlite", "user", options.sqliteuser)
    config.set("sqlite", "host", options.sqlitehost)
    config.set("sqlite", "pass", options.sqlitepass)
    config.set("sqlite", "instance", options.inst)

    #Oracle
    config.set("oracle", "user", options.oracleuser)
    config.set("oracle", "host", options.oraclehost)
    config.set("oracle", "pass", options.oraclepass)
    config.set("oracle", "instance", options.inst)
    
    config.write(file ('test.ini', 'w'))

def main():
    options, args = input()
    config(options)
    
if __name__ == "__main__":
    main()   
