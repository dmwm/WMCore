#!/usr/bin/env python

import commands, os, re, sys
from optparse import OptionParser
from ConfigParser import ConfigParser

def msg():
    print "Usage:"
    print "testrun <dbtype>"
    print "Valid dbtypes are:"
    print "mysql, sqlite"
    exit()

def main():
    
    if len(sys.argv) <= 1:
        print "Invalid number of arguments"
        msg()

    #Get DBType in lowercase letters
    dbtemp = sys.argv[1].lower()
    

    if dbtemp != 'mysql' and dbtemp != 'sqlite':
        print "Invalid DB Type - %s not found" % dbtemp
        msg()

    #Reading settings from configuration file
    cfg = ConfigParser()
    cfg.read('test.ini')
    dbuser = cfg.get(dbtemp, 'user')
    dbpass = cfg.get(dbtemp, 'pass')
    dbhost = cfg.get(dbtemp, 'host')
    dbinst = cfg.get(dbtemp, 'instance')

    #Setting environment variables for each DB type
    if dbtemp == 'mysql':
        os.environ['DIALECT']="MySQL"
        os.environ['DATABASE']="mysql://%s:%s@%s/%s" % (dbuser, dbpass,
                                                        dbhost, dbinst)
    elif dbtemp == "sqlite":
        os.environ['DIALECT']="SQLite"
        os.environ['DATABASE']="sqlite:///%s.lite" % dbinst
    else:
        print "Invalid DB Type - %s not found" % dbtemp
        msg()

    #Searching for the specific testcases to run
    match = []
    out = commands.getoutput('ls *.py')
    out = out.split('\n')

    for x in out:
        m = re.search(os.environ['DIALECT']+'DAO.*_t.py',x)
        if m != None:
            match.append(m.group())

    match.sort()

    #Running the testcases, generating error and output log files
    #
    #testrunoutput.log contains debug info and time values
    #testrunerror.log contains all error and failed tests data

    #Hacky way of getting the correct output for the test run

    #The reason is:
    #getoutput(), used normally, couldnt get the text in order,
    #like loading the testcase from commandline do.

    for x in match:
        print '%s Testcase:' % x        
        out = commands.getoutput('python %s 1>>testrunoutput.log 2>>testrunerror.log '% x)
        print commands.getoutput('tail -n 4 testrunerror.log')
        
        print out            

if __name__ == "__main__":
    main()   
