#!/bin/bash

#This is a script to run all the unittests

#This script goes through a directory and runs all python 
#scripts with the _t.py ending in the filename
#It writes output to a file outfile, and error to a 
#file errorfile, both of which are remade for 
#every running of the script.  
#At the end of the run, it parses the error files 
#looking for failures.

#This script can run SQLite, MySQL, and Oracle, but has 
#not been tested on any but SQLite.  The database dialects 
#are turned on and off by variables runSQLite, etc.

#Should add something here for location testing


#Set variables (1 for true, 0 for false)
runSQLite=1
runMySQL=1
runOracle=1

#Set output
outfile="testOut.out"
errorfile="testErr.err"

#Define locations
PySQLiteDir=$HOME"/pysqlite2"
WMCOREDir="../../../../"

SQLiteDialect="SQLite"
SQLiteDatabase="blank"

MySQLPass="blank"
MySQLUser="blank"
MySQLDatabase="mysql://"$MySQLUser":"$MySQLPass"@cmssrv18:3307/test_"$MySQLUser
MySQLDialect="MySQL"
MySQLDBSock="/var/lib/mysql/mysql.sock"

#OracleDialect="Oracle"
#OracleDatabase="oracle://"$USER":"$USER"_cms2008@cmscald:1521"
OracleDialect="Oracle"
OracleDatabase="blank"


if [[ $OracleDatabase == "blank" ]]
then
    echo "You need to set the OracleDatabase variable by hand"
    exit
fi
if [[ $MySQLUser == "blank" ]]
then
    echo "You need to set the MySQLDatabase variable by hand"
    exit
fi
if [[ $SQLiteDatabase == "blank" ]]
then
    echo "You need to set the SQLite variable by hand"
    exit
fi

#Environment setup
export PYTHONPATH=$PYTHONPATH:/home/sfoulkes/cx_Oracle-4.3.3/build/lib.linux-i686-2.4-10g

echo "Test out written to "$outfile
echo "Test error written to "$errorfile

date > $outfile
date > $errorfile

if [ -e `which python2.4` ]
then 
    echo "Using "`which python2.4`
else
    echo "ERROR: Could not find python2.4"
    echo "ABORT: Critical Error.  Aborting"
    exit
fi


if [[ -e $WMCOREBASE && -n $WMCOREBASE ]]
    then
    export PYTHONPATH=$PYTHONPATH:$WMCOREBASE/src/python
    export PYTHONPATH=$PYTHONPATH:$WMCOREBASE/test/python
else
    if [ -e $WMCOREDir ]
	then
	export WMCOREBASE=$WMCOREDir
	export PYTHONPATH=$PYTHONPATH:$WMCOREDir/src/python
	export PYTHONPATH=$PYTHONPATH:$WMCOREDir/test/python
    else
	echo "ERROR: Could not find WMCORE directory"
	echo "ABORT: Critical error.  Aborting"
	exit
    fi
fi

if [ -d $PySQLiteDir ]
then
    export PYTHONPATH=$PYTHONPATH:$PySQLiteDir
else
    echo "ERROR: Could not find PySQLite"
    echo "ERROR: Aborting SQLite"
    echo $PySQLiteDir
    $runSQLite=0
    exit
fi

#Including these as I do not currently know what is necessary for Oracle
export PATH=$PATH:`pwd`/bin:`pwd`/../../../phedex/PHEDEX/Toolkit/Request/:/home/anzar/DBS-ORACLE/oracle-10.2.0.1/bin:$PATH
export ORACLE_HOME=/home/cms_lumi_test/oracle/product/10.2.0.1.0/client_1
export SQLPATH=/home/anzar/DBS-ORACLE/oracle-10.2.0.1/bin

#Now actually start the tests


#Starting SQLite tests
if [ $runSQLite -eq 1 ]
    then
    echo "Running SQLite tests" >> $outfile
    echo "Running SQLite tests" >> $errorfile

    #Set up the database variables
    export DIALECT=$SQLiteDialect
    export DATABASE=$SQLiteDatabase

    #Actually run the testscripts one at a time
    for testscript in `ls *_t.py`
      do
      echo "Testing "$testscript >> $outfile 
      echo "Testing "$testscript >> $errorfile 
      python2.4 $testscript 1>> $outfile 2>> $errorfile
    done
fi


#Starting MySQL tests
if [ $runMySQL -eq 1 ]
    then
    echo "Running MySQL tests" >> $outfile
    echo "Running MySQL tests" >> $errorfile

    #Set up the database variables
    export DIALECT=$MySQLDialect
    export DATABASE=$MySQLDatabase
    export DBSOCK=$MySQLDBSock

    #Actually run the testscripts one at a time
    for testscript in `ls *_t.py`
      do
      echo "Testing "$testscript >> $outfile
      echo "Testing "$testscript >> $errorfile 
      python2.4 $testscript 1>> $outfile 2>> $errorfile
    done
fi


#Starting Oracle tests
if [ $runOracle -eq 1 ]
    then
    echo "Running Oracle tests" >> $outfile
    echo "Running Oracle tests" >> $errorfile

    #Set up the database variables
    export DIALECT=$OracleDialect
    export DATABASE=$OracleDatabase

    #Actually run the testscripts one at a time
    for testscript in `ls *_t.py`
      do
      echo "Testing "$testscript >> $outfile
      echo "Testing "$testscript >> $errorfile 
      python2.4 $testscript 1>> $outfile 2>> $errorfile
    done

fi

echo "Run complete: Parsing error file"

grep -e "FAILED" -e "Running" -e "Testing" $errorfile



exit
