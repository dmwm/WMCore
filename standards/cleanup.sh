#!/bin/sh

echo "-->remove database from database server"
mysql -u root --socket=$TESTDIR/mysqldata/mysql.sock --exec "drop database ${DBNAME}"
echo "-->Creating MySQL database access string"
export DBSOCK=$TESTDIR/mysqldata/mysql.sock
export MYSQLDATABASE=mysql://${DBUSER}:${DBPASS}@localhost/${DBNAME}
echo '-->Using mysql DB: ' $MYSQLDATABASE
mysql -u root --socket=$TESTDIR/mysqldata/mysql.sock --exec "${SQLCREATE}"
mysql -u root --socket=$TESTDIR/mysqldata/mysql.sock --exec "create database ${DBNAME}"

cd $WMCOREBASE
echo "-->remove log files"
find -name "*.log"|xargs rm
echo "-->remove pyc files"
find -name "*.pyc"|xargs rm
echo "-->removing code quality files"
find -name "quality*.txt"|xargs rm
cd $TESTDIR
echo "-->remove ComponentLog files"
find -name "ComponentLog"|xargs rm
