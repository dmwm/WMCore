#!/bin/sh

echo "-->remove database from database server"
mysql -u root --socket=$TESTDIR/mysqldata/mysql.sock --exec "drop database ${DBNAME}"
echo "-->Creating MySQL database access string"
#export DBSOCK=$TESTDIR/mysqldata/mysql.sock
#export DATABASE=mysql://${DBUSER}:${DBPASS}@localhost/${DBNAME}
echo '-->Using mysql DB: ' $DATABASE
mysql -u root --socket=$TESTDIR/mysqldata/mysql.sock --exec "${SQLCREATE}"
mysql -u root --socket=$TESTDIR/mysqldata/mysql.sock --exec "create database ${DBNAME}"
