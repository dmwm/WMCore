#!/bin/sh

echo "-->remove database from database server"
mysql -u root --socket=$TESTDIR/mysqldata/mysql.sock --exec "drop database ${PROXYDB}"
echo "-->Creating MySQL database access string"
export DBSOCK=$TESTDIR/mysqldata/mysql.sock
export MYSQLDATABASE=mysql://${DBUSER}:${DBPASS}@localhost/${DBNAME}
echo '-->Using mysql DB: ' $PROXYDATABASE
mysql -u root --socket=$TESTDIR/mysqldata/mysql.sock --exec "${PROXYCREATE}"
mysql -u root --socket=$TESTDIR/mysqldata/mysql.sock --exec "create database ${PROXYDB}"

