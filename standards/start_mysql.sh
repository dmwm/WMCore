#!/bin/sh


echo "-->Starting MySQL server"
# NOTE: do this only if there is no mysql database
mkdir -p $TESTDIR/mysqldata
mysql_install_db --datadir=$TESTDIR/mysqldata
mysqld_safe --datadir=$TESTDIR/mysqldata --socket=$TESTDIR/mysqldata/mysql.sock --skip-networking --log-error=$TESTDIR/mysqldata/error.log --pid-file=$TESTDIR/mysqldata/mysqld.pid &
echo 'sleeping to make sure the db exists'
sleep 10

echo "-->Granting access to user for testing"
# granting access
mysql -u root --socket=$TESTDIR/mysqldata/mysql.sock --exec "${SQLCREATE}"
mysql -u root --socket=$TESTDIR/mysqldata/mysql.sock --exec "create database ${DBNAME}"

# ADD BELOW OTHER DATABASES IF NEEDED FOR MYSQL BACKEND TESTS.
mysql -u root --socket=$TESTDIR/mysqldata/mysql.sock --exec "${PROXYCREATE}"
mysql -u root --socket=$TESTDIR/mysqldata/mysql.sock --exec "create database ${PROXYDB}"

