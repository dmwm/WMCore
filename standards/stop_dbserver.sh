#!/bin/sh
echo "-->remove database from database server"
mysql -u root --socket=$TESTDIR/mysqldata/mysql.sock --exec "drop database ${DBNAME}"
export WHO=`whoami`
echo 'I am' $WHO
echo "-->remove database server"
ps auwx|grep $WHO'.*mysql'|awk '{print $2}'|xargs kill -9
echo "-->remove test dir"
rm -rf $TESTDIR

