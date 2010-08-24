#!/bin/sh

echo "-->remove database from database server"
if test -r $DBSOCK 
then
    echo 'using socket'
    echo "-->remove database from database server"
    mysql -u $DBMASTERUSER --password=$DBMASTERPASS --socket=$DBSOCK --exec "drop database ${DBNAME}"
    echo '-->Using mysql DB: ' $DATABASE
    mysql -u $DBMASTERUSER --password=$DBMASTERPASS --socket=$DBSOCK --exec "${SQLCREATE}"
    mysql -u $DBMASTERUSER --password=$DBMASTERPASS --socket=$DBSOCK --exec "create database ${DBNAME}"
    echo '-->Granting super '
    mysql -u $DBMASTERUSER --password=$DBMASTERPASS --socket=$DBSOCK --exec "${GRANTSUPER}"

    mysql -u $DBMASTERUSER --password=$DBMASTERPASS --socket=$DBSOCK --exec "drop database ${PROXYDB}"
    echo '-->Using mysql DB: ' $PROXYDATABASE
    mysql -u $DBMASTERUSER --password=$DBMASTERPASS --socket=$DBSOCK --exec "${PROXYCREATE}"
    mysql -u $DBMASTERUSER --password=$DBMASTERPASS --socket=$DBSOCK --exec "create database ${PROXYDB}"
else
    echo 'using host: ' $DBHOST
    mysql -u $DBMASTERUSER --password=$DBMASTERPASS -h $DBHOST --exec "drop database ${DBNAME}"
    echo '-->Using mysql DB: ' $DATABASE
    mysql -u $DBMASTERUSER --password=$DBMASTERPASS -h $DBHOST --exec "${SQLCREATE}"
    mysql -u $DBMASTERUSER --password= $DBMASTERPASS -h $DBHOST --exec "create database ${DBNAME}"
    echo '-->Granting super '
    mysql -u $DBMASTERUSER --password= $DBMASTERPASS -h $DBHOST --exec "${GRANTSUPER}"

    mysql -u $DBMASTERUSER --password=$DBMASTERPASS -h $DBHOST --exec "drop database ${PROXYDB}"
    echo '-->Using mysql DB: ' $PROXYDATABASE
    mysql -u $DBMASTERUSER --password=$DBMASTERPASS -h $DBHOST --exec "${PROXYCREATE}"
    mysql -u $DBMASTERUSER --password=$DBMASTERPASS -h $DBHOST --exec "create database ${PROXYDB}"
fi


