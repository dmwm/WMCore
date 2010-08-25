#PilotJob startup script 
PILOT_DIR="`pwd`" 
PILOT_CONFIG="KGRwMQpTJ3BpbG90TmFtZScKcDIKUydQaWxvdF84NTIwJwpwMwpzUydzZXJ2ZXJNb2RlJwpwNApJ
MDAKc1MndHFhZGRyZXNzJwpwNQpTJ3ZvY21zMTMuY2Vybi5jaDo4MDMwJwpwNgpzUydzZXJ2ZXJQ
b3J0JwpwNwpJMTAKc1MnVFRMJwpwOApJLTEKc1MncGlsb3RJRCcKcDkKSTg1MjAKcy4=
" 
echo $PILOT_DIR 
echo $PILOT_CONFIG 
 
tar -xf $PILOT_DIR/Pilot.tar > /dev/null 2>&1
cd Pilot 
#( /usr/bin/time ./pilotrun.sh $PILOT_CONFIG 2>&1 ) | gzip > ./pilotrun.log.gz
python PilotClient.py --pconfig="$PILOT_CONFIG" 
echo 'hello world' 
echo $PILOT_CONFIG 
