#/bin/bash

jid=$1
submitNode=$2
sandbox=$3

cp FrameworkJobReport-4540.xml FrameworkJobReport.xml

echo "I am running job "$jid" from submit node "$submitNode" on node "$HOSTNAME
echo "I will get the sandbox from "$sandbox

sleep 120

exit 0
