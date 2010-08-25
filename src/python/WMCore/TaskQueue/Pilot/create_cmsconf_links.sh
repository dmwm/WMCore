#!/usr/bin/bash

MYPILOT="`pwd`"
CONFDIR=$MYPILOT/localConf

mkdir $CONFDIR

for dir in `ls $CMS_PATH`
do
#create the symlinks
ln -s $CMS_PATH/$dir $CONFDIR/.
done

#remove the SITECONF symlink
rm -f $CONFDIR/SITECONF
#go to CMS_PATH
cd $CMS_PATH
#create tarball of SITECONF
tar -zcf $CONFDIR/siteconf.tar.gz SITECONF
#go back to my area of symlinks
cd $CONFDIR
#untar the SITECONF here
tar -zxf siteconf.tar.gz
#remove the tarball 
rm siteconf.tar.gz
#now ready to modify the PheDEx/storage.xml	 
#now set the CMS_PATH variable to this new location
export CMS_PATH="$CONFDIR"
#echo $CMS_PATH
