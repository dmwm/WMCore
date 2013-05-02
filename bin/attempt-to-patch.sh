#!/bin/bash

CODE_PATH="$1"
if [ "x$2" != "x" ]; then
    SOURCE_NAME="$2"
else
    SOURCE_NAME="REPLACEMENT_SOURCE.tar.gz"
fi

# replace code with the input sandbox, if it exists
if [ -s $SOURCE_NAME ]; then
  echo "Replacing sandbox with tarball"
  md5sum $SOURCE_NAME
  ls -lah $SOURCE_NAME
  mv $SOURCE_NAME REPLACEMENT_SOURCE.tar
  MASTER_COMMIT=$(head -n1 REPLACEMENT_SOURCE.tar)
  tail -n +2 REPLACEMENT_SOURCE.tar > user.patch
  cd $CODE_PATH
  git clean -dfx
  git checkout $MASTER_COMMIT
  git apply ../user.patch || true
  git status
  cd ..
  rm $SOURCE_NAME || true
  rm REPLACEMENT_SOURCE.tar || true
  rm user.patch || true
  cd ..
  set +x
  if [[ "x${JOB_NAME}" =~ "-try" ]]
  then 
    echo "Applying sandbox to try job"
  else
    echo "*********************WARNING**********************"
    echo "**************************************************"
    echo "If you're wanting to submit a test sandbox, you"
    echo "need to use the JOBNAME-try target. Otherwise the"
    echo "test results from regular and try jobs get mixed"
    echo "**************************************************"
    rm REPLACEMENT_SOURCE.tar.gz
    exit 2
  fi
  set -x
fi
exit 0

