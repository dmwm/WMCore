help(){
    echo -e $*
    cat <<EOF
    Usage:

    source deploy-unittest-venv.sh [-d] <WMAgent.venv path> [-h]

      -d <WMAgent.venv path>         WMAgent virtual environment root path [Default: ./WMAgent.venv3 if not Activated else \$WMA_ROOT_DIR]
      -h <help>                      Provides help for the current script

    # Example: Deploy unit test environment at /data/WMAgent.venv3/:
    #          source ./deploy-unittest-venv.sh -d /data/WMAgent.venv3/

    # Example: Deploy unit test inside previously activated WMAgent virtual environment:
    #          source ./deploy-unittest-venv.sh

    # Example: Running a single unit test upon sourcing:
    #          ipython -i \$WMA_DEPLOY_DIR/srv/WMCore/test/python/WMCore_t/Services_t/WorkQueue_t/WorkQueue_t.py

    # DEPENDENCIES: This unit-tests setup depends on a fully deployed and initialised agent inside a virtual
    #               environment and the supporting Docker containers running on the Machine:
    #               MariaDB && CouchDB

EOF
}

usage(){
    help $*
}

# Setting default values for all input parameters.
# Command line options overwrite the default values.
# All of the lists from bellow are interval separated.
[[ -n $WMA_ROOT_DIR ]] && venvPath=$WMA_ROOT_DIR || venvPath=$(realpath -m ./WMAgent.venv3)                                   # WMCore virtual environment target path

pythonCmd=python
[[ $($pythonCmd -V 2>&1) =~ Python[[:blank:]]+3.* ]] || pythonCmd=python3

### Searching for the mandatory and optional arguments:
export OPTIND=0
while getopts ":d:h" opt; do
    case ${opt} in
        d)
            venvPath=$OPTARG
            venvPath=$(realpath -m $venvPath) ;;
        h)
            help
            return ;;
        \? )
            msg="\nERROR: Invalid Option: -$OPTARG\n"
            usage "$msg"
            return ;;
        : )
            msg="\nERROR: Invalid Option: -$OPTARG requires an argument\n"
            usage "$msg" ;;
    esac
done
export OPTIND=0

_addWMCoreVenvVar(){
    # Adding a WMCore virtual environment variable to the WMCoreVenvVars array
    # and to the current virtual environment itself
    # :param $1: The variable name
    # :param $2: The actual export value to be used
    local varName=$1
    local exportVal=$2
    WMCoreVenvVars[$varName]=$exportVal
    eval "export $varName=$exportVal"
}

echo "Activating: . $venvPath/bin/activate"
. $venvPath/bin/activate || { echo "Failed to activate virtual environment at $venvPath"; exit 1 ;}

## Install nose packageg
pip install nose

# Loading WMAgent.secrets file
. $venvPath/bin/manage-common.sh
_load_wmasecrets

# Setting up WMCore related environment variables:
_addWMCoreVenvVar MDB_UNITTEST_DB wmagent_unittest
_addWMCoreVenvVar DATABASE        mysql://${MDB_USER}:${MDB_PASS}@127.0.0.1/${MDB_UNITTEST_DB}
_addWMCoreVenvVar DIALECT         MySQL
_addWMCoreVenvVar COUCHURL        http://$COUCH_USER:$COUCH_PASS@$COUCH_HOST:$COUCH_PORT

_WMCoreVenvSet

# Setting up the database to be used for the unttests

docker exec -u root -it  mariadb bash -c "mariadb --socket=\$MDB_SOCKET_FILE --execute \"CREATE DATABASE IF NOT EXISTS $MDB_UNITTEST_DB\""
docker exec -u root -it  mariadb bash -c "mariadb --socket=\$MDB_SOCKET_FILE --execute \"GRANT ALL ON $MDB_UNITTEST_DB.* TO $MDB_USER@localhost\""
docker exec -u root -it  mariadb bash -c "mariadb --socket=\$MDB_SOCKET_FILE --execute \"GRANT ALL ON $MDB_UNITTEST_DB.* TO $MDB_USER@127.0.0.1\""
