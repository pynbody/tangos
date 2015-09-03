DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
export PYTHONPATH=$DIR/modules/:$PYTHONPATH
export PATH=$DIR/tools/:$PATH

if [ -z "$HALODB_ROOT" ]; then
  export HALODB_ROOT=$DIR/../db_galaxies/
fi

if [ -z "$HALODB_DEFAULT_DB" ]; then
    export HALODB_DEFAULT_DB=$DIR/data.db
fi

if [[ -e enivronment_local.sh ]]
then
    source environment_local.sh
fi
