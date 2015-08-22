DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
export PYTHONPATH=$DIR/modules/:$PYTHONPATH
export PATH=$DIR/tools/:$PATH
export HALODB_ROOT=$DIR/../db_galaxies/
export HALODB_DEFAULT_DB=$DIR/data.db
