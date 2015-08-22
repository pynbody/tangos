DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
export PYTHONPATH=$DIR/modules/:$PYTHONPATH
export PATH=$DIR/tools/:$PATH
export HALODB_ROOT=$HOME/Science/db_galaxies/
export HALODB_DEFAULT_DB=$HOME/Science/halodb/data.db
