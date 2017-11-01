setenv TANGOS_PROPERTY_MODULES mytangosproperty
if [[ -e $DIR/environment_local.sh ]]
then
    source $DIR/environment_local.sh
fi
