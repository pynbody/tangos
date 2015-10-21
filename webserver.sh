#!/usr/bin/env bash

source environment.sh
cd webserver
paster serve development.ini $@
