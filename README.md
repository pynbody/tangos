This repository contains the complete code for the halo database, which ingests runs and calculates various properties of the halos (including profiles, images etc) then exposes them through a python interface and webserver.

Quick-start: if you already have a .db file and want to run the webserver
-------------------------------------------------------------------------

If running on a remote server, you will need to forward the appropriate port using `ssh address.of.remote.server -L5000:localhost:5000`. Then follow these instructions:

1. Clone the repository
2. Make sure you have an up-to-date version of python, then type `pip install pylons formalchemy` to install the required web frameworks
3. Put your database file in the halo_database folder, named `data.db` - or edit the `environment.sh` to specify a different location
4. Type `./webserver.sh` to run the web server
5. Browse to <http://localhost:5000>
