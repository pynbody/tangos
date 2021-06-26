Tangos - The agile numerical galaxy organisation system
-------------------------------------------------------

[![Build Status](https://github.com/pynbody/tangos/actions/workflows/build-test.yaml/badge.svg?branch=master)](https://github.com/pynbody/tangos/actions) [![DOI](https://zenodo.org/badge/105990932.svg)](https://zenodo.org/badge/latestdoi/105990932)

_Tangos_ lets you build a database (along the lines of [Eagle](http://icc.dur.ac.uk/Eagle/database.php) 
or [MultiDark](https://www.cosmosim.org/cms/documentation/projects/multidark-bolshoi-project/))
 for your own cosmological and zoom simulations. 
 
It's a python 2.7/3.5+ modular system for generating and querying databases 
and:

 - is designed to store and manage results from your own analysis code;
 - provides web and python interfaces;
 - allows users to construct science-focussed queries, including across entire merger trees, 
   without requiring knowledge of SQL;
   
When building databases, _tangos_:   

 - manages the process of populating the database with science data, including auto-parallelising
   your analysis;
 - can be customised to work with multiple python modules such as 
   [pynbody](http://pynbody.github.io/pynbody/) or [yt](http://yt-project.org) to 
   process raw simulation data;
 - can use your favourite database as the underlying store 
   (by default, [sqlite](https://sqlite.org)), thanks to [sqlalchemy](https://www.sqlalchemy.org).

 
 Getting started
 ---------------
 
 For information on getting started refer to the [tutorials on our github pages](https://pynbody.github.io/tangos/).
 These tutorials are also available in markdown format [within the tangos repository](docs/index.md).
 
 
Acknowledging the code
----------------------
When using _tangos_, please acknowledge it by citing the release paper:
Pontzen & Tremmel, 2018, ApJS 237, 2. [DOI 10.3847/1538-4365/aac832](https://doi.org/10.3847/1538-4365/aac832);  [arXiv:1803.00010](https://arxiv.org/pdf/1803.00010.pdf). Optionally you can also cite the Zenodo DOI for the specific version of _tangos_ that you are using, which may be found [here](https://doi.org/10.5281/zenodo.1243070).

