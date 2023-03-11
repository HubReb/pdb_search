# Off-line Paper Database searcher 

A small, bare-bones application to add publication metadata to a postgresql database for later querying in case no online connection
is available to enable querying one of the freely available online resources, e.g. traveling by train.

The database can be searched by either author or publication title.
If the respective entry has previously added to the database, a search returns:
* paper title
* author
* small summary
* bibtex entry


*Note:* This application was created for only personal usage and its construction reflects that.

## Installation

The repo contains all information required to install the package with poetry. 
Install poetry and run
```
poetry install
```

## Add an entry

```bash
python add.py -c ${config} --section ${section_of_the_config_to_access} -s ${summary_of_the_paper} \
-t {paper_title} -k ${file_to_key_if_your_database_is_encrypted} -b ${file_containing_the_bib_entry} 
```

The configuration file should be encrypted if it contains sensitive information, e.g. a password. 
In this case, the key should be stored in a relatively safe location.

## Search for an entry

### Search by title

```bash
python search.py -c ${config} --section ${section_of_the_config_to_access} \
-t ${paper_title} -k ${file_to_key_if_your_database_is_encrypted}
```

If a title is provided via the ```-t ``` flag, providing an author does not affect the script. 

### Search by author

```bash
python search.py -c ${config} --section ${section_of_the_config_to_access} \
-a ${author_name} -k ${file_to_key_if_your_database_is_encrypted}
```
The ```author name ``` has to have the format ```${last name}, ${first name}```.
The search returns all papers the author has published, regardless of whether the author is listed as first, second,... 
author.
You are then asked which paper's metadata to show.

### Config 

Your configuration should be of the form
```bash
[postgresql]
dbname=your_dbname
user=your_dbuser
password=your_dbuser_password
```
It is recommended to use an encrypted version of this file.