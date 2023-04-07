# Off-line Paper Database searcher 

A small, bare-bones application to add publication metadata to a postgresql database for later querying in case no online connection
is available to enable querying one of the freely available online resources, e.g. traveling by train.

The database can be searched by either author or publication title.
If the respective entry has previously added to the database, a search returns:
* paper title
* author
* small summary
* bibtex entry


*Note:* This application was created for only personal usage and its construction reflects that. If you enter
any problems in your setup, consult the logs.

## Installation

The repo contains all information required to install the package with poetry. 
Install poetry and run
```bash
poetry install
```

## Interaction

Start the interaction with the following command:

```bash
python user_intection.py -c ${config} --section ${section_of_the_config_to_access} \
-k ${file_to_key_if_your_database_is_encrypted} 
```

The configuration file should be encrypted if it contains sensitive information, e.g. a password. 
In this case, the key should be stored in a relatively safe location.

## Search

The following dialog is presented to you 
```
Welcome! Connecting to the database, one moment...
Connected to the database.
What do you want to do?
1) Search the database
2) Add an entry
3) (Q)uit
```
Press 1 to load the search dialog:
```
Search interface
Please choose a method:
1) Search by author
2) Search by paper title
```
### Search by title

Enter the title name and if a paper of that name exists in the database the relevant information will be presented to you.
Note that if several papers with that specific title are present in the database, you will be presented with the list of
the respective authors and asked to choose one author (group).

If no paper is found, you will be informed of it.

### Search by author

Enter the author's name. You are then presented with a list of papers that author has (co-)authored and asked
to select one.
The name should have the format ```${last name}, ${first name}```.

## Add an entry

The program takes you through the steps to add an entry to the database step by step. Note that you are asked
whether you want to provide a file to read the bib entry from or enter the data by hand.

```
Please enter the necessary information
Author(s), please provide a , separated list: ${list_of_author}
Paper title: Fancy new paper
Bibtex key: new key
Do you want to enter the bibtex entry via a separate file?
1) Yes
2) No
Your choice: 
[...]
```

# Config 

Your configuration should be of the form
```
[postgresql]
dbname=your_dbname
user=your_dbuser
password=your_dbuser_password
```
It is recommended to use an encrypted version of this file.

