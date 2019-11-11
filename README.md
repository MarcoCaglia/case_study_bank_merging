# SOBERS Assignment

## Introduction

For this assignment, our client has a website that takes personal export of banking history and displays it in the frontend.
There is already a version that runs for a single bank.
But now you have the task to create a script that parses data from multiple banks.
In the future there, the client wants to add more banks.

## Assignment

You are tasked to create a script that will parse multiple CSV's and create a unified CSV.
There are three different CSV's, this will increase in the future.
The client has a hard requirement that the result is stored as a CSV file.
But JSON and XML will be used in the future, maybe even storing the result in a database.

In the data folder, there are three CSV's with banking data.
Since the CSV's come from different banks, the layout of data can differ.

Create a script according to above-specified requirements.
You are allowed to take as much time as needed, but try to manage your time to around 1 or 2 hours.
Since we are a Python shop, the script must be done with Python.

Scoring is based on:

- Correctness
- Code architecture
- Maintainability
- Testability
- Extendability
- Pythonicness of code

Tests are not required, but plus points for tests.
This is the moment to show your skills.

## Result

The file bank_data_merging.py in the folder src contains the code for one possible solution for the problem at hand.
The desired expandability is mostly given by the algorithm using all .csv files in a specified folder, and by a meta dictionary, which contains information about the structure of each file (or a default entry).

The merging is made by a class, which has three public methods:
- BankMerge.merge()
- BankMerge.export()
- BankMerge.export_to_sql()

Usually, a user would first instantiate the class by passing the directory to it.
The class will the load all csv files it finds in that directory, load them as pandas dataframes and store the in the dictionary,
with the name of the banks as keys (derived from the name of the file) and the dataframes as values. If no directory is passed,
the instance will look for csv files in the current working directory. Please note, that the amount of files in the working directory
can be arbitrarily expanded, as long as each file has a unique name, and its structure is reflected in the meta dictionary or the meta
dictionary has a default entry.

Next, the user can call ther merge method, for which a custom meta dictionary can be passed. For now, this is passed as a path to a
json file. An example, on how such a json can be created is included in the src folder. Please note, that the example json is based
on the current form of the testing data, however, it is not confined to these columns and can be arbitrarily extended.

Lastly, the user can call the export method and specifiy the output format (csv, json or xml), the output name and
the output path. To load the output into a SQL table, the user can use the export_to_csv method.

Please note, that there are more parameters to be customized. For more details please refer to the dicstrings in the code.

## Next Steps

Currently, it would be simplest to run the class in a jupyter notebook. Should that not be desired, I would suggest
wrapping the entire process into a wrapper function, which can then be called as a command line tool.

Furthermore, more testing would be in order. I conducted some very basich testing (pytest) in the file test_bank_data_merging.py,
butof course, this should be extended, before the code is actually put to use.