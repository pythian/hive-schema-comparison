# hive-schema-comparison
A tool to compare hive schemas across clusters


# Hive Schema Copare

Project works in two phases
1) fetch_hive_schema.py <config_file_path> which gets schema from two or more databases specified in the config file (config.json is a sample file). It stores schema in a file named [environmentName]_schema.out which contains a dictionary of the schema (one dictionary per environment/file).

2) A flask project to utilize  schema_compare.py which contains a class DictCompare which has several methods (get_databases, compare_fields, get_fields_by_table, etc.. ) to compare the dictionaries and return lists, namedtuples, or dataframes and display results in HTML web intervace.


### Version
0.0.1

### Tech

Requirements
* [hive_metastore] 
* [thrrift]


running:
```sh
$ python fetch_hive_schema.py <config_file_path>
```
to test methods
```sh
$ ipython schema_compare.py 
```
