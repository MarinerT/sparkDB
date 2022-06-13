# Pyspark on Databricks

The purpose of this repo is to keep a handy file of reusable, shareable code for PySpark, python-based Delta Lake architecture and Databricks commands. 

### Using DButils in Packages

The package 'sparkable_packaging' is all about how to use DBUtils in custom packages outside of the Databricks environment. Essentially, one needs to write the code to start a Spark session in the code itself, otherwise it'll throw errors (you don't actually have to start the Spark session). There are two functions in the module, one for initiating DBUtils from pyspark, and another to try it out, called list_dir. The native function in DBUtils, dbutils.fs.ls() does not have a recursive feature, and list_dir() adds that functionality.

Source: 

https://docs.azuredatabricks.net/user-guide/dev-tools/db-connect.html#access-dbutils

via 

pprasad009 @ https://stackoverflow.com/questions/51885332/how-to-load-databricks-package-dbutils-in-pyspark

