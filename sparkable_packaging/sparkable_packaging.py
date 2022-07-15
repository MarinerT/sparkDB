import shutil, os
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def get_db_utils(spark):
    '''
    input: spark session
    output: an object
    description: returns a callable class to perform Databricks 
                 utility commands outside of a DB environment
    '''
    
    dbutils = None
    if spark.conf.get("spark.databricks.service.client.enabled") == "true":
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    else:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils

# initiate the session to use it local functions
dbutils = get_db_utils(spark)

def list_dir(directory, recurse=False, files=[]):
    '''
    input: a directory as a string, a boolean for recursive lookup, and an empty array for recursive calling
    output: an array of directories and files
    description: like dbutils.fs.ls(), but just returns the file names and as a recursive feature
    '''
    try:
        if not recurse and dbutils.fs.ls(directory):
            files = [file.name for file in dbutils.fs.ls(directory)]
        else:
            for file in dbutils.fs.ls(directory):
                files.append(file.name)
                if file.isDir():
                    listDir(file.path, recurse=True, files=files)
                else:
                    files.append(file.name)
        return files 
    except:
        return os.listdir(directory)

        
    
