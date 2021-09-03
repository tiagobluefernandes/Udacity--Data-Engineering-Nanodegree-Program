#################################################################################################
##### ----- File with small code snippets used during the development of this project ----- #####
#################################################################################################

See the file **Data Dictionary.md** for more details on each table.

# evaluate column values from a spark dataframe
df_test = df.groupBy('entdepu').count().orderBy('count')
        
#convert a sample to pandas
df_test.limit(3).toPandas().head(3)
        
# check column names (schema)
df_test.printSchema()

# copy file from udacity foldet to current project folder (508MB)
#
from shutil import copyfile
dst = './sources/GlobalLandTemperaturesByCity.csv'
copyfile(src, dst)
#

# read Temperature Data with SPARK
df_temp = self.spark.read.option("header",True).csv(src)
df_temp = df_temp.where(df_temp["Country"]=="United States")
df_temp.limit(3).toPandas()

# read Temperature Data with PANDAS 
df_temp = pd.read_csv(src)
df_temp = df_temp[df_temp['Country']=='United States']
df_temp.head(3)

### read Immigration Data with Pandas ###
# fname = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
# df_sas_data = pd.read_sas(fname, 'sas7bdat', encoding="ISO-8859-1")
# print(df_sas_data.shape)
# df_sas_data.head(3)

# check nulls
df_test = df_clean.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_clean.columns]).toPandas()#.show()

# lower case 'state_name'
df_states = df_states.withColumn('state_name', lower(col('state_name')))

# replace values to match demographic data
df_states = df_states.replace({
    'dist. of columbia':'district of columbia',
    'n. carolina':'north carolina',
    's. carolina':'south carolina',
    'n. dakota':'north dakota',
    's. dakota':'south dakota',
    'w. virginia':'west virginia',
})


all_df = [df_inmigration_clean,df_temp_clean,df_modes_clean,df_countries_clean,df_states_clean,
          df_visa_clean,df_airports_clean,df_airport_data_clean,df_cities_coord_clean]
# check for duplicates
for i,df in enumerate(all_df):
    print(i)
    print(df.count())
    print(df.distinct().count())
# check nulls    
for i,df in enumerate(all_df):
    if i in [1,2,3,4,5,6,7,8]:
        continue
    print(i)
    df.printSchema()
    df_0 = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).toPandas()
    df_0.T
    
# substring
df_clean = df_inmigration_clean.withColumn("date_added-split", substring(col("date_added"), 0, 4))
df_clean = df_inmigration_clean.withColumn("date_added-split", df_inmigration_clean['date_added'].substr(0, 4))

# substring all but last character
df_clean = df_temp_clean.withColumn("lat", expr("substring(latitude,1,length(latitude)-1)"))

# check columns printed in pandas
pd.get_option("display.max_columns")
# update
pd.set_option("display.max_columns", None)
for df_name, df in dict_all_df.items():
    lll = df.count()
    print('------------------------------------------------------------')
    print(f'{df_name} has {lll} rows')
    df.printSchema()
    if lll>10:
        display(df.limit(3).toPandas().head(3))
    else:
        display(df.toPandas())
        
# select duplicated rows
from pyspark.sql import Window
w = Window.partitionBy('iata_code')
df = df_airport_data_clean
df = df.select('*', count('iata_code').over(w).alias('dupeCount'))\
    .where('dupeCount > 1')\
    .drop('dupeCount')\
    .toPandas()


# create zip file from folder
import os
import zipfile
folder_path = 'model/'
file_path = 'python.zip'
def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file), 
                       os.path.relpath(os.path.join(root, file), 
                                       os.path.join(path, '..')))
zipf = zipfile.ZipFile(file_path, 'w', zipfile.ZIP_DEFLATED)
zipdir(folder_path, zipf)
zipf.close()