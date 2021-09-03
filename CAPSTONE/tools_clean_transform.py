from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *
import datetime as dt

class Cleaner_and_Transformer:
    """
    Clean data from dataframe and return dataframe
    """
    def __init__(self):
        pass
    
    def rename_and_change_type(self,df,changes_dict):
        """
        Receives a dataframe 'df' and a dictionary of properties to update: 'new_name', 'new_type' or both
        Dictionary example:
        'cicid': {'new_name':'cic_id',
                  'new_type':'integer'}
        -> changes column 'cicid' of dataframe 'df': change column name and column type
        """
        for name in changes_dict.keys():
            if 'new_type' in changes_dict[name].keys():
                new_type = changes_dict[name]['new_type']
                df = df.withColumn(name,df[name].cast(new_type))
            if 'new_name' in changes_dict[name].keys():
                new_name = changes_dict[name]['new_name']
                df = df.withColumnRenamed(name,new_name)
            #df = df.withColumn(name,df[name].cast(new_type)).withColumnRenamed(name,new_name)
        return df
    
    def clean_and_transform_inmigration_data(self,df):
        """
        Clean data from inmigration dataframe and return a cleaned and transformed dataframe
        """
        changes_dict = {
            'cicid': {'new_name':'cic_id',
                      'new_type':'integer'},            
            'i94yr': {'new_name':'year',
                      'new_type':'integer'},            
            'i94mon': {'new_name':'month',
                      'new_type':'integer'},            
            'i94cit': {'new_name':'birth_country',
                      'new_type':'integer'},            
            'i94res': {'new_name':'residence_country',
                      'new_type':'integer'},
            'i94port':{'new_name':'airport_id',
                      'new_type':'string'}, 
            'arrdate': {'new_name':'arrival_date',
                      'new_type':'integer'},
            'i94mode': {'new_name':'arrival_mode_id',
                      'new_type':'integer'},
            'i94addr': {'new_name':'state_name_code',
                      'new_type':'string'},
            'depdate': {'new_name':'departure_date',
                      'new_type':'integer'},
            'i94bir': {'new_name':'repondent_age',
                      'new_type':'integer'},
            'i94visa': {'new_name':'visa_id_code',
                      'new_type':'integer'},
            'dtadfile': {'new_name':'date_Added',
                      'new_type':'integer'},
            'visapost': {'new_name':'visa_issued_department',
                      'new_type':'string'},
            'occup': {'new_name':'occupation',
                      'new_type':'string'},
            'entdepa': {'new_name':'arrival_flag',
                      'new_type':'string'},
            'entdepd': {'new_name':'departure_flag',
                      'new_type':'string'},
            'entdepu': {'new_name':'update_flag',
                      'new_type':'string'},
            'matflag': {'new_name':'match_arrival_departure_flag',
                      'new_type':'string'},
            'biryear': {'new_name':'birth_year',
                      'new_type':'integer'},
            'dtaddto': {'new_name':'allowed_date',
                      'new_type':'integer'},
            'insnum': {'new_name':'ins_number',
                      'new_type':'integer'},
            'admnum': {'new_name':'admission_number',
                      'new_type':'integer'},
            'fltno': {'new_name':'flight_number',
                      'new_type':'string'},
            'visatype': {'new_name':'visa_type',
                      'new_type':'string'},
            'count': {'new_name':'count',
                      'new_type':'integer'},
            'gender': {'new_name':'gender',
                      'new_type':'string'},
            'airline': {'new_name':'airline',
                      'new_type':'string'},
        }
        # 'flight_number' have 'LAND' values -> reason to be 'string'
        df_clean = self.rename_and_change_type(df=df,changes_dict=changes_dict)
    
        # fill null values    
        df_clean = df_clean.fillna({'arrival_mode_id':9})
        df_clean = df_clean.fillna({'state_name_code':'99'})
        
        # transform date columns -> create year, month and day
        get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
        df_clean = df_clean.withColumn("arrival_date", get_date(df_clean.arrival_date))\
            .withColumn("departure_date", get_date(df_clean.departure_date))
        df_clean = df_clean \
            .withColumn("arrival_date-split", split(col("arrival_date"), "-")) \
            .withColumn("arrival_year", col("arrival_date-split")[0]) \
            .withColumn("arrival_month", col("arrival_date-split")[1]) \
            .withColumn("arrival_day", col("arrival_date-split")[2]) \
            .drop("arrival_date-split")
        df_clean = df_clean \
            .withColumn("departure_date-split", split(col("departure_date"), "-")) \
            .withColumn("departure_year", col("departure_date-split")[0]) \
            .withColumn("departure_month", col("departure_date-split")[1]) \
            .withColumn("departure_day", col("departure_date-split")[2]) \
            .drop("departure_date-split")
        df_clean = df_clean.withColumn("date_added_year", substring(col("date_added"), 1, 4))\
            .withColumn("date_added_month", substring(col("date_added"), 5, 2))\
            .withColumn("date_added_day", substring(col("date_added"), 7, 2))\
            .drop('date_added')\
            .withColumn("date_added", concat(col("date_added_year"),lit('-'),col("date_added_month"),lit('-'),col("date_added_day")))
        df_clean = df_clean.withColumn("allowed_date_year", substring(col("allowed_date"), -4, 4))\
            .withColumn("allowed_date_month", substring(col("allowed_date"), -8, 2))\
            .withColumn("allowed_date_day", substring(col("allowed_date"), -6, 2))\
            .drop('allowe_date')\
            .withColumn("allowed_date", concat(col("allowed_date_year"),lit('-'),col("allowed_date_month"),lit('-'),col("allowed_date_day")))
        
        changes_dict_2 = {
            'arrival_date': {'new_type':'date'},
            'departure_date': {'new_type':'date'},
            'allowed_date': {'new_type':'date'},
            'arrival_year': {'new_type':'integer'},
            'arrival_month': {'new_type':'integer'},
            'arrival_day': {'new_type':'integer'},
            'departure_year': {'new_type':'integer'},
            'departure_month': {'new_type':'integer'},
            'departure_day': {'new_type':'integer'},
            'allowed_date_year': {'new_type':'integer'},
            'allowed_date_month': {'new_type':'integer'},
            'allowed_date_day': {'new_type':'integer'}
        }
        df_clean = self.rename_and_change_type(df=df_clean,
                                               changes_dict=changes_dict_2)
        # field not necessary - duplicated information
        df_clean = df_clean.drop('visa_id_code')
        
        return df_clean
        
    def clean_and_transform_temperature(self,df):
        """
        Clean data from temperature dataframe and return a cleaned and transformed dataframe
        """
        
        # filter data for United States and drop empty rows
        df_clean = df.where(df["Country"]=="United States").na.drop("any")
        
        changes_dict = {
            'dt': {'new_name':'date',
                   'new_type':'date'},            
            'AverageTemperature': {'new_name':'average_temperature',
                                   'new_type':'float'},            
            'AverageTemperatureUncertainty': {'new_name':'average_temperature_uncertainty',
                                              'new_type':'float'},            
            'City': {'new_name':'city',
                     'new_type':'string'},            
            'Country': {'new_name':'country',
                        'new_type':'string'},            
            'Latitude': {'new_name':'latitude',
                         'new_type':'string'},            
            'Longitude': {'new_name':'longitude',
                          'new_type':'string'},    
        }
     
        df_clean = self.rename_and_change_type(df_clean,changes_dict)
        df_clean = df_clean \
            .withColumn("date-split", split(col("date"), "-")) \
            .withColumn("year", col("date-split")[0]) \
            .withColumn("month", col("date-split")[1]) \
            .withColumn("day", col("date-split")[2]) \
            .drop("date-split")
        
        changes_dict_2 = {
            'year': {'new_type':'integer'},
            'month': {'new_type':'integer'},
            'day': {'new_type':'integer'},
        }
        df_clean = self.rename_and_change_type(df=df_clean,
                                               changes_dict=changes_dict_2)     
        
        df_clean = df_clean.withColumn("lat", 
                                       when(substring(col("latitude"), -1, 1)=='N',
                                            expr("substring(latitude,1,length(latitude)-1)"))\
                                       .when(substring(col("latitude"), -1, 1)=='S',
                                             -expr("substring(latitude,1,length(latitude)-1)"))\
                                       .otherwise('-'))
        df_clean = df_clean.withColumn("lng", 
                                       when(substring(col("longitude"), -1, 1)=='E',
                                            expr("substring(longitude,1,length(longitude)-1)"))\
                                       .when(substring(col("longitude"), -1, 1)=='W',
                                             -expr("substring(longitude,1,length(longitude)-1)"))\
                                       .otherwise('-'))
        changes_dict_3 = {
            'lat': {'new_type':'float'},
            'lng': {'new_type':'float'},
        }
        df_clean = self.rename_and_change_type(df=df_clean,
                                               changes_dict=changes_dict_3)  
        return df_clean
        
    def clean_and_transform_modes(self,df):
        """
        Clean data from modes dataframe and return a cleaned and transformed dataframe
        """
        changes_dict = {
            'mode_id': {'new_name':'mode_id',
                        'new_type':'integer'},            
            'mode_name': {'new_name':'mode_name',
                          'new_type':'string'}
        }
        df_clean = self.rename_and_change_type(df,changes_dict)
        return df_clean
    
    def clean_and_transform_cities(self,df):
        """
        Clean data from cities dataframe and return a cleaned and transformed dataframe
        """
        # pivot 'race' to create 5 new columns
        # 'American Indian and Alaska Native'
        # 'Asian'
        # 'Black or African-American'
        # 'Hispanic or Latino'
        # 'White'
        cols = ['City','State','Male Population','Female Population','Total Population','Number of Veterans','Foreign-born','Average Household Size','State Code']
        # we will not use 'Median Age'
        df_clean = df.groupBy(cols).pivot('Race').agg(sum('Count').cast('integer'))
        
        changes_dict = {
            'City': {'new_name':'city',
                     'new_type':'string'},            
            'State': {'new_name':'state_name',
                      'new_type':'string'},                    
            'Male Population': {'new_name':'male_population',
                                'new_type':'integer'},           
            'Female Population': {'new_name':'female_population',
                                  'new_type':'integer'},           
            'Total Population': {'new_name':'total_population',
                                 'new_type':'integer'},           
            'Number of Veterans': {'new_name':'veterans',
                                   'new_type':'integer'},           
            'Foreign-born': {'new_name':'foreign_born',
                             'new_type':'integer'},           
            'Average Household Size': {'new_name':'average_household_size',                        
                                       'new_type':'float'},           
            'State Code': {'new_name':'state_code',
                           'new_type':'string'},      
            'American Indian and Alaska Native': {'new_name':'american_indian_and_alaska_native',
                                                  'new_type':'integer'}, 
            'Asian': {'new_name':'asian',
                      'new_type':'integer'}, 
            'Black or African-American': {'new_name':'black_or_african_american',
                                          'new_type':'integer'}, 
            'Hispanic or Latino': {'new_name':'hispanic_or_latino',
                                   'new_type':'integer'}, 
            'White': {'new_name':'white',
                      'new_type':'integer'}, 
        }
    
        df_clean = self.rename_and_change_type(df_clean,changes_dict)
        
        df_clean = df_clean.fillna(0)
        
        return df_clean
    
    def clean_and_transform_countries(self,df):
        """
        Clean data from countries dataframe and return a cleaned and transformed dataframe
        """
        changes_dict = {
            'country_id': {'new_name':'country_id',
                           'new_type':'integer'},            
            'country_name': {'new_name':'country_name',
                             'new_type':'string'}, 
        }
        df_clean = df.withColumn("country_id", trim(col("country_id")))
        df_clean = self.rename_and_change_type(df_clean,changes_dict)
        return df_clean
    
    def clean_and_transform_states(self,df_states,df_cities_clean):
        """
        Clean data from states dataframe and return a cleaned and transformed dataframe
        """
        # enrich the states dataset with data from cities 
        # aggregate by state
        df_cities_data = df_cities_clean.groupBy('state_code').agg(
            sum('male_population').alias("male_population"),
            sum('female_population').alias('female_population'),
            sum('total_population').alias('total_population'),
            sum('veterans').alias('veterans'),
            sum('foreign_born').alias('foreign_born'),
            first('state_name').alias('state_name'),
            sum('american_indian_and_alaska_native').alias('american_indian_and_alaska_native'),
            sum('asian').alias('asian'),
            sum('black_or_african_american').alias('black_or_african_american'),
            sum('hispanic_or_latino').alias('hispanic_or_latino'),
            sum('white').alias('white'),
        )
        # drop 'state_name' column 
        df_cities_data = df_cities_data.drop('state_name')

        changes_dict = {
            'state_id': {'new_name':'state_code',
                         'new_type':'string'},            
            'state_name': {'new_name':'state_name',
                           'new_type':'string'},
        }
        df_states_clean = self.rename_and_change_type(df_states,changes_dict)
        df_states_data = df_states_clean.join(df_cities_data,'state_code',how='left').fillna(0)
        
        return df_states_data

    def clean_and_transform_visa(self,df_visa):
        """
        Clean data from visa dataframe and return a cleaned and transformed dataframe
        """
        changes_dict = {
            'visa_code': {'new_name':'visa_code',
                          'new_type':'integer'},         
            'visa_categorie': {'new_name':'visa_categorie',
                               'new_type':'string'},          
            'visa_type': {'new_name':'visa_type',
                          'new_type':'string'},           
            'visa_type_description': {'new_name':'visa_type_description',
                                      'new_type':'string'},
        }
        df_visa_clean = self.rename_and_change_type(df_visa,changes_dict)
        return df_visa_clean
    
    def clean_and_transform_airports(self,df_airports):
        """
        Clean data from airports dataframe and return a cleaned and transformed dataframe
        """
        changes_dict = {
            'airport_id': {'new_name':'airport_id',
                          'new_type':'string'},         
            'airport_name': {'new_name':'airport_name',
                               'new_type':'string'},          
        }
        df_airports_clean = self.rename_and_change_type(df_airports,changes_dict)
        return df_airports_clean
    
    def clean_and_transform_airport_data(self,df_airport_data):
        """
        Clean data from airport_data dataframe and return a cleaned and transformed dataframe
        """
        changes_dict = {
            'ident': {'new_name':'ent_id',
                      'new_type':'string'},         
            'type': {'new_name':'type',
                     'new_type':'string'},         
            'name': {'new_name':'name',
                     'new_type':'string'},        
            'elevation_ft': {'new_name':'elevation_ft',
                             'new_type':'integer'},         
            'continent': {'new_name':'continent',
                          'new_type':'string'},  
            'iso_country': {'new_name':'iso_country',
                            'new_type':'string'},         
            'iso_region': {'new_name':'iso_region',
                           'new_type':'string'},         
            'municipality': {'new_name':'municipality',
                             'new_type':'string'},         
            'gps_code': {'new_name':'gps_code',
                         'new_type':'string'},         
            'iata_code': {'new_name':'iata_code',
                          'new_type':'string'},         
            'local_code': {'new_name':'local_code',
                           'new_type':'string'},         
            'coordinates': {'new_name':'coordinates',
                            'new_type':'string'},                
        }
        # filter only US airports
        df_airport_data_clean = df_airport_data.where(df_airport_data['iso_country']=='US')
        df_airport_data_clean = self.rename_and_change_type(df_airport_data_clean,changes_dict)
        
        # coordinates
        df_airport_data_clean = df_airport_data_clean \
            .withColumn("coordinates-split", split(col("coordinates"), ",")) \
            .withColumn("lat", col("coordinates-split")[0]) \
            .withColumn("lng", col("coordinates-split")[1]) \
            .drop("coordinates-split")
        changes_dict_2 = {
            'lat': {'new_type':'float'},         
            'lng': {'new_type':'float'},
        }
        df_airport_data_clean = self.rename_and_change_type(df_airport_data_clean,changes_dict_2)
        
        # delete rows with NO iata_code
        df_airport_data_clean = df_airport_data_clean.where(df_airport_data_clean['iata_code']!='')
        
        # delete 'closed' airports
        df_airport_data_clean = df_airport_data_clean.where(df_airport_data_clean['type']!='closed')
        return df_airport_data_clean 
        
    def clean_and_transform_cities_coord(self,df_cities_coord):
        """
        Clean data from cities_coord dataframe and return a cleaned and transformed dataframe
        """
        changes_dict = {
            'city': {'new_name':'city',
                     'new_type':'string'},         
            'city_ascii': {'new_name':'city_ascii',
                           'new_type':'string'},         
            'state_id': {'new_name':'state_id',
                         'new_type':'string'},        
            'state_name': {'new_name':'state_name',
                           'new_type':'string'},         
            'county_fips': {'new_name':'county_fips',
                            'new_type':'integer'},  
            'county_name': {'new_name':'county_name',
                            'new_type':'string'},         
            'lat': {'new_name':'lat',
                    'new_type':'float'},         
            'lng': {'new_name':'lng',
                    'new_type':'float'},         
            'population': {'new_name':'population',
                           'new_type':'integer'},         
            'density': {'new_name':'density',
                        'new_type':'integer'},         
            'source': {'new_name':'source',
                       'new_type':'string'},         
            'military': {'new_name':'military',
                         'new_type':'string'},               
            'incorporated': {'new_name':'incorporated',
                             'new_type':'string'},               
            'timezone': {'new_name':'timezone',
                         'new_type':'string'},               
            'ranking': {'new_name':'ranking',
                        'new_type':'integer'},               
            'zips': {'new_name':'zips',
                     'new_type':'string'},               
            'id': {'new_name':'id',
                   'new_type':'integer'},                              
        }
        # filter only US airports
        df_cities_coord_clean = self.rename_and_change_type(df_cities_coord,changes_dict)
        return df_cities_coord_clean 
    
    def integrity_inmigration_data(self,df_dict):
        """
        Filters inmigration FACT Table entries to data available in DIM Tables
        Receives a dictionary of all dataframes, example:
            dict_all_df = {
                'inmigration' : df_inmigration_clean_all,
                'temperature' : df_temp_clean,
                'modes'       : df_modes_clean,
                'countries'   : df_countries_clean,
                'states'      : df_states_clean,
                'visa'        : df_visa_clean,
                'airports'    : df_airports_clean,
                'airport_data': df_airport_data_clean,
                'cities'      : df_cities_coord_clean,
            }
        and returns a filtered inmigration FACT Table
        """
        df_inmigration = df_dict['inmigration']
        df_modes = df_dict['modes']
        df_countries = df_dict['countries']
        df_states = df_dict['states']
        df_visa = df_dict['visa']
        df_airports = df_dict['airports']
        df_airport_data = df_dict['airport_data']

        df_inmigration = df_inmigration \
            .join(df_states, df_inmigration["state_name_code"] == df_states["state_code"], "left_semi") \
            .join(df_airports, df_inmigration["airport_id"] == df_airports["airport_id"], "left_semi") \
            .join(df_airport_data, df_inmigration["airport_id"] == df_airport_data["iata_code"], "left_semi") \
            .join(df_countries, df_inmigration["birth_country"] == df_countries["country_id"], "left_semi") \
            .join(df_countries, df_inmigration["residence_country"] == df_countries["country_id"], "left_semi") \
            .join(df_visa, df_inmigration["visa_type"] == df_visa["visa_type"], "left_semi") \
            .join(df_modes, df_inmigration["arrival_mode_id"] == df_modes["mode_id"], "left_semi")
        
        return df_inmigration