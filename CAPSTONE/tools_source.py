from pyspark.sql import SparkSession
#from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *

class Sources:
    """
    Import data from sources and return dataframes
    """
    def __init__(self, spark, source_paths):
        self.spark = spark
        self.source_paths = source_paths    
    
    def get_csv(self, filepath, delimiter=','):
        """
        Generic import csv file to spark and return a dataframe
        """
        df = self.spark.read.format('csv').option('header','true').option('delimiter',delimiter).load(filepath)
        return df
    
    def get_inmigration_data(self):
        """
        Import inmigration parquet files and return a dataframe
        """
        df = self.spark.read.parquet(self.source_paths['inmigration_path'])
        return df
    
    def get_temperature_data(self):
        """
        Import temperature csv file and return a dataframe
        """
        df_temperature = self.get_csv(filepath=self.source_paths['temperature_path'])
        return df_temperature
    
    def get_modes_of_arrival(self):
        """
        Import modes csv file and return a dataframe
        """
        df_modes = self.get_csv(filepath=self.source_paths['modes_path'])
        return df_modes
    
    def get_cities_data(self):
        """
        Import us-cities-demographics csv file and return a dataframe
        """
        df_cities = self.get_csv(filepath=self.source_paths['cities_path'], delimiter=';')
        return df_cities

    def get_states_data(self):
        """
        Import states csv file and return a dataframe
        """
        df_states = self.get_csv(filepath=self.source_paths['states_path'])
        return df_states
    
    def get_countries_data(self):
        """
        Import countries csv file and return a dataframe
        """
        df_countries = self.get_csv(filepath=self.source_paths['countries_path'])
        return df_countries
    
    def get_visa_data(self):
        """
        Import visa csv file and return a dataframe
        """
        df_visa = self.get_csv(filepath=self.source_paths['visa_path'])
        return df_visa
    
    def get_airports(self):
        """
        Import airports csv file and return a dataframe
        """
        df_airports = self.get_csv(filepath=self.source_paths['airports_path'])
        return df_airports
    
    def get_airports_data(self):
        """
        Import airport-codes_csv file and return a dataframe
        """
        df_airport_data = self.get_csv(filepath=self.source_paths['airport_data_path'])
        return df_airport_data
    
    def get_cities_coord_data(self):
        """
        Import uscities csv file and return a dataframe
        """
        df_cities_coord = self.get_csv(filepath=self.source_paths['cities_coordinates'])
        return df_cities_coord