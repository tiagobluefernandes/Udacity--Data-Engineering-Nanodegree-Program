from pyspark.sql import SparkSession
from pyspark.sql.functions import *

class Validator:
    """
    Validate database integrity
    """
    def __init__(self, spark, source_dict):
        """
        Instantiate object with current 'spark' session and 'source_dict', example:
            dict_all_df = {
                'inmigration' : {'df'  : df_inmigration_clean,
                                 'file':'./model/facts_inmigration.parquet',
                                 'unique_key': 'cic_id'},
                'modes'       : {'df'  : df_modes_clean,
                                 'file': './model/mode.parquet',
                                 'unique_key': 'mode_id'}
            }
        """
        self.spark = spark
        self.source_dict = source_dict 
        
        ### dataframes
        # inmigration
        self.fact_path = self.source_dict['inmigration']['file']
        self.fact_df = self.spark.read.parquet(self.fact_path)
        # mode
        self.dim_path_mode = self.source_dict['modes']['file']
        self.dim_df_mode = self.spark.read.parquet(self.dim_path_mode)
        # country
        self.dim_path_country = self.source_dict['countries']['file']
        self.dim_df_country = self.spark.read.parquet(self.dim_path_country)
        # state
        self.dim_path_state = self.source_dict['states']['file']
        self.dim_df_state = self.spark.read.parquet(self.dim_path_state)
        # visa
        self.dim_path_visa = self.source_dict['visa']['file']
        self.dim_df_visa = self.spark.read.parquet(self.dim_path_visa)
        # airport
        self.dim_path_airport = self.source_dict['airports']['file']
        self.dim_df_airport = self.spark.read.parquet(self.dim_path_airport)
        # airport_data
        self.dim_path_airport_data = self.source_dict['airport_data']['file']
        self.dim_df_airport_data = self.spark.read.parquet(self.dim_path_airport_data)
        
        #self.all_dfs = [self.fact_df,self.dim_df_mode,self.dim_df_country,self.dim_df_state,
        #                self.dim_df_visa,self.dim_df_airport,self.dim_df_airport_data]
        
    def check_rows(self):
        """
        Checks all Tables looking for empty ones (no rows) and duplicated 'unique keys'
        Retuns a dictionary will table name and status (OK or ERROR), example:
            {'inmigration': 'OK',
             'modes': 'OK',
             'countries': 'OK',
             'states': 'OK',
             'visa': 'OK',
             'airports': 'OK',
             'airport_data': 'OK'}
        """
        source_dict = self.source_dict
        row_status={}
        for name, _dict in source_dict.items():
            # parquet files location
            _path = _dict['file']
            # read files
            _df = self.spark.read.parquet(_path)
            # check if Table has rows
            rows = _df.count()
            # check if 'unique keys' are unique
            unique_key_col = _dict['unique_key']
            rows_unique = _df[[unique_key_col]].distinct().count()
            if (rows == 0) or (rows != rows_unique):
                print(f'{name} TABLE has {rows} rows and {rows_unique} are uniques')
                print('ERROR')
                row_status.update({name:'ERROR'})
            else:
                row_status.update({name:'OK'})
        return row_status
    
    def check_integrity(self):
        """
        Search inmigration FACT Table for data that is not present in the DIM tables
        Returns a status dictionary and a numeric dictionary, example:
            -> status dictionary: 'True' means that all entries in FACT Table Foreign Key are present in DIM Table; 'False' otherwise  
                {'modes': True,
                 'countries': True,
                 'states': True,
                 'visa': True,
                 'airports': True,
                 'airport_data': True}
            -> numeric dictionary: '0' means that 0 rows from FACT Table Foreign Key are new values (not in DIM Table)
                {'modes': 0,
                 'countries': 0,
                 'states': 0,
                 'visa': 0,
                 'airports': 0,
                 'airport_data': 0}
        """
        
        ### check integrity
        # mode
        mode_integrity = self.fact_df.select(col('arrival_mode_id')).distinct() \
                             .join(self.dim_df_mode, self.fact_df["arrival_mode_id"] == self.dim_df_mode["mode_id"], "left_anti") \
                             .count()
        # country
        country_integrity_1 = self.fact_df.select(col('birth_country')).distinct() \
                             .join(self.dim_df_country, self.fact_df["birth_country"] == self.dim_df_country["country_id"], "left_anti") \
                             .count()
        country_integrity_2 = self.fact_df.select(col('residence_country')).distinct() \
                             .join(self.dim_df_country, self.fact_df["residence_country"] == self.dim_df_country["country_id"], "left_anti") \
                             .count()
        country_integrity = country_integrity_1 + country_integrity_2
        # state
        state_integrity = self.fact_df.select(col('state_name_code')).distinct() \
                             .join(self.dim_df_state, self.fact_df["state_name_code"] == self.dim_df_state["state_code"], "left_anti") \
                             .count()
        # visa
        visa_integrity = self.fact_df.select(col('visa_type')).distinct() \
                             .join(self.dim_df_visa, self.fact_df["visa_type"] == self.dim_df_visa["visa_type"], "left_anti") \
                             .count()
        # airport
        airport_integrity = self.fact_df.select(col('airport_id')).distinct() \
                             .join(self.dim_df_airport, self.fact_df["airport_id"] == self.dim_df_airport["airport_id"], "left_anti") \
                             .count()
        # airport_data
        airport_data_integrity = self.fact_df.select(col('airport_id')).distinct() \
                             .join(self.dim_df_airport_data, self.fact_df["airport_id"] == self.dim_df_airport_data["iata_code"], "left_anti") \
                             .count()
        
        result={'modes':mode_integrity == 0,
                'countries':country_integrity == 0,
                'states':state_integrity == 0,
                'visa':visa_integrity == 0,
                'airports':airport_integrity == 0,
                'airport_data':airport_data_integrity == 0}
        
        n_rows={'modes':mode_integrity,
                'countries':country_integrity,
                'states':state_integrity,
                'visa':visa_integrity,
                'airports':airport_integrity,
                'airport_data':airport_data_integrity}
        return result, n_rows
