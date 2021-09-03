# Capstone Project - Data Dictionary

## **FACT Table**

### **inmigration**
* cic_id                            : integer 
* year                              : integer
* month                             : integer 
* birth_country                     : integer 
* residence_country                 : integer
* airport_id                        : string 
* arrival_date                      : date
* arrival_mode_id                   : integer 
* state_name_code                   : string
* departure_date                    : date 
* repondent_age                     : integer 
* count                             : integer 
* visa_issued_department            : string 
* occupation                        : string 
* arrival_flag                      : string 
* departure_flag                    : string 
* update_flag                       : string 
* match_arrival_departure_flag      : string 
* birth_year                        : integer 
* allowed_date                      : date 
* gender                            : string 
* ins_number                        : integer 
* airline                           : string 
* admission_number                  : integer 
* flight_number                     : string 
* visa_type                         : string 
* arrival_year                      : integer 
* arrival_month                     : integer 
* arrival_day                       : integer
* departure_year                    : integer 
* departure_month                   : integer 
* departure_day                     : integer 
* date_added_year                   : string
* date_added_month                  : string 
* date_added_day                    : string 
* date_added                        : string 
* allowed_date_year                 : integer 
* allowed_date_month                : integer 
* allowed_date_day                  : integer 


## **DIM Tables**

### **modes**
* mode_id                           : integer 
* mode_name                         : string 

### **countries**
* country_id                        : integer 
* country_name                      : string

### **states**
* state_code                        : string 
* state_name                        : string 
* male_population                   : long 
* female_population                 : long 
* total_population                  : long 
* veterans                          : long 
* foreign_born                      : long 
* american_indian_and_alaska_native : long 
* asian                             : long 
* black_or_african_american         : long 
* hispanic_or_latino                : long 
* white                             : long 

### **visa**
* visa_code                         : integer 
* visa_categorie                    : string 
* visa_type                         : string 
* visa_type_description             : string 

### **airports**
* airport_id                        : string 
* airport_name                      : string 

### **airport_data**
* ent_id                            : string 
* type                              : string 
* name                              : string 
* elevation_ft                      : integer 
* continent                         : string 
* iso_country                       : string 
* iso_region                        : string 
* municipality                      : string 
* gps_code                          : string 
* iata_code                         : string 
* local_code                        : string 
* coordinates                       : string 
* lat                               : float 
* lng                               : float 

### **cities**
* city                              : string 
* city_ascii                        : string 
* state_id                          : string 
* state_name                        : string 
* county_fips                       : integer 
* county_name                       : string 
* lat                               : float 
* lng                               : float 
* population                        : integer 
* density                           : integer 
* source                            : string 
* military                          : string 
* incorporated                      : string 
* timezone                          : string 
* ranking                           : integer 
* zips                              : string 
* id                                : integer 

### **temperature**
* date                              : date 
* average_temperature               : float 
* average_temperature_uncertainty   : float 
* city                              : string 
* country                           : string 
* latitude                          : string
* longitude                         : string 
* year                              : integer 
* month                             : integer 
* day                               : integer 
* lat                               : float 
* lng                               : float