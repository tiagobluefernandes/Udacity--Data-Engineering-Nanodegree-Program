# Capstone Project - Initial Data Dictionary

## Initial Data 
### File: immigration_data_sample.csv OR sas_data folder with parquet files -> Dataframe: inmigration
 * cicid                           : integer - CIC id
 * i94yr                           : integer - 4 digit year
 * i94mon                          : integer - Numeric month
 * i94cit                          : integer - Country id
 * i94res                          : integer - Country id (duplicated column)
 * i94port                         : string  - Airport id code
 * arrdate                         : integer - Arrival date in the USA (unformatted)
 * i94mode                         : integer - Mode id number
 * i94addr                         : string  - State name abbreviation
 * depdate                         : integer - Departure date in the USA (unformatted)
 * i94bir                          : integer - Age of respondent in years
 * i94visa                         : integer - Visa id number
 * count                           : integer - Used for summary statistics
 * dtadfile                        : integer - Character Date Field; date added to I-94 Files (CIC does not use)
 * visapost                        : string  - Department of State where where Visa was issued (CIC does not use)
 * occup                           : string  - Occupation that will be performed in U.S.  (CIC does not use)
 * entdepa                         : string  - Arrival Flag; admitted or paroled into the U.S. (CIC does not use)
 * entdepd                         : string  - Departure Flag; departed, lost I-94 or is deceased (CIC does not use)
 * entdepu                         : string  - Update Flag (values: Y, U or None); either apprehended, overstayed, adjusted to perm residence (CIC does not use)
 * matflag                         : string  - Match flag (Match of arrival and departure records)
 * biryear                         : integer - 4 digit year of birth
 * dtaddto                         : string  - Character Date Field; date to which admitted to U.S.; allowed to stay until (CIC does not use)
 * gender                          : string  - Non-immigrant sex
 * insnum                          : integer - INS number (there are values that are not integers)
 * airline                         : string  - Airline used to arrive in U.S.
 * admnum                          : integer - Admission Number
 * fltno                           : integer - Flight number of Airline used to arrive in U.S. 
 * visatype                        : string  - Class of admission legally admitting the non-immigrant to temporarily stay in U.S.
### File: us-cities-demographics -> Dataframe: cities
 * city                            : string  - City name
 * state                           : string  - State name
 * median_age                      : float   - Median Age by city
 * male_population                 : integer - Male population by city
 * female_population               : integer - Female population by city
 * total_population                : integer - Total population by city
 * number_of_veterans              : integer - Number of veterans by city
 * foreign_born                    : integer - Number of people who were born in another country and currently live in the US
 * average_household_size          : float   - Average number of people who occupy a single housing unit
 * state_id                        : string  - State name abbreviation
 * race                            : string  - Race group name (1 out of 5 groups) 
 * count                           : integer - Number of people from a race by city
### File: airport-codes_csv -> Dataframe: airport_data
 * ident                           : string  - Airport id
 * type                            : string  - Airport size
 * name                            : string  - Airport name
 * elevation_ft                    : float   - Airport elevation in feet
 * continent                       : string  - Airport continent
 * iso_country                     : string  - Airport country (ISO-2)
 * iso_region                      : string  - Airport country and region (ISO-2)
 * municipality                    : string  - Airport city
 * gps_code                        : string  - Airport gps code
 * iata_code                       : string  - IATA code
 * local_code                      : string  - Local code
 * coordinates                     : string  - Airport Latitude and Longitude coordinates
### File: visa-codes -> Dataframe: visa
 * visa_code                       : integer - Visa id number
 * visa_categorie                  : string  - Visa id name
 * visa_type                       : string  - Visa type id
 * visa_type_description           : string  - Visa type description
### File: airports -> Dataframe: airports
 * airport_id                      : string  - Airport id code
 * airport_name                    : string  - Airport name
### File: states -> Dataframe: states
 * state_id                        : string  - State name abbreviation
 * state_name                      : string  - State name
### File: countries -> Dataframe: countries
 * country_id                      : integer - Country id number
 * country_name                    : string  - Country name
### File: modes -> Dataframe: modes
 * mode_id                         : integer - Mode id number
 * mode_name                       : string  - Mode name
### File: GlobalLandTemperaturesByCity -> Dataframe: temperature
 * dt                              : date    - Date
 * average_temperature             : float   - Average Temperature
 * average_temperature_uncertainty : float   - Average Temperature Uncertainty
 * city                            : string  - City
 * country                         : string  - Country
 * latitude                        : string  - Latitude
 * longitude                       : string  - Longitude
### File: uscities -> Dataframe: cities_coord
 * check 'https://simplemaps.com/data/us-cities' for details
