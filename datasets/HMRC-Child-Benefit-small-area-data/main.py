from gssutils import * 
import json 
import os
from urllib.parse import urljoin
from gssutils.metadata import THEME
import numpy as np
import math
import numpy as np

trace = TransformTrace()
df = pd.DataFrame()

info = json.load(open('info.json')) 
#etl_title = info["Name"] 
#etl_publisher = info["Producer"][0] 
#print("Publisher: " + etl_publisher) 
#print("Title: " + etl_title) 

scraper = Scraper(seed="info.json")   
scraper 

scraper.select_dataset(title=lambda x: x.lower().startswith('child benefit small area statistics: august 2019'))
scraper

""
# #Post processing

def post_processing_dataframe(df):
    df.rename(columns={'OBS' : 'Value', 'DATAMARKER' : 'Marker'}, inplace=True)
    df['Period'] = df['Period'].str[-4:]
    df["Period"] = df["Period"].map(lambda x: "2019" if x == "gdom" else "2019")
    
    #removing hidden cells 
    df.drop(df[(( df['Value']  == "") | ((df['Value'] == 0) & (df['TEMP - AGE, GENDER, FAMILY SIZE'] == "" )))].index, inplace = True) 
    df.drop(df[(( df['TEMP - DEFINED BY']  == "") & (df['TEMP - AGE, GENDER, FAMILY SIZE'] == ""))].index, inplace = True)


    df['Age'] = df["TEMP - AGE, GENDER, FAMILY SIZE"].map(lambda x: "total" if x == "" 
                                                          else ("under-5" if x == "Under 5"
                                                               else ("5-to-10" if x == "5 to 10"
                                                                    else ('11-to-15' if x == "11 to 15" 
                                                                          else("16-to-19" if x == "16 to 19" else "total")))))
                                                                         
    
    df["Gender"] = df["TEMP - AGE, GENDER, FAMILY SIZE"].map(lambda x: "T" if x == "" 
                                                             else ("M" if x == "Boys"
                                                                  else ("F" if x == "Girls" 
                                                                        else "U" if x == "Unknown" else "T")))
                                                            
    df["Family Size"] = df["TEMP - AGE, GENDER, FAMILY SIZE"].map(lambda x: "total" if x == " " 
                                                                  else ("one-child" if x == "One child" 
                                                                        else("two-children" if x == "Two children" 
                                                                             else("three-or-more-children" if x == "Three or more\nchildren" 
                                                                                  else ("three-or-more-children" if x == "Three or more children" 
                                                                                        else "total" if x == "All families" else "total")))))
    


    df["Geography Level"] = df["Geography Level"].map(lambda x: "lower-layer-super-output-area" if x == "LSOA code" 
                                                      else ("data-zone" if x == "Data Zone code" 
                                                            else ("region" if x == "Area Code1"else "unknown"  )))
    
    
    df["Unit"] = df["Unit"].map(lambda x: "children" if x == "Number of children for whom Child Benefit is received" else ("families" if x == "Number of families in receipt of Child Benefit" else  "UNKNOWN"))
    df["Measure Type"] = "Count"
    df['Value'] = df['Value'].astype(int)
    return df

""
#Distribution 2: 2019 - Number of families and children in a live Child Benefit award by electoral ward
# RUNNING LAST DUE TO SIZE 

""
#Distribution 3: 2019 - Number of families and children in a live Child Benefit award
tabs_region = { tab.name: tab for tab in scraper.distributions[2].as_databaker() }
tab = tabs_region["Regions (GB) "]

period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
defined_by = tab.excel_ref('E5').expand(RIGHT).is_not_blank()
age_gender_family_size = tab.excel_ref('E6').expand(RIGHT)
geography_code = tab.filter(contains_string('Area Code1')).shift(0,4).expand(DOWN)
temp_missing_geography_codes = tab.excel_ref('C7').expand(DOWN).is_not_blank()
geography_level = tab.excel_ref('B4')
unit = tab.excel_ref('D4').expand(RIGHT).is_not_blank()
observations = age_gender_family_size.fill(DOWN).is_not_blank()


dimensions = [
    HDim(period, 'Period', CLOSEST, LEFT),
    HDim(geography_code, 'Geography Code', DIRECTLY, LEFT),
    HDim(defined_by, 'TEMP - DEFINED BY', CLOSEST, LEFT),
    HDim(age_gender_family_size, 'TEMP - AGE, GENDER, FAMILY SIZE', DIRECTLY, ABOVE),
    HDim(temp_missing_geography_codes, "TEMP - Missing area code", CLOSEST, ABOVE),
    HDim(geography_level, "Geography Level", CLOSEST, LEFT),
    HDim(unit, "Unit", CLOSEST, LEFT),
    ]
cs = ConversionSegment(tab, dimensions, observations)
region = cs.topandas()
post_processing_dataframe(region)

""
#Distribution 4: 2019 - East Midlands 
tabs_east_midlands = { tab.name: tab for tab in scraper.distributions[3].as_databaker() }
tab = tabs_east_midlands["East Midlands"]
tidied_sheets = {}
tidy_sheet_list = [] 
cs_list = [] 

tab_length = len(tab.excel_ref('A')) 
batch_number = 10 
number_of_iterations = math.ceil(tab_length/batch_number) 

# +
for i in range(0, number_of_iterations):
    Min = str(7 + batch_number * i)  # data starts on row 4
    Max = str(int(Min) + batch_number - 1)

    period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
    defined_by = tab.filter(contains_string('All Children')).expand(RIGHT)
    age_gender_family_size = tab.filter(contains_string('All Children')).shift(0,1).expand(RIGHT) 
    geography_code = tab.excel_ref('E'+Min+':E'+Max).is_not_blank()
    temp_missing_geography_codes = tab.excel_ref('B'+Min+':B'+Max).is_not_blank()
    geography_level = tab.excel_ref('E4')
    unit = tab.excel_ref('H4').expand(RIGHT).is_not_blank()
    observations = geography_code.waffle(age_gender_family_size) 
    
    dimensions = [
        HDim(period, 'Period', CLOSEST, LEFT),
        HDim(geography_code, 'Geography Code', DIRECTLY, LEFT),
        HDim(defined_by, 'TEMP - DEFINED BY', CLOSEST, LEFT),
        HDim(age_gender_family_size, 'TEMP - AGE, GENDER, FAMILY SIZE', DIRECTLY, ABOVE),
        HDim(temp_missing_geography_codes, "TEMP - Missing area code", CLOSEST, ABOVE),
        HDim(geography_level, "Geography Level", CLOSEST, LEFT),
        HDim(unit, "Unit", CLOSEST, LEFT),
        ]
                
    if len(observations) != 0: # only use ConversionSegment if there is data
        cs_iteration = ConversionSegment(tab, dimensions, observations) # creating the conversionsegment
        tidy_sheet_iteration = cs_iteration.topandas() # turning conversionsegment into a pandas dataframe
        cs_list.append(cs_iteration) # add to list
        tidy_sheet_list.append(tidy_sheet_iteration) # add to list

east_midlands = pd.concat(tidy_sheet_list, sort=False)
post_processing_dataframe(east_midlands)        
        
# -

""
#Distribution 5: 2019 - East of England 
tabs_east_of_england = { tab.name: tab for tab in scraper.distributions[4].as_databaker() }
tab = tabs_east_of_england["East of England"]
tidied_sheets = {}
tidy_sheet_list = [] 
cs_list = [] 

tab_length = len(tab.excel_ref('A')) 
batch_number = 10 
number_of_iterations = math.ceil(tab_length/batch_number) 

# +
for i in range(0, number_of_iterations):
    Min = str(7 + batch_number * i)  # data starts on row 4
    Max = str(int(Min) + batch_number - 1)

    period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
    defined_by = tab.filter(contains_string('All Children')).expand(RIGHT)
    age_gender_family_size = tab.filter(contains_string('All Children')).shift(0,1).expand(RIGHT) 
    geography_code = tab.excel_ref('E'+Min+':E'+Max).is_not_blank()
    temp_missing_geography_codes = tab.excel_ref('B'+Min+':B'+Max).is_not_blank()
    geography_level = tab.excel_ref('E4')
    unit = tab.excel_ref('F4').expand(RIGHT).is_not_blank()
    #savepreviewhtml(unit, fname= tab.name + ".html") 
    observations = geography_code.waffle(age_gender_family_size) 
    
    dimensions = [
        HDim(period, 'Period', CLOSEST, LEFT),
        HDim(geography_code, 'Geography Code', DIRECTLY, LEFT),
        HDim(defined_by, 'TEMP - DEFINED BY', CLOSEST, LEFT),
        HDim(age_gender_family_size, 'TEMP - AGE, GENDER, FAMILY SIZE', DIRECTLY, ABOVE),
        HDim(temp_missing_geography_codes, "TEMP - Missing area code", CLOSEST, ABOVE),
        HDim(geography_level, "Geography Level", CLOSEST, LEFT),
        HDim(unit, "Unit", CLOSEST, LEFT),
        ]
                
    if len(observations) != 0: # only use ConversionSegment if there is data
        cs_iteration = ConversionSegment(tab, dimensions, observations) # creating the conversionsegment
        tidy_sheet_iteration = cs_iteration.topandas() # turning conversionsegment into a pandas dataframe
        cs_list.append(cs_iteration) # add to list
        tidy_sheet_list.append(tidy_sheet_iteration) # add to list
        
east_of_england = pd.concat(tidy_sheet_list, sort=False)
post_processing_dataframe(east_of_england)
# -

#Distribution 6: 2019 - London
tabs_london = { tab.name: tab for tab in scraper.distributions[5].as_databaker() }
tab = tabs_london["London"]
tidied_sheets = {}
tidy_sheet_list = [] 
cs_list = [] 

tab_length = len(tab.excel_ref('B')) 
batch_number = 10 
number_of_iterations = math.ceil(tab_length/batch_number) 

# +
for i in range(0, number_of_iterations):
    Min = str(7 + batch_number * i)  # data starts on row 7
    Max = str(int(Min) + batch_number - 1)

    period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
    defined_by = tab.excel_ref('G5').expand(RIGHT)
    age_gender_family_size = tab.excel_ref('G6').expand(RIGHT)
    geography_code = tab.excel_ref('E'+Min+':E'+Max).is_not_blank()
    temp_missing_geography_codes = tab.excel_ref('B'+Min+':B'+Max).is_not_blank()
    geography_level = tab.excel_ref('E4')
    unit = tab.excel_ref('G4').expand(RIGHT).is_not_blank()
    observations = geography_code.waffle(age_gender_family_size) 
    
    dimensions = [
        HDim(period, 'Period', CLOSEST, LEFT),
        HDim(geography_code, 'Geography Code', DIRECTLY, LEFT),
        HDim(defined_by, 'TEMP - DEFINED BY', CLOSEST, LEFT),
        HDim(age_gender_family_size, 'TEMP - AGE, GENDER, FAMILY SIZE', DIRECTLY, ABOVE),
        HDim(temp_missing_geography_codes, "TEMP - Missing area code", CLOSEST, ABOVE),
        HDim(geography_level, "Geography Level", CLOSEST, LEFT),
        HDim(unit, "Unit", CLOSEST, LEFT),
        ]    
    
    if len(observations) != 0: # only use ConversionSegment if there is data
        cs_iteration = ConversionSegment(tab, dimensions, observations) # creating the conversionsegment
        tidy_sheet_iteration = cs_iteration.topandas() # turning conversionsegment into a pandas dataframe
        cs_list.append(cs_iteration) # add to list
        tidy_sheet_list.append(tidy_sheet_iteration) # add to list


london = pd.concat(tidy_sheet_list, sort=False)
post_processing_dataframe(london)
# -



""
#Distribution 7: 2019 - North East
tabs_north_east = { tab.name: tab for tab in scraper.distributions[6].as_databaker() }
tab = tabs_north_east["North East "]
tidied_sheets = {}
tidy_sheet_list = [] 
cs_list = [] 

tab_length = len(tab.excel_ref('A')) 
batch_number = 10 
number_of_iterations = math.ceil(tab_length/batch_number) 

# +
for i in range(0, number_of_iterations):
    Min = str(7 + batch_number * i)  # data starts on row 4
    Max = str(int(Min) + batch_number - 1)

    period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
    defined_by = tab.filter(contains_string('All Children')).expand(RIGHT)
    age_gender_family_size = tab.filter(contains_string('All Children')).shift(0,1).expand(RIGHT) 
    geography_code = tab.excel_ref('E'+Min+':E'+Max).is_not_blank()
    temp_missing_geography_codes = tab.excel_ref('B'+Min+':B'+Max).is_not_blank()
    geography_level = tab.excel_ref('E4')
    unit = tab.excel_ref('F4').expand(RIGHT).is_not_blank()
    #savepreviewhtml(unit, fname= tab.name + ".html") 
    observations = geography_code.waffle(age_gender_family_size) 
    
    dimensions = [
        HDim(period, 'Period', CLOSEST, LEFT),
        HDim(geography_code, 'Geography Code', DIRECTLY, LEFT),
        HDim(defined_by, 'TEMP - DEFINED BY', CLOSEST, LEFT),
        HDim(age_gender_family_size, 'TEMP - AGE, GENDER, FAMILY SIZE', DIRECTLY, ABOVE),
        HDim(temp_missing_geography_codes, "TEMP - Missing area code", CLOSEST, ABOVE),
        HDim(geography_level, "Geography Level", CLOSEST, LEFT),
        HDim(unit, "Unit", CLOSEST, LEFT),
        ]
                
    if len(observations) != 0: # only use ConversionSegment if there is data
        cs_iteration = ConversionSegment(tab, dimensions, observations) # creating the conversionsegment
        tidy_sheet_iteration = cs_iteration.topandas() # turning conversionsegment into a pandas dataframe
        cs_list.append(cs_iteration) # add to list
        tidy_sheet_list.append(tidy_sheet_iteration) # add to list
        
north_east = pd.concat(tidy_sheet_list, sort=False)
post_processing_dataframe(north_east)
# -

""
#Distribution 8: 2019 - North West
tabs_north_west = { tab.name: tab for tab in scraper.distributions[7].as_databaker() }
tab = tabs_north_west["North West "]
tidied_sheets = {}
tidy_sheet_list = [] 
cs_list = [] 

tab_length = len(tab.excel_ref('A')) 
batch_number = 10 
number_of_iterations = math.ceil(tab_length/batch_number) 

# +
for i in range(0, number_of_iterations):
    Min = str(7 + batch_number * i)  # data starts on row 4
    Max = str(int(Min) + batch_number - 1)

    period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
    defined_by = tab.filter(contains_string('All Children')).expand(RIGHT)
    age_gender_family_size = tab.filter(contains_string('All Children')).shift(0,1).expand(RIGHT) 
    geography_code = tab.excel_ref('E'+Min+':E'+Max).is_not_blank()
    temp_missing_geography_codes = tab.excel_ref('B'+Min+':B'+Max).is_not_blank()
    geography_level = tab.excel_ref('E4')
    unit = tab.excel_ref('F4').expand(RIGHT).is_not_blank()
    #savepreviewhtml(unit, fname= tab.name + ".html") 
    observations = geography_code.waffle(age_gender_family_size) 
    
    dimensions = [
        HDim(period, 'Period', CLOSEST, LEFT),
        HDim(geography_code, 'Geography Code', DIRECTLY, LEFT),
        HDim(defined_by, 'TEMP - DEFINED BY', CLOSEST, LEFT),
        HDim(age_gender_family_size, 'TEMP - AGE, GENDER, FAMILY SIZE', DIRECTLY, ABOVE),
        HDim(temp_missing_geography_codes, "TEMP - Missing area code", CLOSEST, ABOVE),
        HDim(geography_level, "Geography Level", CLOSEST, LEFT),
        HDim(unit, "Unit", CLOSEST, LEFT),
        ]
                
    if len(observations) != 0: # only use ConversionSegment if there is data
        cs_iteration = ConversionSegment(tab, dimensions, observations) # creating the conversionsegment
        tidy_sheet_iteration = cs_iteration.topandas() # turning conversionsegment into a pandas dataframe
        cs_list.append(cs_iteration) # add to list
        tidy_sheet_list.append(tidy_sheet_iteration) # add to list

north_west = pd.concat(tidy_sheet_list, sort=False)
post_processing_dataframe(north_west)
# -

""
#Distribution 9 - Scottish Data Zone 
tabs_scotland = { tab.name: tab for tab in scraper.distributions[8].as_databaker() }
tab = tabs_scotland["Scotland"]
tidied_sheets = {}
tidy_sheet_list = [] 
cs_list = [] 

tab_length = len(tab.excel_ref('A')) 
batch_number = 10 
number_of_iterations = math.ceil(tab_length/batch_number) 

# +
for i in range(0, number_of_iterations):
    Min = str(7 + batch_number * i)  # data starts on row 4
    Max = str(int(Min) + batch_number - 1)

    period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
    defined_by = tab.filter(contains_string('All Children')).expand(RIGHT)
    age_gender_family_size = tab.filter(contains_string('All Children')).shift(0,1).expand(RIGHT) 
    geography_code = tab.excel_ref('E'+Min+':E'+Max).is_not_blank()
    temp_missing_geography_codes = tab.excel_ref('C'+Min+':C'+Max).is_not_blank()
    geography_level = tab.excel_ref('E4')
    unit = tab.excel_ref('G4').expand(RIGHT).is_not_blank()
    observations = geography_code.waffle(age_gender_family_size) 
        
    dimensions = [
        HDim(period, 'Period', CLOSEST, LEFT),
        HDim(geography_code, 'Geography Code', DIRECTLY, LEFT),
        HDim(defined_by, 'TEMP - DEFINED BY', CLOSEST, LEFT),
        HDim(age_gender_family_size, 'TEMP - AGE, GENDER, FAMILY SIZE', DIRECTLY, ABOVE),
        HDim(temp_missing_geography_codes, "TEMP - Missing area code", CLOSEST, ABOVE),
        HDim(geography_level, "Geography Level", CLOSEST, LEFT),
        HDim(unit, "Unit", CLOSEST, LEFT),
        ]
                
    if len(observations) != 0: # only use ConversionSegment if there is data
        cs_iteration = ConversionSegment(tab, dimensions, observations) # creating the conversionsegment
        tidy_sheet_iteration = cs_iteration.topandas() # turning conversionsegment into a pandas dataframe
        cs_list.append(cs_iteration) # add to list
        tidy_sheet_list.append(tidy_sheet_iteration) # add to list
        
scotland = pd.concat(tidy_sheet_list, sort=False)
post_processing_dataframe(scotland)        
# -

""
#Distribution 10: 2019 - South East
tabs_south_east = { tab.name: tab for tab in scraper.distributions[9].as_databaker() }
tab = tabs_south_east["South East"]
tidied_sheets = {}
tidy_sheet_list = [] 
cs_list = [] 

tab_length = len(tab.excel_ref('A')) 
batch_number = 10 
number_of_iterations = math.ceil(tab_length/batch_number) 

for i in range(0, number_of_iterations):
    Min = str(7 + batch_number * i)  # data starts on row 4
    Max = str(int(Min) + batch_number - 1)

    period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
    defined_by = tab.filter(contains_string('All Children')).expand(RIGHT)
    age_gender_family_size = tab.filter(contains_string('All Children')).shift(0,1).expand(RIGHT) 
    geography_code = tab.excel_ref('E'+Min+':E'+Max).is_not_blank()
    temp_missing_geography_codes = tab.excel_ref('B'+Min+':B'+Max).is_not_blank()
    geography_level = tab.excel_ref('E4')
    unit = tab.excel_ref('F4').expand(RIGHT).is_not_blank()
    #savepreviewhtml(unit, fname= tab.name + ".html") 
    observations = geography_code.waffle(age_gender_family_size) 
    
    dimensions = [
        HDim(period, 'Period', CLOSEST, LEFT),
        HDim(geography_code, 'Geography Code', DIRECTLY, LEFT),
        HDim(defined_by, 'TEMP - DEFINED BY', CLOSEST, LEFT),
        HDim(age_gender_family_size, 'TEMP - AGE, GENDER, FAMILY SIZE', DIRECTLY, ABOVE),
        HDim(temp_missing_geography_codes, "TEMP - Missing area code", CLOSEST, ABOVE),
        HDim(geography_level, "Geography Level", CLOSEST, LEFT),
        HDim(unit, "Unit", CLOSEST, LEFT),
        ]
                
    if len(observations) != 0: # only use ConversionSegment if there is data
        cs_iteration = ConversionSegment(tab, dimensions, observations) # creating the conversionsegment
        tidy_sheet_iteration = cs_iteration.topandas() # turning conversionsegment into a pandas dataframe
        cs_list.append(cs_iteration) # add to list
        tidy_sheet_list.append(tidy_sheet_iteration) # add to list

south_east = pd.concat(tidy_sheet_list, sort=False)
post_processing_dataframe(south_east)

""
 #Distribution 11: 2019 - South West
tabs_south_west = { tab.name: tab for tab in scraper.distributions[10].as_databaker() }
tab = tabs_south_west["South West"]
tidied_sheets = {}
tidy_sheet_list = [] 
cs_list = [] 

tab_length = len(tab.excel_ref('A')) 
batch_number = 10 
number_of_iterations = math.ceil(tab_length/batch_number) 

for i in range(0, number_of_iterations):
    Min = str(7 + batch_number * i)  # data starts on row 4
    Max = str(int(Min) + batch_number - 1)

    period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
    defined_by = tab.filter(contains_string('All Children')).expand(RIGHT)
    age_gender_family_size = tab.filter(contains_string('All Children')).shift(0,1).expand(RIGHT) 
    geography_code = tab.excel_ref('E'+Min+':E'+Max).is_not_blank()
    temp_missing_geography_codes = tab.excel_ref('B'+Min+':B'+Max).is_not_blank()
    geography_level = tab.excel_ref('E4')
    unit = tab.excel_ref('F4').expand(RIGHT).is_not_blank()
    #savepreviewhtml(unit, fname= tab.name + ".html") 
    observations = geography_code.waffle(age_gender_family_size) 
    
    dimensions = [
        HDim(period, 'Period', CLOSEST, LEFT),
        HDim(geography_code, 'Geography Code', DIRECTLY, LEFT),
        HDim(defined_by, 'TEMP - DEFINED BY', CLOSEST, LEFT),
        HDim(age_gender_family_size, 'TEMP - AGE, GENDER, FAMILY SIZE', DIRECTLY, ABOVE),
        HDim(temp_missing_geography_codes, "TEMP - Missing area code", CLOSEST, ABOVE),
        HDim(geography_level, "Geography Level", CLOSEST, LEFT),
        HDim(unit, "Unit", CLOSEST, LEFT),
        ]
                
    if len(observations) != 0: # only use ConversionSegment if there is data
        cs_iteration = ConversionSegment(tab, dimensions, observations) # creating the conversionsegment
        tidy_sheet_iteration = cs_iteration.topandas() # turning conversionsegment into a pandas dataframe
        cs_list.append(cs_iteration) # add to list
        tidy_sheet_list.append(tidy_sheet_iteration) # add to list

south_west = pd.concat(tidy_sheet_list, sort=False)
post_processing_dataframe(south_west)

""


""
#Distribution 12: 2019 - Wales
tabs_wales = { tab.name: tab for tab in scraper.distributions[11].as_databaker() }
tab = tabs_wales["Wales "]
tidied_sheets = {}
tidy_sheet_list = [] 
cs_list = [] 

tab_length = len(tab.excel_ref('A')) 
batch_number = 10 
number_of_iterations = math.ceil(tab_length/batch_number) 

for i in range(0, number_of_iterations):
    Min = str(7 + batch_number * i)  # data starts on row 4
    Max = str(int(Min) + batch_number - 1)

    period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
    defined_by = tab.filter(contains_string('All Children')).expand(RIGHT)
    age_gender_family_size = tab.filter(contains_string('All Children')).shift(0,1).expand(RIGHT) 
    geography_code = tab.excel_ref('E'+Min+':E'+Max).is_not_blank()
    temp_missing_geography_codes = tab.excel_ref('B'+Min+':B'+Max).is_not_blank()
    geography_level = tab.excel_ref('E4')
    unit = tab.excel_ref('F4').expand(RIGHT).is_not_blank()
    #savepreviewhtml(unit, fname= tab.name + ".html") 
    observations = geography_code.waffle(age_gender_family_size) 
    
    dimensions = [
        HDim(period, 'Period', CLOSEST, LEFT),
        HDim(geography_code, 'Geography Code', DIRECTLY, LEFT),
        HDim(defined_by, 'TEMP - DEFINED BY', CLOSEST, LEFT),
        HDim(age_gender_family_size, 'TEMP - AGE, GENDER, FAMILY SIZE', DIRECTLY, ABOVE),
        HDim(temp_missing_geography_codes, "TEMP - Missing area code", CLOSEST, ABOVE),
        HDim(geography_level, "Geography Level", CLOSEST, LEFT),
        HDim(unit, "Unit", CLOSEST, LEFT),
        ]
                
    if len(observations) != 0: # only use ConversionSegment if there is data
        cs_iteration = ConversionSegment(tab, dimensions, observations) # creating the conversionsegment
        tidy_sheet_iteration = cs_iteration.topandas() # turning conversionsegment into a pandas dataframe
        cs_list.append(cs_iteration) # add to list
        tidy_sheet_list.append(tidy_sheet_iteration) # add to list

wales = pd.concat(tidy_sheet_list, sort=False)
post_processing_dataframe(wales)

""
#Distribution 13: 2019 - West Midlands
tabs_west_midlands = { tab.name: tab for tab in scraper.distributions[12].as_databaker() }
tab = tabs_west_midlands["West Midlands"]
tidied_sheets = {}
tidy_sheet_list = [] 
cs_list = [] 

tab_length = len(tab.excel_ref('A')) 
batch_number = 10 
number_of_iterations = math.ceil(tab_length/batch_number) 

for i in range(0, number_of_iterations):
    Min = str(7 + batch_number * i)  # data starts on row 4
    Max = str(int(Min) + batch_number - 1)

    period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
    defined_by = tab.filter(contains_string('All Children')).expand(RIGHT)
    age_gender_family_size = tab.filter(contains_string('All Children')).shift(0,1).expand(RIGHT) 
    geography_code = tab.excel_ref('E'+Min+':E'+Max).is_not_blank()
    temp_missing_geography_codes = tab.excel_ref('B'+Min+':B'+Max).is_not_blank()
    geography_level = tab.excel_ref('E4')
    unit = tab.excel_ref('F4').expand(RIGHT).is_not_blank()
    #savepreviewhtml(unit, fname= tab.name + ".html") 
    observations = geography_code.waffle(age_gender_family_size) 
    
    dimensions = [
        HDim(period, 'Period', CLOSEST, LEFT),
        HDim(geography_code, 'Geography Code', DIRECTLY, LEFT),
        HDim(defined_by, 'TEMP - DEFINED BY', CLOSEST, LEFT),
        HDim(age_gender_family_size, 'TEMP - AGE, GENDER, FAMILY SIZE', DIRECTLY, ABOVE),
        HDim(temp_missing_geography_codes, "TEMP - Missing area code", CLOSEST, ABOVE),
        HDim(geography_level, "Geography Level", CLOSEST, LEFT),
        HDim(unit, "Unit", CLOSEST, LEFT),
        ]
                
    if len(observations) != 0: # only use ConversionSegment if there is data
        cs_iteration = ConversionSegment(tab, dimensions, observations) # creating the conversionsegment
        tidy_sheet_iteration = cs_iteration.topandas() # turning conversionsegment into a pandas dataframe
        cs_list.append(cs_iteration) # add to list
        tidy_sheet_list.append(tidy_sheet_iteration) # add to list

west_midlands = pd.concat(tidy_sheet_list, sort=False)
post_processing_dataframe(west_midlands)

""
#Distribution 14: 2019 - Yorkshire and the Humber
tabs_yorkshire_humber = { tab.name: tab for tab in scraper.distributions[13].as_databaker() }
tab = tabs_yorkshire_humber["Yorkshire and the Humber"]
tidied_sheets = {}
tidy_sheet_list = [] 
cs_list = [] 

tab_length = len(tab.excel_ref('A')) 
batch_number = 10 
number_of_iterations = math.ceil(tab_length/batch_number) 

for i in range(0, number_of_iterations):
    Min = str(7 + batch_number * i)  # data starts on row 4
    Max = str(int(Min) + batch_number - 1)

    period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
    defined_by = tab.filter(contains_string('All Children')).expand(RIGHT)
    age_gender_family_size = tab.filter(contains_string('All Children')).shift(0,1).expand(RIGHT) 
    geography_code = tab.excel_ref('E'+Min+':E'+Max).is_not_blank()
    temp_missing_geography_codes = tab.excel_ref('B'+Min+':B'+Max).is_not_blank()
    geography_level = tab.excel_ref('E4')
    unit = tab.excel_ref('F4').expand(RIGHT).is_not_blank()
    #savepreviewhtml(unit, fname= tab.name + ".html") 
    observations = geography_code.waffle(age_gender_family_size) 
    
    dimensions = [
        HDim(period, 'Period', CLOSEST, LEFT),
        HDim(geography_code, 'Geography Code', DIRECTLY, LEFT),
        HDim(defined_by, 'TEMP - DEFINED BY', CLOSEST, LEFT),
        HDim(age_gender_family_size, 'TEMP - AGE, GENDER, FAMILY SIZE', DIRECTLY, ABOVE),
        HDim(temp_missing_geography_codes, "TEMP - Missing area code", CLOSEST, ABOVE),
        HDim(geography_level, "Geography Level", CLOSEST, LEFT),
        HDim(unit, "Unit", CLOSEST, LEFT),
        ]
                
    if len(observations) != 0: # only use ConversionSegment if there is data
        cs_iteration = ConversionSegment(tab, dimensions, observations) # creating the conversionsegment
        tidy_sheet_iteration = cs_iteration.topandas() # turning conversionsegment into a pandas dataframe
        cs_list.append(cs_iteration) # add to list
        tidy_sheet_list.append(tidy_sheet_iteration) # add to list

yorkshire_humber = pd.concat(tidy_sheet_list, sort=False)
post_processing_dataframe(yorkshire_humber)

""
#Distribution 2: 2019 - Number of families and children in a live Child Benefit award by electoral ward
tabs_electoral_ward = { tab.name: tab for tab in scraper.distributions[1].as_databaker() }
tab = tabs_electoral_ward["Electoral Ward"]
tidied_sheets = {}
tidy_sheet_list = [] 
cs_list = [] 

tab_length = len(tab.excel_ref('A')) 
batch_number = 10 
number_of_iterations = math.ceil(tab_length/batch_number) 

for i in range(0, number_of_iterations):
    Min = str(7 + batch_number * i)  # data starts on row 4
    Max = str(int(Min) + batch_number - 1)

    period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
    defined_by = tab.excel_ref('F5').expand(RIGHT).is_not_blank()
    age_gender_family_size = tab.excel_ref('F6').expand(RIGHT) 
    geography_code = tab.excel_ref('D'+Min+':D'+Max).is_not_blank()
    temp_missing_geography_codes = tab.excel_ref('B'+Min+':B'+Max).is_not_blank()
    geography_level = tab.excel_ref('D4')
    unit = tab.excel_ref('E4').expand(RIGHT).is_not_blank()
    observations = geography_code.waffle(age_gender_family_size) 
    
    dimensions = [
        HDim(period, 'Period', CLOSEST, LEFT),
        HDim(geography_code, 'Geography Code', DIRECTLY, LEFT),
        HDim(defined_by, 'TEMP - DEFINED BY', CLOSEST, LEFT),
        HDim(age_gender_family_size, 'TEMP - AGE, GENDER, FAMILY SIZE', DIRECTLY, ABOVE),
        HDim(temp_missing_geography_codes, "TEMP - Missing area code", CLOSEST, ABOVE),
        HDim(geography_level, "Geography Level", CLOSEST, LEFT),
        HDim(unit, "Unit", CLOSEST, LEFT),
        ]
                
    if len(observations) != 0: # only use ConversionSegment if there is data
        cs_iteration = ConversionSegment(tab, dimensions, observations) # creating the conversionsegment
        tidy_sheet_iteration = cs_iteration.topandas() # turning conversionsegment into a pandas dataframe
        cs_list.append(cs_iteration) # add to list
        tidy_sheet_list.append(tidy_sheet_iteration) # add to list

electoral_ward = pd.concat(tidy_sheet_list, sort=False)
post_processing_dataframe(electoral_ward)

""
#concatenating all the distributions togther - Easy to output all data togther once multiple measure types can be handeld
merged_data = pd.concat([region, east_midlands , east_of_england, london,  north_east, north_west, scotland, south_east, south_west, wales, west_midlands, yorkshire_humber, electoral_ward], ignore_index=True)

""
#checking for peace of mind 
numOfRows = merged_data.shape[0]
print ("Number of rows in dataframe : ", numOfRows)
# row in which value of 'Unit' column is children
seriesObj = merged_data.apply(lambda x: True if x['Unit'] == "children" else False , axis=1)
# Count number of True in series
numOfRows = len(seriesObj[seriesObj == True].index)
print('Number of Rows in dataframe in which Unit = children : ', numOfRows)
# row in which value of 'Unit' column is families
seriesObj = merged_data.apply(lambda x: True if x['Unit'] == "families" else False , axis=1)
# Count number of True in series
numOfRows = len(seriesObj[seriesObj == True].index)
print('Number of Rows in dataframe in which Unit = families : ', numOfRows)

""
#seperating out into two datasets depending on families and children unit

#create unique list of Unit's (children, familes)
unique_units = merged_data.Unit.unique()
print(unique_units)

#create a data frame dictionary to store data frames
DataFrameDict = {elem : pd.DataFrame for elem in unique_units}

for key in DataFrameDict.keys():
    DataFrameDict[key] = merged_data[:][merged_data.Unit == key]
tidy_children_stats_df = DataFrameDict['children']
tidy_families_stats_df = DataFrameDict['families']

""
tidy_children_stats_df = tidy_children_stats_df[['Period', 'Geography Code', 'Geography Level', 'Age', 'Gender', 'Value']]
tidy_families_stats_df = tidy_families_stats_df[['Period', 'Geography Code', 'Geography Level', 'Family Size', 'Value']]


""
# Output filenames
fn = ['children-observations.csv','families-observations.csv']
# Comments
co = [
    'Annual geographical estimates at Lower Super Output Area and Data Zone of the number of children claiming Child Benefit',
    'Annual geographical estimates at Lower Super Output Area and Data Zone of the number of families claiming Child Benefit'
]
# Description
de = [
    'Annual geographical estimates at Lower Super Output Area and Data Zone of the number of children claiming Child Benefit.',
    'Annual geographical estimates at Lower Super Output Area and Data Zone of the number of families claiming Child Benefit.'
]
# Title
ti = [
    'Child Benefit small area statistics - Children receiving Child benefit',
    'Child Benefit small area statistics - Families in receipt of Child benefit'
]
# Paths
pa = ['/children', '/families']

""
try:
    i = 0
    csvName = fn[i]
    out = Path('out')
    out.mkdir(exist_ok=True)
    tidy_children_stats_df.drop_duplicates().to_csv(out / csvName, index = False)
    tidy_children_stats_df.drop_duplicates().to_csv(out / (csvName + '.gz'), index = False, compression='gzip')
    
    scraper.dataset.family = 'towns-high-streets'
    scraper.dataset.description = scraper.dataset.description + '\n' + de[i]
    scraper.dataset.comment = co[i]
    scraper.dataset.title = ti[i]

    dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name)).lower() + pa[i]
    scraper.set_base_uri('http://gss-data.org.uk')
    scraper.set_dataset_id(dataset_path)

    csvw_transform = CSVWMapping()
    csvw_transform.set_csv(out / csvName)
    csvw_transform.set_mapping(json.load(open('info.json')))
    csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
    csvw_transform.write(out / f'{csvName}-metadata.json')

    with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
        metadata.write(scraper.generate_trig())
except Exception as s:
    print(str(s))

""
#changing unit to families for second output 
with open("info.json", "r") as read_file:
    data = json.load(read_file)
    print("Unit: ", data["transform"]["columns"]["Value"]["unit"] )
    data["transform"]["columns"]["Value"]["unit"] = "http://gss-data.org.uk/def/concept/measurement-units/families" 
    print("Unit changed to: ", data["transform"]["columns"]["Value"]["unit"] )

""
try:
    i = 1
    csvName = fn[i]
    out = Path('out')
    out.mkdir(exist_ok=True)
    tidy_families_stats_df.drop_duplicates().to_csv(out / csvName, index = False)
    tidy_families_stats_df.drop_duplicates().to_csv(out / (csvName + '.gz'), index = False, compression='gzip')
    
    scraper.dataset.family = 'towns-high-streets'
    scraper.dataset.description = scraper.dataset.description + '\n' + de[i]
    scraper.dataset.comment = co[i]
    scraper.dataset.title = ti[i]

    dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name)).lower() + pa[i]
    scraper.set_base_uri('http://gss-data.org.uk')
    scraper.set_dataset_id(dataset_path)

    csvw_transform = CSVWMapping()
    csvw_transform.set_csv(out / csvName)
    csvw_transform.set_mapping(data)
    csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
    csvw_transform.write(out / f'{csvName}-metadata.json')

    with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
        metadata.write(scraper.generate_trig())
except Exception as s:
    print(str(s))

""

