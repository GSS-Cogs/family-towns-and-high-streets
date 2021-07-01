#!/usr/bin/env python
# coding: utf-8

# In[215]:


from gssutils import *
import json
import os
from urllib.parse import urljoin
from gssutils.metadata import THEME
import numpy as np
import math
from pandas import ExcelWriter
import copy

trace = TransformTrace()
df = pd.DataFrame()

cubes = Cubes('info.json')

info = json.load(open('info.json'))
#etl_title = info["Name"]
#etl_publisher = info["Producer"][0]
#print("Publisher: " + etl_publisher)
#print("Title: " + etl_title)

scraper = Scraper(seed="info.json")
scraper


# In[216]:


scraper.select_dataset(title=lambda x: x.lower().startswith('child benefit small area statistics: august 2019'))
scraper


# In[217]:


# #Post processing

def post_processing_dataframe(df):
    df.rename(columns={'OBS' : 'Value', 'DATAMARKER' : 'Marker'}, inplace=True)
    df['Period'] = df['Period'].str[-4:]
    df["Period"] = df["Period"].map(lambda x: "year/2019" if x == "gdom" else "year/2019")

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
                                                            else ("electoral-ward" if x == "Area Code1"else "unknown"  )))



    df['Geography Level'] = df.apply(lambda x: 'country' if x['TEMP - Missing area code']== "United Kingdom"
                                             else ( 'country' if x['TEMP - Missing area code']== "Great Britain"
                                                   else ( 'country' if x['TEMP - Missing area code']== "England and Wales"
                                                         else ( 'country' if x['TEMP - Missing area code']== "England"
                                                               else ( 'country' if x['TEMP - Missing area code']== "Wales"
                                                                     else ( 'country' if x['TEMP - Missing area code']== "Scotland"
                                                                           else ( 'country' if x['TEMP - Missing area code']== "Northern Ireland"
                                                                                 else x['Geography Level'] )))))), axis=1)

    df['Geography Level'] = df.apply(lambda x: 'region' if x['TEMP - Missing area code']== "North East"
                                             else ( 'region' if x['TEMP - Missing area code']== "North West"
                                                   else ( 'region' if x['TEMP - Missing area code']== "Yorkshire and the Humber"
                                                         else ( 'region' if x['TEMP - Missing area code']== "East Midlands"
                                                               else ( 'region' if x['TEMP - Missing area code']== "West Midlands"
                                                                     else ( 'region' if x['TEMP - Missing area code']== "East of England"
                                                                           else ( 'region' if x['TEMP - Missing area code']== "London"
                                                                                 else ( 'region' if x['TEMP - Missing area code']== "South West"
                                                                                     else x['Geography Level'] ))))))), axis=1)


    df["Unit"] = df["Unit"].map(lambda x: "children" if x == "Number of children for whom Child Benefit is received" else ("families" if x == "Number of families in receipt of Child Benefit" else  "UNKNOWN"))
    df["Measure Type"] = "Count"
    df['Value'] = df['Value'].astype(int)
    return df


# In[218]:


#Distribution 2: 2019 - Number of families and children in a live Child Benefit award by electoral ward
# RUNNING LAST DUE TO SIZE


# In[219]:


#Distribution 3: 2019 - Number of families and children in a live Child Benefit award

distribution = scraper.distributions[2]
xls = pd.ExcelFile(distribution.downloadURL)
with ExcelWriter("temp.xls") as writer:
    for sheet in xls.sheet_names:
        if len(sheet) < 32:
            pd.read_excel(xls, sheet).to_excel(writer, sheet, index=False)
    writer.save()
tabs_region = loadxlstabs("temp.xls")

#tabs_region = { tab.name: tab for tab in scraper.distributions[2].as_databaker() }
tab = tabs_region[0]

period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
defined_by = tab.excel_ref('E5').expand(RIGHT).is_not_blank()
age_gender_family_size = tab.excel_ref('E6').expand(RIGHT)
geography_code = tab.filter(contains_string('Area Code1')).shift(0,4).expand(DOWN)
temp_missing_geography_codes = tab.excel_ref('C7').expand(DOWN).is_not_blank()
geography_level = tab.excel_ref('B4')
unit = tab.excel_ref('D4').expand(RIGHT).is_not_blank()
########################### values removed for now causing duplication
remove = tab.excel_ref('E7').expand(RIGHT)
remove_dups_for_now = remove.fill(DOWN).is_not_blank() - tab.excel_ref('B13').expand(RIGHT).expand(DOWN)
#################################
observations = age_gender_family_size.fill(DOWN).is_not_blank() - remove_dups_for_now
#savepreviewhtml(observations)


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


# In[220]:


#Distribution 4: 2019 - East Midlands

distribution = scraper.distributions[3]
xls = pd.ExcelFile(distribution.downloadURL)
with ExcelWriter("temp.xls") as writer:
    for sheet in xls.sheet_names:
        if len(sheet) < 32:
            pd.read_excel(xls, sheet).to_excel(writer, sheet, index=False)
    writer.save()
tabs_east_midlands = loadxlstabs("temp.xls")

#tabs_east_midlands = { tab.name: tab for tab in scraper.distributions[3].as_databaker() }
tab = tabs_east_midlands[0]
tidied_sheets = {}
tidy_sheet_list = []
cs_list = []

tab_length = len(tab.excel_ref('A'))
batch_number = 10
number_of_iterations = math.ceil(tab_length/batch_number)


# In[221]:


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

    if len(geography_code) != 0:
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


# In[222]:


#Distribution 5: 2019 - East of England

distribution = scraper.distributions[4]
xls = pd.ExcelFile(distribution.downloadURL)
with ExcelWriter("temp.xls") as writer:
    for sheet in xls.sheet_names:
        if len(sheet) < 32:
            pd.read_excel(xls, sheet).to_excel(writer, sheet, index=False)
    writer.save()
tabs_east_of_england = loadxlstabs("temp.xls")

#tabs_east_of_england = { tab.name: tab for tab in scraper.distributions[4].as_databaker() }
tab = tabs_east_of_england[0]
tidied_sheets = {}
tidy_sheet_list = []
cs_list = []

tab_length = len(tab.excel_ref('A'))
batch_number = 10
number_of_iterations = math.ceil(tab_length/batch_number)


# In[223]:


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

    if len(geography_code) != 0:

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


# In[224]:


#Distribution 6: 2019 - London

distribution = scraper.distributions[5]
xls = pd.ExcelFile(distribution.downloadURL)
with ExcelWriter("temp.xls") as writer:
    for sheet in xls.sheet_names:
        if len(sheet) < 32:
            pd.read_excel(xls, sheet).to_excel(writer, sheet, index=False)
    writer.save()
tabs_london = loadxlstabs("temp.xls")

#tabs_london = { tab.name: tab for tab in scraper.distributions[5].as_databaker() }
tab = tabs_london[0]
tidied_sheets = {}
tidy_sheet_list = []
cs_list = []

tab_length = len(tab.excel_ref('B'))
batch_number = 10
number_of_iterations = math.ceil(tab_length/batch_number)


# In[225]:


for i in range(0, number_of_iterations):
    Min = str(7 + batch_number * i)  # data starts on row 7
    Max = str(int(Min) + batch_number - 1)

    period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
    defined_by = tab.excel_ref('G5').expand(RIGHT)
    age_gender_family_size = tab.excel_ref('G6').expand(RIGHT)
    geography_code = tab.excel_ref('E'+Min+':E'+Max).is_not_blank()
    temp_missing_geography_codes = tab.excel_ref('B'+Min+':B'+Max).is_not_blank()
    geography_level = tab.excel_ref('E4')
    unit = tab.excel_ref('F4').expand(RIGHT).is_not_blank()
    observations = geography_code.waffle(age_gender_family_size)

    if len(geography_code) != 0:

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


# In[226]:


#Distribution 7: 2019 - North East

distribution = scraper.distributions[6]
xls = pd.ExcelFile(distribution.downloadURL)
with ExcelWriter("temp.xls") as writer:
    for sheet in xls.sheet_names:
        if len(sheet) < 32:
            pd.read_excel(xls, sheet).to_excel(writer, sheet, index=False)
    writer.save()
tabs_north_east = loadxlstabs("temp.xls")

#tabs_north_east = { tab.name: tab for tab in scraper.distributions[6].as_databaker() }
tab = tabs_north_east[0]
tidied_sheets = {}
tidy_sheet_list = []
cs_list = []

tab_length = len(tab.excel_ref('A'))
batch_number = 10
number_of_iterations = math.ceil(tab_length/batch_number)


# In[227]:


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

    if len(geography_code) != 0:

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


# In[228]:


#Distribution 8: 2019 - North West

distribution = scraper.distributions[7]
xls = pd.ExcelFile(distribution.downloadURL)
with ExcelWriter("temp.xls") as writer:
    for sheet in xls.sheet_names:
        if len(sheet) < 32:
            pd.read_excel(xls, sheet).to_excel(writer, sheet, index=False)
    writer.save()
tabs_north_west = loadxlstabs("temp.xls")

#tabs_north_west = { tab.name: tab for tab in scraper.distributions[7].as_databaker() }
tab = tabs_north_west[0]
tidied_sheets = {}
tidy_sheet_list = []
cs_list = []

tab_length = len(tab.excel_ref('A'))
batch_number = 10
number_of_iterations = math.ceil(tab_length/batch_number)


# In[229]:


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

    if len(geography_code) != 0:

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


# In[230]:


#Distribution 9 - Scottish Data Zone

distribution = scraper.distributions[8]
xls = pd.ExcelFile(distribution.downloadURL)
with ExcelWriter("temp.xls") as writer:
    for sheet in xls.sheet_names:
        if len(sheet) < 32:
            pd.read_excel(xls, sheet).to_excel(writer, sheet, index=False)
    writer.save()
tabs_scotland = loadxlstabs("temp.xls")

#tabs_scotland = { tab.name: tab for tab in scraper.distributions[8].as_databaker() }
tab = tabs_scotland[0]
tidied_sheets = {}
tidy_sheet_list = []
cs_list = []

tab_length = len(tab.excel_ref('A'))
batch_number = 10
number_of_iterations = math.ceil(tab_length/batch_number)


# In[231]:


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

    if len(geography_code) != 0:

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


# In[232]:


#Distribution 10: 2019 - South East

distribution = scraper.distributions[9]
xls = pd.ExcelFile(distribution.downloadURL)
with ExcelWriter("temp.xls") as writer:
    for sheet in xls.sheet_names:
        if len(sheet) < 32:
            pd.read_excel(xls, sheet).to_excel(writer, sheet, index=False)
    writer.save()
tabs_south_east = loadxlstabs("temp.xls")

#tabs_south_east = { tab.name: tab for tab in scraper.distributions[9].as_databaker() }
tab = tabs_south_east[0]
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

    if len(geography_code) != 0:

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


# In[233]:


#Distribution 11: 2019 - South West

distribution = scraper.distributions[10]
xls = pd.ExcelFile(distribution.downloadURL)
with ExcelWriter("temp.xls") as writer:
   for sheet in xls.sheet_names:
       if len(sheet) < 32:
           pd.read_excel(xls, sheet).to_excel(writer, sheet, index=False)
   writer.save()
tabs_south_west = loadxlstabs("temp.xls")

#tabs_south_west = { tab.name: tab for tab in scraper.distributions[10].as_databaker() }
tab = tabs_south_west[0]
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

   if len(geography_code) != 0:

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


# In[234]:


#Distribution 12: 2019 - Wales

distribution = scraper.distributions[11]
xls = pd.ExcelFile(distribution.downloadURL)
with ExcelWriter("temp.xls") as writer:
    for sheet in xls.sheet_names:
        if len(sheet) < 32:
            pd.read_excel(xls, sheet).to_excel(writer, sheet, index=False)
    writer.save()
tabs_wales = loadxlstabs("temp.xls")

#tabs_wales = { tab.name: tab for tab in scraper.distributions[11].as_databaker() }
tab = tabs_wales[0]
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

    if len(geography_code) != 0:

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


# In[235]:


#Distribution 13: 2019 - West Midlands

distribution = scraper.distributions[12]
xls = pd.ExcelFile(distribution.downloadURL)
with ExcelWriter("temp.xls") as writer:
    for sheet in xls.sheet_names:
        if len(sheet) < 32:
            pd.read_excel(xls, sheet).to_excel(writer, sheet, index=False)
    writer.save()
tabs_west_midlands = loadxlstabs("temp.xls")

#tabs_west_midlands = { tab.name: tab for tab in scraper.distributions[12].as_databaker() }
tab = tabs_west_midlands[0]
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

    if len(geography_code) != 0:

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


# In[236]:


#Distribution 14: 2019 - Yorkshire and the Humber

distribution = scraper.distributions[13]
xls = pd.ExcelFile(distribution.downloadURL)
with ExcelWriter("temp.xls") as writer:
    for sheet in xls.sheet_names:
        if len(sheet) < 32:
            pd.read_excel(xls, sheet).to_excel(writer, sheet, index=False)
    writer.save()
tabs_yorkshire_humber = loadxlstabs("temp.xls")

#tabs_yorkshire_humber = { tab.name: tab for tab in scraper.distributions[13].as_databaker() }
tab = tabs_yorkshire_humber[0]
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

    if len(geography_code) != 0:

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


# In[237]:


#Distribution 2: 2019 - Number of families and children in a live Child Benefit award by electoral ward

distribution = scraper.distributions[1]
xls = pd.ExcelFile(distribution.downloadURL)
with ExcelWriter("temp.xls") as writer:
    for sheet in xls.sheet_names:
        if len(sheet) < 32:
            pd.read_excel(xls, sheet).to_excel(writer, sheet, index=False)
    writer.save()
tabs_electoral_ward = loadxlstabs("temp.xls")

#tabs_electoral_ward = { tab.name: tab for tab in scraper.distributions[1].as_databaker() }
tab = tabs_electoral_ward[0]
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
    temp_missing_geography_codes = tab.excel_ref('C'+Min+':C'+Max).expand(LEFT).is_not_blank()
    geography_level = tab.excel_ref('D4')
    unit = tab.excel_ref('E4').expand(RIGHT).is_not_blank()
    observations = geography_code.waffle(age_gender_family_size)

    if len(geography_code) != 0:

        dimensions = [
            HDim(period, 'Period', CLOSEST, LEFT),
            HDim(geography_code, 'Geography Code', DIRECTLY, LEFT),
            HDim(defined_by, 'TEMP - DEFINED BY', CLOSEST, LEFT),
            HDim(age_gender_family_size, 'TEMP - AGE, GENDER, FAMILY SIZE', DIRECTLY, ABOVE),
            HDim(temp_missing_geography_codes, "TEMP - Missing area code", DIRECTLY, LEFT),
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


# In[238]:


#concatenating all the distributions togther - Easy to output all data togther once multiple measure types can be handeld
merged_data = pd.concat([region, east_midlands , east_of_england, london,  north_east, north_west, scotland, south_east, south_west, wales, west_midlands, yorkshire_humber, electoral_ward], ignore_index=True)


# In[239]:


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


# In[240]:


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


# In[241]:


tidy_children_stats_df = tidy_children_stats_df[['Period', 'Geography Code', 'Geography Level', 'Age', 'Gender', 'Value']]
tidy_families_stats_df = tidy_families_stats_df[['Period', 'Geography Code', 'Geography Level', 'Family Size', 'Value']]


tidy_children_stats_df = tidy_children_stats_df.drop_duplicates()
tidy_families_stats_df = tidy_families_stats_df.drop_duplicates()

print("Duplicates children")
duplicateDFRow = tidy_children_stats_df[tidy_children_stats_df.duplicated()]
print(duplicateDFRow)

print("Duplicates families")
duplicateDFRow = tidy_families_stats_df[tidy_families_stats_df.duplicated()]
print(duplicateDFRow)


# In[242]:


# Output filenames
fn = ['children-observations','families-observations']
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


# In[243]:


try:
    i = 0
    csvName = fn[i]
    out = Path('out')
    out.mkdir(exist_ok=True)

    scraper1 = copy.deepcopy(scraper)

    scraper1.dataset.family = 'towns-high-streets'
    scraper1.dataset.description = scraper.dataset.description + '\n' + de[i]
    scraper1.dataset.comment = co[i]
    scraper1.dataset.title = ti[i]

    cubes.add_cube(scraper1, tidy_children_stats_df.drop_duplicates(), csvName)

except Exception as s:
    print(str(s))


# In[244]:


#changing unit to families for second output
with open("info.json", "r") as read_file:
    data = json.load(read_file)
    print("Unit: ", data["transform"]["columns"]["Value"]["unit"] )
    data["transform"]["columns"]["Value"]["unit"] = "http://gss-data.org.uk/def/concept/measurement-units/families"
    print("Unit changed to: ", data["transform"]["columns"]["Value"]["unit"] )


# In[245]:


try:
    i = 1
    csvName = fn[i]
    out = Path('out')
    out.mkdir(exist_ok=True)

    scraper1 = copy.deepcopy(scraper)

    scraper1.dataset.family = 'towns-high-streets'
    scraper1.dataset.description = scraper.dataset.description + '\n' + de[i]
    scraper1.dataset.comment = co[i]
    scraper1.dataset.title = ti[i]

    cubes.add_cube(scraper1, tidy_families_stats_df.drop_duplicates(), csvName)

except Exception as s:
    print(str(s))


# In[246]:


cubes.output_all()

