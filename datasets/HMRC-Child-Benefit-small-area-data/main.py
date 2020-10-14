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

ditributions_required_set_1 = [
    '2019 - East Midlands',
    '2019 - East of England',
    '2019 - London',
    '2019 - North East',
    '2019 - North West',
    '2019 - Scottish Data Zone',
    '2019 - South East',
    '2019 - South West',
    '2019 - Wales',
    '2019 - West Midlands ',
    '2019 - Yorkshire and the Humber',
    #'Number of families and children in a live Child Benefit award by electoral ward'
]

ditributions_required_set_2 = [
    'Number of families and children in a live Child Benefit award'
]

#Large, takes slighly longer to run
ditributions_required_set_3 = [
    'Number of families and children in a live Child Benefit award by electoral ward'
]

"""
#### Firstly Transforming data for :
     Child Benefit small area statistics: Number of children for whom Child Benefit is received
          Table structure : Period', 'Area Code', 'Age', 'Gender', 'Measure Type', 'Unit', 'Value', 'Measure Type', 'Unit'
"""

tidied_sheets = {} # to be filled with each tab of data from each distribution
for distribution in scraper.distributions:
    
    if distribution.title in ditributions_required_set_1:
        tabs = distribution.as_databaker()
        
        for tab in tabs:
                        
            unique_identifier = distribution.title + ' - ' + tab.name # title of dataset + tab name
            link = distribution.downloadURL
            columns = ['Period', 'Area Code', 'Age', 'Gender', 'Measure Type', 'Unit', 'Value', 'Measure Type', 'Unit']
            trace.start(scraper.title, unique_identifier, columns, link)
            
            tidy_sheet_list = [] # list of dataframes for each iteration
            cs_list = [] # list of conversionsegments for each iteration
            
            '''Iterating the databaking process'''
            tab_length = len(tab.excel_ref('A')) # number of rows of data
            batch_number = 10 # iterates over this many rows at a time
            number_of_iterations = math.ceil(tab_length/batch_number) # databaking will iterate this many times
 
            period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
            trace.Period("Period year taken from sheet name : " )
                   
            defined_by = tab.filter(contains_string('All Children')).expand(RIGHT)
            age_gender = tab.filter(contains_string('All Children')).shift(0,1).expand(RIGHT) - tab.filter(contains_string('All Families')).shift(0,1).expand(RIGHT)
            
            trace.Age("Defined in cells J6 to M6")
            trace.Gender("Defined in cells O6 to A6")

            for i in range(0, number_of_iterations):
                Min = str(8 + batch_number * i)  # data starts on row 10
                Max = str(int(Min) + batch_number - 1) 
               
                if tab.name == 'Scotland':
                    area_code = tab.excel_ref('F'+Min+':F'+Max).is_not_blank()
                    trace.Area_Code('Value taken from column "Data Zone name" - F7 expanded down')
                    temp_missing_area_codes = tab.excel_ref('C'+Min+':C'+Max).is_not_blank()
                    observations = area_code.waffle(age_gender) 
                else:    
                    area_code = tab.excel_ref('F'+Min+':F'+Max).is_not_blank()
                    trace.Area_Code('Value taken from column "LSOA name" - F7 expanded down')
                    temp_missing_area_codes = tab.excel_ref('B'+Min+':B'+Max).is_not_blank()
                    observations = area_code.waffle(age_gender) 
                    
                dimensions = [
                    HDim(period, 'Period', CLOSEST, LEFT),
                    HDim(area_code, 'Area Code', DIRECTLY, LEFT),
                    HDim(defined_by, 'TEMP - DEFINED BY', CLOSEST, LEFT),
                    HDim(age_gender, 'TEMP - AGE, GENDER', DIRECTLY, ABOVE),
                    HDim(temp_missing_area_codes, "TEMP - Missing area code", CLOSEST, ABOVE),
                ]
                
                if len(observations) != 0: # only use ConversionSegment if there is data
                    cs_iteration = ConversionSegment(tab, dimensions, observations) # creating the conversionsegment
                    tidy_sheet_iteration = cs_iteration.topandas() # turning conversionsegment into a pandas dataframe
                    cs_list.append(cs_iteration) # add to list
                    tidy_sheet_list.append(tidy_sheet_iteration) # add to list
                    
            df_1 = pd.concat(tidy_sheet_list, sort=False)
               
            #Store
            trace.store(unique_identifier, df_1)
            tidied_sheets[unique_identifier] = df_1 
            
            # trace
            trace.with_preview(cs_list[0])

    elif distribution.title in ditributions_required_set_2: 
        
        tabs = distribution.as_databaker()
        tabs = [tab for tab in tabs if 'metadata' not in tab.name.lower()] # unwanted tabs
        
        for tab in tabs:
                        
            unique_identifier = distribution.title + ' - ' + tab.name # title of dataset + tab name
            link = distribution.downloadURL
            columns = ['Period', 'Area Code', 'Age', 'Gender', 'Measure Type', 'Unit', 'Value', 'Measure Type', 'Unit']
            trace.start(scraper.title, unique_identifier, columns, link)
            
            period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
            trace.Period("Period year taken from sheet name : " )

            if tab.name == 'Regions (GB) ':
                area_code = tab.filter(contains_string('Area Code1')).shift(0,4).expand(DOWN)
                temp_missing_area_codes = tab.excel_ref('C7').expand(DOWN).is_not_blank()
                defined_by = tab.filter(contains_string('All children')).expand(RIGHT)
                age_gender = tab.filter(contains_string('All children')).shift(0,1).expand(RIGHT) - tab.filter(contains_string('All families')).shift(0,1).expand(RIGHT)
                trace.Age("Defined in cells J6 to M6")
                trace.Gender("Defined in cells O6 to A6")
                observations = area_code.shift(1,0).waffle(age_gender) 
                
            dimensions = [
                HDim(period, 'Period', CLOSEST, LEFT),
                HDim(area_code, 'Area Code', DIRECTLY, LEFT),
                HDim(defined_by, 'TEMP - DEFINED BY', CLOSEST, LEFT),
                HDim(age_gender, 'TEMP - AGE, GENDER', DIRECTLY, ABOVE),
                HDim(temp_missing_area_codes, "TEMP - Missing area code", CLOSEST, ABOVE),
            ]
            cs = ConversionSegment(tab, dimensions, observations)
            df_2 = cs.topandas()
            trace.with_preview(cs) 
            
    

###############################################################################
# Disttribution : 'Number of families and children in a live Child Benefit award by electoral ward'
# - seems to run faster when done seperately. 

tidied_sheets = {} # to be filled with each tab of data from each distribution
for distribution in scraper.distributions:    
    if distribution.title in ditributions_required_set_3:
        tabs = distribution.as_databaker()
        
        for tab in tabs:
            print(tab.name)
            unique_identifier = distribution.title + ' - ' + tab.name # title of dataset + tab name
            link = distribution.downloadURL
            columns = ['Period', 'Area Code', 'Family Size', 'Measure Type', 'Unit', 'Value', 'Measure Type', 'Unit']
            trace.start(scraper.title, unique_identifier, columns, link)

            tidy_sheet_list = [] # list of dataframes for each iteration
            cs_list = [] # list of conversionsegments for each iteration

            '''Iterating the databaking process'''
            tab_length = len(tab.excel_ref('B'))
            batch_number = 10 
            number_of_iterations = math.ceil(tab_length/batch_number)

            period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
            trace.Period("Period year taken from sheet name : " )

            for i in range(0, number_of_iterations):
                Min = str(8 + batch_number * i)  # data starts on row 8
                Max = str(int(Min) + batch_number - 1) 
                   
                area_code = tab.excel_ref('D'+Min+':D'+Max).is_not_blank()
                trace.Area_Code('Value taken from column "Area Code 1" - D7 expanded down')
                temp_missing_area_codes = tab.excel_ref('B'+Min+':B'+Max).is_not_blank()
                defined_by = tab.filter(contains_string('All children')).expand(RIGHT)
                age_gender = tab.filter(contains_string('All children')).shift(0,1).expand(RIGHT) - tab.filter(contains_string('All families')).shift(0,1).expand(RIGHT)
                observations = area_code.waffle(age_gender)

                dimensions = [
                    HDim(period, 'Period', CLOSEST, LEFT),
                    HDim(area_code, 'Area Code', DIRECTLY, LEFT),
                    HDim(defined_by, 'TEMP - DEFINED BY', CLOSEST, LEFT),
                    HDim(age_gender, 'TEMP - AGE, GENDER', DIRECTLY, ABOVE),
                    HDim(temp_missing_area_codes, "TEMP - Missing area code", CLOSEST, ABOVE),
                ]

                if len(observations) != 0: # only use ConversionSegment if there is data
                    cs_iteration = ConversionSegment(tab, dimensions, observations) # creating the conversionsegment
                    tidy_sheet_iteration = cs_iteration.topandas() # turning conversionsegment into a pandas dataframe
                    cs_list.append(cs_iteration) # add to list
                    tidy_sheet_list.append(tidy_sheet_iteration) # add to list

            df_3 = pd.concat(tidy_sheet_list, sort=False)

            #Store
            trace.store(unique_identifier, df_3)
            tidied_sheets[unique_identifier] = df_3 

            # trace
            trace.with_preview(cs_list[0])

""
# #Post processing
combined_data = [df_1, df_2, df_3]

for df in combined_data:
    df.rename(columns={'OBS' : 'Value', 'DATAMARKER' : 'Marker'}, inplace=True)
    df['Period'] = df['Period'].str[-4:]
    df["Period"] = df["Period"].map(lambda x: "2019" if x == "gdom" else "2019")

    f1=((df['TEMP - DEFINED BY'] =='All Children'))
    df.loc[f1,'TEMP - AGE, GENDER'] = 'All Children'
    df['Area Code'] = np.where(df['Area Code'] == "", df['TEMP - Missing area code'], df['Area Code'])

    #removing hidden cells 
    df.drop(df[((df['TEMP - DEFINED BY'] =='') & (df['TEMP - AGE, GENDER'] =='') &( df['Area Code']  == "") &( df['Value']  == 0))].index, inplace = True) 
    df.drop(df[(( df['Value']  == "") | ((df['Value'] == 0) & (df['TEMP - AGE, GENDER'] == "" )))].index, inplace = True) 

    df["Age"] = df["TEMP - AGE, GENDER"].map(lambda x: "total" if x == "All Children" else ("under-5" if x == "Under 5" 
                                                                                                      else ("11-to-15" if x == "11 to 15" else ("16-to-19" if x == "16 to 19" else "total"))))
    df["Gender"] = df["TEMP - AGE, GENDER"].map(lambda x: "M" if x == "Boys" else ("F" if x == "Girls" 
                                                                                                      else ("U" if x == "Unknown" else "T")))
    df['Unit'] = "Number of children for whom Child Benefit is received"
    trace.Unit("Defined in cells H4 as : Number of children for whom Child Benefit is received")
    df["Measure Type"] = "Count"
    trace.Measure_Type('Hardcoded as Count')

    # drop temp columns 
    df = df.drop(['TEMP - DEFINED BY', 'TEMP - AGE, GENDER','TEMP - Missing area code' ], axis=1)

join = pd.concat([df_1, df_2, df_3])
tidy_children_stats = join[['Period', 'Area Code', 'Age', 'Gender', 'Measure Type', 'Unit', 'Value']]
tidy_children_stats   

""
csvName = "number-of-children-for-whom-child-benefit-is-received"
out = Path('out')
out.mkdir(exist_ok=True)
tidy_children_stats.drop_duplicates().to_csv(out / csvName, index = False)

###############################################################################
# ________________________________________________________________________________________________________________

###############################################################################
#
# #### Secondaly Transforming data for :
#      Child Benefit small area statistics: Number of families in receipt of Child Benefit
#      
#      Table structure : Period', 'Area Code', 'Family Size 'Measure Type', 'Unit', 'Value', 'Measure Type', 'Unit'

tidied_sheets = {} # to be filled with each tab of data from each distribution
for distribution in scraper.distributions:
    
    if distribution.title in ditributions_required_set_1:
        tabs = distribution.as_databaker()
        
        for tab in tabs:
                        
            unique_identifier = distribution.title + ' - ' + tab.name # title of dataset + tab name
            link = distribution.downloadURL
            columns = ['Period', 'Area Code', 'Family Size', 'Measure Type', 'Unit', 'Value', 'Measure Type', 'Unit']
            trace.start(scraper.title, unique_identifier, columns, link)
            
            tidy_sheet_list = [] # list of dataframes for each iteration
            cs_list = [] # list of conversionsegments for each iteration
            
            '''Iterating the databaking process'''
            tab_length = len(tab.excel_ref('A')) # number of rows of data
            batch_number = 10 # iterates over this many rows at a time
            number_of_iterations = math.ceil(tab_length/batch_number) # databaking will iterate this many times
 
            period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
            trace.Period("Period year taken from sheet name : " )
            
            defined_by = tab.filter(contains_string('All Families')).expand(RIGHT)
            family_size = tab.filter(contains_string('All Families')).shift(0,1).expand(RIGHT)
            trace.Family_Size("Defined in cells M6 across")

            for i in range(0, number_of_iterations):
                Min = str(7 + batch_number * i)  # data starts on row 7
                Max = str(int(Min) + batch_number - 1) 
               
                if tab.name == "Scotland":    
                    area_code = tab.excel_ref('F'+Min+':F'+Max).is_not_blank()
                    temp_missing_area_codes = tab.excel_ref('C8')
                    observations = area_code.shift(11,0).waffle(family_size)       
                else:    
                    area_code = tab.excel_ref('F'+Min+':F'+Max)#.is_not_blank()
                    trace.Area_Code('Value taken from column "LSOA name" - F7 expanded down')
                    temp_missing_area_codes = tab.excel_ref('B'+Min+':B'+Max).is_not_blank()
                    defined_by = tab.filter(contains_string('All Families')).expand(RIGHT)
                    family_size = tab.filter(contains_string('All Families')).shift(0,1).expand(RIGHT)
                    if tab.name == "East of England":
                        observations = area_code.shift(8,0).waffle(family_size) 
                    else:   
                        observations = area_code.shift(11,0).waffle(family_size) 
                     
                dimensions = [
                    HDim(period, 'Period', CLOSEST, LEFT),
                    HDim(area_code, 'Area Code', DIRECTLY, LEFT),
                    HDim(defined_by, 'TEMP - DEFINED BY', CLOSEST, LEFT),
                    HDim(family_size, 'Family Size', DIRECTLY, ABOVE),
                    HDim(temp_missing_area_codes, "TEMP - Missing area code", CLOSEST, ABOVE),
                ]
                
                if len(observations) != 0: # only use ConversionSegment if there is data
                    cs_iteration = ConversionSegment(tab, dimensions, observations) # creating the conversionsegment
                    tidy_sheet_iteration = cs_iteration.topandas() # turning conversionsegment into a pandas dataframe
                    cs_list.append(cs_iteration) # add to list
                    tidy_sheet_list.append(tidy_sheet_iteration) # add to list
                    
            df_4 = pd.concat(tidy_sheet_list, sort=False)
               
            #Store
            trace.store(unique_identifier, df_4)
            tidied_sheets[unique_identifier] = df_4 
            
            # trace
            trace.with_preview(cs_list[0])

    elif distribution.title in ditributions_required_set_2: 
        
        tabs = distribution.as_databaker()
        
        for tab in tabs:
                        
            unique_identifier = distribution.title + ' - ' + tab.name # title of dataset + tab name
            link = distribution.downloadURL
            columns = ['Period', 'Area Code', 'Family Size', 'Measure Type', 'Unit', 'Value', 'Measure Type', 'Unit']
            trace.start(scraper.title, unique_identifier, columns, link)
            
            period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
            trace.Period("Period year taken from sheet name : " )

            if tab.name == 'Regions (GB) ':
                area_code = tab.filter(contains_string('Area Code1')).shift(0,4).expand(DOWN)
                temp_missing_area_codes = tab.excel_ref('C7').expand(DOWN).is_not_blank()
                defined_by = tab.filter(contains_string('All families')).expand(RIGHT)
                family_size = tab.filter(contains_string('All families')).shift(0,1).expand(RIGHT) 
                trace.Family_Size("Defined in cells M6 across")
            else:
                continue
            
            observations = family_size.shift(0,1).fill(DOWN)
                
            dimensions = [
                HDim(period, 'Period', CLOSEST, LEFT),
                HDim(area_code, 'Area Code', DIRECTLY, LEFT),
                HDim(defined_by, 'TEMP - DEFINED BY', CLOSEST, LEFT),
                HDim(family_size, 'Family Size', DIRECTLY, ABOVE),
                HDim(temp_missing_area_codes, "TEMP - Missing area code", CLOSEST, ABOVE),
            ]
            cs = ConversionSegment(tab, dimensions, observations)
            df_5 = cs.topandas()
            trace.with_preview(cs) 

            

###############################################################################
# Disttribution : 'Number of families and children in a live Child Benefit award by electoral ward'
# - seems to run faster when done seperately. 

tidied_sheets = {} # to be filled with each tab of data from each distribution
for distribution in scraper.distributions:    
    if distribution.title in ditributions_required_set_3:
        tabs = distribution.as_databaker()
        
        for tab in tabs:
            print(tab.name)
            unique_identifier = distribution.title + ' - ' + tab.name # title of dataset + tab name
            link = distribution.downloadURL
            columns = ['Period', 'Area Code', 'Family Size', 'Measure Type', 'Unit', 'Value', 'Measure Type', 'Unit']
            trace.start(scraper.title, unique_identifier, columns, link)

            tidy_sheet_list = [] # list of dataframes for each iteration
            cs_list = [] # list of conversionsegments for each iteration

            '''Iterating the databaking process'''
            tab_length = len(tab.excel_ref('B'))
            batch_number = 10 
            number_of_iterations = math.ceil(tab_length/batch_number)

            period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
            trace.Period("Period year taken from sheet name : " )

            for i in range(0, number_of_iterations):
                Min = str(8 + batch_number * i)  # data starts on row 8
                Max = str(int(Min) + batch_number - 1) 
                   
                area_code = tab.excel_ref('D'+Min+':D'+Max)#.is_not_blank()
                temp_missing_area_codes = tab.excel_ref('B'+Min+':B'+Max).is_not_blank()
                defined_by = tab.filter(contains_string('All Families')).expand(RIGHT)
                family_size = tab.excel_ref('Q6').expand(RIGHT)
                temp_obs_placement = tab.excel_ref('P'+Min+':P'+Max).is_not_blank()
                observations = temp_obs_placement.waffle(family_size)

                dimensions = [
                    HDim(period, 'Period', CLOSEST, LEFT),
                    HDim(area_code, 'Area Code', DIRECTLY, LEFT),
                    HDim(defined_by, 'TEMP - DEFINED BY', CLOSEST, LEFT),
                    HDim(family_size, 'Family Size', DIRECTLY, ABOVE),
                    HDim(temp_missing_area_codes, "TEMP - Missing area code", CLOSEST, ABOVE),
                ]

                if len(observations) != 0: # only use ConversionSegment if there is data
                    cs_iteration = ConversionSegment(tab, dimensions, observations) # creating the conversionsegment
                    tidy_sheet_iteration = cs_iteration.topandas() # turning conversionsegment into a pandas dataframe
                    cs_list.append(cs_iteration) # add to list
                    tidy_sheet_list.append(tidy_sheet_iteration) # add to list

            df_6 = pd.concat(tidy_sheet_list, sort=False)

            #Store
            trace.store(unique_identifier, df_6)
            tidied_sheets[unique_identifier] = df_6 

            # trace
            trace.with_preview(cs_list[0])

""
 #Post processing
combined_data_2 = [df_4, df_5, df_6]

for df in combined_data_2:
    df.rename(columns={'OBS' : 'Value', 'DATAMARKER' : 'Marker'}, inplace=True)
    df['Period'] = df['Period'].str[-4:]
    df["Period"] = df["Period"].map(lambda x: "2019" if x == "gdom" else "2019")
    df['Area Code'] = np.where(df['Area Code'] == "", df['TEMP - Missing area code'], df['Area Code'])

    #removing hidden cells 
    df.drop(df[((df['TEMP - DEFINED BY'] =='') & (df['Family Size'] =='') &( df['Area Code']  == "") &( df['Value']  == 0))].index, inplace = True) 
    df.drop(df[(( df['Value']  == "") | ((df['Value'] == 0) & (df['Family Size'] == "" )))].index, inplace = True) 


    df["Family Size"] = df["Family Size"].map(lambda x: "total" if x == "All Families" 
                                                              else ("one-child" if x == "One child" else ("two-children" if x == "Two children" 
                                                                                                          else ("three-or-more-children" if x == "Three or more\nchildren" else "total"  ))))
    
    
    df["Unit"] = "Number of families in receipt of Child Benefit"
    trace.Unit("Defined in cells H4 as : Number of children for whom Child Benefit is received")
    df["Measure Type"] = "Count"
    trace.Measure_Type('Hardcoded as Count')
    
    # drop temp columns 
    df = df.drop(['TEMP - DEFINED BY','TEMP - Missing area code' ], axis=1)

join_2 = pd.concat([df_4, df_5, df_6])
tidy_families_stats = join_2[['Period', 'Area Code', 'Family Size', 'Measure Type', 'Unit', 'Value']]
tidy_families_stats

""
csvName = "number-of-families-in-receipt-of-child-benefit"
out = Path('out')
out.mkdir(exist_ok=True)
tidy_families_stats.drop_duplicates().to_csv(out / csvName, index = False)

""


""


""


#########################################################################################################
# ### CODE BELOW HAS NOT BEEN TESTED ####
# ########################################################################################################
# ##############################################################################
# out = Path('out')
# out.mkdir(exist_ok=True)
# merged.to_csv(out / 'observations.csv', index = False)
#
# scraper.dataset.family = 'towns-and-high-streets'
#
# scraper.dataset.theme = THEME[scraper.dataset.family]
#
# scraper.dataset.description = scraper.dataset.description + 
#     """
#         \nArea codes implemented in line with GSS Coding and Naming policy
#         \nThe figures have been independently rounded to the nearest 5. This can lead to components as shown not summing totals as shown
#     """
# scraper.dataset.title = 'Child Benefit small area statistics'
# scraper.dataset.comment = 'Annual geographical estimates at Lower Super Output Area and Data Zone of the number of families and children claiming Child Benefit as at specified date.'
# dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name))
# scraper.set_base_uri('http://gss-data.org.uk')
# scraper.set_dataset_id(dataset_path)
#
# csvw_transform = CSVWMapping()
# csvw_transform.set_csv(out / 'observations.csv')
# csvw_transform.set_mapping(json.load(open('info.json')))
# csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
# csvw_transform.write(out / 'observations.csv-metadata.json')
# with open(out / 'observations.csv-metadata.trig', 'wb') as metadata:
#     metadata.write(scraper.generate_trig())

""

