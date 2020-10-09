from gssutils import * 
import json 
import os
from urllib.parse import urljoin
from gssutils.metadata import THEME
import numpy as np

info = json.load(open('info.json')) 
#etl_title = info["Name"] 
#etl_publisher = info["Producer"][0] 
#print("Publisher: " + etl_publisher) 
#print("Title: " + etl_title) 

scraper = Scraper(seed="info.json")   
scraper 

scraper.select_dataset(title=lambda x: x.lower().startswith('child benefit small area statistics: august 2019'))
scraper

ditributions_required = [
    'Number of families and children in a live Child Benefit award by electoral ward',
    'Number of families and children in a live Child Benefit award',
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
]

trace = TransformTrace()
df = pd.DataFrame()

# ### Firstly Transforming data for :
#     Child Benefit small area statistics: Number of children for whom Child Benefit is received
#     
#     Table structure : Period', 'Area Code', 'Age', 'Gender', 'Measure Type', 'Unit', 'Value', 'Measure Type', 'Unit'

# +
tidied_sheets = {} # to be filled with each tab of data from each distribution
for distribution in scraper.distributions:
    
    if distribution.title in ditributions_required:
        tabs = distribution.as_databaker()
        tabs = [tab for tab in tabs if 'metadata' not in tab.name.lower()] # unwanted tabs
        
        for tab in tabs:
                        
            # Information required for tracer
            unique_identifier = distribution.title + ' - ' + tab.name # title of dataset + tab name
            link = distribution.downloadURL
            columns = ['Period', 'Area Code', 'Age', 'Gender', 'Measure Type', 'Unit', 'Value', 'Measure Type', 'Unit']
            trace.start(scraper.title, unique_identifier, columns, link)
            
            period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
            trace.Period("Period year taken from sheet name : " )
                   
            defined_by = tab.filter(contains_string('All Children')).expand(RIGHT)
            age_gender = tab.filter(contains_string('All Children')).shift(0,1).expand(RIGHT) - tab.filter(contains_string('All Families')).shift(0,1).expand(RIGHT)
            trace.Age("Defined in cells J6 to M6")
            trace.Gender("Defined in cells O6 to A6")
        
            if tab.name == 'Scotland':
                area_code = tab.filter(contains_string('Data Zone name')).shift(0,4).expand(DOWN)
                temp_missing_area_codes = tab.excel_ref('C8')
                observations = area_code.waffle(age_gender) 
                
            elif tab.name == 'Electoral Ward':
                area_code = tab.filter(contains_string('Area Code1')).shift(0,4).expand(DOWN)
                temp_missing_area_codes = tab.excel_ref('B7').expand(DOWN).is_not_blank()
                defined_by = tab.filter(contains_string('All children')).expand(RIGHT)
                age_gender = tab.filter(contains_string('All children')).shift(0,1).expand(RIGHT) - tab.filter(contains_string('All families')).shift(0,1).expand(RIGHT)
                trace.Age("Defined in cells J6 to M6")
                trace.Gender("Defined in cells O6 to A6")
                observations = area_code.waffle(age_gender) 
                
            elif tab.name == 'Regions (GB) ':
                area_code = tab.filter(contains_string('Area Code1')).shift(0,4).expand(DOWN)
                temp_missing_area_codes = tab.excel_ref('C7').expand(DOWN).is_not_blank()
                defined_by = tab.filter(contains_string('All children')).expand(RIGHT)
                age_gender = tab.filter(contains_string('All children')).shift(0,1).expand(RIGHT) - tab.filter(contains_string('All families')).shift(0,1).expand(RIGHT)
                trace.Age("Defined in cells J6 to M6")
                trace.Gender("Defined in cells O6 to A6")
                observations = area_code.shift(1,0).waffle(age_gender) 
                
            else:    
                area_code = tab.filter(contains_string('LSOA name')).shift(0,4).expand(DOWN)#.is_not_blank()
                trace.Area_Code("Selected as all non blank values from cell ref F10 down")
                temp_missing_area_codes = tab.excel_ref('B8')
                observations = area_code.waffle(age_gender) 


            unit = "Number of children for whom Child Benefit is received"
            trace.Unit("Defined in cells H4 as : Number of children for whom Child Benefit is received")
            
            measure_type = 'Count'
            trace.Measure_Type('Hardcoded as Count')
        
            
        
            dimensions = [
                HDim(period, 'Period', CLOSEST, LEFT),
                HDim(area_code, 'Area Code', DIRECTLY, LEFT),
                HDim(defined_by, 'TEMP - DEFINED BY', CLOSEST, LEFT),
                HDim(age_gender, 'TEMP - AGE, GENDER', DIRECTLY, ABOVE),
                HDim(temp_missing_area_codes, "TEMP - Missing area code", CLOSEST, ABOVE),
                HDimConst('Unit', unit),
                HDimConst('Measure Type', measure_type),
            ]
            tidy_sheet = ConversionSegment(tab, dimensions, observations)
            trace.with_preview(tidy_sheet)
            savepreviewhtml(tidy_sheet, fname= tab.name + ".html") 
            trace.store("combined_dataframe", tidy_sheet.topandas())
            
            

# +
import numpy as np

df = trace.combine_and_trace('Child Benefit small area statistics: ', "combined_dataframe")
df.rename(columns={'OBS' : 'Value', 'DATAMARKER' : 'Marker'}, inplace=True)

#Post processing
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
# drop temp columns 
df = df.drop(['TEMP - DEFINED BY', 'TEMP - AGE, GENDER','TEMP - Missing area code' ], axis=1)
trace.render()
# -

from IPython.core.display import HTML
for col in df:
    if col not in ['Value']:
        df[col] = df[col].astype('category')
        display(HTML(f"<h2>{col}</h2>"))
        display(df[col].cat.categories) 

# ### Output tale for "Child Benefit small area statistics: Number of children for whom Child Benefit is received"

tidy_children_stats = df[['Period', 'Area Code', 'Age', 'Gender', 'Measure Type', 'Unit', 'Value']]
tidy_children_stats

# ______________________________________________________________________________________________________________

# ### Secondaly Transforming data for :
#     Child Benefit small area statistics: Number of families in receipt of Child Benefit
#     
#     Table structure : Period', 'Area Code', 'Family Size 'Measure Type', 'Unit', 'Value', 'Measure Type', 'Unit'

# +
tidied_sheets = {} # to be filled with each tab of data from each distribution
for distribution in scraper.distributions:
    
    if distribution.title in ditributions_required:
        tabs = distribution.as_databaker()
        tabs = [tab for tab in tabs if 'metadata' not in tab.name.lower()] # unwanted tabs
        
        for tab in tabs:
                        
            # Information required for tracer
            unique_identifier = distribution.title + ' - ' + tab.name # title of dataset + tab name
            link = distribution.downloadURL
            columns = ['Period', 'Area Code', 'Family Size', 'Measure Type', 'Unit', 'Value', 'Measure Type', 'Unit']
            trace.start(scraper.title, unique_identifier, columns, link)
            
            period = tab.excel_ref('B2') #TAKEN FROM SHEET TITLE
            trace.Period("Period year taken from sheet name : " )
            
            defined_by = tab.filter(contains_string('All Families')).expand(RIGHT)
            family_size = tab.filter(contains_string('All Families')).shift(0,1).expand(RIGHT)
            trace.Family_Size("Defined in cells M6 across")
        
            if tab.name == 'Scotland':
                area_code = tab.filter(contains_string('Data Zone name')).shift(0,4).expand(DOWN)
                temp_missing_area_codes = tab.excel_ref('C8')
                
            elif tab.name == 'Electoral Ward':
                area_code = tab.filter(contains_string('Area Code1')).shift(0,4).expand(DOWN)
                temp_missing_area_codes = tab.excel_ref('B7').expand(DOWN).is_not_blank()
                defined_by = tab.filter(contains_string('All families')).expand(RIGHT)
                family_size = tab.filter(contains_string('All families')).shift(0,1).expand(RIGHT) 
                trace.Family_Size("Defined in cells M6 across")
                
            elif tab.name == 'Regions (GB) ':
                area_code = tab.filter(contains_string('Area Code1')).shift(0,4).expand(DOWN)
                temp_missing_area_codes = tab.excel_ref('C7').expand(DOWN).is_not_blank()
                defined_by = tab.filter(contains_string('All families')).expand(RIGHT)
                family_size = tab.filter(contains_string('All families')).shift(0,1).expand(RIGHT) 
                trace.Family_Size("Defined in cells M6 across")
                
            else:    
                area_code = tab.filter(contains_string('LSOA name')).shift(0,4).expand(DOWN)#.is_not_blank()
                trace.Area_Code("Selected as all non blank values from cell ref F10 down")
                temp_missing_area_codes = tab.excel_ref('B8')
                
            observations = family_size.shift(0,1).fill(DOWN)
            
            unit = "Number of families in receipt of Child Benefit"
            trace.Unit("Defined in cells H4 as : Number of families in receipt of Child Benefit")
            
            measure_type = 'Count'
            trace.Measure_Type('Hardcoded as Count')
        
            dimensions = [
                HDim(period, 'Period', CLOSEST, LEFT),
                HDim(area_code, 'Area Code', DIRECTLY, LEFT),
                HDim(defined_by, 'TEMP - DEFINED BY', CLOSEST, LEFT),
                HDim(family_size, 'Family Size', DIRECTLY, ABOVE),
                HDim(temp_missing_area_codes, "TEMP - Missing area code", CLOSEST, ABOVE),
                HDimConst('Unit', unit),
                HDimConst('Measure Type', measure_type),
            ]
            tidy_sheet = ConversionSegment(tab, dimensions, observations)
            trace.with_preview(tidy_sheet)
            savepreviewhtml(tidy_sheet, fname= tab.name + ".html") 
            trace.store("combined_dataframe_2", tidy_sheet.topandas())
            
            

# +
df = trace.combine_and_trace('Child Benefit small area statistics: Families', "combined_dataframe_2")
df.rename(columns={'OBS' : 'Value', 'DATAMARKER' : 'Marker'}, inplace=True)

#Post processing
df['Period'] = df['Period'].str[-4:]
df["Period"] = df["Period"].map(lambda x: "2019" if x == "gdom" else "2019")

f1=((df['TEMP - DEFINED BY'] =='All Families') | (df['TEMP - DEFINED BY'] =='All families') )
df.loc[f1,'Family Size'] = 'All Families'
df['Area Code'] = np.where(df['Area Code'] == "", df['TEMP - Missing area code'], df['Area Code'])

#removing hidden cells 
df.drop(df[((df['TEMP - DEFINED BY'] =='') & (df['Family Size'] =='') &( df['Area Code']  == "") &( df['Value']  == 0))].index, inplace = True) 
df.drop(df[(( df['Value']  == "") | ((df['Value'] == 0) & (df['Family Size'] == "" )))].index, inplace = True) 


df["Family Size"] = df["Family Size"].map(lambda x: "total" if x == "All Families" 
                                                              else ("one-child" if x == "One child" else ("two-children" if x == "Two children" 
                                                                                                          else ("three-or-more-children" if x == "Three or more\nchildren" else "total"  ))))
# drop temp columns 
df = df.drop(['TEMP - DEFINED BY','TEMP - Missing area code' ], axis=1)
df
# -

from IPython.core.display import HTML
for col in df:
    if col not in ['Value']:
        df[col] = df[col].astype('category')
        display(HTML(f"<h2>{col}</h2>"))
        display(df[col].cat.categories) 

# ### Output tale for "Child Benefit small area statistics: Number of families in receipt of Child Benefit"

tidy_families_stats = df[['Period', 'Area Code', 'Family Size', 'Measure Type', 'Unit', 'Value']]
tidy_families_stats

# ### Dataset has been outputted into two seperate tables as advised: 
# #### tidy_children_stats
# ####   tidy_families_stats
#









# +


#########################################################################################################
# ### CODE BELOW HAS NOT BEEN TESTED ####
# ########################################################################################################
out = Path('out')
out.mkdir(exist_ok=True)
merged.to_csv(out / 'observations.csv', index = False)

scraper.dataset.family = 'towns-and-high-streets'

scraper.dataset.theme = THEME[scraper.dataset.family]

scraper.dataset.description = scraper.dataset.description + 
    """
        \nArea codes implemented in line with GSS Coding and Naming policy
        \nThe figures have been independently rounded to the nearest 5. This can lead to components as shown not summing totals as shown
    """
scraper.dataset.title = 'Child Benefit small area statistics'
scraper.dataset.comment = 'Annual geographical estimates at Lower Super Output Area and Data Zone of the number of families and children claiming Child Benefit as at specified date.'
dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name))
scraper.set_base_uri('http://gss-data.org.uk')
scraper.set_dataset_id(dataset_path)

csvw_transform = CSVWMapping()
csvw_transform.set_csv(out / 'observations.csv')
csvw_transform.set_mapping(json.load(open('info.json')))
csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
csvw_transform.write(out / 'observations.csv-metadata.json')
with open(out / 'observations.csv-metadata.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())

""

