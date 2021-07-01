#!/usr/bin/env python
# coding: utf-8

# In[7]:


# # LSOA estimates of properties not connected to the gas network

from gssutils import *
import json

cubes = Cubes('info.json')

info = json.load(open('info.json'))
#scraper = Scraper(seed="info.json")
scraper = Scraper(info['landingPage'])
scraper


# In[8]:


datasetTitle = 'LSOA estimates of properties not connected to the gas network'

tabs = { tab.name: tab for tab in scraper.distributions[0].as_databaker() }
list(tabs)

trace = TransformTrace()
df = pd.DataFrame()

for name, tab in tabs.items():

    columns=["Period", "Local Authority Name", "Local Authority Code", 'MSOA Name', 'MSOA Code', 'LSOA Name', 'LSOA Code', 'Number of domestic gas meters', 'Number of properties', 'Marker', "Measure Type", "Unit"]
    trace.start(datasetTitle, tab, columns, scraper.distributions[0].downloadURL)

    if 'Title' in name or 'ANNEX Sub-national publications' in name:
        continue
    period = name[0:4]
    trace.Period("Period year taken from sheet name : " + period)

    local_authority_name = tab.filter(contains_string('Local Authority Name')).shift(0,1).expand(DOWN).is_not_blank()
    trace.Local_Authority_Name("Selected as all non blank values from cell ref A3 down")

    local_authority_code = tab.filter(contains_string('Local Authority Code')).shift(0,1).expand(DOWN).is_not_blank()
    trace.Local_Authority_Code("Selected as all non blank values from cell ref B3 down")

    msoa_name = tab.filter(contains_string('MSOA Name')).shift(0,1).expand(DOWN).is_not_blank()
    trace.MSOA_Name("Selected as all non blank values from cell ref C3 down")

    msoa_code = tab.filter(contains_string('Middle Layer Super Output Area (MSOA) Code')).shift(0,1).expand(DOWN).is_not_blank()
    trace.MSOA_Code("Selected as all non blank values from cell ref D3 down")

    lsoa_name = tab.filter(contains_string('LSOA Name')).shift(0,1).expand(DOWN).is_not_blank()
    trace.LSOA_Name("Selected as all non blank values from cell ref E3 down")

    lsoa_code = tab.filter(contains_string('Lower Layer Super Output Area (LSOA) Code')).shift(0,1).expand(DOWN).is_not_blank()
    trace.LSOA_Code("Selected as all non blank values from cell ref F3 down")

    num_gas_meters = tab.filter(contains_string('Number of domestic gas meters')).shift(0,1).expand(DOWN).is_not_blank()
    trace.Number_of_domestic_gas_meters("Selected as all non blank values from cell ref G3 down")

    num_properties = tab.filter(contains_string('Number of properties3')).shift(0,1).expand(DOWN).is_not_blank()
    trace.Number_of_properties("Selected as all non blank values from cell ref H3 down")


    ##### Note will be 2 different measure types, although % can be derieved. Will need updated when handling of multiple measture types is in place
    measure_type = 'Properties not connected to gas network'
    trace.Measure_Type("Hardcoded as 'Properties not connected to gas network' ")
    #####################################################

    unit = 'Count'
    trace.Unit("Hardcoded as 'Count' ")

    observations = tab.filter(contains_string('Estimated number of properties not connected to the gas network 4')).shift(0,1).expand(DOWN).is_not_blank()
    dimensions = [
        HDimConst('Period', period),
        HDim(local_authority_name, 'Local Authority Name', DIRECTLY, LEFT),
        HDim(local_authority_code, 'Local Authority Code', DIRECTLY, LEFT),
        HDim(msoa_name, 'MSOA Name', DIRECTLY, LEFT),
        HDim(msoa_code, 'Middle Layer Super Output Area (MSOA) Code', DIRECTLY, LEFT),
        HDim(lsoa_name, 'LSOA Name', DIRECTLY, LEFT),
        HDim(lsoa_code, 'Lower Layer Super Output Area (LSOA) Code', DIRECTLY, LEFT),
        HDim(num_gas_meters, 'Number of domestic gas meters', DIRECTLY, LEFT),
        HDim(num_properties, 'Number of properties', DIRECTLY, LEFT),
        HDimConst('Measure Type', measure_type),
        HDimConst('Unit', unit),
        ]
    tidy_sheet = ConversionSegment(tab, dimensions, observations)
    trace.with_preview(tidy_sheet)
    #savepreviewhtml(tidy_sheet)
    trace.store("combined_dataframe", tidy_sheet.topandas())


df = trace.combine_and_trace(datasetTitle, "combined_dataframe")
df.rename(columns={'OBS' : 'Value', 'DATAMARKER' : 'Marker'}, inplace=True)
df['Marker'] = df['Marker'].map(lambda x: "unknown" if x == ".." else "estimated")

from IPython.core.display import HTML
for col in df:
    if col not in ['Value']:
        df[col] = df[col].astype('category')
        display(HTML(f"<h2>{col}</h2>"))
        display(df[col].cat.categories)

df = df.replace({'Period' : {
    '2015' : '1st October 2014 - 1st October 2015',
    '2016' : '15th July 2016 – 15th July 2017',
    '2017' : '15th June 2017 – 15th June 2018',
    '2018' : '15th May 2018 – 15th May 2019',
    '2019' : '16th May 2019 – 15th May 2020'
}})


tidy = df[['Period', 'Local Authority Name', 'Local Authority Code', 'MSOA Name', 'Middle Layer Super Output Area (MSOA) Code',
           'LSOA Name', 'Lower Layer Super Output Area (LSOA) Code', 'Number of domestic gas meters', 'Number of properties',
           'Measure Type', 'Unit', 'Value', 'Marker']]

for column in tidy:
    if column in ('Period', 'Local Authority Name', 'MSOA Name', 'LSOA Name' ,'Measure Type'):
        tidy[column] = tidy[column].map(lambda x: pathify(x))

tidy


# In[9]:


del tidy['Local Authority Name']
del tidy['MSOA Name']
del tidy['LSOA Name']
del tidy['Measure Type']
del tidy['Unit']


# In[10]:


tidy = tidy.rename(columns={'Middle Layer Super Output Area (MSOA) Code':'Middle Layer Super Output Area',
                           'Lower Layer Super Output Area (LSOA) Code':'Lower Layer Super Output Area',
                           'Local Authority Code':'Local Authority'})


# In[11]:


df = tidy['Period'].str.split('-', 6, expand=True)
df['d1'] = df[0].astype(str) + " " + df[1].astype(str) + " " + df[2].astype(str)
df['d2'] = df[3].astype(str) + " " + df[4].astype(str) + " " + df[5].astype(str)
df.drop([0,1,2,3,4,5], axis=1, inplace=True)
df


# In[12]:


df['d1'] = pd.to_datetime(pd.Series(df['d1']))
df['d2'] = pd.to_datetime(pd.Series(df['d2']))

df['diff'] = (df['d2'] - df['d1']).dt.days
df['val'] = 'gregorian-interval/' + df['d1'].astype(str) + 'T00:00:00/P' + df['diff'].astype(str) + 'D'

tidy['Period'] = df['val']


# In[13]:


tidy['Value'][tidy['Value'] == ''] = 0


# In[14]:


del tidy['Number of domestic gas meters']
del tidy['Number of properties']


# In[15]:


#tidy.head(6)


# In[16]:


import os
from urllib.parse import urljoin

notes = 'Link to methodology: https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/920993/sub-national-consumption-methodology-guidance-2020.pdf'

csvName = 'observations.csv'
out = Path('out')
out.mkdir(exist_ok=True)
#tidy.drop_duplicates().to_csv(out / csvName, index = False)
#tidy.drop_duplicates().to_csv(out / (csvName + '.gz'), index = False, compression='gzip')

scraper.dataset.family = 'towns-high-streets'
scraper.dataset.description = """
LSOA level estimates for the number of properties without mains gas. Estimates at local authority and MSOA levels are also available.

This dataset estimates the number of properties not connected to the gas network, at an LSOA level for England, Wales & Scotland

The estimates are produced by comparing the number of properties for each LSOA (as given by VOA estimates of domestic properties) with estimates for the number of domestic gas meters in each LSOA (as given in BEIS's sub-national consumption statistics).

    1. A methodology and guidance booklet containing further information
        about all sub-national energy consumption datasets:
        https://www.gov.uk/government/statistics/regional-energy-data-guidance-note

From 2015 onwards, in compliance with the ONS geography policy, the local authorities in Scotland and Wales have been re-ordered.

The current data set has been revised to incorporate Valuation Office Agency (VOA) estimates of the number of properties in England and Wales local authorities.
The number of properties in Scotland uses data published by the Scottish Government

Please note that there is no definitive source for the number of properties not on the gas grid, so BEIS estimates these figures by subtracting the number of domestic gas meters from the estimated number of properties

In some cases, the estimated number of domestic gas meters in an area is greater than the number of properties. The likely explanation for this is due to the fact that BEIS sub-national statistics use an industry cut off of 73,200kWh to determine whether a gas meter is domestic or not, with  all meters with consumption of 73,200 kWh or below assumed to be domestic. This means a number of smaller commercial/industrial consumers  are allocated as domestic and therefore the estimates of the number of properties without gas is an underestimate of the true number.

Data for number of properties by local authority is obtained from the sources linked below
Scotland:
https://www.nrscotland.gov.uk/statistics-and-data/statistics/statistics-by-theme/households/household-estimates/2018
England and Wales:
https://www.gov.uk/government/statistics/council-tax-stock-of-properties-2018

To estimate the number of properties by LSOA and MSOA in Scotland, the LA level household totals were split in proportion to the number of domestic electricity
meters in each LSOA and MSOA. For more information about this estimation, see methodology and guidance booklet

In the case that the estimated number of gas meters in an LSOA is greater than the number of properties, it is assumed that there are no properties not connected to the gas grid in that area
Unallocated meters are meters that were not matched to a Local Authority due to in complete or a lack of address information.
The percentage listed for Great Britain excludes unallocated meters from the denominator
"""
scraper.dataset.comment = 'LSOA level estimates for the number of properties without mains gas. Estimates at local authority and MSOA levels are also available.'

cubes.add_cube(scraper, tidy.drop_duplicates(), csvName)


# In[16]:


cubes.output_all()

