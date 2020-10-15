#!/usr/bin/env python
# coding: utf-8

# In[3]:


# # LSOA estimates of properties not connected to the gas network

from gssutils import *
import json

info = json.load(open('info.json'))
#scraper = Scraper(seed="info.json")
scraper = Scraper(info['landingPage'])
scraper


# In[4]:


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
df['Marker'] = df['Marker'].map(lambda x: "unknown" if x == ".." else "")

from IPython.core.display import HTML
for col in df:
    if col not in ['Value']:
        df[col] = df[col].astype('category')
        display(HTML(f"<h2>{col}</h2>"))
        display(df[col].cat.categories)

df = df.replace({'Period' : {
    '2015' : '1st October 2014 - 31st September 2015',
    '2016' : '15th July 2016 – 15th July 2017',
    '2017' : '15th June 2017 – 15th June 2018',
    '2018' : '15th May 2018 – 15th May 2019'
}})


tidy = df[['Period', 'Local Authority Name', 'Local Authority Code', 'MSOA Name', 'Middle Layer Super Output Area (MSOA) Code',
           'LSOA Name', 'Lower Layer Super Output Area (LSOA) Code', 'Number of domestic gas meters', 'Number of properties',
           'Measure Type', 'Unit', 'Value', 'Marker']]

for column in tidy:
    if column in ('Period', 'Local Authority Name', 'MSOA Name', 'LSOA Name' ,'Measure Type'):
        tidy[column] = tidy[column].map(lambda x: pathify(x))

tidy


# In[5]:


out = Path('out')
out.mkdir(exist_ok=True)

df.drop_duplicates().to_csv(out / 'observations.csv', index = False)

scraper.dataset.description = "Link to methodology: https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/920993/sub-national-consumption-methodology-guidance-2020.pdf"
scraper.dataset.comment = """
Statistician Responsible:
Adam Bricknell

Prepared by:
Oliver Hendey
EnergyEfficiency.Stats@beis.gov.uk
020 7215 0222"""
scraper.dataset.contactPoint = "mailto:EnergyEfficiency.Stats@beis.gov.uk"

with open(out / 'observations.csv-metadata.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())

trace.render()


# In[5]:




