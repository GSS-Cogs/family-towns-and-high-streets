#!/usr/bin/env python
# coding: utf-8

# In[41]:


from gssutils import *
import json
from zipfile import ZipFile
from io import BytesIO
from ntpath import basename

info = json.load(open('info.json'))
etl_title = info["title"]
etl_publisher = info["publisher"]
print("Publisher: " + etl_publisher)
print("Title: " + etl_title)

cubes = Cubes("info.json")

scraper = Scraper(seed="info.json")
scraper


# In[42]:


trace = TransformTrace()
tidied_sheets = {} # dataframes will be stored in here

for distribution in scraper.distributions:

    # LSOA data first
    if distribution.downloadURL.endswith('zip') and 'LSOA' in distribution.title:
        with ZipFile(BytesIO(scraper.session.get(distribution.downloadURL).content)) as zip:
            for name in zip.namelist()[1:]:
                if 'LSOA' in basename(name):
                    with zip.open(name, 'r') as file:

                        link = distribution.downloadURL
                        file_name = name.split("/")[-1].split(".")[0] # the name of the file

                        columns = ['Period', 'LA Name', 'LA Code', 'MSOA Name', 'MSOA Code', 'LSOA Name', 'LSOA Code', 'METERS', 'MEAN', 'MEDIAN', 'Value']

                        trace.start(scraper.title, name, columns, link)

                        table = pd.read_csv(file, dtype=str)

                        year = file_name[-4:] # year is last 4 characters of file_name
                        table['Period'] = year

                        trace.Period("Value taken from CSV file name: {}".format(year))
                        trace.LA_Name("Values taken from 'LAName' column")
                        trace.LA_Code("Values taken from 'LACode' column")
                        trace.MSOA_Name("Values taken from 'MSOAName' column")
                        trace.MSOA_Code("Values taken from 'MSOACode' column")
                        trace.LSOA_Name("Values taken from 'LSOAName' column")
                        trace.LSOA_Code("Values taken from 'LSOACode' column")
                        trace.METERS("Values taken from 'METERS' column")
                        trace.MEAN("Values taken from 'MEAN' column")
                        trace.MEDIAN("Values taken from 'MEDIAN' column")
                        trace.Value("Values taken from 'KWH' column")

                        # rename column
                        table = table.rename(columns={'Consumption (kWh)':'Value'})
                        trace.Value('Rename column from "KWH" to "Value"')

                        trace.store(file_name, table)
                        tidied_sheets[file_name] = table

    # MSOA domestic data second
    elif distribution.downloadURL.endswith('zip') and 'MSOA domestic' in distribution.title:
        with ZipFile(BytesIO(scraper.session.get(distribution.downloadURL).content)) as zip:
            for name in zip.namelist()[1:]:
                if 'MSOA' in basename(name):
                    with zip.open(name, 'r') as file:

                        link = distribution.downloadURL
                        file_name = name.split("/")[-1].split(".")[0] # the name of the file

                        columns = ['Period', 'LA Name', 'LA Code', 'MSOA Name', 'MSOA Code', 'METERS', 'MEAN', 'MEDIAN', 'Value']

                        trace.start(scraper.title, name, columns, link)

                        table = pd.read_csv(file, dtype=str)

                        year = file_name[-4:] # year is last 4 characters of file_name
                        table['Period'] = year

                        trace.Period("Value taken from CSV file name: {}".format(year))
                        trace.LA_Name("Values taken from 'LAName' column")
                        trace.LA_Code("Values taken from 'LACode' column")
                        trace.MSOA_Name("Values taken from 'MSOAName' column")
                        trace.MSOA_Code("Values taken from 'MSOACode' column")
                        trace.METERS("Values taken from 'METERS' column")
                        trace.MEAN("Values taken from 'MEAN' column")
                        trace.MEDIAN("Values taken from 'MEDIAN' column")
                        trace.Value("Values taken from 'KWH' column")

                        # rename column
                        table = table.rename(columns={'Consumption (kWh)':'Value'})
                        trace.Value('Rename column from "KWH" to "Value"')

                        trace.store(file_name, table)
                        tidied_sheets[file_name] = table

    # MSOA 'Non' domestic data next
    elif distribution.downloadURL.endswith('zip') and 'MSOA non domestic' in distribution.title:
        with ZipFile(BytesIO(scraper.session.get(distribution.downloadURL).content)) as zip:
            for name in zip.namelist()[1:]:
                if 'MSOA' in basename(name):
                    with zip.open(name, 'r') as file:

                        link = distribution.downloadURL
                        file_name = name.split("/")[-1].split(".")[0] # the name of the file

                        columns = ['Period', 'LA Name', 'LA Code', 'MSOA Name', 'MSOA Code', 'METERS', 'MEAN', 'MEDIAN', 'Value']

                        trace.start(scraper.title, name, columns, link)

                        table = pd.read_csv(file, dtype=str)

                        year = file_name[-4:] # year is last 4 characters of file_name
                        table['Period'] = year

                        trace.Period("Value taken from CSV file name: {}".format(year))
                        trace.LA_Name("Values taken from 'LAName' column")
                        trace.LA_Code("Values taken from 'LACode' column")
                        trace.MSOA_Name("Values taken from 'MSOAName' column")
                        trace.MSOA_Code("Values taken from 'MSOACode' column")
                        trace.METERS("Values taken from 'METERS' column")
                        trace.MEAN("Values taken from 'MEAN' column")
                        trace.MEDIAN("Values taken from 'MEDIAN' column")
                        trace.Value("Values taken from 'KWH' column")

                        # rename column
                        table = table.rename(columns={'KWH':'Value'})
                        trace.Value('Rename column from "KWH" to "Value"')

                        # reordering columns
                        trace.store(file_name, table)
                        tidied_sheets[file_name] = table


# In[43]:


#for key in tidied_sheets:
#    print(key)
# -

trace.render("spec_v1.html")

# Only output LSOA data for now until PMD4 can handle multiple outputs
lsoa_dat = pd.DataFrame(tidied_sheets['LSOA_GAS_2019'])
for key in tidied_sheets:
    if 'LSOA' in key:
        print('joining: ' + key)
        lsoa_dat = pd.concat([lsoa_dat,pd.DataFrame(tidied_sheets[key])], sort=False)

# Remove attributes for now until we dicide whow we are handling them
del lsoa_dat['Local Authority Name']
del lsoa_dat['MSOA Name']
del lsoa_dat['LSOA Name']
del lsoa_dat['Number of consuming meters']
del lsoa_dat['Mean consumption (kWh per meter)']
del lsoa_dat['Median consumption (kWh per meter)']


# In[44]:


#Rename the columns to match the Electricity pipeline
'''
lsoa_dat = lsoa_dat.rename(columns=
                           {
                               'METERS': 'Total number of domestic electricity meters',
                               'MEAN': 'Mean domestic electricity consumption kWh per meter',
                               'MEDIAN': 'Median domestic electricity consumption kWh per meter'
                           })
'''


lsoa_dat = lsoa_dat.rename(columns=
                           {
                               'Period': 'Year',
                               'Local Authority Code': 'Local Authority',
                               'Middle Layer Super Output Area (MSOA) Code': 'Middle Layer Super Output Area',
                               'Lower Layer Super Output Area (LSOA) Code': 'Lower Layer Super Output Area'
                           })
lsoa_dat['Year'] = 'year/' + lsoa_dat['Year'].astype(str)


# In[45]:


lsoa_dat.head(10)


# In[46]:


import os
from urllib.parse import urljoin

notes = 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/853104/sub-national-methodology-guidance.pdf'

csvName = 'lsoa_observations'
out = Path('out')
out.mkdir(exist_ok=True)
#lsoa_dat.drop_duplicates().to_csv(out / csvName, index = False)
#lsoa_dat.drop_duplicates().to_csv(out / (csvName + '.gz'), index = False, compression='gzip')

scraper.dataset.family = 'towns-high-streets'
scraper.dataset.description = scraper.dataset.description + '\nGuidance documentation can be found here:\n' + notes
#scraper.dataset.comment = 'Total domestic gas consumption, number of meters, mean and median consumption for LSOA regions across England, Wales & Scotland'
scraper.dataset.comment = 'Total domestic gas consumption for LSOA regions across England, Wales & Scotland'
scraper.dataset.title = 'Lower Super Output Areas (LSOA) gas consumption'

cubes.add_cube(scraper, lsoa_dat.drop_duplicates(), csvName)


# In[47]:


cubes.output_all()


# In[48]:


"""metadata_json = open(f"./out/{csvName}.csv-metadata.json", "r")
metadata = json.load(metadata_json)
metadata_json.close()

for obj in metadata["tables"][0]["tableSchema"]["columns"]:
    if obj["name"] in ["number_of_non_consuming_meters", "number_of_meters"] :
        obj.pop('valueUrl', None)

metadata_json = open(f"./out/{csvName}.csv-metadata.json", "w")
json.dump(metadata, metadata_json, indent=4)
metadata_json.close()
"""

