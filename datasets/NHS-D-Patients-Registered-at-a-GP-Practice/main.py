#!/usr/bin/env python
# coding: utf-8

# In[154]:


from gssutils import *
import json
import datetime
import glob
import numpy as np

info = json.load(open('info.json'))
etl_title = info["title"]
etl_publisher = info["publisher"]
print("Publisher: " + etl_publisher)
print("Title: " + etl_title)

cubes = Cubes('info.json')

scraper = Scraper(seed="info.json")
scraper


# In[155]:



dataURLS = {'Single year of age (GP practice-males)' : 'https://files.digital.nhs.uk/52/59662A/gp-reg-pat-prac-sing-age-male.csv',
            '5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice)' : 'https://files.digital.nhs.uk/79/FC50B1/gp-reg-pat-prac-quin-age.csv',
            'Single year of age (GP practice-females)' : 'https://files.digital.nhs.uk/D1/31EEE3/gp-reg-pat-prac-sing-age-female.csv',
            'Totals (GP practice-all persons)' : 'https://files.digital.nhs.uk/40/2232E5/gp-reg-pat-prac-all.csv',
            'Single year of age (Commissioning Regions-STPs-CCGs-PCNs)' : 'https://files.digital.nhs.uk/89/FC6DB4/gp-reg-pat-prac-sing-age-regions.csv'}


# In[156]:


trace = TransformTrace()
tidied_data = {} # dataframes will be stored in here

def TimeFormatter(value):
    # function to return time as yyyy-mm-dd
    value_split = value[:2] + '-' + value[2:5].title() + '-' + value[5:]
    value_as_datetime = datetime.datetime.strptime(value_split, '%d-%b-%Y')
    new_value = datetime.datetime.strftime(value_as_datetime, '%Y-%m-%d')
    return new_value

"""
# hacky way of returning latest data
current_date = datetime.datetime.now().date()
current_month = current_date.strftime('%B')
while scraper.distributions == []:
    scraper.select_dataset(title=lambda x: current_month in x, latest=True)
    current_date = datetime.datetime(current_date.year, current_date.month-1, current_date.day)
    current_month = current_date.strftime('%B')
    """

for title, link in dataURLS.items():

    print(title)
    print(link)

    if 'males' in title.lower(): # also covers females

        with open("info.json", "r") as jsonFile:
            data = json.load(jsonFile)

            data["dataURL"] = link

        with open("info.json", "w") as jsonFile:
            json.dump(data, jsonFile, indent = 2)

        scraper = Scraper(seed="info.json")
        scraper.distributions[0].title = title

        columns = ['Period', 'CCG_CODE', 'ONS_CCG_CODE', 'ORG_CODE', 'POSTCODE', 'SEX', 'AGE', 'Value']
        trace.start(scraper.title, title, columns, link)
        df = scraper.distributions[0].as_pandas()

        trace.Period('Values taken from "EXTRACT_DATE" column')
        trace.CCG_CODE('Values taken from "CCG_CODE" column')
        trace.ONS_CCG_CODE('Values taken from "ONS_CCG_CODE" column')
        trace.ORG_CODE('Values taken from "ORG_CODE" column')
        trace.POSTCODE('Values taken from "POSTCODE" column')
        trace.SEX('Values taken from "SEX" column')
        trace.AGE('Values taken from "AGE" column')
        trace.Value('Values taken from "NUMBER_OF_PATIENTS" column')

        df = df.rename(columns={'EXTRACT_DATE':'Period', 'NUMBER_OF_PATIENTS':'Value'})
        trace.Period('Rename column from "EXTRACT_DATE" to "Period"')
        trace.Value('Rename column from "NUMBER_OF_PATIENTS" to "Value"')

        df['Period'] = df['Period'].apply(TimeFormatter)
        trace.Period('Values have been formatted to "yyyy-mm-dd"')

        #trace.store(title, df)
        tidied_data[title] = df

    elif 'age groups' in title.lower():

        with open("info.json", "r") as jsonFile:
            data = json.load(jsonFile)

            data["dataURL"] = link

        with open("info.json", "w") as jsonFile:
            json.dump(data, jsonFile, indent = 2)

        scraper = Scraper(seed="info.json")
        scraper.distributions[0].title = title

        columns = ['Period', 'PUBLICATION', 'ORG_TYPE', 'ORG_CODE', 'ONS_CODE', 'POSTCODE', 'SEX', 'AGE_GROUP_5', 'Value']
        trace.start(scraper.title, title, columns, link)
        df = scraper.distributions[0].as_pandas()

        trace.Period('Values taken from "EXTRACT_DATE" column')
        trace.PUBLICATION('Values taken from "PUBLICATION" column')
        trace.ORG_TYPE('Values taken from "ORG_TYPE" column')
        trace.ORG_CODE('Values taken from "ORG_CODE" column')
        trace.ONS_CODE('Values taken from "ONS_CODE" column')
        trace.POSTCODE('Values taken from "POSTCODE" column')
        trace.SEX('Values taken from "SEX" column')
        trace.AGE_GROUP_5('Values taken from "AGE_GROUP_5" column')
        trace.Value('Values taken from "NUMBER_OF_PATIENTS" column')

        df = df.rename(columns={'EXTRACT_DATE':'Period', 'NUMBER_OF_PATIENTS':'Value'})
        trace.Period('Rename column from "EXTRACT_DATE" to "Period"')
        trace.Value('Rename column from "NUMBER_OF_PATIENTS" to "Value"')

        df['Period'] = df['Period'].apply(TimeFormatter)
        trace.Period('Values have been formatted to "yyyy-mm-dd"')

        # reordered df
        df = df[columns]

        #trace.store(title, df)
        tidied_data[title] = df

    elif 'totals' in title.lower():

        with open("info.json", "r") as jsonFile:
            data = json.load(jsonFile)

            data["dataURL"] = link

        with open("info.json", "w") as jsonFile:
            json.dump(data, jsonFile, indent = 2)

        scraper = Scraper(seed="info.json")
        scraper.distributions[0].title = title

        columns = ['Period', 'PUBLICATION', 'TYPE', 'CCG_CODE', 'ONS_CCG_CODE', 'CODE', 'POSTCODE', 'SEX', 'AGE', 'Value']
        trace.start(scraper.title, title, columns, link)
        df = scraper.distributions[0].as_pandas()

        trace.Period('Values taken from "EXTRACT_DATE" column')
        trace.PUBLICATION('Values taken from "PUBLICATION" column')
        trace.TYPE('Values taken from "TYPE" column')
        trace.CCG_CODE('Values taken from "CCG_CODE" column')
        trace.ONS_CCG_CODE('Values taken from "ONS_CCG_CODE" column')
        trace.CODE('Values taken from "CODE" column')
        trace.POSTCODE('Values taken from "POSTCODE" column')
        trace.SEX('Values taken from "SEX" column')
        trace.AGE('Values taken from "AGE" column')
        trace.Value('Values taken from "NUMBER_OF_PATIENTS" column')

        df = df.rename(columns={'EXTRACT_DATE':'Period', 'NUMBER_OF_PATIENTS':'Value'})
        trace.Period('Rename column from "EXTRACT_DATE" to "Period"')
        trace.Value('Rename column from "NUMBER_OF_PATIENTS" to "Value"')

        df['Period'] = df['Period'].apply(TimeFormatter)
        trace.Period('Values have been formatted to "yyyy-mm-dd"')

        # reordered df
        df = df[columns]

        #trace.store(title, df)
        tidied_data[title] = df

    elif 'region' in title.lower():

        with open("info.json", "r") as jsonFile:
            data = json.load(jsonFile)

            data["dataURL"] = link

        with open("info.json", "w") as jsonFile:
            json.dump(data, jsonFile, indent = 2)

        scraper = Scraper(seed="info.json")
        scraper.distributions[0].title = title

        columns = ['Period', 'PUBLICATION', 'ORG_TYPE', 'ORG_CODE', 'ONS_CODE', 'SEX', 'AGE', 'Value']
        trace.start(scraper.title, title, columns, link)
        df = scraper.distributions[0].as_pandas()

        trace.Period('Values taken from "EXTRACT_DATE" column')
        trace.PUBLICATION('Values taken from "PUBLICATION" column')
        trace.ORG_TYPE('Values taken from "ORG_TYPE" column')
        trace.ORG_CODE('Values taken from "ORG_CODE" column')
        trace.ONS_CODE('Values taken from "ONS_CODE" column')
        trace.SEX('Values taken from "SEX" column')
        trace.AGE('Values taken from "AGE" column')
        trace.Value('Values taken from "NUMBER_OF_PATIENTS" column')

        df = df.rename(columns={'EXTRACT_DATE':'Period', 'NUMBER_OF_PATIENTS':'Value'})
        trace.Period('Rename column from "EXTRACT_DATE" to "Period"')
        trace.Value('Rename column from "NUMBER_OF_PATIENTS" to "Value"')

        df['Period'] = df['Period'].apply(TimeFormatter)
        trace.Period('Values have been formatted to "yyyy-mm-dd"')

        # reordered df
        df = df[columns]

        #trace.store(title, df)
        tidied_data[title] = df

df


# In[157]:


del tidied_data['Totals (GP practice-all persons)']['PUBLICATION']
del tidied_data['Totals (GP practice-all persons)']['TYPE']
del tidied_data['Totals (GP practice-all persons)']['CCG_CODE']

#del tidied_data['Single year of age (GP practice-females)']['PUBLICATION']
#del tidied_data['Single year of age (GP practice-females)']['TYPE']
del tidied_data['Single year of age (GP practice-females)']['CCG_CODE']

#del tidied_data['Single year of age (GP practice-males)']['PUBLICATION']
#del tidied_data['Single year of age (GP practice-males)']['TYPE']
del tidied_data['Single year of age (GP practice-males)']['CCG_CODE']


# In[158]:


tidied_data['Totals (GP practice-all persons)'] = tidied_data['Totals (GP practice-all persons)'].rename(columns={
    #'CCG_CODE': 'CCG Code',
    'ONS_CCG_CODE': 'ONS CCG Code',
    'CODE': 'Practice Code',
    'POSTCODE': 'Post Code',
    'SEX': 'Sex',
    'AGE': 'Age'
})

tidied_data['Single year of age (GP practice-females)'] = tidied_data['Single year of age (GP practice-females)'].rename(columns={
    #'CCG_CODE': 'CCG Code',
    'ONS_CCG_CODE': 'ONS CCG Code',
    'ORG_CODE': 'Practice Code',
    'POSTCODE': 'Post Code',
    'SEX': 'Sex',
    'AGE': 'Age'
})

tidied_data['Single year of age (GP practice-males)'] = tidied_data['Single year of age (GP practice-males)'].rename(columns={
    #'CCG_CODE': 'CCG Code',
    'ONS_CCG_CODE': 'ONS CCG Code',
    'ORG_CODE': 'Practice Code',
    'POSTCODE': 'Post Code',
    'SEX': 'Sex',
    'AGE': 'Age'
})

tidied_data['5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice)'] = tidied_data['5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice)'].rename(columns={
    'ORG_TYPE': 'ORG Type',
    'ORG_CODE': 'ORG Code',
    'ONS_CODE': 'ONS Code',
    'POSTCODE': 'Post Code',
    'SEX': 'Sex',
    'AGE_GROUP_5': 'Age'
})
#tidied_data['5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice)']


# In[159]:


# In the 5 year age group only GP data has a post code so extract it and add to the GP dataset
# but it does not have an ONS Code or CCG Code so map it from the other data
gp5yr = tidied_data['5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice)']
gp5yr = gp5yr[gp5yr['ORG Type'] == 'GP']
del gp5yr['ORG Type']
del gp5yr['PUBLICATION']
del gp5yr['ONS Code']
gp5yr = gp5yr.rename(columns={'ORG Code': 'Practice Code'})

un = tidied_data['Single year of age (GP practice-females)'][['Practice Code','ONS CCG Code']]
#od = tidied_data['Single year of age (GP practice-females)'][['Practice Code','CCG Code']]

un = pd.DataFrame(un.groupby(['Practice Code', 'ONS CCG Code']).size().reset_index().rename(columns={0:'count'}))
#od = pd.DataFrame(od.groupby(['Practice Code', 'CCG Code']).size().reset_index().rename(columns={0:'count'}))

del un['count']
#del od['count']

gp5yr['ONS CCG Code'] = gp5yr['Practice Code'].map(un.set_index('Practice Code')['ONS CCG Code'])
#gp5yr['CCG Code'] = gp5yr['Practice Code'].map(od.set_index('Practice Code')['CCG Code'])
#gp5yr


# In[160]:


gp_practice = pd.concat([tidied_data['Totals (GP practice-all persons)'],
                         tidied_data['Single year of age (GP practice-females)'],
                         tidied_data['Single year of age (GP practice-males)'],
                         gp5yr], sort=True)

#gp_practice['CCG Code'] = gp_practice['CCG Code'].apply(pathify)
gp_practice['Practice Code'] = gp_practice['Practice Code'].apply(pathify)
gp_practice['Age'] = gp_practice['Age'].str.replace('_','T')
gp_practice['Age'] = 'Y' + gp_practice['Age'].astype(str)
gp_practice['Sex'] = gp_practice['Sex'].replace({
    'ALL':'T',
    'FEMALE':'F',
    'FEMALES':'F',
    'MALE':'M',
    'MALES':'M',
    'UNKNOWN':'U'
})
gp_practice['Period'] = 'day/' + gp_practice['Period'].astype(str)
gp_practice['Post Code'] = gp_practice['Post Code'].str.replace(' ', '')
gp_practice['Age'] = gp_practice['Age'].str.replace('+', '-plus')
gp_practice = gp_practice[['Period','ONS CCG Code','Post Code','Practice Code','Age','Sex','Value']]

gp_practice = gp_practice.drop_duplicates()
# +
#print('All: ' + str(gp_practice['Period'].count()))

#for c in gp_practice:
#    if c != 'Value':
#        print('Column: ' + c)
#        print('Row count: ' + str(gp_practice[c].count()))
#        print(list(gp_practice[c].unique()))
#        print("#######################################")


# In[161]:


import os
from urllib.parse import urljoin

notes = f"""
Data for this publication are extracted each month as a snapshot in time from the Primary Care Registration database within the NHAIS (National Health Application and Infrastructure Services) system.
GP Practice; Primary Care Network (PCN); Sustainability and transformation partnership (STP); Clinical Commissioning Group (CCG) and NHS England Commissioning Region level data are released in single year of age (SYOA) and 5-year age bands, both of which finish at 95+, split by gender. In addition, organisational mapping data is available to derive STP; PCN; CCG and Commissioning Region associated with a GP practice and is updated each month to give relevant organisational mapping.
Quarterly publications in January, April, July and October will include Lower Layer Super Output Area (LSOA) populations and a spotlight report.
The outbreak of Coronavirus (COVID-19) has led to changes in the work of General Practices and subsequently the data within this publication. Until activity in this healthcare setting stabilises, we urge caution in drawing any conclusions from these data without consideration of the country
"""

csvName = 'gp_observations'

scraper.dataset.family = 'towns-and-high-streets'
scraper.dataset.description = notes
scraper.dataset.comment = 'Data for this publication are extracted each month as a snapshot in time from the Primary Care Registration database within the NHAIS (National Health Application and Infrastructure Services) system.'
scraper.dataset.title = 'Patients Registered at a GP Practice - GP'

cubes.add_cube(scraper, gp_practice, csvName)

"""out = Path('out')
out.mkdir(exist_ok=True)
gp_practice.drop_duplicates().to_csv(out / csvName, index = False)
gp_practice.drop_duplicates().to_csv(out / (csvName + '.gz'), index = False, compression='gzip')

dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name) + '/gp').lower()
scraper.set_base_uri('http://gss-data.org.uk')
scraper.set_dataset_id(dataset_path)

csvw_transform = CSVWMapping()
csvw_transform.set_csv(out / csvName)
csvw_transform.set_mapping(json.load(open('info.json')))
csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
csvw_transform.write(out / f'{csvName}-metadata.json')

with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())"""


# In[162]:


del tidied_data['Single year of age (Commissioning Regions-STPs-CCGs-PCNs)']['PUBLICATION']
del tidied_data['5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice)']['PUBLICATION']
del tidied_data['5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice)']['Post Code']

# Get rid of the GP data because we have added to to the first dataset
other5yr = tidied_data['5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice)']
other5yr = other5yr[other5yr['ORG Type'] != 'GP']
tidied_data['5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice)'] = other5yr
#tidied_data['5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice)'].head(10)

tidied_data['Single year of age (Commissioning Regions-STPs-CCGs-PCNs)'] = tidied_data['Single year of age (Commissioning Regions-STPs-CCGs-PCNs)'].rename(columns={
    'ORG_TYPE': 'ORG Type',
    'ONS_CODE': 'ONS Code',
    'ORG_CODE': 'ORG Code',
    'SEX': 'Sex',
    'AGE': 'Age'
})
#tidied_data['Single year of age (Commissioning Regions-STPs-CCGs-PCNs)'].head(10)


# In[163]:


org_practice = pd.concat([tidied_data['Single year of age (Commissioning Regions-STPs-CCGs-PCNs)'],
                         tidied_data['5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice)']
                         ], sort=True)

org_practice['Sex'] = org_practice['Sex'].replace({
    'ALL':'T',
    'FEMALE':'F',
    'FEMALES':'F',
    'MALE':'M',
    'MALES':'M',
    'UNKNOWN':'U'
})


# In[164]:


#print('All: ' + str(org_practice['Period'].count()))

#for c in org_practice:
#    if c != 'Value':
#        print('Column: ' + c)
#        print('Row count: ' + str(org_practice[c].count()))
#        print(list(org_practice[c].unique()))
#        print("#######################################")


# In[165]:


# We can map CCG, STP and COMM regions to Geograpgy codes but not PCN.
# So publish PCN on its own and the rest together
print('Org data count before removing PCN data: ' + str(org_practice['Age'].count()))
pcn_practice = org_practice[org_practice['ORG Type'] == 'PCN']
print('PCN data count: ' + str(pcn_practice['Age'].count()))
org_practice = org_practice[org_practice['ORG Type'] != 'PCN']
print('Org data count after before removing PCN data: ' + str(org_practice['Age'].count()))


# In[166]:


# Pull in the mapping files and concat
#ccgMap = pd.read_csv('../../Reference/nhs-ccg-map-to-geography.csv')
#stpMap = pd.read_csv('../../Reference/nhs-stp-map-to-geography.csv')
#comMap = pd.read_csv('../../Reference/nhs-commissioning-region-map-to-geography.csv')
#allMap = pd.concat([ccgMap, stpMap, comMap])
# Map the 3 ORG codes (CCG, STP and COMM Region)
#org_practice['ORG Code'] = org_practice['ORG Code'].map(allMap.set_index('NHS Code')['Geog Code'])


# In[167]:


del org_practice['ORG Code']
org_practice['ORG Type'] = org_practice['ORG Type'].apply(pathify)
org_practice = org_practice.rename(columns={'ONS Code': 'ONS ORG Code'})
org_practice['Age'] = org_practice['Age'].str.replace('_','T')
org_practice['Age'] = 'Y' + org_practice['Age'].astype(str)
org_practice['Period'] = 'day/' + org_practice['Period'].astype(str)
org_practice = org_practice[['Period','ONS ORG Code','ORG Type','Age','Sex','Value']]
org_practice['Age'] = org_practice.apply(lambda x: str(x['Age']).replace('.0', '') if '.0' in str(x['Age']) else x['Age'], axis = 1)
org_practice['Age'] = org_practice['Age'].str.replace('+', '-plus')
org_practice['Age'].unique()
org_practice = org_practice.drop_duplicates()
org_practice


# In[168]:


notes = f"""
Data for this publication are extracted each month as a snapshot in time from the Primary Care Registration database within the NHAIS (National Health Application and Infrastructure Services) system.
GP Practice; Primary Care Network (PCN); Sustainability and transformation partnership (STP); Clinical Commissioning Group (CCG) and NHS England Commissioning Region level data are released in single year of age (SYOA) and 5-year age bands, both of which finish at 95+, split by gender. In addition, organisational mapping data is available to derive STP; PCN; CCG and Commissioning Region associated with a GP practice and is updated each month to give relevant organisational mapping.
Quarterly publications in January, April, July and October will include Lower Layer Super Output Area (LSOA) populations and a spotlight report.
The outbreak of Coronavirus (COVID-19) has led to changes in the work of General Practices and subsequently the data within this publication. Until activity in this healthcare setting stabilises, we urge caution in drawing any conclusions from these data without consideration of the country
"""

csvName = 'org_observations'

scraper.dataset.family = 'towns-and-high-streets'
scraper.dataset.description = notes
scraper.dataset.comment = 'Data for this publication are extracted each month as a snapshot in time from the Primary Care Registration database within the NHAIS (National Health Application and Infrastructure Services) system.'
scraper.dataset.title = 'Patients Registered at a GP Practice - CCG, STP, Comm Region'

cubes.add_cube(scraper, org_practice, csvName)

"""out = Path('out')
out.mkdir(exist_ok=True)
org_practice.drop_duplicates().to_csv(out / csvName, index = False)
org_practice.drop_duplicates().to_csv(out / (csvName + '.gz'), index = False, compression='gzip')

dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name) + '/org').lower()
scraper.set_base_uri('http://gss-data.org.uk')
scraper.set_dataset_id(dataset_path)

csvw_transform = CSVWMapping()
csvw_transform.set_csv(out / csvName)
csvw_transform.set_mapping(json.load(open('info.json')))
csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
csvw_transform.write(out / f'{csvName}-metadata.json')

with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())"""


# In[169]:


del pcn_practice['ONS Code']
pcn_practice = pcn_practice.rename(columns={'ORG Code': 'PCN Code'})
pcn_practice['Age'] = pcn_practice['Age'].str.replace('_','T')
pcn_practice['Age'] = 'Y' + pcn_practice['Age'].astype(str)
pcn_practice['ORG Type'] = pcn_practice['ORG Type'].apply(pathify)
pcn_practice['PCN Code'] = pcn_practice['PCN Code'].apply(pathify)
pcn_practice['Period'] = 'day/' + pcn_practice['Period'].astype(str)
pcn_practice['Age'] = pcn_practice.apply(lambda x: str(x['Age']).replace('.0', '') if '.0' in str(x['Age']) else x['Age'], axis = 1)
pcn_practice['Age'] = pcn_practice['Age'].str.replace('+', '-plus')
pcn_practice = pcn_practice[['Period','PCN Code','ORG Type','Age','Sex','Value']]
pcn_practice = pcn_practice.drop_duplicates()
#pcn_practice.head()


# In[170]:


notes = f"""
Data for this publication are extracted each month as a snapshot in time from the Primary Care Registration database within the NHAIS (National Health Application and Infrastructure Services) system.
GP Practice; Primary Care Network (PCN); Sustainability and transformation partnership (STP); Clinical Commissioning Group (CCG) and NHS England Commissioning Region level data are released in single year of age (SYOA) and 5-year age bands, both of which finish at 95+, split by gender. In addition, organisational mapping data is available to derive STP; PCN; CCG and Commissioning Region associated with a GP practice and is updated each month to give relevant organisational mapping.
Quarterly publications in January, April, July and October will include Lower Layer Super Output Area (LSOA) populations and a spotlight report.
The outbreak of Coronavirus (COVID-19) has led to changes in the work of General Practices and subsequently the data within this publication. Until activity in this healthcare setting stabilises, we urge caution in drawing any conclusions from these data without consideration of the country
"""

csvName = 'pcn_observations'

scraper.dataset.family = 'towns-high-streets'
scraper.dataset.description = notes
scraper.dataset.comment = 'Data for this publication are extracted each month as a snapshot in time from the Primary Care Registration database within the NHAIS (National Health Application and Infrastructure Services) system.'
scraper.dataset.title = 'Patients Registered at a GP Practice - PCN'

cubes.add_cube(scraper, pcn_practice, csvName)

"""out = Path('out')
out.mkdir(exist_ok=True)
pcn_practice.drop_duplicates().to_csv(out / csvName, index = False)
pcn_practice.drop_duplicates().to_csv(out / (csvName + '.gz'), index = False, compression='gzip')

dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name) + '/pcn').lower()
scraper.set_base_uri('http://gss-data.org.uk')
scraper.set_dataset_id(dataset_path)

csvw_transform = CSVWMapping()
csvw_transform.set_csv(out / csvName)
csvw_transform.set_mapping(json.load(open('info.json')))
csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
csvw_transform.write(out / f'{csvName}-metadata.json')

with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())"""


# In[174]:


with open("info.json", "r") as jsonFile:
    data = json.load(jsonFile)

    data["dataURL"] = 'https://files.digital.nhs.uk/EA/E45667/gp-reg-pat-prac-map.csv'

with open("info.json", "w") as jsonFile:
    json.dump(data, jsonFile, indent = 2)

scraper = Scraper(seed="info.json")

scraper.distributions[0].title = title

df = scraper.distributions[0].as_pandas()

df = df[['PRACTICE_CODE', 'PRACTICE_NAME', 'PCN_CODE', 'PCN_NAME', 'CCG_CODE', 'CCG_NAME']]

df


# In[196]:


out = Path('codelists')
out.mkdir(exist_ok=True)

path = 'out/*'


# In[ ]:


ptcCode = df[['PRACTICE_NAME', 'PRACTICE_CODE']]
ptcCode = ptcCode.rename(columns = {'PRACTICE_NAME' : 'Label', 'PRACTICE_CODE' : 'Notation'})
ptcCode = ptcCode.drop_duplicates()
ptcCode['Parent Notation'] = ''
ptcCode['Sort Priority'] = ptcCode.index
ptcCode['Label'] = ptcCode['Label'].str.capitalize()
ptcCode['Notation'] = ptcCode.apply(lambda x: pathify(x['Notation']), axis = 1)
ptcCode

ptcCode.drop_duplicates().to_csv(out / 'practice-code.csv', index = False)


# In[201]:


pcnCode = df[['PCN_NAME', 'PCN_CODE']]
pcnCode = pcnCode.rename(columns = {'PCN_NAME' : 'Label', 'PCN_CODE' : 'Notation'})
pcnCode = pcnCode.drop_duplicates()
pcnCode['Parent Notation'] = ''
pcnCode['Sort Priority'] = pcnCode.index
pcnCode['Label'] = ptcCode['Label'].str.capitalize()
pcnCode['Notation'] = pcnCode.apply(lambda x: pathify(x['Notation']), axis = 1)

pcnCode['Label'] = pcnCode.apply(lambda x: 'Unallocated' if x['Notation'] == 'u' else x['Label'], axis = 1)

pcnCode.drop_duplicates().to_csv(out / 'pcn-code.csv', index = False)


# In[172]:


cubes.output_all()

