# -*- coding: utf-8 -*-
from gssutils import * 
import json 
import datetime

info = json.load(open('info.json')) 
etl_title = info["title"] 
etl_publisher = info["publisher"]
print("Publisher: " + etl_publisher) 
print("Title: " + etl_title) 

scraper = Scraper(seed="info.json")   
scraper 

trace = TransformTrace()
tidied_data = {} # dataframes will be stored in here

def TimeFormatter(value):
    # function to return time as yyyy-mm-dd
    value_split = value[:2] + '-' + value[2:5].title() + '-' + value[5:]
    value_as_datetime = datetime.datetime.strptime(value_split, '%d-%b-%Y')
    new_value = datetime.datetime.strftime(value_as_datetime, '%Y-%m-%d')
    return new_value 

# hacky way of returning latest data
current_date = datetime.datetime.now().date()
current_month = current_date.strftime('%B')
while scraper.distributions == []:
    scraper.select_dataset(title=lambda x: current_month in x, latest=True)
    current_date = datetime.datetime(current_date.year, current_date.month-1, current_date.day)
    current_month = current_date.strftime('%B')

for dist in scraper.distributions:
    # ignore any non csvs
    if not dist.downloadURL.endswith('.csv'):
        continue
        
    # ignore mapping csv - is just a reference csv, no data
    if 'mapping' in dist.title.lower():
        continue
        
    link = dist.downloadURL
    title = dist.title.split(':')[-1].strip()
    
    if 'males' in dist.title.lower(): # also covers females
        columns = ['Period', 'CCG_CODE', 'ONS_CCG_CODE', 'ORG_CODE', 'POSTCODE', 'SEX', 'AGE', 'Value']
        trace.start(scraper.title, title, columns, link)
        df = dist.as_pandas()
        
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
        
        trace.store(title, df)
        tidied_data[title] = df
        
    elif 'age groups' in dist.title.lower():
        columns = ['Period', 'PUBLICATION', 'ORG_TYPE', 'ORG_CODE', 'ONS_CODE', 'POSTCODE', 'SEX', 'AGE_GROUP_5', 'Value']
        trace.start(scraper.title, title, columns, link)
        df = dist.as_pandas()
        
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
        
        trace.store(title, df)
        tidied_data[title] = df
        
    elif 'totals' in dist.title.lower():
        columns = ['Period', 'PUBLICATION', 'TYPE', 'CCG_CODE', 'ONS_CCG_CODE', 'CODE', 'POSTCODE', 'SEX', 'AGE', 'Value']
        trace.start(scraper.title, title, columns, link)
        df = dist.as_pandas()
        
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
        
        trace.store(title, df)
        tidied_data[title] = df
        
    elif 'region' in dist.title.lower():
        columns = ['Period', 'PUBLICATION', 'ORG_TYPE', 'ORG_CODE', 'ONS_CODE', 'SEX', 'AGE', 'Value']
        trace.start(scraper.title, title, columns, link)
        df = dist.as_pandas()
        
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
        
        trace.store(title, df)
        tidied_data[title] = df

out = Path('out')
out.mkdir(exist_ok=True)

#trace.render("spec_v1.html")
"""
for key in tidied_data:
    df = tidied_data[key]
    df.drop_duplicates().to_csv(out / f'{key}.csv', index=False)
"""
for key in tidied_data:
    print(key)

# First dataset includes the following data
    #Totals (GP practice-all persons)
    #Single year of age (GP practice-females)
    #Single year of age (GP practice-males)
    
    # Only GP data from the following
    #5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice)

# +
del tidied_data['Totals (GP practice-all persons)']['PUBLICATION']
del tidied_data['Totals (GP practice-all persons)']['TYPE']
del tidied_data['Totals (GP practice-all persons)']['CCG_CODE']

#del tidied_data['Single year of age (GP practice-females)']['PUBLICATION']
#del tidied_data['Single year of age (GP practice-females)']['TYPE']
del tidied_data['Single year of age (GP practice-females)']['CCG_CODE']

#del tidied_data['Single year of age (GP practice-males)']['PUBLICATION']
#del tidied_data['Single year of age (GP practice-males)']['TYPE']
del tidied_data['Single year of age (GP practice-males)']['CCG_CODE']

# +
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
tidied_data['5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice)']

# +
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
gp5yr

# +
#print('Main: ' + str(tidied_data['Totals (GP practice-all persons)'].count()))
#print('Female: ' + str(tidied_data['Single year of age (GP practice-females)'].count()))
#print('Males: ' + str(tidied_data['Single year of age (GP practice-males)'].count()))
#tidied_data['Single year of age (GP practice-females)']['Sex'].unique()
#print('Males: ' + str(tidied_data['Single year of age (GP practice-males)'].count()))
#tidied_data['Single year of age (GP practice-females)'].head(10)
#gp_practice.head(10)
# -

gp_practice = pd.concat([tidied_data['Totals (GP practice-all persons)'],
                         tidied_data['Single year of age (GP practice-females)'], 
                         tidied_data['Single year of age (GP practice-males)'],
                         gp5yr], sort=True)
#gp_practice['CCG Code'] = gp_practice['CCG Code'].apply(pathify)
gp_practice['Practice Code'] = gp_practice['Practice Code'].apply(pathify)
gp_practice['Age'] = gp_practice['Age'].str.replace('_','-') 
gp_practice['Age'] = gp_practice['Age'].apply(pathify)
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
gp_practice = gp_practice[['Period','ONS CCG Code','Post Code','Practice Code','Age','Sex','Value']]

# +
#print('All: ' + str(gp_practice['Period'].count()))

#for c in gp_practice:
#    if c != 'Value':
#        print('Column: ' + c)
#        print('Row count: ' + str(gp_practice[c].count()))
#        print(list(gp_practice[c].unique()))
#        print("#######################################")

# +
import os
from urllib.parse import urljoin

notes = f"""
Data for this publication are extracted each month as a snapshot in time from the Primary Care Registration database within the NHAIS (National Health Application and Infrastructure Services) system.
GP Practice; Primary Care Network (PCN); Sustainability and transformation partnership (STP); Clinical Commissioning Group (CCG) and NHS England Commissioning Region level data are released in single year of age (SYOA) and 5-year age bands, both of which finish at 95+, split by gender. In addition, organisational mapping data is available to derive STP; PCN; CCG and Commissioning Region associated with a GP practice and is updated each month to give relevant organisational mapping.
Quarterly publications in January, April, July and October will include Lower Layer Super Output Area (LSOA) populations and a spotlight report.
The outbreak of Coronavirus (COVID-19) has led to changes in the work of General Practices and subsequently the data within this publication. Until activity in this healthcare setting stabilises, we urge caution in drawing any conclusions from these data without consideration of the country
"""

csvName = 'gp_observations.csv'
out = Path('out')
out.mkdir(exist_ok=True)
gp_practice.drop_duplicates().to_csv(out / csvName, index = False)
#gp_practice.drop_duplicates().to_csv(out / (csvName + '.gz'), index = False, compression='gzip')

scraper.dataset.family = 'towns-high-streets'
scraper.dataset.description = notes
scraper.dataset.comment = 'Data for this publication are extracted each month as a snapshot in time from the Primary Care Registration database within the NHAIS (National Health Application and Infrastructure Services) system.'
scraper.dataset.title = 'Patients Registered at a GP Practice - '

dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name) + '/gp').lower()
scraper.set_base_uri('http://gss-data.org.uk')
scraper.set_dataset_id(dataset_path)

csvw_transform = CSVWMapping()
csvw_transform.set_csv(out / csvName)
csvw_transform.set_mapping(json.load(open('info.json')))
csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
csvw_transform.write(out / f'{csvName}-metadata.json')

with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())

# +
#del tidied_data['Single year of age (Commissioning Regions-STPs-CCGs-PCNs)']['PUBLICATION']
#del tidied_data['5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice)']['PUBLICATION']

# +
#tidied_data['Single year of age (Commissioning Regions-STPs-CCGs-PCNs)'] = tidied_data['Single year of age (Commissioning Regions-STPs-CCGs-PCNs)'].rename(columns={
#    'ORG_TYPE': 'ORG Type',
#    'ORG_CODE': 'ORG Code',
#    'ONS_CODE': 'ONS Code',
#    'SEX': 'Sex',
#    'AGE': 'Age'
#})



# +
#tidied_data['5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice)'].head(10)
#tidied_data['5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice)']['ORG Type'].unique()

# +
#d = tidied_data['5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice)']
#d = d[d['Post Code'].notnull()]
#tidied_data['5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice)'].head(10)

# +
#d['ORG Type'].unique()

# +
#list(tidied_data['Single year of age (Commissioning Regions-STPs-CCGs-PCNs)']['ORG_TYPE'].unique())

# +
#scraper.dataset.family = 'towns-high-streets'
#codelistcreation = ['Age'] 
#df = gp_practice
#codeclass = CSVCodelists()
#for cl in codelistcreation:
#    if cl in df.columns:
#        codeclass.create_codelists(pd.DataFrame(df[cl]), 'codelists', scraper.dataset.family, Path(os.getcwd()).name.lower())
# -


