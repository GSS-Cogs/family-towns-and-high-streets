# -*- coding: utf-8 -*-
# Population estimates - small area based by single year of age - England and Wales 
from gssutils import * 
import pandas as pd
import json
from datetime import datetime

scraper = Scraper(seed='info.json')

# Fix up some metadata

from gssutils.metadata import GOV
scraper.dataset.comment = 'Estimates of the population for England and Wales by Geographical 2011 Output areas, Gender and Age'
scraper.dataset.publisher = GOV['office-for-national-statistics']
scraper.dataset.creator = scraper.dataset.publisher
scraper.dataset.family = 'towns-high-streets'
scraper.dataset.title = 'Population estimates by Output Area Geographies, Gender and Age, England and Wales'
scraper.dataset

# +
#--------------------------------------------------------------------------------------------------------------
# This section was used to pull in the test set of data from the file data.csv (probably now deleted)
#joined_dat = pd.read_csv("data.csv")
#joined_dat = joined_dat[['DATE', 'GEOGRAPHY_CODE','GENDER_NAME','C_AGE_NAME','C_AGE_TYPE', 'OBS_VALUE']]
#joined_dat.head(10)

# +
#joined_dat = joined_dat.rename(columns={
#    'DATE': 'Date',
#    'GEOGRAPHY_CODE': 'Geography',
#    'GENDER_NAME': 'Gender',
#    'C_AGE_NAME': 'Age',
#    'C_AGE_TYPE': 'Age Type',
#    'OBS_VALUE': 'Value'
#})
#joined_dat['Value'] = pd.to_numeric(joined_dat['Value'], downcast='integer')

# +
#joined_dat['Age'] = joined_dat['Age'].apply(pathify)
#joined_dat['Age Type'] = joined_dat['Age Type'].apply(pathify)
#joined_dat['Gender'] = joined_dat['Gender'].replace({'Total': 'T', 'Male': 'M', 'Female': 'F'})
#joined_dat['Date'] = 'year/' + joined_dat['Date'].astype(str)

#del joined_dat['Age Type']
#joined_dat.head(10)

# +
# Output the data to CSV
#csvName = 'observations.csv'
#out = Path('out')
#out.mkdir(exist_ok=True)
#joined_dat.drop_duplicates().to_csv(out / (csvName + '.gz'), index = False, compression='gzip')
#joined_dat.drop_duplicates().to_csv(out / (csvName), index = False)
#--------------------------------------------------------------------------------------------------------------
# -

# Create seme temporary data so we can output a small csv file to feed into the Mapping class and then delete it
tempdat = {'Date': [2018, 2018, 2018, 2018],
        'Geography Code': ['E001597900', 'E001597900', 'E001597900', 'E001597900'],
        'Gender': ['T', 'T', 'T', 'T'],
        'Age': ['Y0T15', 'Y16T64', 'Y18T21', 'Y25T49'],
        'Value': [66, 251, 226, 12]
        }
tempdat = pd.DataFrame(tempdat, columns = ['Date', 'Geography Code', 'Gender', 'Age', 'Value'])

# +
from urllib.parse import urljoin

csvName = 'observations.csv'
out = Path('out')
# Create the temp csv
tempdat.to_csv(out / csvName, index = False)

dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/{Path(os.getcwd()).name}'))
scraper.set_base_uri('http://gss-data.org.uk')
scraper.set_dataset_id(dataset_path)
csvw_transform = CSVWMapping()
csvw_transform.set_csv(out / csvName)
csvw_transform.set_mapping(json.load(open('info.json')))
csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
csvw_transform.write(out / f'{csvName}-metadata.json')
# Delete the temp csv
(out / csvName).unlink()

with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())
# -
#--------------------------------------------------------------------------------------------------------------
# This section is for converting the data that Mike A extracted into a suitable datacude and zipped files
"""
out = Path('out')
out.mkdir(exist_ok=True)
base = 'gs://pipeline-stream-population-estimates/'
# 0 to 19
for i in range(21, 22):
    fle = str(i) + '_CensusPop_LMA_ages.csv'
    print (base + fle)
    dat = pd.read_csv(base + fle)
    dat = dat[['DATE','GEOGRAPHY_CODE','GENDER_NAME','C_AGE_NAME','MEASURES_NAME','OBS_VALUE']]
    dat = dat.rename(columns={'DATE':'Date','GEOGRAPHY_CODE':'Geography Code','GENDER_NAME':'Gender','C_AGE_NAME':'Age','MEASURES_NAME':'Measure Type','OBS_VALUE':'Value'})
    dat['Gender'] = dat['Gender'].replace({'Total':'T','Male':'M','Female':'F'})
    dat['Date'] = 'year/' + dat['Date'].astype(str)
    dat['Age'] = dat['Age'].apply(pathify)
    dat['Age'] = dat['Age'].replace({
        'All Ages':'all-ages',
        'aged-0-to-15':'Y0T15',
        'aged-16':'Y_E16',
        'aged-16-to-64':'Y16T64',
        'aged-16-to-24':'Y16T24',
        'aged-16-to-17':'Y16T17',
        'aged-18-to-24':'Y18T24',
        'aged-18-to-21':'Y18T21',
        'aged-25-to-49':'Y25T49',
        'aged-50-to-64':'Y50T64',
        'aged-65':'Y_GE65'
    })
    # Get rid of all the rows with 'Percent' in Measure Type column
    dat = dat[dat['Measure Type'] == 'Value']
    del dat['Measure Type']
    
    csvName = 'observations' + str(i) + '.csv'
    print('Original file row count: ' + str(dat['Date'].count()))
    dat = dat.drop_duplicates()
    print('Original file row count after dropping duplicates: ' + str(dat['Date'].count()))
    dat.drop_duplicates().to_csv(out / (csvName + '.gz'), index = False, compression='gzip')
    if i == 0:
        joined_dat = dat
        #for c in dat.columns:
        #    if c != 'Value':
        #        print(c)
        #        print(dat[c].unique())
        #        print('-----------------------------------------------------------')
    else:
        joined_dat = pd.concat([joined_dat,dat])
    
    print('Joined Data count AFTER Concat: ' + str(joined_dat['Date'].count()))
    print('---------------------------------------------------------------------')
    del dat
    
joined_dat.drop_duplicates().to_csv(out / ('observations.csv.gz'), index = False, compression='gzip')

#--------------------------------------------------------------------------------------------------------------
"""

# +
#joined_dat['Geography Code'].unique()

# +
#joined_dat['Date'].count()

# +
#code1 = 'E00000005'
#code2 = 'E00094862'
