# -*- coding: utf-8 -*-
# Population estimates - small area based by single year of age - England and Wales 
from gssutils import * 
import pandas as pd
import json
from datetime import datetime

scraper = Scraper(seed='info.json')

joined_dat = pd.read_csv("data.csv")
joined_dat = joined_dat[['DATE', 'GEOGRAPHY_CODE','GENDER_NAME','C_AGE_NAME','C_AGE_TYPE', 'OBS_VALUE']]
joined_dat.head(10)

joined_dat = joined_dat.rename(columns={
    'DATE': 'Date',
    'GEOGRAPHY_CODE': 'Geography',
    'GENDER_NAME': 'Gender',
    'C_AGE_NAME': 'Age',
    'C_AGE_TYPE': 'Age Type',
    'OBS_VALUE': 'Value'
})
joined_dat['Value'] = pd.to_numeric(joined_dat['Value'], downcast='integer')

joined_dat['Age'] = joined_dat['Age'].apply(pathify)
joined_dat['Age Type'] = joined_dat['Age Type'].apply(pathify)
joined_dat['Gender'] = joined_dat['Gender'].replace({'Total': 'T', 'Male': 'M', 'Female': 'F'})
joined_dat['Date'] = 'year/' + joined_dat['Date'].astype(str)
joined_dat.head(10)

# Output the data to CSV
csvName = 'observations.csv'
out = Path('out')
out.mkdir(exist_ok=True)
#joined_dat.drop_duplicates().to_csv(out / (csvName + '.gz'), index = False, compression='gzip')
joined_dat.drop_duplicates().to_csv(out / (csvName), index = False)

# +
from urllib.parse import urljoin

scraper.dataset.family = 'towns-high-streets'
scraper.dataset.comment = 'Estimates of the population for England and Wales by Geographical Output areas, Gender and Age'
scraper.dataset.title = 'Population estimates by output areas, England and Wales'

dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}'))
scraper.set_base_uri('http://gss-data.org.uk')
scraper.set_dataset_id(dataset_path)
csvw_transform = CSVWMapping()
csvw_transform.set_csv(out / csvName)
csvw_transform.set_mapping(json.load(open('info.json')))
csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
csvw_transform.write(out / f'{csvName}-metadata.json')
with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())
# -




