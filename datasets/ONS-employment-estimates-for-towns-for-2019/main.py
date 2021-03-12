# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.10.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# + tags=[]
import json
import pandas as pd
from gssutils import *
# -

cubes = Cubes('info.json')

info = json.load(open('info.json'))
dataURL = info['dataURL']
dataURL

scraper = Scraper(seed='info.json')

# +
descr = '''
Metadata for employment, 2019:

1.Towns are defined as a selection of Built Up Area Sub Divisions (BUASD's) or Built Up Areas (BUA's) with Census 2011 populations greater than 5,000 and less than 225,000 outside of the London Nomenclature of Territorial Units for Statistics (NUTS1).

2. Employment figures for 2019 were compiled from the Business Register and Employment Survey (BRES) 2019 provisional data. These estimates can be subject to revisions.

3. BRES survey records a job at the location of an employees workplace. Employment figures include employees and working proprietors.

4. In order to ensure confidentiality of business data, while maximising the detail of estimates available to users, the Business Register and Employment Survey (BRES) applies different rounding rules depending on the size and the nature of the estimates. 

5. For confidentiality reasons and issues regarding the quality of the estimates, data suppression has been applied to some cells and the value of the estimate has been replaced with !. 


On the publication of the dataset

This is not a regular publication. It is just data we have used in our analysis that hasn’t yet been published in this format. 

'''

title = 'Employment estimates for towns for 2019'
scraper.dataset.title = title
scraper.dataset.description = descr
# -

table = pd.read_excel(dataURL,'EMPLOYMENT')
table = table.drop(columns='TOWN')

df = pd.melt(table, id_vars=['BUA11CD', 'BUA11NM', 'RNG'], var_name='Period', value_name='Value')

# +
df.rename(columns= {
    'BUA11CD' : 'CDID',
    'BUA11NM' : 'Town',
    'RNG' : 'Region'
}, inplace=True)

df = df.sort_values(['CDID', 'Town', 'Region'])
# -

df['Value'] = df['Value'].astype(float).round().astype(int)

df['Town'] = df['Town'].map(lambda x: x.strip('BAU').strip('BAUSD'))

df = df.replace({'Region' : {'East of England' : 'E12000006',
                             'East Midlands' : 'E12000004',
                             'North East' : 'E12000001',
                             'North West' : 'E12000002',
                             'South East' : 'E12000008',
                             'South West' : 'E12000009',
                             'West Midlands' : 'E12000005',
                             'Yorkshire and The Humber' : 'E12000003',
                             'Wales' : 'W92000004'}})

df['Measure Type'] = 'count'
df['Unit'] = 'person'
df = df.drop(columns=['Town'], axis =1)

df = df[['CDID', 'Region', 'Period', 'Value', 'Measure Type', 'Unit']]

cubes.add_cube(scraper, df, title)
cubes.output_all()
