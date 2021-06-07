#!/usr/bin/env python
# coding: utf-8

# In[1]:


# -*- coding: utf-8 -*-
from gssutils import *
import json
import requests
import os
from urllib.parse import urljoin, urlparse
import numpy as np

#trace = TransformTrace()
#tidied_sheets = {}
#scraper = Scraper(seed="info.json")
#info = json.load(open('info.json'))
#scraper = Scraper(info['landingPage'])

#for i in scraper.distributions:
#    print(i.title)
#    print(i.downloadURL)
#    print(i.mediaType)


# %%


from urllib.request import Request, urlopen
#from bs4 import BeautifulSoup
from urllib.parse import urljoin


"""info = json.load(open('info.json'))

req = Request(info["landingPage"], headers={'User-Agent': 'Mozilla/5.0'})
html = urlopen(req).read()
plaintext = html.decode('utf8')
soup = BeautifulSoup(plaintext, 'html.parser')


pubPages = soup.findAll('ul', class_= 'collections-list')
for page in pubPages:
    pubs = page.findAll('a')
    for i in pubs:
        print(urljoin("https://www.gov.scot", i['href']))"""


# In[2]:



#tabs = { tab for tab in scraper.distributions[0].as_databaker() }
#len(tabs)


# In[3]:




"""
for tab in tabs:
    if 'SIMD 2020v2 ranks' == tab.name:
        table = tab

columns=["Data Zone", "Intermediate Zone", "Council Area", "Total Population", "Working Age Population", "Rank Type"]
trace.start(scraper.distributions[0].title, table, columns, scraper.distributions[0].downloadURL)
data_zone = table.filter(contains_string('Data_Zone')).shift(0,1).expand(DOWN).is_not_blank()
trace.Data_Zone("Selected as all non blank values from cell ref A2 down")
intermediate_zone = table.filter(contains_string('Intermediate_Zone')).shift(0,1).expand(DOWN).is_not_blank()
trace.Intermediate_Zone("Selected as all non blank values from cell ref B2 down")
council_area = table.filter(contains_string('Council_area')).shift(0,1).expand(DOWN).is_not_blank()
trace.Council_Area("Selected as all non blank values from cell ref C2 down")
total_population = table.filter(contains_string('Total_population')).shift(0,1).expand(DOWN).is_not_blank()
trace.Total_Population("Selected as all non blank values from cell ref D2 down")
working_age_population = table.filter(contains_string('Working_age_population')).shift(0,1).expand(DOWN).is_not_blank()
trace.Working_Age_Population("Selected as all non blank values from cell ref E2 down")
rank_type = table.filter(contains_string('SIMD2020v2_Rank')).expand(RIGHT).is_not_blank()
trace.Rank_Type("Selected as all non blank values from cell ref F3 right")
observation = table.filter(contains_string('SIMD2020v2_Rank')).shift(0,1).expand(DOWN).expand(RIGHT).is_not_blank()
dimensions = [
    HDim(data_zone, 'Data Zone', DIRECTLY, LEFT),
    HDim(intermediate_zone, 'Intermediate Zone', DIRECTLY, LEFT),
    HDim(council_area, 'Council Area', DIRECTLY, LEFT),
    HDim(total_population, 'Total Population', DIRECTLY, LEFT),
    HDim(working_age_population, 'Working Age Population', DIRECTLY, LEFT),
    HDim(rank_type, 'Rank Type', DIRECTLY, ABOVE)
]
tidy_sheet = ConversionSegment(table, dimensions, observation)
# savepreviewhtml(tidy_sheet)
trace.with_preview(tidy_sheet)
trace.store("ranks_dataframe", tidy_sheet.topandas())
tidied_sheets["SIMD 2020v2 ranks"] = tidy_sheet.topandas()
"""
"""
url = 'https://www.gov.scot/binaries/content/documents/govscot/publications/statistics/2020/01/scottish-index-of-multiple-deprivation-2020-indicator-data/documents/simd_2020_indicators/simd_2020_indicators/govscot%3Adocument/SIMD%2B2020v2%2B-%2Bindicators.xlsx'
r = requests.get(url, allow_redirects=True)
open('indicators.xls', 'wb').write(r.content)
tabs = loadxlstabs('indicators.xls')
trace = TransformTrace()
"""
"""
for tab in tabs:
    if tab.name == 'Data':
        table = tab
title = scraper.distributions[0].title + "Indicators"
columns=["Data Zone", "Intermediate Zone", "Council Area", "Indicator Type"]
trace.start(title, table, columns, url)
data_zone = table.filter(contains_string('Data_Zone')).shift(0,1).expand(DOWN).is_not_blank()
trace.Data_Zone("Selected as all non blank values from cell ref A2 down")
intermediate_zone = table.filter(contains_string('Intermediate_Zone')).shift(0,1).expand(DOWN).is_not_blank()
trace.Intermediate_Zone("Selected as all non blank values from cell ref B2 down")
council_area = table.filter(contains_string('Council_area')).shift(0,1).expand(DOWN).is_not_blank()
trace.Council_Area("Selected as all non blank values from cell ref C2 down")
observation = table.filter(contains_string('Total_population')).shift(0,1).expand(DOWN).expand(RIGHT).is_not_blank()
indicator = table.filter(contains_string('Total_population')).expand(RIGHT).is_not_blank()
trace.Council_Area("Selected as all non blank values from cell ref D1 right")
dimensions = [
    HDim(data_zone, 'Data Zone', DIRECTLY, LEFT),
    HDim(intermediate_zone, 'Intermediate Zone', DIRECTLY, LEFT),
    HDim(council_area, 'Council Area', DIRECTLY, LEFT),
    HDim(indicator, 'Indicator Type', DIRECTLY, ABOVE)
]
tidy_sheet = ConversionSegment(table, dimensions, observation)
# savepreviewhtml(tidy_sheet)
trace.with_preview(tidy_sheet)
trace.store("indicators_dataframe", tidy_sheet.topandas())
tidied_sheets["Data"] = tidy_sheet.topandas()
"""


# In[4]:


# Sheet names
sn = ['SIMD 2020v2 ranks','Data']
# Output filenames
fn = ['ranks-observations','indicators-observations']
# Comments
co = [
    '2020 Rank data relating to the Scottish Index of Multiple Deprivation - a tool for identifying areas with relatively high levels of deprivation.',
    '2020 Indicator data relating to the Scottish Index of Multiple Deprivation - a tool for identifying areas with relatively high levels of deprivation.'
]
# Description
de = [
    'SIMD ranks data zones from most deprived (ranked 1) to least deprived (ranked 6,976). People using SIMD will often focus on the data zones below a certain rank, for example, the 5%, 10%, 15% or 20% most deprived data zones in Scotland. Link to information booklet: https://www.gov.scot/publications/scottish-index-multiple-deprivation-2020/',
    'SIMD ranks data zones from most deprived (ranked 1) to least deprived (ranked 6,976). People using SIMD will often focus on the data zones below a certain rank, for example, the 5%, 10%, 15% or 20% most deprived data zones in Scotland. Link to information booklet: https://www.gov.scot/publications/scottish-index-multiple-deprivation-2020/'
]
# Title
ti = [
    'Scottish Index of Multiple Deprivation - Ranks', 'Scottish Index of Multiple Deprivation - Indicators',
    'Scottish Index of Multiple Deprivation - Ranks', 'Scottish Index of Multiple Deprivation - Indicators'
]
# Paths
pa = ['/ranks', '/indicators']


# In[5]:


# need to change the dataURLa to the indicators one
with open("info.json", "r") as jsonFile:
    data = json.load(jsonFile)

data["dataURL"] = "https://www.gov.scot/binaries/content/documents/govscot/publications/statistics/2020/01/scottish-index-of-multiple-deprivation-2020-ranks-and-domain-ranks/documents/scottish-index-of-multiple-deprivation-2020-ranks-and-domain-ranks/scottish-index-of-multiple-deprivation-2020-ranks-and-domain-ranks/govscot%3Adocument/SIMD%2B2020v2%2B-%2Branks.xlsx"
data["transform"]["columns"]["Value"]["unit"] = "http://gss-data.org.uk/def/concept/measurement-units/ranks"

with open("info.json", "w") as jsonFile:
    json.dump(data, jsonFile, indent = 2)

trace = TransformTrace()
tidied_sheets = {}
scraper = Scraper(seed="info.json")
scraper.distributions[0].title = "Scottish Indicators of Multiple Deprivation 2020"

info = 'info.json'
cubes = Cubes(info)

scraper


# In[6]:


try:
    i = 0
    tab = scraper.distributions[0].as_pandas(sheet_name=sn[i])

    tbls = []
    tab.columns = tab.columns.str.replace('_',' ')
    for x in range(5, len(tab.columns)):
        cols = [0,1,2,3,4,x]
        dat = tab.iloc[:, cols]
        dat['Deprivation Type'] = dat.columns[5]
        dat = dat.rename(columns={dat.columns[5]:'Value'})
        dat = dat[[dat.columns[0],dat.columns[6],dat.columns[3],dat.columns[4],dat.columns[5]]]
        tbls.append(dat)

        k = 0
    for t in tbls:
        if k == 0:
            joined_dat = t
        else:
            joined_dat = pd.concat([joined_dat,t])
        k = k + 1

    joined_dat['Deprivation Type'] = joined_dat['Deprivation Type'].apply(pathify)

    csvName = fn[i]
    out = Path('out')
    out.mkdir(exist_ok=True)
    #joined_dat.drop_duplicates().to_csv(out / csvName, index = False)

    scraper.dataset.family = 'towns-and-high-streets'
    scraper.dataset.description = scraper.dataset.description + '\n' + de[i]
    scraper.dataset.comment = co[i]
    scraper.dataset.title = ti[i]

    dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name)).lower() + pa[i]
    scraper.set_base_uri('http://gss-data.org.uk')
    scraper.set_dataset_id(dataset_path)

    joined_dat['Measure Type'] = 'deprivation-rank'
    joined_dat['Unit'] = 'rank'

    cubes.add_cube(scraper, joined_dat, csvName)

    #csvw_transform = CSVWMapping()
    #csvw_transform.set_csv(out / csvName)
    #csvw_transform.set_mapping(json.load(open('info.json')))
    #csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
    #csvw_transform.write(out / f'{csvName}-metadata.json')

    #with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
    #    metadata.write(scraper.generate_trig())

except Exception as s:
    print(str(s))


# In[7]:


joined_dat


# In[8]:


print(joined_dat.head(5))
del joined_dat


# In[9]:



# need to change the dataURLa to the indicators one
with open("info.json", "r") as jsonFile:
    data = json.load(jsonFile)

data["dataURL"] = "https://www.gov.scot/binaries/content/documents/govscot/publications/statistics/2020/01/scottish-index-of-multiple-deprivation-2020-indicator-data/documents/simd_2020_indicators/simd_2020_indicators/govscot%3Adocument/SIMD%2B2020v2%2B-%2Bindicators.xlsx"
data["transform"]["columns"]["Value"]["unit"] = "http://gss-data.org.uk/def/concept/measurement-units/indicators"

with open("info.json", "w") as jsonFile:
    json.dump(data, jsonFile, indent = 2)

trace = TransformTrace()
tidied_sheets = {}
scraper = Scraper(seed="info.json")
scraper.distributions[0].title = "Scottish Index of Multiple Deprivation 2020"
scraper


# In[10]:



try:
    i = 1
    tab = scraper.distributions[0].as_pandas(sheet_name=sn[i])

    tbls = []
    tab.columns = tab.columns.str.replace('_',' ')
    for x in range(5, len(tab.columns)):
        cols = [0,1,2,3,4,x]
        dat = tab.iloc[:, cols]
        dat['Deprivation Indicator'] = dat.columns[5]
        dat = dat.rename(columns={dat.columns[5]:'Value'})
        dat = dat[[dat.columns[0],dat.columns[6],dat.columns[3],dat.columns[4],dat.columns[5]]]
        tbls.append(dat)

        k = 0
    for t in tbls:
        if k == 0:
            joined_dat = t
        else:
            joined_dat = pd.concat([joined_dat,t])
        k = k + 1

    joined_dat["Indicator Type"] = joined_dat["Deprivation Indicator"]
    joined_dat["High Level Indicator"] = joined_dat["Deprivation Indicator"]

    joined_dat['Indicator Type'] = joined_dat['Indicator Type'].replace({
        'Income rate':'Percentage',
        'Income count':'Count',
        'Employment rate':'Percentage',
        'Employment count':'Count',
        'CIF':'Standardised ratio',
        'ALCOHOL':'Standardised ratio',
        'DRUG':'Standardised ratio',
        'SMR':'Standardised ratio',
        'DEPRESS':'Percentage',
        'LBWT':'Percentage',
        'EMERG':'Standardised ratio',
        'Attendance':'Percentage',
        'Attainment':'Score',
        'no qualifications':'Standardised ratio',
        'not participating':'Percentage',
        'University':'Percentage',
        'drive petrol':'Time (minutes)',
        'drive GP':'Time (minutes)',
        'drive post':'Time (minutes)',
        'drive primary':'Time (minutes)',
        'drive retail':'Time (minutes)',
        'drive secondary':'Time (minutes)',
        'PT GP':'Time (minutes)',
        'PT post':'Time (minutes)',
        'PT retail':'Time (minutes)',
        'Broadband':'Percentage',
        'crime count':'Count',
        'crime rate':'Rate per 10,000 population',
        'overcrowded count':'Count',
        'nocentralheat count':'Count',
        'overcrowded rate':'Percentage',
        'nocentralheat rate':'Percentage'
     })

    joined_dat['High Level Indicator'] = joined_dat['High Level Indicator'].replace({
        'Income rate':'Income',
        'Income count':'Income',
        'Employment rate':'Employment',
        'Employment count':'Employment',
        'CIF':'Health',
        'ALCOHOL':'Health',
        'DRUG':'Health',
        'SMR':'Health',
        'DEPRESS':'Health',
        'LBWT':'Health',
        'EMERG':'Health',
        'Attendance':'Education, Skills and Training',
        'Attainment':'Education, Skills and Training',
        'no qualifications':'Education, Skills and Training',
        'not participating':'Education, Skills and Training',
        'University':'Education, Skills and Training',
        'drive petrol':'Geographic Access to Services',
        'drive GP':'Geographic Access to Services',
        'drive post':'Geographic Access to Services',
        'drive primary':'Geographic Access to Services',
        'drive retail':'Geographic Access to Services',
        'drive secondary':'Geographic Access to Services',
        'PT GP':'Geographic Access to Services',
        'PT post':'Geographic Access to Services',
        'PT retail':'Geographic Access to Services',
        'Broadband':'Geographic Access to Services',
        'crime count':'Crime',
        'crime rate':'Crime',
        'overcrowded count':'Housing',
        'nocentralheat count':'Housing',
        'overcrowded rate':'Housing',
        'nocentralheat rate':'Housing'
     })
    df = joined_dat
    df['Deprivation Indicator'] = df['Deprivation Indicator'].apply(pathify)
    df['Indicator Type'] = df['Indicator Type'].apply(pathify)
    df['High Level Indicator'] = df['High Level Indicator'].apply(pathify)

    df['Marker'] = ''
    df['Marker'][df['Value'] == '*'] = 'suppressed-or-population-zero'
    df['Value'][df['Value'] == '*'] = 0
    df = df.rename(columns={"High Level Indicator":"High Level Domain"})
    #df = df[['Data Zone','Deprivation Indicator','Indicator Type','High Level Indicator','Total population','Working age population','Marker','Value']]
    df = df[['Data Zone','Deprivation Indicator','Indicator Type','High Level Domain','Total population','Working age population','Marker','Value']]

    df['Value'] = df.apply(lambda x: x['Value']*100 if 'percentage' in x['Indicator Type'] else x['Value'], axis = 1)

    df['Value'] = df.apply(lambda x: round(x['Value'], 2), axis = 1)

    df = df.replace({'Deprivation Indicator' : {'nocentralheat-count' : 'no-central-heat-count',
                                                'nocentralheat-rate' : 'no-central-heat-rate'}})

    df['Measure Type'] = df['Deprivation Indicator']

    df['Unit'] = df['Indicator Type']

    df = df.drop(columns = ['Deprivation Indicator', 'Indicator Type'])

    df['Value'] = df.apply(lambda x: np.nan if x['Marker'] != '' else x['Value'], axis = 1)

    csvName = fn[i]

    scraper.dataset.family = 'towns-and-high-streets'
    scraper.dataset.description = scraper.dataset.description + '\n' + de[i]
    scraper.dataset.comment = co[i]
    scraper.dataset.title = ti[i]

    dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name)).lower() + pa[i]
    scraper.set_base_uri('http://gss-data.org.uk')
    scraper.set_dataset_id(dataset_path)

    cubes.add_cube(scraper, df, csvName)

    #csvw_transform = CSVWMapping()
    #csvw_transform.set_csv(out / csvName)
    #csvw_transform.set_mapping(json.load(open('info.json')))
    #csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
    #csvw_transform.write(out / f'{csvName}-metadata.json')

    #with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
    #    metadata.write(scraper.generate_trig())

except Exception as s:
    print(str(s))


# In[11]:


# Need to change the dataURL back to the RANK URL ready for the next run
with open("info.json", "r") as jsonFile:
    data = json.load(jsonFile)

data["dataURL"] = "https://www.gov.scot/binaries/content/documents/govscot/publications/statistics/2020/01/scottish-index-of-multiple-deprivation-2020-ranks-and-domain-ranks/documents/scottish-index-of-multiple-deprivation-2020-ranks-and-domain-ranks/scottish-index-of-multiple-deprivation-2020-ranks-and-domain-ranks/govscot%3Adocument/SIMD%2B2020v2%2B-%2Branks.xlsx"
data["transform"]["columns"]["Value"]["unit"] = "http://gss-data.org.uk/def/concept/measurement-units/ranks"

with open("info.json", "w") as jsonFile:
    json.dump(data, jsonFile, indent = 2)


# In[18]:


dfTest = df[['Measure Type', 'Unit']]
dfTest = dfTest.drop_duplicates()
dfTest


# In[13]:


cubes.output_all()

