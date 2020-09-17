#!/usr/bin/env python
# coding: utf-8
# %%

# %%


from gssutils import *
import pandas as pd
import json
import os
import string
import re
from zipfile import ZipFile, is_zipfile
from io import BytesIO, TextIOWrapper

def left(s, amount):
    return s[:amount]
def right(s, amount):
    return s[-amount:]
def mid(s, offset, amount):
    return s[offset:offset+amount]
def decimal(s):
    try:
        float(s)
        if float(s) >= 1:
            return False
        else:
            return True
    except ValueError:
        return True
def cellLoc(cell):
    return right(str(cell), len(str(cell)) - 2).split(" ", 1)[0]
def cellCont(cell):
    return re.findall(r"'([^']*)'", cell)[0]
def col2num(col):
    num = 0
    for c in col:
        if c in string.ascii_letters:
            num = num * 26 + (ord(c.upper()) - ord('A')) + 1
    return num
def colnum_string(n):
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string
def excelRange(bag):
    xvalues = []
    yvalues = []
    for cell in bag:
        coordinate = cellLoc(cell)
        xvalues.append(''.join([i for i in coordinate if not i.isdigit()]))
        yvalues.append(int(''.join([i for i in coordinate if i.isdigit()])))
    high = 0
    low = 0
    for i in xvalues:
        if col2num(i) >= high:
            high = col2num(i)
        if low == 0:
            low = col2num(i)
        elif col2num(i) < low:
            low = col2num(i)
        highx = colnum_string(high)
        lowx = colnum_string(low)
    highy = str(max(yvalues))
    lowy = str(min(yvalues))

    return '{' + lowx + lowy + '-' + highx + highy + '}'
def infoTransform(tabName, tabTitle, tabColumns):

    dictList = []

    with open('info.json') as info:
        data = info.read()

    infoData = json.loads(data)

    columnInfo = {}

    for i in tabColumns:
        underI = i.replace(' ', '_')
        columnInfo[i] = getattr(getattr(trace, underI), 'var')

    dicti = {'name' : tabName,
             'title' : tabTitle,
             'columns' : columnInfo}

    if infoData.get('transform').get('transformStage') == None:
        infoData['transform']['transformStage'] = []
        dictList.append(dicti)
    else:
        dictList = infoData['transform']['transformStage']
        index = next((index for (index, d) in enumerate(dictList) if d["name"] == tabName), None)
        if index is None :
            dictList.append(dicti)
        else:
            dictList[index] = dicti

    infoData['transform']['transformStage'] = dictList

    with open('info.json', 'w') as info:
        info.write(json.dumps(infoData, indent=4).replace('null', '"Not Applicable"'))
def infoComments(tabName, tabColumns):

    with open('info.json') as info:
        data = info.read()

    infoData = json.loads(data)

    columnInfo = {}

    for i in tabColumns:
        comments = []
        underI = i.replace(' ', '_')
        for j in getattr(getattr(trace, underI), 'comments'):
            if j == []:
                continue
            else:
                comments.append(':'.join(str(j).split(':', 3)[3:])[:-2].strip().lstrip('\"').rstrip('\"'))
        columnInfo[i] = comments

    columnInfo = {key:val for key, val in columnInfo.items() if val != ""}
    columnInfo = {key:val for key, val in columnInfo.items() if val != []}

    dicti = {'name' : tabName,
             'columns' : columnInfo}

    dictList = infoData['transform']['transformStage']
    index = next((index for (index, d) in enumerate(dictList) if d["name"] == tabName), None)
    if index is None :
        print('Tab not found in Info.json')
    else:
        dictList[index]['postTransformNotes'] = dicti

    with open('info.json', 'w') as info:
        info.write(json.dumps(infoData, indent=4).replace('null', '"Not Applicable"'))
def infoNotes(notes):

    with open('info.json') as info:
        data = info.read()

    infoData = json.loads(data)

    infoData['transform']['Stage One Notes'] = notes

    with open('info.json', 'w') as info:
        info.write(json.dumps(infoData, indent=4).replace('null', '"Not Applicable"'))

info = json.load(open('info.json'))
etl_title = info["title"]
etl_publisher = info["publisher"][0]
print("Publisher: " + etl_publisher)
print("Title: " + etl_title)

scraper = Scraper(seed="info.json")
#scraper.title


# %%


out = Path('out')
out.mkdir(exist_ok=True)

trace = TransformTrace()


# %%


df = pd.DataFrame()

datasetTitle = scraper.title

for distribution in scraper.distributions:
    if distribution.downloadURL.endswith('zip') and 'LSOA' in distribution.title:
        print(distribution.title)
        #datasetTitle = pathify(distribution.title)
        with ZipFile(BytesIO(scraper.session.get(distribution.downloadURL).content)) as zip:
            for name in zip.namelist()[1:]:
                with zip.open(name, 'r') as file:

                    link = distribution.downloadURL

                    columns = ['Year', 'Local Authority', 'Middle Layer Super Output Area', 'Lower Layer Super Output Area', 'Total number of domestic electricity meters', 'Mean domestic electricity consumption kWh per meter', 'Median domestic electricity consumption kWh per meter', 'Value']
                    trace.start(datasetTitle, name, columns, link)

                    print(name)
                    table = pd.read_csv(file)

                    table['Year'] = 'year/' + name[:-4][-4:]
                    trace.Year("Value taken from CSV file name: {}", var = name[18:])
                    trace.Local_Authority("Values taken from 'LACode' field")
                    trace.Middle_Layer_Super_Output_Area("Values taken from 'MSOACode' field")
                    trace.Lower_Layer_Super_Output_Area("Values taken from 'LSOACode' field")
                    trace.Total_number_of_domestic_electricity_meters("Values taken from 'METERS' field")
                    trace.Mean_domestic_electricity_consumption_kWh_per_meter("Values taken from 'MEAN' field")
                    trace.Median_domestic_electricity_consumption_kWh_per_meter("Values taken from 'MEDIAN' field")
                    trace.Value("Values taken from 'KWH' field")

                    df = df.append(table, ignore_index = True)

df = df.drop(['LAName', 'MSOAName', 'LSOAName'], axis=1)

df = df.rename(columns={'LACode':'Local Authority',
                        'MSOACode':'Middle Layer Super Output Area',
                        'LSOACode':'Lower Layer Super Output Area',
                        'METERS':'Total number of domestic electricity meters',
                        'KWH':'Value',
                        'MEAN':'Mean domestic electricity consumption kWh per meter',
                        'MEDIAN':'Median domestic electricity consumption kWh per meter'})

trace.Local_Authority("Rename column from 'LACode' to 'Local Authority'")
trace.Middle_Layer_Super_Output_Area("Rename column from 'MSOACode' to 'Middle Layer Super Output Area'")
trace.Lower_Layer_Super_Output_Area("Rename column from 'LSOACode' to 'Lower Layer Super Output Area'")
trace.Total_number_of_domestic_electricity_meters("Rename column from 'METERS' to 'Total number of domestic electricity meters'")
trace.Mean_domestic_electricity_consumption_kWh_per_meter("Rename column from 'MEAN' to 'Mean domestic electricity consumption kWh per meter'")
trace.Median_domestic_electricity_consumption_kWh_per_meter("Rename column from 'MEDIAN' to 'Median domestic electricity consumption kWh per meter'")
trace.Value("Rename column from 'KWH' to 'Value'")

infoNotes("""
Guidance documentation can be found here: https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/853104/sub-national-methodology-guidance.pdf
Year, Local Authority, Middle Layer Super Output Area, Lower Layer Super Output Area, Total number of domestic electricity meters, Mean Domestic electricity consumption kWh per meter, Median domestic electricity consumption kWh per meter, Value
Or if having the three geography causes problems:
Year, Lower Layer Super Output Area, Total number of domestic electricity meters, Mean Domestic electricity consumption kWh per meter, Median domestic electricity consumption kWh per meter, Value""")

df = df[['Year', 'Local Authority', 'Middle Layer Super Output Area', 'Lower Layer Super Output Area', 'Total number of domestic electricity meters', 'Mean domestic electricity consumption kWh per meter', 'Median domestic electricity consumption kWh per meter', 'Value']]

#df.drop_duplicates().to_csv(out / 'observations.csv', index = False)

df.head(10)


# %%
#del df['Local Authority']
#del df['Middle Layer Super Output Area']

del df['Total number of domestic electricity meters']
del df['Mean domestic electricity consumption kWh per meter']
del df['Median domestic electricity consumption kWh per meter']
#### OUTPUTTING LSOA DATA AS A SINGLE DATASET

# %%
import os
from urllib.parse import urljoin

notes = 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/853104/sub-national-methodology-guidance.pdf'

csvName = 'lsoa_observations.csv'
out = Path('out')
out.mkdir(exist_ok=True)
df.drop_duplicates().to_csv(out / csvName, index = False)
#df.drop_duplicates().to_csv(out / (csvName + '.gz'), index = False, compression='gzip')
# Output a subset of the data to get the Mapping class to work
#df[:10].to_csv(out / csvName, index = False)

scraper.dataset.family = 'towns-high-streets'
scraper.dataset.description = scraper.dataset.description + '\nGuidance documentation can be found here:\n' + notes
#scraper.dataset.comment = 'Total domestic electricity consumption, number of meters, mean and median consumption for LSOA regions across England, Wales & Scotland'
scraper.dataset.comment = 'Total domestic electricity consumption for LSOA regions across England, Wales & Scotland'
scraper.dataset.title = 'Lower Super Output Areas (LSOA) electricity consumption'

dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name)).lower()
scraper.set_base_uri('http://gss-data.org.uk')
scraper.set_dataset_id(dataset_path)


csvw_transform = CSVWMapping()
csvw_transform.set_csv(out / csvName)
csvw_transform.set_mapping(json.load(open('info.json')))
csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
csvw_transform.write(out / f'{csvName}-metadata.json')
# Remove subset of data
#out / csvName).unlink()
with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())

# %%
"""
df = pd.DataFrame()

for distribution in scraper.distributions:
    if distribution.downloadURL.endswith('zip') and 'MSOA domestic' in distribution.title:
        print(distribution.title)
        #datasetTitle = pathify(distribution.title)
        with ZipFile(BytesIO(scraper.session.get(distribution.downloadURL).content)) as zip:
            for name in zip.namelist()[1:]:
                with zip.open(name, 'r') as file:
                    print(name)
                    table = pd.read_csv(file)

                    table['Year'] = 'year/' + name[:-4][-4:]

                    df = df.append(table, ignore_index = True)

df = df.drop(['LAName', 'MSOAName'], axis=1)

df = df.rename(columns={'LACode':'Local Authority',
                        'MSOACode':'Middle Layer Super Output Area',
                        'METERS':'Total number of domestic electricity meters',
                        'KWH':'Value',
                        'MEAN':'Mean domestic electricity consumption kWh per meter',
                        'MEDIAN':'Median domestic electricity consumption kWh per meter'})

df = df[['Year', 'Local Authority', 'Middle Layer Super Output Area', 'Total number of domestic electricity meters', 'Mean domestic electricity consumption kWh per meter', 'Median domestic electricity consumption kWh per meter', 'Value']]

#df.drop_duplicates().to_csv(out / f'{datasetTitle}_observations.csv', index = False)

#df
"""


# %%
#del df['Local Authority']
#del df['Middle Layer Super Output Area']

#del df['Total number of domestic electricity meters']
#del df['Mean domestic electricity consumption kWh per meter']
#del df['Median domestic electricity consumption kWh per meter']
#### OUTPUTTING LSOA DATA AS A SINGLE DATASET

# %%
#df.head(10)

# %%
"""
import os
from urllib.parse import urljoin

notes = 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/853104/sub-national-methodology-guidance.pdf'

csvName = 'msoa_observations.csv'
out = Path('out')
out.mkdir(exist_ok=True)
df.drop_duplicates().to_csv(out / csvName, index = False)
#df.drop_duplicates().to_csv(out / (csvName + '.gz'), index = False, compression='gzip')
# Output a subset of the data to get the Mapping class to work
#df[:10].to_csv(out / csvName, index = False)

scraper.dataset.family = 'towns-high-streets'
scraper.dataset.description = scraper.dataset.description + '\nGuidance documentation can be found here:\n' + notes
#scraper.dataset.comment = 'Total domestic electricity consumption, number of meters, mean and median consumption for MSOA regions across England, Wales & Scotland'
scraper.dataset.comment = 'Total domestic electricity consumption for MSOA regions across England, Wales & Scotland'
scraper.dataset.title = 'Middle Super Output Areas (MSOA) electricity consumption'

dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name)).lower()
scraper.set_base_uri('http://gss-data.org.uk')
scraper.set_dataset_id(dataset_path)


csvw_transform = CSVWMapping()
csvw_transform.set_csv(out / csvName)
csvw_transform.set_mapping(json.load(open('info.json')))
csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
csvw_transform.write(out / f'{csvName}-metadata.json')
# Remove subset of data
#out / csvName).unlink()
with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())
"""

# %%
"""
df = pd.DataFrame()

for distribution in scraper.distributions:
    if distribution.downloadURL.endswith('zip') and 'MSOA non domestic' in distribution.title:
        print(distribution.title)
        #datasetTitle = pathify(distribution.title)
        with ZipFile(BytesIO(scraper.session.get(distribution.downloadURL).content)) as zip:
            for name in zip.namelist()[1:]:
                with zip.open(name, 'r') as file:
                    print(name)
                    table = pd.read_csv(file)

                    table['Year'] = 'year/' + name[:-4][-4:]

                    df = df.append(table, ignore_index = True)

df = df.drop(['LAName', 'MSOAName'], axis=1)

df = df.rename(columns={'LACode':'Local Authority',
                        'MSOACode':'Middle Layer Super Output Area',
                        'METERS':'Total number of non domestic electricity meters',
                        'KWH':'Value',
                        'MEAN':'Mean non domestic electricity consumption kWh per meter',
                        'MEDIAN':'Median non domestic electricity consumption kWh per meter'})

df = df[['Year', 'Local Authority', 'Middle Layer Super Output Area', 'Total number of non domestic electricity meters', 'Mean non domestic electricity consumption kWh per meter', 'Median non domestic electricity consumption kWh per meter', 'Value']]

#df.drop_duplicates().to_csv(out / f'{datasetTitle}_observations.csv', index = False)

#df
"""


# %%

