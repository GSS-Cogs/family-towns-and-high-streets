#!/usr/bin/env python
# coding: utf-8

# In[85]:


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
scraper

tidied_sheets = {}


# In[86]:


out = Path('out')
out.mkdir(exist_ok=True)

#trace = TransformTrace()


# In[87]:


df = pd.DataFrame()

for distribution in scraper.distributions:
    if distribution.downloadURL.endswith('zip') and 'LSOA' in distribution.title:
        print(distribution.title)
        #datasetTitle = pathify(distribution.title)
        with ZipFile(BytesIO(scraper.session.get(distribution.downloadURL).content)) as zip:
            for name in zip.namelist()[1:]:
                with zip.open(name, 'r') as file:
                    print(name)
                    table = pd.read_csv(file)

                    table['Year'] = 'year/' + name[:-4][-4:]

                    df = df.append(table, ignore_index = True)

df = df.drop(['LAName', 'MSOAName', 'LSOAName'], axis=1)

df = df.rename(columns={'LACode':'Local Authority',
                        'MSOACode':'Middle Layer Super Output Area',
                        'LSOACode':'Lower Layer Super Output Area',
                        'METERS':'Total number of domestic electricity meters',
                        'KWH':'Value',
                        'MEAN':'Mean domestic electricity consumption kWh per meter',
                        'MEDIAN':'Median domestic electricity consumption kWh per meter'})

df = df[['Year', 'Local Authority', 'Middle Layer Super Output Area', 'Lower Layer Super Output Area', 'Total number of domestic electricity meters', 'Mean domestic electricity consumption kWh per meter', 'Median domestic electricity consumption kWh per meter', 'Value']]

df.drop_duplicates().to_csv(out / 'observations.csv', index = False)

df


# In[88]:


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

df


# In[89]:


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

df


# In[90]:


scraper.dataset.comment = scraper.description

scraper.dataset.description = "Guidance documentation can be found here: https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/853104/sub-national-methodology-guidance.pdf"


# In[91]:


with open(out / 'observations.csv-metadata.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())

#trace.output()


# In[91]:




