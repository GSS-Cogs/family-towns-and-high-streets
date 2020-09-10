#!/usr/bin/env python
# coding: utf-8

# In[28]:


from gssutils import *
import pandas as pd
import json
import os
import string
import re
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


# In[29]:


trace = TransformTrace()


# In[30]:


#LSAO domestic electricity 2010-18

LSOAdistribution = scraper.distributions[0]
display(LSOAdistribution)

LSOAlink = LSOAdistribution.downloadURL


# In[31]:


tabs = { tab: tab for tab in LSOAdistribution.as_databaker() }

for tab in tabs:

    if tab.name.lower().strip() not in ['title', 'annex sub-national publication']:

        columns = ['Period', 'Local Authority Code', 'MSOA Code', 'LSOA Code', 'Total Number of Meters', 'Domestic Use', 'Mean Consumption', 'Median Consumption', 'Measure Type', 'Unit']
        trace.start(LSOAdistribution.title, tab, columns, LSOAlink)

        pivot = tab.filter(contains_string('Local Authority Name')).shift(DOWN)

        localAuthorityCode = pivot.shift(RIGHT).expand(DOWN).is_not_blank()
        trace.Local_Authority_Code("Values given in range {}", var = excelRange(localAuthorityCode))

        MSOACode = pivot.shift(3, 0).expand(DOWN).is_not_blank()
        trace.MSOA_Code("Values given in range {}", var = excelRange(MSOACode))

        LSOACode = pivot.shift(5, 0).expand(DOWN).is_not_blank()
        trace.LSOA_Code("Values given in range {}", var = excelRange(LSOACode))

        meters = pivot.shift(6, 0).expand(DOWN).is_not_blank()
        trace.Total_Number_of_Meters("Values given in range {}", var = excelRange(meters))

        mean = pivot.shift(8, 0).expand(DOWN).is_not_blank()
        trace.Mean_Consumption("Values given in range {}", var = excelRange(mean))

        median = pivot.shift(9, 0).expand(DOWN).is_not_blank()
        trace.Median_Consumption("Values given in range {}", var = excelRange(median))

        year = tab.name.replace('r', '')
        trace.Period("Value given in name of tab as {}", var = year)

        domestic = 'yes'
        trace.Domestic_Use("Source File only contains Domestic use observations")

        observations = pivot.shift(7, 0).expand(DOWN).is_not_blank()

        measureType = 'Electricity Consumption'
        trace.Measure_Type('Hardcoded as: {}', var = measureType)

        unit = 'kWh'
        trace.Unit('Hardcoded as: {}', var = unit)

        dimensions = [
            HDimConst('Period', year),
            HDim(localAuthorityCode, 'Local Authority Code', DIRECTLY, LEFT),
            HDim(MSOACode, 'MSOA Code', DIRECTLY, LEFT),
            HDim(LSOACode, 'LSOA Code', DIRECTLY, LEFT),
            HDim(meters, 'Total Number of Meters', DIRECTLY, LEFT),
            HDimConst('Domestic Use', domestic),
            HDim(mean, 'Mean Consumption', DIRECTLY, RIGHT),
            HDim(median, 'Median Consumption', DIRECTLY, RIGHT),
            HDimConst('Measure Type', measureType),
            HDimConst('Unit', unit)
        ]

        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        trace.with_preview(tidy_sheet)

        tabTitle = pivot.shift(0, -1)

        infoTransform(tab.name, cellCont(str(tabTitle)), columns)

        trace.store(tab.name + '_' + LSOAdistribution.title, tidy_sheet.topandas())

        tidied_sheets[tab.name + '_' + LSOAdistribution.title] = tidy_sheet.topandas()


# In[32]:


#MSAO domestic electricity 2010-18

MSOAdistribution = scraper.distributions[2]
display(MSOAdistribution)

MSOAlink = MSOAdistribution.downloadURL


# In[33]:


tabs = { tab: tab for tab in MSOAdistribution.as_databaker() }

for tab in tabs:

    if tab.name.lower().strip() not in ['title', 'annex sub-national publications']:

        columns = ['Period', 'Local Authority Code', 'MSOA Code', 'Total Number of Meters', 'Domestic Use', 'Mean Consumption', 'Median Consumption', 'Measure Type', 'Unit']
        trace.start(MSOAdistribution.title, tab, columns, MSOAlink)

        pivot = tab.filter(contains_string('Local Authority Name')).shift(DOWN)

        localAuthorityCode = pivot.shift(RIGHT).expand(DOWN).is_not_blank()
        trace.Local_Authority_Code("Values given in range {}", var = excelRange(localAuthorityCode))

        MSOACode = pivot.shift(3, 0).expand(DOWN).is_not_blank()
        trace.MSOA_Code("Values given in range {}", var = excelRange(MSOACode))

        meters = pivot.shift(4, 0).expand(DOWN).is_not_blank()
        trace.Total_Number_of_Meters("Values given in range {}", var = excelRange(meters))

        mean = pivot.shift(6, 0).expand(DOWN).is_not_blank()
        trace.Mean_Consumption("Values given in range {}", var = excelRange(mean))

        median = pivot.shift(7, 0).expand(DOWN).is_not_blank()
        trace.Median_Consumption("Values given in range {}", var = excelRange(median))

        year = tab.name.replace('r', '')
        trace.Period("Value given in name of tab as {}", var = year)

        domestic = 'yes'
        trace.Domestic_Use("Source File only contains Domestic use observations")

        observations = pivot.shift(5, 0).expand(DOWN).is_not_blank()

        measureType = 'Electricity Consumption'
        trace.Measure_Type('Hardcoded as: {}', var = measureType)

        unit = 'kWh'
        trace.Unit('Hardcoded as: {}', var = unit)

        dimensions = [
            HDimConst('Period', year),
            HDim(localAuthorityCode, 'Local Authority Code', DIRECTLY, LEFT),
            HDim(MSOACode, 'MSOA Code', DIRECTLY, LEFT),
            HDim(meters, 'Total Number of Meters', DIRECTLY, LEFT),
            HDimConst('Domestic Use', domestic),
            HDim(mean, 'Mean Consumption', DIRECTLY, RIGHT),
            HDim(median, 'Median Consumption', DIRECTLY, RIGHT),
            HDimConst('Measure Type', measureType),
            HDimConst('Unit', unit)
        ]

        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        trace.with_preview(tidy_sheet)

        tabTitle = pivot.shift(0, -1)

        infoTransform(tab.name, cellCont(str(tabTitle)), columns)

        trace.store(tab.name + '_' + MSOAdistribution.title, tidy_sheet.topandas())

        tidied_sheets[tab.name + '_' + MSOAdistribution.title] = tidy_sheet.topandas()


# In[34]:


#MSAO non-domestic electricity 2010-18

MSOANDdistribution = scraper.distributions[4]
display(MSOANDdistribution)

MSOANDlink = MSOANDdistribution.downloadURL


# In[35]:


tabs = { tab: tab for tab in MSOANDdistribution.as_databaker() }

for tab in tabs:

    if tab.name.lower().strip() not in ['title', 'annex sub-national publications']:

        columns = ['Period', 'Local Authority Code', 'MSOA Code', 'Total Number of Meters', 'Domestic Use', 'Mean Consumption', 'Median Consumption', 'Measure Type', 'Unit']
        trace.start(MSOANDdistribution.title, tab, columns, MSOANDlink)

        pivot = tab.filter(contains_string('Local Authority Name')).shift(DOWN)

        localAuthorityCode = pivot.shift(RIGHT).expand(DOWN).is_not_blank()
        trace.Local_Authority_Code("Values given in range {}", var = excelRange(localAuthorityCode))

        MSOACode = pivot.shift(3, 0).expand(DOWN).is_not_blank()
        trace.MSOA_Code("Values given in range {}", var = excelRange(MSOACode))

        meters = pivot.shift(4, 0).expand(DOWN).is_not_blank()
        trace.Total_Number_of_Meters("Values given in range {}", var = excelRange(meters))

        mean = pivot.shift(6, 0).expand(DOWN).is_not_blank()
        trace.Mean_Consumption("Values given in range {}", var = excelRange(mean))

        median = pivot.shift(7, 0).expand(DOWN).is_not_blank()
        trace.Median_Consumption("Values given in range {}", var = excelRange(median))

        year = tab.name.replace('r', '')
        trace.Period("Value given in name of tab as {}", var = year)

        domestic = 'no'
        trace.Domestic_Use("Source File only contains Non-Domestic use observations")

        observations = pivot.shift(5, 0).expand(DOWN).is_not_blank()

        measureType = 'Electricity Consumption'
        trace.Measure_Type('Hardcoded as: {}', var = measureType)

        unit = 'kWh'
        trace.Unit('Hardcoded as: {}', var = unit)

        dimensions = [
            HDimConst('Period', year),
            HDim(localAuthorityCode, 'Local Authority Code', DIRECTLY, LEFT),
            HDim(MSOACode, 'MSOA Code', DIRECTLY, LEFT),
            HDim(meters, 'Total Number of Meters', DIRECTLY, LEFT),
            HDimConst('Domestic Use', domestic),
            HDim(mean, 'Mean Consumption', DIRECTLY, RIGHT),
            HDim(median, 'Median Consumption', DIRECTLY, RIGHT),
            HDimConst('Measure Type', measureType),
            HDimConst('Unit', unit)
        ]

        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        trace.with_preview(tidy_sheet)

        tabTitle = pivot.shift(0, -1)

        infoTransform(tab.name, cellCont(str(tabTitle)), columns)

        trace.store(tab.name + '_' + MSOANDdistribution.title, tidy_sheet.topandas())

        tidied_sheets[tab.name + '_' + MSOANDdistribution.title] = tidy_sheet.topandas()


# In[39]:


out = Path('out')
out.mkdir(exist_ok=True)

pd.set_option('display.float_format', lambda x: '%.2f' % x)

for key in tidied_sheets:
    print("{}: {}".format(key, tidied_sheets[key]))
    df = tidied_sheets[key]
    df.drop_duplicates().to_csv(out / f'{key}.csv', index = False)

