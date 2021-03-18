#!/usr/bin/env python
# coding: utf-8

# In[120]:


from gssutils import *
import pandas as pd
import json

scraper = Scraper(seed = 'info.json')
scraper

trace = TransformTrace()
cubes = Cubes("info.json")


# In[121]:


for i in scraper.distributions:
    display(i)


# In[122]:


# extract latest distribution and datasetTitle
distribution = scraper.distribution(title=lambda t: 'Inter-Departmental Business Register' in t)
datasetTitle = distribution.title
print(distribution.downloadURL)
print(datasetTitle)


# In[123]:


tabs = distribution.as_databaker()


# In[124]:


for tab in tabs:
    print(tab.name)


# In[125]:


columns = ["Period", "Town Id", "Size Band"]


# In[126]:


if tab.name == 'Table':
    trace.start(datasetTitle, tab, columns, distribution.downloadURL)
    print(tab.name)
    cell = tab.excel_ref("A1")
    remove = tab.filter(contains_string("Total")).expand(DOWN)


    period = cell.fill(DOWN).is_not_blank().is_not_whitespace()
    trace.Period("Defined from cell A2 and down")

    town_id = cell.shift(1,0).fill(DOWN)
    trace.Town_Id("Defined from cell B2 and down")

    size_band = cell.shift(2,0).fill(RIGHT)-remove
    trace.Size_Band("Defined from cell D1 and right excluding remove")

    observations = size_band.waffle(town_id)-remove

    dimensions =[
        HDim(period, "Period", DIRECTLY, LEFT),
        HDim(town_id, "Town Id", DIRECTLY, LEFT),
        HDim(size_band, "Size Band", DIRECTLY, ABOVE),
    ]
    tidy_sheet = ConversionSegment(tab, dimensions, observations)
    savepreviewhtml(tidy_sheet, fname=tab.name +'Preview.html')
    trace.with_preview(tidy_sheet)
    trace.store("combined_dataframe", tidy_sheet.topandas())


# In[127]:


df = trace.combine_and_trace(datasetTitle, "combined_dataframe")

df['Measure Type'] = 'Count'
df['Unit'] = 'Local Unit'

df


# In[128]:


def left(s, amount):
    return s[:amount]
def date_time (date):
    if len(date)  == 6:
        return 'year/'+ left(date, 4)
df['Period'] =  df["Period"].apply(date_time)
trace.Period("Period is formatted to have pattern year/{year}")


# In[129]:


df.rename(columns = {"OBS":"Value", 'Town Id' : 'Region', 'Size Band' : 'Employees'}, inplace = True)

df = df[['Period', 'Region', 'Employees', 'Value']]

df['Employees'] = df.apply(lambda x: x['Employees'].split(' ')[0], axis = 1)

df['Value'] = df['Value'].astype(float).astype(int)

df = df.replace({'Employees' : {'250+' : '250 Plus'} })

df


# In[130]:


COLUMNS_TO_NOT_PATHIFY = ['Period', 'Value', 'Region']


# In[131]:


for col in df.columns.values.tolist():
    if col in COLUMNS_TO_NOT_PATHIFY:
        continue
    try:
        df[col] = df[col].apply(pathify)
    except Exception as err:
        raise Exception('Failed to pathify column "{}".'.format(col)) from err


# In[132]:


cubes.add_cube(scraper, df.drop_duplicates(), datasetTitle)
cubes.output_all()


# In[133]:


trace.render("spec_v1.html")

