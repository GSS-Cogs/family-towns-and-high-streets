#!/usr/bin/env python
# coding: utf-8
# %%
from gssutils import *
import pandas as pd
import json

scraper = Scraper(seed = 'info.json')
scraper

trace = TransformTrace()
cubes = Cubes("info.json")

# %%
for i in scraper.distributions:
    display(i)

# %%
# extract latest distribution and datasetTitle
distribution = scraper.distribution(title=lambda t: 'Inter-Departmental Business Register' in t)
datasetTitle = distribution.title
print(distribution.downloadURL)
print(datasetTitle)

# %%
tabs = distribution.as_databaker()

# %%
for tab in tabs:
    print(tab.name)

# %%
columns = ["Period", "Town Id", "Size Band"]

# %%
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

# %%
df = trace.combine_and_trace(datasetTitle, "combined_dataframe")
df


# %%
def left(s, amount):
    return s[:amount]
def date_time (date):
    if len(date)  == 6:
        return 'year/'+ left(date, 4)
df['Period'] =  df["Period"].apply(date_time)
trace.Period("Period is formated to have pattern year/{year}")

# %%
df.rename(columns = {"OBS":"Value"}, inplace = True)

# %%
COLUMNS_TO_NOT_PATHIFY = ['Period', 'Value', 'Town Id']

# %%
for col in df.columns.values.tolist():
    if col in COLUMNS_TO_NOT_PATHIFY:
        continue
    try:
        df[col] = df[col].apply(pathify)
    except Exception as err:
        raise Exception('Failed to pathify column "{}".'.format(col)) from err

# %%
cubes.add_cube(scraper, df.drop_duplicates(), datasetTitle)
cubes.output_all()

# %%
trace.render("spec_v1.html")
