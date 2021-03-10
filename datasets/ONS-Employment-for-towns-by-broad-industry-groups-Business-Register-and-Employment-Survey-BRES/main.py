#!/usr/bin/env python
# coding: utf-8
# +
from gssutils import *
import pandas as pd
import json

scraper = Scraper(seed = 'info.json')
scraper

cubes = Cubes("info.json")

trace = TransformTrace()
# -

for i in scraper.distributions:
    display(i)

distribution = scraper.distribution(title= lambda t: "Business Register and Employment Survey (BRES), 2018" in t)
datasetTitle = distribution.title
print(distribution.downloadURL)
print(datasetTitle)

tabs = distribution.as_databaker()

for tab in tabs:
    print(tab.name)

columns = ["Region", "Industry"]

tab_names_to_process = ["Employment_by_industry", "Part-time_employees", "Full-time_employees"]
for tab_name in tab_names_to_process:
    if tab.name not in [x.name for x in tabs]:
        raise ValueError(f'Aborting. A tab named {tab_name} required but not found')
    
        # Select the tab in question
    tab = [x for x in tabs if x.name == tab_name][0]
    print(tab.name)
    trace.start(datasetTitle, tab, columns, distribution.downloadURL)
    cell = tab.excel_ref("A2").is_not_blank().is_not_whitespace()
    
    region = cell.fill(DOWN)
#     savepreviewhtml(town_code, fname=tab.name + "Preview.html")
    
    industry = cell.shift(1, 0).fill(RIGHT)
#     savepreviewhtml(industry, fname=tab.name + "Preview.html")
    
    period = 2018
#     savepreviewhtml(period, fname=tab.name + "Preview.html")
    
    observations = industry.waffle(region)
#     savepreviewhtml(observations, fname=tab.name + "Preview.html")
    
    dimensions = [
        HDim(region, "Region", DIRECTLY, LEFT),
        HDim(industry, "Industry", DIRECTLY, ABOVE),
        HDimConst("Period", 2018)
    ]
    tidy_sheet = ConversionSegment(tab, dimensions, observations)
    savepreviewhtml(tidy_sheet, fname = tab.name+"Preview.html")
    trace.with_preview(tidy_sheet)
    trace.store("combined_dataframe", tidy_sheet.topandas())

df = trace.combine_and_trace(datasetTitle, "combined_dataframe")
df

df['DATAMARKER'].unique()

df['Industry'].unique()

df[df['Industry'].str.endswith('(N)')]
