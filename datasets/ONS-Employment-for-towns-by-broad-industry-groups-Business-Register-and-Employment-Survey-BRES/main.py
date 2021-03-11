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

columns = ["Region", "Industry", "Period"]

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
    trace.Region("Defined from cell A2 and down")
    
    industry = cell.shift(1, 0).fill(RIGHT)
    trace.Industry("Defined from cell D2 and right")
    
    period = 2018
    trace.Period("Hardcoded as 2018")
    
    observations = industry.waffle(region)

    
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

df['Industry'] = df['Industry'].map(lambda x: 'mining-quarrying-utilities' 
                                    if (x == 'Mining, quarrying & utilities (B,D and E)') 
                                    else 'arts-entertainment-recreation-other-services' 
                                    if (x == 'Arts, entertainment, recreation & other services (R,S,T and U)')
                                    else 'total' if (x == 'Total') else x)
trace.Industry("If multiple letters are referenced in the label, label is pathified manually")

df['Industry'] = df['Industry'].map(lambda x: x[-2] if x[-1]== ')' else x )
trace.Industry("If a single letter is referenced in the label, letter is returned")

df['Industry'] = df['Industry'].map(lambda x: "http://gss-data.org.uk/def/trade/concept/standard-industrial-classification-2007/"+x 
                                    if len(x) == 1 
                                    else "http://gss-data.org.uk/data/gss_data/trade/ons-employment-for-towns-by-broad-industry-groups#concept/industry/"+x
                                   if len(x) == 5 
                                    else "http://gss-data.org.uk/data/gss_data/trade/ons-employment-for-towns-by-broad-industry-groups#concept/industry"+x)
trace.Industry("prefix are added to values")

df

cubes.add_cube(scraper, df.drop_duplicates(), datasetTitle)
cubes.output_all()

trace.render("spec_v1.html")
