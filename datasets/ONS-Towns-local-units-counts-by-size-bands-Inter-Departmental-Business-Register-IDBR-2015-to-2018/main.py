#!/usr/bin/env python
# coding: utf-8
# %%
from gssutils import *
import pandas as pd
import json

scraper = Scraper(seed = 'info.json')
scraper

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
columns = ["Period", "Town Id", "Bua Buasd", "Size Band"]

# %%
if tab.name == 'Table':
    print(tab.name)
    cell = tab.excel_ref("A1")


    period = cell.fill(DOWN)

    town_id = cell.shift(1,0).fill(DOWN)


    bua_buasd = cell.shift(2,0).fill(DOWN)

    size_band = cell.shift(2,0).fill(RIGHT)


    observations = size_band.waffle(bua_buasd)
#     savepreviewhtml(observations,fname=tab.name + "Preview.html")
    
    dimensions =[
        HDim(period, "Period", DIRECTLY, LEFT),
        HDim(town_id, "Town Id", DIRECTLY, LEFT),
        HDim(bua_buasd, "Bua Buasd", DIRECTLY, LEFT),
        HDim(size_band, "Size Band", DIRECTLY, ABOVE),
    ]

# %%
