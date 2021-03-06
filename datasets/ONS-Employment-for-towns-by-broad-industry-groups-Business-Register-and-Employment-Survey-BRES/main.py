# In[58]:

from gssutils import *
import pandas as pd
import json

def left(s, amount):
    return s[:amount]

def right(s, amount):
    return s[-amount:]

def mid(s, offset, amount):
    return s[offset:offset+amount]

scraper = Scraper(seed = 'info.json')
scraper

cubes = Cubes("info.json")

trace = TransformTrace()


# In[76]:


for i in scraper.distributions:
    display(i)


# In[77]:


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
        HDimConst("Period", 2018),
        HDimConst("Employment Type", tab.name)
    ]
    tidy_sheet = ConversionSegment(tab, dimensions, observations)
    savepreviewhtml(tidy_sheet, fname = tab.name+"Preview.html")
    trace.with_preview(tidy_sheet)
    trace.store("combined_dataframe", tidy_sheet.topandas())



# In[78]:


df = trace.combine_and_trace(datasetTitle, "combined_dataframe")
df

df['Industry'] = df['Industry'].map(lambda x: 'mining-quarrying-utilities'
                                    if (x == 'Mining, quarrying & utilities (B,D and E)')
                                    else 'arts-entertainment-recreation-other-services'
                                    if (x == 'Arts, entertainment, recreation & other services (R,S,T and U)')
                                    else 'wholesale'
                                    if (x == 'Wholesale (Part G)')
                                    else 'retail'
                                    if (x == 'Retail (Part G)')
                                    else 'motor-trades'
                                    if (x == 'Motor trades (Part G)')
                                    else 'total' if (x == 'Total') else x)
trace.Industry("If multiple letters are referenced in the label, label is pathified manually")

df['Industry'] = df['Industry'].map(lambda x: x[-2] if x[-1]== ')' else x )
trace.Industry("If a single letter is referenced in the label, letter is returned")

df['Industry'] = df['Industry'].map(lambda x: "http://gss-data.org.uk/def/trade/concept/standard-industrial-classification-2007/"+pathify(x)
                                    if len(x) == 1
                                    else "http://gss-data.org.uk/data/gss_data/trade/ons-employment-for-towns-by-broad-industry-groups#concept/industry/"+pathify(x)
                                   if len(x) == 5
                                    else "http://gss-data.org.uk/data/gss_data/trade/ons-employment-for-towns-by-broad-industry-groups#concept/industry/"+pathify(x))
trace.Industry("prefix are added to values")

df = df.rename(columns={'OBS' : 'Value', 'DATAMARKER' : 'Marker'})

df['Measure Type'] = 'count'
df['Unit'] = 'persons'

df = df.replace({'Marker' : {'!' : 'suppressed'},
                 'Employment Type' : {'Employment_by_industry' : 'all',
                                      'Full-time_employees' : 'full-time',
                                      'Part-time_employees' : 'part-time'}})

df['Period'] = df.apply(lambda x: 'year/' + str(x['Period']), axis = 1)

df['Value'] = pd.to_numeric(df['Value'], errors='coerce')

df['Value'] = df['Value'].astype('Int64')

df = df[['Period', 'Region', 'Industry', 'Employment Type', 'Value', 'Marker', 'Measure Type', 'Unit']]

df


# In[79]:


COLUMNS_TO_NOT_PATHIFY = ['Region', 'Period', 'Value', 'Marker', 'Industry']

for col in df.columns.values.tolist():
	if col in COLUMNS_TO_NOT_PATHIFY:
		continue
	try:
		df[col] = df[col].apply(pathify)
	except Exception as err:
		raise Exception('Failed to pathify column "{}".'.format(col)) from err


# In[80]:


cubes.add_cube(scraper, df.drop_duplicates(), datasetTitle)
cubes.output_all()

trace.render("spec_v1.html")


# In[81]:


from IPython.core.display import HTML
for col in df:
    if col not in ['Value']:
        df[col] = df[col].astype('category')
        display(HTML(f"<h2>{col}</h2>"))
        display(df[col].cat.categories)


# In[81]:




