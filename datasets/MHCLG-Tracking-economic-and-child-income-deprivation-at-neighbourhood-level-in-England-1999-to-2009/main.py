#!/usr/bin/env python
# coding: utf-8

# In[59]:


from gssutils import *
import json
import math
import os
from urllib.parse import urljoin
import copy

info = json.load(open('info.json'))
etl_title = info["title"]
etl_publisher = info["publisher"]
print("Publisher: " + etl_publisher)
print("Title: " + etl_title)

cubes = Cubes('info.json')

scraper = Scraper(seed="info.json")
scraper


# In[60]:


trace = TransformTrace()

scraper.select_dataset(title=lambda x: x.lower().startswith('tracking'))

def excelRange(bag):
    min_x = min([cell.x for cell in bag])
    max_x = max([cell.x for cell in bag])
    min_y = min([cell.y for cell in bag])
    max_y = max([cell.y for cell in bag])
    top_left_cell = xypath.contrib.excel.excel_location(bag.filter(lambda x: x.x == min_x and x.y == min_y))
    bottom_right_cell = xypath.contrib.excel.excel_location(bag.filter(lambda x: x.x == max_x and x.y == max_y))
    return f"{top_left_cell}:{bottom_right_cell}"

def PeriodFromColumnName(value):
    # returns just the year from the column name -> which contains the dataset name
    year = value.split(',')[-1].strip()
    return year

def ScoreOrRank(value):
    # tidies up dimension within list_of_transform_type_2 datasets
    if value.lower().startswith('average score'):
        return 'Average Score'
    elif value.lower().startswith('rank of average score'):
        return 'Rank of Average Score'
    elif value.lower().startswith('average rank'):
        return 'Average Rank'
    elif value.lower().startswith('rank of average rank'):
        return 'Rank of Average Rank'


# there are 2 distinct types of format
list_of_transform_type_1 = [
    'Economic deprivation index: rank',
    'Economic deprivation index: income deprivation domain score',
    'Economic deprivation index: income deprivation domain rank',
    'Economic deprivation index: employment deprivation domain score',
    'Economic deprivation index: employment deprivation domain rank',
    'Children in income-deprived households index: score',
    'Children in income-deprived households index: rank',
    'Economic deprivation index: income deprivation domain numerator',
    'Economic deprivation index: income deprivation domain denominator',
    'Economic deprivation index: employment deprivation domain numerator',
    'Economic deprivation index: employment deprivation domain denominator',
    'Children in income-deprived households index: numerator',
    'Children in income-deprived households index: denominator',
    'Economic deprivation index: score',
    'Total population used to calculate local authority district and economic deprivation index summary measure'
]

list_of_transform_type_2 = [
    'Local authority district: economic deprivation index and domains average ranks and scores',
    'Local authority district: children in income-deprived households index average ranks and scores'
]

tidied_sheets = {} # to be filled with each tab of data
for distribution in scraper.distributions:

    if distribution.title in list_of_transform_type_1:
        tabs = distribution.as_databaker()
        tabs = [tab for tab in tabs if 'metadata' not in tab.name.lower()] # unwanted tabs

        for tab in tabs:
            # run assertions here
            assert tab.excel_ref('A1').value == 'lsoacode'
            assert tab.excel_ref('B1').value == 'lsoaname'
            assert tab.excel_ref('C1').value == 'lauacode'
            assert tab.excel_ref('D1').value == 'lauaname'

            # trace info
            unique_identifier = distribution.title + ' - ' + tab.name # title of dataset + tab name
            link = distribution.downloadURL
            columns = ['Period', 'lsoacode', 'lsoaname', 'lauacode', 'lauaname', 'Value']
            trace.start(scraper.title, unique_identifier, columns, link)

            tidy_sheet_list = [] # list of dataframes for each iteration
            cs_list = [] # list of conversionsegments for each iteration

            '''Iterating the databaking process'''
            tab_length = len(tab.excel_ref('A')) # number of rows of data
            batch_number = 10 # iterates over this many rows at a time
            number_of_iterations = math.ceil(tab_length/batch_number) # databaking will iterate this many times

            for i in range(0, number_of_iterations):
                Min = str(2 + batch_number * i)  # data starts on row 2
                Max = str(int(Min) + batch_number - 1)

                '''
                use "Min" and "Max" to specify a range of cells
                instead of selecting cells using ".expand(DOWN)"
                '''
                lsoa_code = tab.excel_ref('A'+Min+':A'+Max).is_not_blank()
                lsoa_name = tab.excel_ref('B'+Min+':B'+Max).is_not_blank()

                laua_code = tab.excel_ref('C'+Min+':C'+Max).is_not_blank()
                laua_name = tab.excel_ref('D'+Min+':D'+Max).is_not_blank()

                period = tab.excel_ref('E1').expand(RIGHT).is_not_blank()
                # will be the same range of cells for each iteration

                obs = period.waffle(lsoa_code)

                dimensions = [
                        HDim(period, 'Period', DIRECTLY, ABOVE),
                        HDim(lsoa_code, 'lsoacode', DIRECTLY, LEFT),
                        HDim(lsoa_name, 'lsoaname', DIRECTLY, LEFT),
                        HDim(laua_code, 'lauacode', DIRECTLY, LEFT),
                        HDim(laua_name, 'lauaname', DIRECTLY, LEFT),
                        ]

                if len(obs) != 0: # only use ConversionSegment if there is data
                    cs_iteration = ConversionSegment(tab, dimensions, obs) # creating the conversionsegment
                    tidy_sheet_iteration = cs_iteration.topandas() # turning conversionsegment into a pandas dataframe
                    cs_list.append(cs_iteration) # add to list
                    tidy_sheet_list.append(tidy_sheet_iteration) # add to list

            tidy_sheet = pd.concat(tidy_sheet_list, sort=False) # dataframe for the whole tab

            # trace
            # tracing is more hardcoded
            trace.Period('Values given in range {}', excelRange(period))
            trace.lsoacode('Value taken from column "lsoacode" - A2 expanded down')
            trace.lsoaname('Value taken from column "lsoaname" - B2 expanded down')
            trace.lauacode('Value taken from column "lauacode" - C2 expanded down')
            trace.lauaname('Value taken from column "lsoacode" - D2 expanded down')
            trace.Value('Value taken from period columns - E2:O2 expanded down')
            trace.with_preview(cs_list[0])

            # some tidying up
            tidy_sheet = tidy_sheet.rename(columns={'OBS':'Value', 'DATAMARKER':'Marker'})
            trace.Value('Renamed "OBS" column as "Value"')
            tidy_sheet['Period'] = tidy_sheet['Period'].apply(PeriodFromColumnName)
            trace.Period('Value changed to only include the year')

            if 'Marker' in tidy_sheet.columns:
                trace.add_column('Marker')
                trace.Marker('Value taken from "Value" where a value is surpressed')
                trace.Marker('Renamed "DATAMARKER" column as "Marker"')
                tidy_sheet = tidy_sheet[[
                        'Period', 'lsoacode', 'lsoaname', 'lauacode', 'lauaname', 'Marker', 'Value'
                        ]]
            else:
                tidy_sheet = tidy_sheet[[
                        'Period', 'lsoacode', 'lsoaname', 'lauacode', 'lauaname', 'Value'
                        ]]

            trace.store(unique_identifier, tidy_sheet)
            tidied_sheets[unique_identifier] = tidy_sheet


    elif distribution.title in list_of_transform_type_2:
        tabs = distribution.as_databaker()
        tabs = [tab for tab in tabs if 'metadata' not in tab.name.lower()] # unwanted tabs

        for tab in tabs:
            # trace info
            unique_identifier = distribution.title + ' - ' + tab.name
            link = distribution.downloadURL
            columns = ['Period', 'lauacode', 'lauaname', 'Dimension 1', 'Value']
            trace.start(scraper.title, unique_identifier, columns, link)

            pivot = tab.filter(contains_string('lauacode'))
            lauacode = pivot.fill(DOWN).is_not_blank()
            lauaname = lauacode.shift(1, 0)
            period = pivot.shift(0, -1).expand(RIGHT).is_not_blank()
            score_rank = pivot.shift(2, 0).expand(RIGHT).is_not_blank()
            obs = lauacode.waffle(score_rank)

            dimensions = [
                    HDim(lauacode, 'lauacode', DIRECTLY, LEFT),
                    HDim(lauaname, 'lauaname', DIRECTLY, LEFT),
                    HDim(period, 'Period', CLOSEST, LEFT),
                    HDim(score_rank, 'Dimension 1', DIRECTLY, ABOVE)
                    ]

            cs = ConversionSegment(tab, dimensions, obs)
            tidy_sheet = cs.topandas()
            trace.with_preview(cs)

            # trace
            trace.Period('Values given in range {}', excelRange(period))
            trace.lauacode('Values given in range {}', excelRange(lauacode))
            trace.lauaname('Values given in range {}', excelRange(lauaname))
            trace.Dimension_1('Values given in range {}', excelRange(score_rank))
            trace.Value('Values given in range {}', excelRange(obs))

            # some tidying up
            tidy_sheet = tidy_sheet.rename(columns={'OBS':'Value'})
            trace.Value('Renamed "OBS" column as "Value"')
            tidy_sheet['Period'] = tidy_sheet['Period'].apply(lambda x: int(float(x))) # removing the '.0'
            trace.Period('Removed ".0" from year')
            tidy_sheet['Dimension 1'] = tidy_sheet['Dimension 1'].apply(ScoreOrRank)
            trace.Dimension_1('Tidied up dimensions to take one of the values                               ["Average Score", "Rank of Average Score", "Average Rank", "Rank of Average Rank"]')
            tidy_sheet = tidy_sheet[[
                    'Period', 'lauacode', 'lauaname', 'Dimension 1', 'Value'
                    ]]

            trace.store(unique_identifier, tidy_sheet)
            tidied_sheets[unique_identifier] = tidy_sheet

del tidy_sheet
del tabs
del obs

out = Path('out')
out.mkdir(exist_ok=True)

'''
for key in tidied_sheets:
    df = tidied_sheets[key]
    df.drop_duplicates().to_csv(out / f'{key}.csv', index=False)
'''

for key in tidied_sheets:
    print(key)
    #print('Count: ' + str(tidied_sheets[key]['Year'].count()))
    if 'Economic Deprivation Indicator' in tidied_sheets[key].columns:
        del tidied_sheets[key]['Economic Deprivation Indicator']
    if 'Period' in tidied_sheets[key].columns:
        tidied_sheets[key] = tidied_sheets[key].rename(columns={'Period': 'Year'})
        tidied_sheets[key]['Year'] = 'year/' + tidied_sheets[key]['Year'].astype(str)
    if 'lsoaname' in tidied_sheets[key].columns:
        del tidied_sheets[key]['lsoaname']
        tidied_sheets[key] = tidied_sheets[key].rename(columns={'lsoacode': 'Lower Layer Super Output Area'})
    if 'lauaname' in tidied_sheets[key].columns:
        del tidied_sheets[key]['lauaname']
        tidied_sheets[key] = tidied_sheets[key].rename(columns={'lauacode': 'Local Authority'})
    if 'Marker' in tidied_sheets[key].columns:
        tidied_sheets[key]['Value'][tidied_sheets[key]['Marker'] == '*'] = '0'
        tidied_sheets[key]['Marker'][tidied_sheets[key]['Marker'] == '*'] = 'suppressed'
        tidied_sheets[key]['Marker'] = tidied_sheets[key]['Marker'].fillna('')
    ind = ''
    if 'EDI' in key:
        ind = 'EDI'
    elif 'IDD' in key:
        ind = 'IDD'
    elif 'EDD' in key:
        ind = 'EDD'
    elif 'CIDI' in key:
        ind = 'CIDI'
    if len(ind) > 0:
        tidied_sheets[key].insert(1,'Economic Deprivation Indicator',ind)

    #print(tidied_sheets[key].head(10))


# In[61]:


ranksDat = []
scoreDat = []
denomDat = []
numerDat = []
populDat = []

for key in tidied_sheets:
    if ('score & rank' not in key) & ('score and rank' not in key):
        if 'rank' in key:
            ranksDat.append(tidied_sheets[key])
        elif 'score' in key:
            scoreDat.append(tidied_sheets[key])
            if 'Marker' not in tidied_sheets[key].columns:
                tidied_sheets[key].insert(len(list(tidied_sheets[key].columns)) - 1,'Marker','')
        elif 'numerator' in key:
            numerDat.append(tidied_sheets[key])
        elif 'denominator' in key:
            denomDat.append(tidied_sheets[key])
        elif 'Population' in key:
            populDat.append(tidied_sheets[key])

del tidied_sheets

ranks = pd.concat(ranksDat, sort=False)
score = pd.concat(scoreDat, sort=False)
numer = pd.concat(numerDat, sort=False)
denom = pd.concat(denomDat, sort=False)
popul = pd.concat(populDat, sort=False)

del ranksDat, scoreDat, numerDat, denomDat, populDat

all_dat = [ranks, score, numer, denom, popul]
del ranks, score, numer, denom, popul


# In[62]:


#all_dat[0].head(10)


# In[63]:


scraper.dataset.family = 'towns-high-streets'

n = [
    "ranks-observations",
    "scores-observations",
    "numerators-observations",
    "denominators-observations",
    "population-observations"
]

t = [
    "Tracking economic and child income deprivation at neighbourhood level in England: Ranks",
    "Tracking economic and child income deprivation at neighbourhood level in England: Scores",
    "Tracking economic and child income deprivation at neighbourhood level in England: Numerators",
    "Tracking economic and child income deprivation at neighbourhood level in England: Denominators",
    "Tracking economic and child income deprivation at neighbourhood level in England: Total Population (all ages)"
]

d_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name)).lower()
pa = [
    "ranks",
    "scores",
    "numerators",
    "denominators",
    "count"
]

c = [
    "Ranks tracking levels of economic deprivation in England by Lower Layer Super Output Areas (LSOA).",
    "Scores tracking levels of economic deprivation in England by Lower Layer Super Output Areas (LSOA).",
    "Numerators tracking levels of economic deprivation in England by Lower Layer Super Output Areas (LSOA).",
    "Denominators tracking levels of economic deprivation in England by Lower Layer Super Output Areas (LSOA).",
    "Population counts in England by Lower Layer Super Output Areas (LSOA) used to calculate economic deprivation Indicators."
]

dt = [
    'integer',
    'double',
    'integer',
    'integer',
    'integer'
]

mt = [
    "deprivation",
    "deprivation",
    "deprivation",
    "deprivation",
    "persons",
]

description = """
    This Statistical Release presents key findings from the Economic Deprivation Index (EDI) and the
    Children in Income Deprived households Index (CIDI), hereafter referred to collectively as the
    ‘economic deprivation indices’. These indices track neighbourhood-level deprivation each year
    on a consistent basis, taking account of changes to the tax and benefit systems
    over this period. They are produced using the same general methodology as the Income and
    Employment deprivation domains of the English Indices of Deprivation (with slightly narrower
    definitions of income and employment deprivation). As such, the economic deprivation indices
    complement the Indices of Deprivation 2010.
    Report can be found here:
    https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/36446/Tracking_Neighbourhoods_Stats_Release.pdf
"""
mt


# In[64]:


out = Path('out')
out.mkdir(exist_ok=True)
scraper.dataset.description = description

with open('info.json') as f:
    jsn = json.load(f)
i = 0
for dat in all_dat:

    scraper1 = copy.deepcopy(scraper)

    jsn["transform"]["columns"]["Value"]["unit"] = "http://gss-data.org.uk/def/concept/measurement-units/" + mt[i]
    jsn["transform"]["columns"]["Value"]["measure"] = "http://gss-data.org.uk/def/measure/" + pa[i]
    jsn["transform"]["columns"]["Value"]["datatype"] = dt[i]
    if dt[i] == 'integer':
        dat['Value'] = pd.to_numeric(dat['Value'], downcast='integer')
    csvName = n[i]
    dat = dat.drop_duplicates()
    #dat.drop_duplicates().to_csv(out / csvName, index = False, header=True)
    #dat.drop_duplicates().to_csv(out / (csvName + '.gz'), index = False, compression='gzip')
    scraper1.dataset.comment = c[i]
    scraper1.dataset.title = t[i]
    dataset_path = d_path + "/" + pa[i]
    scraper1.set_base_uri('http://gss-data.org.uk')
    scraper1.set_dataset_id(dataset_path)
    j = 0

    for year in dat['Year'].unique():

        if j == 0:
            # For the first the chunk, create a primary graph graph_uri and csv_name
            graph_uri = f"http://gss-data.org.uk/graph/gss_data/towns-high-streets/mhclg-tracking-economic-and-child-income-deprivation-at-neighbourhood-level-in-england-1999-to-2009/{csvName}"
            csv_name = csvName
            cubes.add_cube(scraper1, dat[dat['Year'] == year], csv_name, graph=csvName)
            j += 1

        else:
            # For subsequent chunk to add, create a secondary graph graph_uri and csv_name
            graph_uri = f"http://gss-data.org.uk/graph/gss_data/towns-high-streets/mhclg-tracking-economic-and-child-income-deprivation-at-neighbourhood-level-in-england-1999-to-2009/{csvName}/{str(year).replace('/', '-')}"
            csv_name = csvName + f'-{year}'.replace('/', '-')
            cubes.add_cube(scraper1, dat[dat['Year'] == year], csv_name, graph=csvName, override_containing_graph=graph_uri, suppress_catalog_and_dsd_output=True)
    i = i + 1


# In[65]:


cubes.output_all()


# In[66]:


"""
Was unsure on a dimension name for the score/rank dimension within the datasets:
'Local authority district: economic deprivation index and domains average ranks and scores'
'Local authority district: children in income-deprived households index average ranks and scores'
so dimension is called 'Dimension 1'
"""


# In[67]:


#all_dat[0].head(10)


# In[68]:


#del all_dat[0]['Economic Derivation Indicator']
#del all_dat[1]['Economic Derivation Indicator']
#del all_dat[2]['Economic Derivation Indicator']
#del all_dat[3]['Economic Derivation Indicator']


# In[68]:




