#!/usr/bin/env python
# coding: utf-8

# In[176]:





# In[177]:


from gssutils import *
import json

info = json.load(open('info.json'))
etl_title = info["title"]
etl_publisher = info["publisher"]
print("Publisher: " + etl_publisher)
print("Title: " + etl_title)

scraper = Scraper(seed="info.json")
scraper


# In[178]:


def excelRange(bag):
    min_x = min([cell.x for cell in bag])
    max_x = max([cell.x for cell in bag])
    min_y = min([cell.y for cell in bag])
    max_y = max([cell.y for cell in bag])
    top_left_cell = xypath.contrib.excel.excel_location(bag.filter(lambda x: x.x == min_x and x.y == min_y))
    bottom_right_cell = xypath.contrib.excel.excel_location(bag.filter(lambda x: x.x == max_x and x.y == max_y))
    return f"{top_left_cell}:{bottom_right_cell}"

def monthToNumber(month):

    return {
            'January' : '01',
            'February' : '02',
            'March' : '03',
            'April' : '04',
            'May' : '05',
            'June' : '06',
            'July' : '07',
            'August' : '08',
            'September' : '09',
            'October' : '10',
            'November' : '11',
            'December' : '12'
    }[month]


def sanitize_work_situation_family_sheets(value):
    if value == "In-work families":
        return "In Work"
    elif value == "Out-of-work families":
        return "Out of Work"
    else:
        return "All"


def sanitize_work_situation_children_sheets(value):
    if value == "Children within in-work families":
        return "In Work"
    elif value == "Children within out-of-work families":
        return "Out of Work"
    else:
        return "All"


def sanitize_family_type_in_sheets(value):
    if value == None: return 'All'
    value = value.lower()
    if 'lone parent' in value:
        return 'Lone Parent'
    elif value == 'couples':
        return 'Couples'
    else:
        return 'All'


# In[179]:


for i in scraper.distributions:
    print(i.title)


# In[180]:


tidied_sheets = {} # dataframes will be stored in here
cubes = Cubes("info.json")


# In[181]:


scraper.dataset.family = 'towns-high-streets'


# In[182]:


for dist in scraper.distributions:

    dataset_title = dist.title

    if '(LSOA)' in dist.title or 'scottish' in dist.title.lower():

        print(dist.title)

        tabs = dist.as_databaker()

        for tab in tabs:

            tab_name = dataset_title + ' - ' + tab.name

            if 'table' in tab.name.lower():

                date = tab.filter(contains_string('Number of')).value.split(': ')[-1]
                year = date.split(' ')[-1].strip()
                month = date.split(' ')[0].strip()
                month = monthToNumber(month)
                date = 'month/'+ year + '-' + month

                pivot = tab.filter(contains_string('Number of'))

                local_authority_code = tab.filter(contains_string('Local authority code')).fill(DOWN).is_not_blank()
                LSOA_code = local_authority_code.shift(2, 0).is_not_blank()
                situation = tab.excel_ref('F2').expand(RIGHT).is_not_blank()
                obs = LSOA_code.shift(2,0).expand(RIGHT).is_not_blank()

                if 'children' in tab.filter(contains_string('Number of')).value:
                    measure_type = 'children'
                else:
                    measure_type = 'families'

                dimensions = [
                    HDimConst('Period', date),
                    HDim(local_authority_code, 'Local Authority', DIRECTLY, LEFT),
                    HDim(LSOA_code, 'Lower Layer Super Output Area', DIRECTLY, LEFT),
                    HDim(situation, 'Situation', DIRECTLY, ABOVE),
                    HDimConst('Measure Type', measure_type),
                    HDimConst('Unit', 'thousands')
                    ]

                tidy_sheet = ConversionSegment(tab, dimensions, obs)
                #savepreviewhtml(tidy_sheet, fname= pathify(tab_name) + "-Preview.html")

                tidy_sheet_aspandas = tidy_sheet.topandas()
                tidy_sheet_aspandas = tidy_sheet_aspandas.rename(columns={'OBS':'Value'})

                tidied_sheets[tab_name] = tidy_sheet_aspandas


# In[186]:


df = pd.concat([i for i in tidied_sheets.values()])

df = df[df["Situation"] != "National Childcare Indicator (NI 118)"]

df['Family Type'] = df.apply(lambda x: 'Lone Parent' if 'lone parent' in str(x['Situation']).lower() else ('Couples' if 'couples' in str(x['Situation']).lower() else 'All'), axis = 1)
df['Work Situation'] = df.apply(lambda x: 'In Work' if 'in-work' in str(x['Situation']).lower() else ('Out of Work' if 'out-of-work' in str(x['Situation']).lower().replace(" ", "") else 'All'), axis = 1)

df['Marker'] = df.apply(lambda x: 'suppressed' if x['Value'] == 0.0 else '', axis = 1)
df['Value'] = df.apply(lambda x: '' if x['Marker'] == 'suppressed' else x['Value'], axis = 1)

df['Benefit Type'] = df['Situation']

df = df.replace({'Benefit Type' : {
            'All Child Benefit recipient families' : 'Child Benefit',
            'All Tax Credits recipient families' : 'Tax Credits',
            'In-work families receiving WTC and CTC' : 'WTC and CTC',
            'In-work families receiving CTC only' : 'CTC only',
            'In-work families receiving WTC only' : 'WTC only',
            'Total in-work families' : 'All',
            'In-work families benefitting from the childcare element' : 'Childcare Element',
            'Total lone parents families' : 'All',
            'Lone parent families benefitting from the childcare element' : 'Childcare Element',
            'Total out-of- work families' : 'All',
            'Out-of-work lone parents families' : 'All',
            'Out-of-work couples families' : 'All',
            'All children within families registered for Child Benefit' : 'Child Benefit',
            'All children within families receiving Tax Credits' : 'Tax Credits',
            'Children within in-work families receiving WTC and CTC' : 'WTC and CTC',
            'Children within in-work families receiving CTC only' : 'CTC only',
            'Total children within in-work families' : 'All',
            'Children within in-work lone parents families' : 'All',
            'Total children within out-of-work families' : 'All',
            'Children within out-of-work lone parents families' : 'All',
            'Children within out-of-work couples families' : 'All'}})

df = df.drop(columns=['Situation'])

df = df[['Period', 'Local Authority', 'Lower Layer Super Output Area', 'Family Type', 'Work Situation', 'Benefit Type', 'Value', 'Marker', 'Measure Type', 'Unit']]

df


# In[184]:


scraper.dataset.title = 'Personal tax credits finalised award statistics, LSOA and Data Zone'
scraper.dataset.comment = 'These statistics provide detailed geographical estimates of the number of families and the number of children in families in receipt of tax credits.'
scraper.dataset.description = """
These statistics focus on the number of families benefiting from tax credits in
England, Scotland, and Wales. They are based on a snapshot of the finalised award year data, which in turn is based on 100% of tax
credit administrative data available for that period, and so they are not subject to sampling error. Within England and Wales, the number of families and children are
broken down by Lower Super Output Area (LSOA), and within Scotland they are broken down by Scottish Data Zone. This publication excludes any cases where the
claimants live outside the UK or where we cannot locate a region or area.LSOA level estimates for the number of properties without mains gas. Estimates at local authority and MSOA levels are also available.
"""

cubes.add_cube(scraper, df, scraper.dataset.title)


# In[185]:


cubes.output_all()


# In[187]:


import glob
import numpy as np

out = Path('codelists')
out.mkdir(exist_ok=True)

CODELISTS = True

path = 'out/*'

csv_files = [i for i in glob.glob(path.format('csv')) if 'json' not in i and 'trig' not in i]

if CODELISTS:
    for i in csv_files:
        df = pd.read_csv(i)
        for col in df.columns:
            if col not in ['Period', 'Lower Layer Super Output Area', 'Local authority', 'Measure Type', 'Unit', 'Value', 'Marker']:
                if os.path.exists(out / f'{pathify(col)}.csv'):
                    dfcodeexists = pd.read_csv(out / f'{pathify(col)}.csv')
                    dfcodeexists = dfcodeexists[['Label', 'Notation']]
                    dfcode = df[[col]].drop_duplicates()
                    dfcode = dfcode.replace("", float("NaN"))
                    dfcode = dfcode.dropna(subset = [col])
                    dfcode['Notation'] = dfcode.apply(lambda x: pathify(x[col]), axis = 1)
                    dfcode = dfcode.rename(columns={dfcode.columns[0]: 'Label'})
                    dfcode['Label'] = dfcode['Label'].str.replace("-", " ").str.capitalize()
                    dfcode = pd.concat([dfcodeexists, dfcode], sort = False).drop_duplicates()
                    dfcode['Parent Notation'] = ''
                    dfcode['Sort Priority'] = np.arange(dfcode.shape[0])
                else:
                    dfcode = df[[col]].drop_duplicates()
                    dfcode = dfcode.replace("", float("NaN"))
                    dfcode = dfcode.dropna(subset = [col])
                    dfcode['Notation'] = dfcode.apply(lambda x: pathify(x[col]), axis = 1)
                    dfcode = dfcode.rename(columns={dfcode.columns[0]: 'Label'})
                    dfcode['Label'] = dfcode['Label'].str.replace("-", " ").str.capitalize()
                    dfcode['Parent Notation'] = ''
                    dfcode['Sort Priority'] = np.arange(dfcode.shape[0])
                dfcode.drop_duplicates().to_csv(out / f'{pathify(col)}.csv', index = False)

