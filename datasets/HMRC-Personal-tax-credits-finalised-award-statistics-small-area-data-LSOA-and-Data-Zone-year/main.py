#!/usr/bin/env python
# coding: utf-8

# In[1]:


from gssutils import *
import json

info = json.load(open('info.json'))
etl_title = info["title"]
etl_publisher = info["publisher"]
print("Publisher: " + etl_publisher)
print("Title: " + etl_title)

scraper = Scraper(seed="info.json")
scraper

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


# In[2]:


trace = TransformTrace()
tidied_sheets = {} # dataframes will be stored in here
cubes = Cubes("info.json")


# In[3]:


# latest data
scraper.select_dataset(title=lambda x: 'small area data' in x, latest=True)
scraper.dataset.family = 'trade'

for distribution in scraper.distributions:

    link = distribution.downloadURL
    dataset_title = distribution.title # title of dataset
    period = scraper.dataset.title[-12:] # time pulled from dataset title
    print(distribution.title)

    if 'LSOA' in distribution.title:

        tabs = distribution.as_databaker() # reading in dataset as databaker

        for tab in tabs:

            tab_name = dataset_title + ' - ' + tab.name # makes a unique name for each tab

            footnotes = tab.filter(contains_string('Footnotes')).expand(DOWN).expand(RIGHT) # to be removed

            if tab.name.lower() == 'families':
                # tables differ between tabs
                columns = [
                    'Date', 'Local authority', 'Lower Layer Super Output Area', 'Work Situation', 'Family Type', 'Value', 'Measure Type', 'Unit', 'Benefit Type'
                ]

                trace.start(dataset_title, tab, columns, link)

                date = tab.filter(contains_string('Number of')).value.split(': ')[-1]
                year = date.split(' ')[-1].strip()
                month = date.split(' ')[0].strip()
                month = monthToNumber(month)
                date = year + '/' + month

                local_authority_code = tab.filter(contains_string('Local authority code')).fill(DOWN).is_not_number().is_not_blank() - footnotes
                LSOA_code = local_authority_code.shift(2, 0)

                work_situation = tab.filter(contains_string('All Child Benefit recipient families')).expand(RIGHT).is_not_blank()
                family_type = tab.filter(contains_string('All families')).expand(RIGHT).is_not_blank()
                benefit_type = tab.filter(contains_string('WTC and CTC')).expand(RIGHT).is_not_blank()
                obs = tab.excel_ref("G10").expand(DOWN).expand(RIGHT).is_not_blank()

                # tracing dimensions
                trace.Date("Value taken from dataset title: {}".format(date))
                trace.Local_authority("Values given in range {}", excelRange(local_authority_code))
                trace.Lower_Layer_Super_Output_Area("Values given in range {}", excelRange(LSOA_code))
                trace.Work_Situation("Values given in range {}", excelRange(work_situation))
                trace.Family_Type("Values given in range {}", excelRange(family_type))
                trace.Benefit_Type("Values given in range {}", excelRange(benefit_type))
                trace.Value("Values given in range {}", excelRange(obs))
                trace.Measure_Type("Hardcoded as Count")
                trace.Unit("Hardcoded as families")


                dimensions = [
                    HDimConst('Date', date),
                    HDim(local_authority_code, 'Local authority', DIRECTLY, LEFT),
                    HDim(LSOA_code, 'Lower Layer Super Output Area', DIRECTLY, LEFT),
                    HDim(work_situation, 'Work Situation', CLOSEST, LEFT),
                    HDim(family_type, 'Family Type', CLOSEST, LEFT),
                    HDim(benefit_type, 'Benefit Type', CLOSEST, LEFT),
                    HDimConst('Measure Type', 'families'),
                    HDimConst('Unit', 'thousands')
                    ]

                tidy_sheet = ConversionSegment(tab, dimensions, obs)
                trace.with_preview(tidy_sheet)
#                 savepreviewhtml(tidy_sheet)

                tidy_sheet_aspandas = tidy_sheet.topandas()
                tidy_sheet_aspandas = tidy_sheet_aspandas.rename(columns={'OBS':'Value'})

                # Remove all rows with related to national childcare Indicator and group Benifit Type values as per spec
                tidy_sheet_aspandas = tidy_sheet_aspandas[tidy_sheet_aspandas["Benefit Type"] != "National Childcare Indicator (NI 118)1"]
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Work Situation"] == 'All Child Benefit recipient families'),'Benefit Type'] = 'CTC'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Work Situation"] == 'All tax credits recipient families'),'Benefit Type'] = 'WTC'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Benefit Type"] == 'Benefitting from the childcare element'),'Benefit Type'] = 'All'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Benefit Type"] == 'Total lone parents'),'Benefit Type'] = 'All'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Benefit Type"] == 'Total in-work'),'Benefit Type'] = 'All'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Benefit Type"] == 'CTC only'),'Benefit Type'] = 'CTC'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Benefit Type"] == 'WTC only'),'Benefit Type'] = 'WTC'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Family Type"] == 'Total out of work'),'Benefit Type'] = 'CTC'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Family Type"] == 'Lone parents'),'Benefit Type'] = 'CTC'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Family Type"] == 'Couples'),'Benefit Type'] = 'CTC'
                trace.Benefit_Type("Values grouped to CTC, WTC, All, WTC and CTC")



                tidy_sheet_aspandas["Work Situation"] = tidy_sheet_aspandas["Work Situation"].apply(sanitize_work_situation_family_sheets)
                trace.Work_Situation("Values grouped to In work, Out of work and all")

                tidy_sheet_aspandas["Family Type"] = tidy_sheet_aspandas["Family Type"].apply(sanitize_family_type_in_sheets)
                trace.Family_Type("Values grouped to Couples, Lone Parent and all")


                tidy_sheet_aspandas = tidy_sheet_aspandas[columns]

                trace.store(tab_name, tidy_sheet_aspandas)
                tidied_sheets[tab_name] = tidy_sheet_aspandas



            elif tab.name.lower() == 'children':

                columns = [
                    'Date', 'Local authority', 'Lower Layer Super Output Area',
                    'Work Situation', 'Family Type', 'Value', 'Measure Type', 'Unit', 'Benefit Type'
                ]

                trace.start(dataset_title, tab, columns, link)

                date = tab.filter(contains_string('Number of')).value.split(': ')[-1]
                year = date.split(' ')[-1].strip()
                month = date.split(' ')[0].strip()
                month = monthToNumber(month)
                date = year + '/' + month

                local_authority_code = tab.filter(contains_string('Local authority code')).fill(DOWN).is_not_number().is_not_blank() - footnotes
                LSOA_code = local_authority_code.shift(2, 0)
                work_situation = tab.filter(contains_string('All children within families registered for Child Benefit')).expand(RIGHT).is_not_blank()
                family_type = tab.filter(contains_string('All children within families registered for Child Benefit')).shift(0,1).expand(RIGHT).is_not_blank()
                benefit_type = tab.filter(contains_string('WTC and CTC')).expand(RIGHT).is_not_blank()

                obs = tab.excel_ref("G9").expand(DOWN).expand(RIGHT).is_not_blank()

                # tracing dimensions

                trace.Date("Value taken from dataset title: {}".format(date))
                trace.Local_authority("Values given in range {}", excelRange(local_authority_code))
                trace.Lower_Layer_Super_Output_Area("Values given in range {}", excelRange(LSOA_code))
                trace.Work_Situation("Values given in range {}", excelRange(work_situation))
                trace.Family_Type("Values given in range {}", excelRange(family_type))
                trace.Benefit_Type("Values given in range {}", excelRange(benefit_type))
                trace.Value("Values given in range {}", excelRange(obs))
                trace.Measure_Type("Hardcoded as Count")
                trace.Unit("Hardcoded as children")

                dimensions = [
                    HDimConst('Date', date),
                    HDim(local_authority_code, 'Local authority', DIRECTLY, LEFT),
                    HDim(LSOA_code, 'Lower Layer Super Output Area', DIRECTLY, LEFT),
                    HDim(work_situation, 'Work Situation', CLOSEST, LEFT),
                    HDim(family_type, 'Family Type', CLOSEST, LEFT),
                    HDim(benefit_type, 'Benefit Type', CLOSEST, LEFT),
                    HDimConst('Measure Type', 'children'),
                    HDimConst('Unit', 'thousands')
                    ]

                tidy_sheet = ConversionSegment(tab, dimensions, obs)
#                 savepreviewhtml(tidy_sheet)
                trace.with_preview(tidy_sheet)

                tidy_sheet_aspandas = tidy_sheet.topandas()
                tidy_sheet_aspandas = tidy_sheet_aspandas.rename(columns={'OBS':'Value'})

                # Group Benifit Type values as per spec
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Work Situation"] == 'All children within families registered for Child Benefit'),'Benefit Type'] = 'CTC'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Work Situation"] == 'All children within families receiving tax credits'),'Benefit Type'] = 'WTC'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Benefit Type"] == 'CTC only'),'Benefit Type'] = 'CTC'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Benefit Type"] == 'Total children'),'Benefit Type'] = 'All'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Benefit Type"] == 'Lone parents'),'Benefit Type'] = 'All'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Work Situation"] == 'Children within out-of-work families'),'Benefit Type'] = 'CTC'
                trace.Benefit_Type("Values grouped to CTC, WTC, All, WTC and CTC")

                tidy_sheet_aspandas["Work Situation"] = tidy_sheet_aspandas["Work Situation"].apply(sanitize_work_situation_children_sheets)
                trace.Work_Situation("Values grouped to In work, Out of work and all")

                tidy_sheet_aspandas["Family Type"] = tidy_sheet_aspandas["Family Type"].apply(sanitize_family_type_in_sheets)
                trace.Family_Type("Values grouped to Couples, Lone Parent and all")

                tidy_sheet_aspandas = tidy_sheet_aspandas[columns]

                trace.store(tab_name, tidy_sheet_aspandas)
                tidied_sheets[tab_name] = tidy_sheet_aspandas

    elif 'scottish data zone' in distribution.title.lower():
        # scottish data zone has slightly different format

        tabs = distribution.as_databaker() # reading in dataset as databaker

        for tab in tabs:

            tab_name = dataset_title + ' - ' + tab.name # makes a unique name for each tab

            footnotes = tab.filter(contains_string('Footnotes')).expand(DOWN).expand(RIGHT) # to be removed

            if tab.name.lower() == 'family': # different tab name to other datasets
                # tables differ between tabs
                columns = [
                    'Date', 'Local authority', 'Data Zone code',
                    'Work Situation', 'Family Type', 'Value', 'Measure Type', 'Unit', 'Benefit Type'
                ]

                trace.start(dataset_title, tab, columns, link)

                date = tab.filter(contains_string('Number of')).value.split(': ')[-1]
                year = date.split(' ')[-1].strip()
                month = date.split(' ')[0].strip()
                month = monthToNumber(month)
                date = year + '/' + month


                local_authority_code = tab.filter(contains_string('Local authority code')).fill(DOWN).is_not_number().is_not_blank() - footnotes
                data_zone_code = local_authority_code.shift(2, 0)
                work_situation = tab.filter(contains_string('All Child Benefit recipient families')).expand(RIGHT).is_not_blank()
                family_type = tab.filter(contains_string('All families')).expand(RIGHT).is_not_blank()
                benefit_type = tab.filter(contains_string('WTC and CTC')).expand(RIGHT).is_not_blank()

                obs = tab.excel_ref("G10").expand(DOWN).expand(RIGHT).is_not_blank()

                # tracing dimensions

                trace.Date("Value taken from dataset title: {}".format(date))
                trace.Local_authority("Values given in range {}", excelRange(local_authority_code))
                trace.Data_Zone_code("Values given in range {}", excelRange(data_zone_code))
                trace.Work_Situation("Values given in range {}", excelRange(work_situation))
                trace.Family_Type("Values given in range {}", excelRange(family_type))
                trace.Benefit_Type("Values given in range {}", excelRange(benefit_type))
                trace.Value("Values given in range {}", excelRange(obs))
                trace.Measure_Type("Hardcoded as families")
                trace.Unit("Hardcoded as thousands")

                dimensions = [
                    HDimConst('Date', date),
                    HDim(local_authority_code, 'Local authority', DIRECTLY, LEFT),
                    HDim(data_zone_code, 'Data Zone code', DIRECTLY, LEFT),
                    HDim(work_situation, 'Work Situation', CLOSEST, LEFT),
                    HDim(family_type, 'Family Type', CLOSEST, LEFT),
                    HDim(benefit_type, 'Benefit Type', CLOSEST, LEFT),
                    HDimConst('Measure Type', 'families'),
                    HDimConst('Unit', 'thousands')
                    ]

                tidy_sheet = ConversionSegment(tab, dimensions, obs)
                trace.with_preview(tidy_sheet)
#                 savepreviewhtml(tidy_sheet)

                tidy_sheet_aspandas = tidy_sheet.topandas()
                tidy_sheet_aspandas = tidy_sheet_aspandas.rename(columns={'OBS':'Value'})

                tidy_sheet_aspandas = tidy_sheet_aspandas[tidy_sheet_aspandas["Benefit Type"] != "National Childcare Indicator (NI 118)1"]
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Work Situation"] == 'All Child Benefit recipient families'),'Benefit Type'] = 'CTC'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Work Situation"] == 'All tax credits recipient families'),'Benefit Type'] = 'WTC'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Benefit Type"] == 'Benefitting from the childcare element'),'Benefit Type'] = 'All'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Benefit Type"] == 'Total lone parents'),'Benefit Type'] = 'All'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Benefit Type"] == 'Total in-work'),'Benefit Type'] = 'All'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Benefit Type"] == 'CTC only'),'Benefit Type'] = 'CTC'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Benefit Type"] == 'WTC only'),'Benefit Type'] = 'WTC'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Family Type"] == 'Total out of work'),'Benefit Type'] = 'CTC'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Family Type"] == 'Lone parents'),'Benefit Type'] = 'CTC'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Family Type"] == 'Couples'),'Benefit Type'] = 'CTC'
                trace.Benefit_Type("Values grouped to CTC, WTC, All, WTC and CTC")

                tidy_sheet_aspandas["Work Situation"] = tidy_sheet_aspandas["Work Situation"].apply(sanitize_work_situation_family_sheets)
                trace.Work_Situation("Values grouped to In work, Out of work and all")

                tidy_sheet_aspandas["Family Type"] = tidy_sheet_aspandas["Family Type"].apply(sanitize_family_type_in_sheets)
                trace.Family_Type("Values grouped to Couples, Lone Parent and all")

                tidy_sheet_aspandas = tidy_sheet_aspandas[columns]

                trace.store(tab_name, tidy_sheet_aspandas)
                tidied_sheets[tab_name] = tidy_sheet_aspandas

            elif tab.name.lower() == 'children':
                columns = [
                    'Date', 'Local authority', 'Data Zone code',
                    'Work Situation', 'Family Type', 'Value', 'Measure Type', 'Unit', 'Benefit Type'
                ]

                trace.start(dataset_title, tab, columns, link)

                date = tab.filter(contains_string('Number of')).value.split(': ')[-1]
                year = date.split(' ')[-1].strip()
                month = date.split(' ')[0].strip()
                month = monthToNumber(month)
                date = year + '/' + month

                local_authority_code = tab.filter(contains_string('Local authority code')).fill(DOWN).is_not_number().is_not_blank() - footnotes
                data_zone_code = local_authority_code.shift(2, 0)

                work_situation = tab.filter(contains_string('All children within families registered for Child Benefit')).expand(RIGHT).is_not_blank()
                family_type = tab.filter(contains_string('All children within families registered for Child Benefit')).shift(0,1).expand(RIGHT).is_not_blank()
                benefit_type = tab.filter(contains_string('WTC and CTC')).expand(RIGHT).is_not_blank()
                obs = tab.excel_ref("G9").expand(DOWN).expand(RIGHT).is_not_blank()

                # tracing dimensions

                trace.Date("Value taken from dataset title: {}".format(date))
                trace.Local_authority("Values given in range {}", excelRange(local_authority_code))
                trace.Data_Zone_code("Values given in range {}", excelRange(data_zone_code))
                trace.Work_Situation("Values given in range {}", excelRange(work_situation))
                trace.Family_Type("Values given in range {}", excelRange(family_type))
                trace.Benefit_Type("Values given in range {}", excelRange(benefit_type))
                trace.Value("Values given in range {}", excelRange(obs))
                trace.Measure_Type("Hardcoded as children")
                trace.Unit("Hardcoded as thousands")

                dimensions = [
                    HDimConst('Date', date),
                    HDim(local_authority_code, 'Local authority', DIRECTLY, LEFT),
                    HDim(data_zone_code, 'Data Zone code', DIRECTLY, LEFT),
                    HDim(work_situation, 'Work Situation', CLOSEST, LEFT),
                    HDim(family_type, 'Family Type', CLOSEST, LEFT),
                    HDim(benefit_type, 'Benefit Type', CLOSEST, LEFT),
                    HDimConst('Measure Type', 'children'),
                    HDimConst('Unit', 'thousands')
                ]

                tidy_sheet = ConversionSegment(tab, dimensions, obs)
                trace.with_preview(tidy_sheet)

                tidy_sheet_aspandas = tidy_sheet.topandas()
                tidy_sheet_aspandas = tidy_sheet_aspandas.rename(columns={'OBS':'Value'})

                # Group Benifit Type values as per spec
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Work Situation"] == 'All children within families registered for Child Benefit'),'Benefit Type'] = 'CTC'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Work Situation"] == 'All children within families receiving tax credits'),'Benefit Type'] = 'WTC'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Benefit Type"] == 'CTC only'),'Benefit Type'] = 'CTC'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Benefit Type"] == 'Total children'),'Benefit Type'] = 'All'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Benefit Type"] == 'Lone parents'),'Benefit Type'] = 'All'
                tidy_sheet_aspandas.loc[(tidy_sheet_aspandas["Work Situation"] == 'Children within out-of-work families'),'Benefit Type'] = 'CTC'
                trace.Benefit_Type("Values grouped to CTC, WTC, All, WTC and CTC")

                tidy_sheet_aspandas["Work Situation"] = tidy_sheet_aspandas["Work Situation"].apply(sanitize_work_situation_children_sheets)
                trace.Work_Situation("Values grouped to In work, Out of work and all")

                tidy_sheet_aspandas["Family Type"] = tidy_sheet_aspandas["Family Type"].apply(sanitize_family_type_in_sheets)
                trace.Family_Type("Values grouped to Couples, Lone Parent and all")

                tidy_sheet_aspandas = tidy_sheet_aspandas[columns]

                trace.store(tab_name, tidy_sheet_aspandas)
                tidied_sheets[tab_name] = tidy_sheet_aspandas


# tidied_sheets["Scottish Data Zones - Children"]
# tidied_sheets['Lower Layer Super Output Area (LSOA): North East - Children'].tail(50)
# tidied_sheets['Scottish Data Zones - Families'].tail(50)


# In[35]:


for key in tidied_sheets:
    print(key)

df = pd.concat(tidied_sheets.values())
df['Lower Layer Super Output Area'] = df['Lower Layer Super Output Area'].fillna(df['Data Zone code'])
df = df.drop(['Data Zone code'], axis=1)
df = df.rename(columns={'Date' : 'Period', 'Local authority' : 'Local Authority'})
df = df[['Period', 'Local Authority', 'Lower Layer Super Output Area', 'Family Type', 'Work Situation', 'Benefit Type', 'Value', 'Measure Type', 'Unit']]
df['Value'] = df['Value'].astype(int)
df = df.replace({'Period' : {'2017/08' : '2017/18'}})

df


# In[36]:


COLUMNS_TO_NOT_PATHIFY = ['Local Authority', 'Period', 'Lower Layer Super Output Area', 'Value']

for col in df.columns.values.tolist():
	if col in COLUMNS_TO_NOT_PATHIFY:
		continue
	try:
		df[col] = df[col].apply(pathify)
	except Exception as err:
		raise Exception('Failed to pathify column "{}".'.format(col)) from err

df


# In[ ]:


scraper.dataset.description = """
These statistics focus on the number of families benefiting from tax credits in
England, Scotland, and Wales. They are based on a snapshot of the finalised award year data, which in turn is based on 100% of tax
credit administrative data available for that period, and so they are not subject to sampling error. Within England and Wales, the number of families and children are
broken down by Lower Super Output Area (LSOA), and within Scotland they are broken down by Scottish Data Zone. This publication excludes any cases where the
claimants live outside the UK or where we cannot locate a region or area.LSOA level estimates for the number of properties without mains gas. Estimates at local authority and MSOA levels are also available.
"""
scraper.dataset.title = "Personal tax credits: finalised award statistics - small area data (LSOA and Data Zone)"

scraper.dataset.comment = 'These statistics provide detailed geographical estimates of the number of families in receipt of tax credits by LSOA and Data Zones'


# In[34]:


csvName = 'observations.csv'
cubes.add_cube(scraper, df.drop_duplicates(), csvName)


# In[ ]:


cubes.output_all()
trace.render("spec_v1.html")


# In[ ]:


#import os
#from urllib.parse import urljoin

#csvName = 'observations.csv'
#out = Path('out')
#out.mkdir(exist_ok=True)
#tidy.drop_duplicates().to_csv(out / csvName, index = False)
#tidy.drop_duplicates().to_csv(out / (csvName + '.gz'), index = False, compression='gzip')

#scraper.dataset.family = 'towns-high-streets'
#scraper.dataset.description = """
#These statistics focus on the number of families benefiting from tax credits in
#England, Scotland, and Wales. They are based on a snapshot of the finalised award year data, which in turn is based on 100% of tax
#credit administrative data available for that period, and so they are not subject to sampling error. Within England and Wales, the number of families and children are
#broken down by Lower Super Output Area (LSOA), and within Scotland they are broken down by Scottish Data Zone. This publication excludes any cases where the
#claimants live outside the UK or where we cannot locate a region or area.LSOA level estimates for the number of properties without mains gas. Estimates at local authority and MSOA levels are also available.
#"""
#scraper.dataset.title = "Personal tax credits: finalised award statistics - small area data (LSOA and Data Zone)"

#scraper.dataset.comment = 'These statistics provide detailed geographical estimates of the number of families in receipt of tax credits by LSOA and Data Zones'

#dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name)).lower()
#scraper.set_base_uri('http://gss-data.org.uk')
#scraper.set_dataset_id(dataset_path)

#csvw_transform = CSVWMapping()
#csvw_transform.set_csv(out / csvName)
#csvw_transform.set_mapping(json.load(open('info.json')))
#csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
#csvw_transform.write(out / f'{csvName}-metadata.json')

#with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
#    metadata.write(scraper.generate_trig())


# In[4]:




