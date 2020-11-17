from gssutils import * 
import json 
import os
from urllib.parse import urljoin


scraper = Scraper(seed="info.json")   
scraper 

scraper.select_dataset(title=lambda t: 'data 2020' in t)
scraper


# region
#tabs = { tab for tab in scraper.distributions[0].as_databaker() }
# list(tabs)
# endregion

# region
#df = pd.DataFrame()
#tidied_sheets = {}
# endregion

# region
# Place each table in separate variables 
#for tab in tabs:
#    if tab.name == 'Table 1':
#        tab1 = tab
#    elif tab.name == 'Table 2':
#        tab2 = tab
#    elif tab.name == 'Table 2':
#        tab2 = tab
#    elif tab.name == 'Table 3':
#        tab3 = tab
#    elif tab.name == 'Table 4':
#        tab4 = tab
#    elif tab.name == 'Table 5':
#        tab5 = tab
# endregion


# region
# # +
# Processing each table starting from table 1 
#region = tab1.excel_ref('A4').expand(DOWN).is_not_blank()
#remove_from_region = tab1.filter("1 Household and fuel poverty numbers at region level come from the national fuel poverty statistics, 2018:").assert_one().expand(DOWN).is_not_blank()
# region remove_from_region = region -

# endregion
# region = tab1.filter(contains_string('Region')).shift(0,1).expand(DOWN).is_not_blank()
"""
number_of_households = tab1.filter(contains_string('Number of households1')).shift(0,1).expand(DOWN).is_not_blank()
observations = tab1.filter(contains_string('Number of households in fuel poverty1')).shift(0,1).expand(DOWN).is_not_blank()
dimensions = [
    HDim(region, 'Region', DIRECTLY, LEFT),
    HDim(number_of_households, 'Number of households', DIRECTLY, LEFT)
]
tidy_sheet = ConversionSegment(tab1, dimensions, observations)
savepreviewhtml(tidy_sheet)
tidied_sheets["Table 1"] = tidy_sheet.topandas()




# +

# Processing table 2

region = tab2.filter(contains_string('Region')).shift(0,1).expand(DOWN).is_not_blank()
la_name = tab2.filter(contains_string('LA Name')).shift(0,1).expand(DOWN).is_not_blank()
la_code = tab2.filter(contains_string('LA Code')).shift(0,1).expand(DOWN).is_not_blank()
remove_from_la_code = tab2.filter("1 The geographies are based on pre-2012 geography codes. More information on geography code changes can be found at the ONS website:").assert_one().expand(DOWN).is_not_blank()
la_code = la_code - remove_from_la_code
number_of_households = tab2.filter(contains_string('Number of households1')).shift(0,1).expand(DOWN).is_not_blank()
observations = tab2.filter(contains_string('Number of households in fuel poverty1')).shift(0,1).expand(DOWN).is_not_blank()
dimensions = [
    HDim(la_code, 'LA Code', DIRECTLY, LEFT),
    HDim(la_name, 'LA Name', DIRECTLY, LEFT),
    HDim(region, 'Region', DIRECTLY, LEFT),
    HDim(number_of_households, 'Number of households', DIRECTLY, LEFT)
]
tidy_sheet = ConversionSegment(tab2, dimensions, observations)
savepreviewhtml(tidy_sheet)
tidied_sheets["Table 2"] = tidy_sheet.topandas()
# -

# Processing table 3
lsoa_code = tab3.filter(contains_string('LSOA Code')).shift(0,1).expand(DOWN).is_not_blank()
remove_from_lsoa_code = tab3.filter('1 The geographies are based on pre-2012 geography codes. More information on geography code changes can be found at the ONS website:').assert_one().expand(DOWN).is_not_blank()
lsoa_code = lsoa_code - remove_from_lsoa_code
lsoa_name = tab3.filter(contains_string('LSOA Name')).shift(0,1).expand(DOWN).is_not_blank()
region = tab3.filter(contains_string('Region')).shift(0,1).expand(DOWN).is_not_blank()
la_name = tab3.filter(contains_string('LA Name')).shift(0,1).expand(DOWN).is_not_blank()
la_code = tab3.filter(contains_string('LA Code')).shift(0,1).expand(DOWN).is_not_blank()
number_of_households = tab3.filter(contains_string('Number of households1')).shift(0,1).expand(DOWN).is_not_blank()
observations = tab3.filter(contains_string('Number of households in fuel poverty1')).shift(0,1).expand(DOWN).is_not_blank()
dimensions = [
    HDim(lsoa_code, 'LSOA Code', DIRECTLY, LEFT),
    HDim(lsoa_name, 'LSOA Name', DIRECTLY, LEFT),
    HDim(la_code, 'LA Code', DIRECTLY, LEFT),
    HDim(la_name, 'LA Name', DIRECTLY, LEFT),
    HDim(region, 'Region', DIRECTLY, LEFT),
    HDim(number_of_households, 'Number of households', DIRECTLY, LEFT)
]
tidy_sheet = ConversionSegment(tab3, dimensions, observations)
savepreviewhtml(tidy_sheet)
tidied_sheets["Table 3"] = tidy_sheet.topandas()

# Processing table 4
county = tab4.excel_ref('B5').expand(DOWN).is_not_blank()
county_code = tab4.filter(contains_string('County code')).shift(0,1).expand(DOWN).is_not_blank()
remove_from_county_code = tab4.filter('1 The geographies are based on pre-2012 geography codes. More information on geography code changes can be found at the ONS website:').expand(DOWN).is_not_blank()
county_code = county_code - remove_from_county_code
number_of_households = tab4.filter(contains_string('Number of households1')).shift(0,1).expand(DOWN).is_not_blank()
observations = tab4.filter(contains_string('Number of households in fuel poverty1')).shift(0,1).expand(DOWN).is_not_blank()
dimensions = [
    HDim(county_code, 'County Code', DIRECTLY, LEFT),
    HDim(county, 'County', DIRECTLY, LEFT),
    HDim(number_of_households, 'Number of households', DIRECTLY, LEFT)
]
tidy_sheet = ConversionSegment(tab4, dimensions, observations)
savepreviewhtml(tidy_sheet)
tidied_sheets["Table 4"] = tidy_sheet.topandas()

# Processing table 5
region = tab5.filter(contains_string('Region')).shift(0,1).expand(DOWN).is_not_blank()
parlimentary_c = tab5.filter('Parliamentary Constituency').shift(0,1).expand(DOWN).is_not_blank()
parlimentary_c_c = tab5.filter(contains_string('Parliamentary Constituency Code')).shift(0,1).expand(DOWN).is_not_blank()
number_of_households = tab5.filter(contains_string('Number of households1')).shift(0,1).expand(DOWN).is_not_blank()
observations = tab5.filter(contains_string('Number of households in fuel poverty1')).shift(0,1).expand(DOWN).is_not_blank()
dimensions = [
    HDim(parlimentary_c, 'Parliamentary Constituency', DIRECTLY, LEFT),
    HDim(parlimentary_c_c, 'Parlimentary Constituency Code', DIRECTLY, LEFT),
    HDim(region, 'Region', DIRECTLY, LEFT),
    HDim(number_of_households, 'Number of households', DIRECTLY, LEFT)
]
tidy_sheet = ConversionSegment(tab5, dimensions, observations)
savepreviewhtml(tidy_sheet)
tidied_sheets["Table 5"] = tidy_sheet.topandas()

# Post Process
# import pandas as pd
# dataframe = pd.concat(tidied_sheets)
tidied_sheets["Table 5"]
"""
# endregion

# region
# Sheet names
sn = ['Table 1','Table 2', 'Table 3', 'Table 4', 'Table 5']

joined_dat = []
# endregion

try:
    i = 0
    tab = scraper.distributions[0].as_pandas(sheet_name=sn[i])
    tab = tab[tab['Unnamed: 1'].notna()]
    tbls = []
    for x in range(1, len(tab.columns)):
        cols = [0,x]
        tab = tab.rename(columns={tab.columns[x]: tab.iloc[0,x], tab.columns[0]:'Region'})
        dat = tab.iloc[:, cols]
        dat['Household Measure'] = dat.columns[1]
        dat = dat.rename(columns={dat.columns[1]:'Value'})
        dat = dat[[dat.columns[0],dat.columns[2],dat.columns[1]]]
        dat = dat.iloc[1:]
        dat = dat[dat['Household Measure'].notna()]
        tbls.append(dat)
    
    k = 0
    for t in tbls:
        if k == 0:
            joined_dat1 = t
        else:
            joined_dat1 = pd.concat([joined_dat1,t])   
        k = k + 1
    # Assuming 'East' is 'East of England'
    joined_dat1['Region'] = joined_dat1['Region'].str.strip().replace({
        'North East':'E12000001',
        'North West':'E12000002' ,
        'Yorkshire and The Humber':'E12000003',
        'East Midlands':'E12000004',
        'West Midlands':'E12000005',
        'East':'E12000006',
        'London':'E12000007',
        'South East':'E12000008',
        'South West':'E12000009'
    })
    
    joined_dat.append(joined_dat1)
    del joined_dat1
except Exception as s:
    print(str(s))

try:
    i = 1
    tab = scraper.distributions[0].as_pandas(sheet_name=sn[i])
    tab = tab[tab['Unnamed: 1'].notna()]
    tbls = []
    for x in range(3, len(tab.columns)):
        cols = [0,x]
        tab = tab.rename(columns={tab.columns[x]: tab.iloc[0,x], tab.columns[0]:'Local Authority'})
        dat = tab.iloc[:, cols]
        dat['Household Measure'] = dat.columns[1]
        dat = dat.rename(columns={dat.columns[1]:'Value'})
        dat = dat[[dat.columns[0],dat.columns[2],dat.columns[1]]]
        dat = dat.iloc[1:]
        dat = dat[dat['Household Measure'].notna()]
        tbls.append(dat)
    
    k = 0
    for t in tbls:
        if k == 0:
            joined_dat2 = t
        else:
            joined_dat2 = pd.concat([joined_dat2,t])   
        k = k + 1
    
    joined_dat.append(joined_dat2)
    del joined_dat2
except Exception as s:
    print(str(s))

try:
    i = 2
    tab = scraper.distributions[0].as_pandas(sheet_name=sn[i])
    tab = tab[tab['Unnamed: 1'].notna()]
    tbls = []
    for x in range(5, len(tab.columns)):
        cols = [0,x]
        tab = tab.rename(columns={tab.columns[x]: tab.iloc[0,x], tab.columns[0]:'Lower Layer Super Output Area'})
        dat = tab.iloc[:, cols]
        dat['Household Measure'] = dat.columns[1]
        dat = dat.rename(columns={dat.columns[1]:'Value'})
        dat = dat[[dat.columns[0],dat.columns[2],dat.columns[1]]]
        dat = dat.iloc[1:]
        dat = dat[dat['Household Measure'].notna()]
        tbls.append(dat)
    
    k = 0
    for t in tbls:
        if k == 0:
            joined_dat3 = t
        else:
            joined_dat3 = pd.concat([joined_dat3,t])   
        k = k + 1
    joined_dat.append(joined_dat3)
    del joined_dat3
except Exception as s:
    print(str(s))

try:
    i = 3
    tab = scraper.distributions[0].as_pandas(sheet_name=sn[i])
    tab = tab[tab['Unnamed: 1'].notna()]
    tbls = []

    for x in range(2, len(tab.columns)):
        cols = [0,x]
        tab = tab.rename(columns={tab.columns[x]: tab.iloc[0,x], tab.columns[0]:'County Code'})
        dat = tab.iloc[:, cols]
        dat['Household Measure'] = dat.columns[1]
        dat = dat.rename(columns={dat.columns[1]:'Value'})
        dat = dat[[dat.columns[0],dat.columns[2],dat.columns[1]]]
        dat = dat.iloc[1:]
        dat = dat[dat['Household Measure'].notna()]
        tbls.append(dat)
    
    k = 0
    for t in tbls:
        if k == 0:
            joined_dat4 = t
        else:
            joined_dat4 = pd.concat([joined_dat4,t])   
        k = k + 1

    joined_dat.append(joined_dat4)
    del joined_dat4
except Exception as s:
    print(str(s))

try:
    i = 4
    tab = scraper.distributions[0].as_pandas(sheet_name=sn[i])
    tab = tab[tab['Unnamed: 1'].notna()]
    tbls = []
    for x in range(3, len(tab.columns)):
        cols = [0,x]
        tab = tab.rename(columns={tab.columns[x]: tab.iloc[0,x], tab.columns[0]:'Parliamentary Constituency'})
        dat = tab.iloc[:, cols]
        dat['Household Measure'] = dat.columns[1]
        dat = dat.rename(columns={dat.columns[1]:'Value'})
        dat = dat[[dat.columns[0],dat.columns[2],dat.columns[1]]]
        dat = dat.iloc[1:]
        dat = dat[dat['Household Measure'].notna()]
        tbls.append(dat)
    
    k = 0
    for t in tbls:
        if k == 0:
            joined_dat5 = t
        else:
            joined_dat5 = pd.concat([joined_dat5,t])   
        k = k + 1
        
    joined_dat.append(joined_dat5)
    del joined_dat5
except Exception as s:
    print(str(s))

i = 0
all_dat = pd.DataFrame(columns=['Geography Code', 'Geography Level', 'Household Measure', 'Value'])
for j in joined_dat:
    #print('Joined data ' + str(i+1) + ': ' + str(j['Value'].count()))
    j.insert(1, 'Geography Level', j.columns[0])
    j = j.rename(columns={j.columns[0]:'Geography Code'})
    #print(list(j.columns))
    
    all_dat = pd.concat([all_dat,j])
    #print(all_dat['Value'].count())
    i = i + 1

# region
# Get rid of the proportions
all_dat['Household Measure'] = all_dat['Household Measure'].str.strip().replace({
    'Number of households1':'Total Households',
    'Number of households in fuel poverty1':'Households in fuel poverty',
    'Proportion of households fuel poor (%)':'Proportion of households fuel poor'
})

all_dat = all_dat[all_dat['Household Measure'] != 'Proportion of households fuel poor']
all_dat['Household Measure'] = all_dat['Household Measure'].apply(pathify)
all_dat['Geography Level'] = all_dat['Geography Level'].apply(pathify)
#all_dat['Household Measure'].unique()
#all_dat.head(20)
# endregion

# region
yr = '2020'
csvName = 'observations.csv'
out = Path('out')
out.mkdir(exist_ok=True)
all_dat.drop_duplicates().to_csv(out / (csvName + '.gz'), index = False, compression='gzip')
all_dat.drop_duplicates().to_csv(out / csvName, index = False)

notes = """
Report available here:

https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/882192/fuel-poverty-sub-regional-2020.pdf

Household and fuel poverty numbers at region level come from the national fuel poverty statistics, 2018

Geographies are based on pre-2012 geography codes. More information on geography code changes can be found at the ONS website

Estimates should only be used to look at general trends and identify areas of particularly high or low fuel poverty. See Sub-regional fuel poverty report, 2020

Estimates of fuel poverty at Lower Super Output Area (LSOA) should be treated with caution. The estimates should only be used to look at general trends and identify areas of particularly high or low fuel poverty. They should not be used to identify trends over time within an LSOA, or to compare LSOAs with similar fuel poverty levels due to very small sample sizes and consequent instability in estimates at this level. See Sub-regional fuel poverty report, 2020
"""

scraper.dataset.family = 'towns-high-streets'
scraper.dataset.description = scraper.dataset.description + notes
scraper.dataset.comment = f'Fuel poverty data measured as low income high costs {yr} - Region, Local Authority, LSOA, County and Parliamentary Constituency'
scraper.dataset.title = 'Sub-regional fuel poverty data - England'

dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name)).lower()
scraper.set_base_uri('http://gss-data.org.uk')
scraper.set_dataset_id(dataset_path)

csvw_transform = CSVWMapping()
csvw_transform.set_csv(out / csvName)
csvw_transform.set_mapping(json.load(open('info.json')))
csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
csvw_transform.write(out / f'{csvName}-metadata.json')

with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())
    
# endregion

# region
#scraper.dataset.family = 'towns-high-streets'
#codelistcreation = ['Geography Level','Household Measure'] 
#df = all_dat
#codeclass = CSVCodelists()
#for cl in codelistcreation:
#    if cl in df.columns:
#        df[cl] = df[cl].str.replace("-"," ")
#        df[cl] = df[cl].str.capitalize()
#        codeclass.create_codelists(pd.DataFrame(df[cl]), 'codelists', scraper.dataset.family, Path(os.getcwd()).name.lower())
# endregion


