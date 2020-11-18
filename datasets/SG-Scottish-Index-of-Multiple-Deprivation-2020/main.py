# -*- coding: utf-8 -*-
from gssutils import * 
import json 
import requests
import os
from urllib.parse import urljoin

# +
# Make sure the dataURL and Unit values are correctb to begin with
with open("info.json", "r") as jsonFile:
    data = json.load(jsonFile)

data["dataURL"] = "https://www.gov.scot/binaries/content/documents/govscot/publications/statistics/2020/01/scottish-index-of-multiple-deprivation-2020-ranks-and-domain-ranks/documents/scottish-index-of-multiple-deprivation-2020-ranks-and-domain-ranks/scottish-index-of-multiple-deprivation-2020-ranks-and-domain-ranks/govscot%3Adocument/SIMD%2B2020v2%2B-%2Branks.xlsx"
data["transform"]["columns"]["Value"]["unit"] = "http://gss-data.org.uk/def/concept/measurement-units/ranks"

with open("info.json", "w") as jsonFile:
    json.dump(data, jsonFile)
# -

trace = TransformTrace()
tidied_sheets = {}
scraper = Scraper(seed="info.json")   
scraper.distributions[0].title = "Scottish Index of Multiple Deprivation 2020"
scraper 

# +
#tabs = { tab for tab in scraper.distributions[0].as_databaker() }
#len(tabs)
# -


"""
for tab in tabs:
    if 'SIMD 2020v2 ranks' == tab.name:
        table = tab
        
columns=["Data Zone", "Intermediate Zone", "Council Area", "Total Population", "Working Age Population", "Rank Type"]
trace.start(scraper.distributions[0].title, table, columns, scraper.distributions[0].downloadURL)

data_zone = table.filter(contains_string('Data_Zone')).shift(0,1).expand(DOWN).is_not_blank()
trace.Data_Zone("Selected as all non blank values from cell ref A2 down")

intermediate_zone = table.filter(contains_string('Intermediate_Zone')).shift(0,1).expand(DOWN).is_not_blank()
trace.Intermediate_Zone("Selected as all non blank values from cell ref B2 down")

council_area = table.filter(contains_string('Council_area')).shift(0,1).expand(DOWN).is_not_blank()
trace.Council_Area("Selected as all non blank values from cell ref C2 down")

total_population = table.filter(contains_string('Total_population')).shift(0,1).expand(DOWN).is_not_blank()
trace.Total_Population("Selected as all non blank values from cell ref D2 down")

working_age_population = table.filter(contains_string('Working_age_population')).shift(0,1).expand(DOWN).is_not_blank()
trace.Working_Age_Population("Selected as all non blank values from cell ref E2 down")

rank_type = table.filter(contains_string('SIMD2020v2_Rank')).expand(RIGHT).is_not_blank()
trace.Rank_Type("Selected as all non blank values from cell ref F3 right")
observation = table.filter(contains_string('SIMD2020v2_Rank')).shift(0,1).expand(DOWN).expand(RIGHT).is_not_blank()



dimensions = [
    HDim(data_zone, 'Data Zone', DIRECTLY, LEFT),
    HDim(intermediate_zone, 'Intermediate Zone', DIRECTLY, LEFT),
    HDim(council_area, 'Council Area', DIRECTLY, LEFT),
    HDim(total_population, 'Total Population', DIRECTLY, LEFT),
    HDim(working_age_population, 'Working Age Population', DIRECTLY, LEFT),
    HDim(rank_type, 'Rank Type', DIRECTLY, ABOVE)
]


tidy_sheet = ConversionSegment(table, dimensions, observation)
# savepreviewhtml(tidy_sheet)
trace.with_preview(tidy_sheet)

trace.store("ranks_dataframe", tidy_sheet.topandas())
tidied_sheets["SIMD 2020v2 ranks"] = tidy_sheet.topandas()
"""
"""
url = 'https://www.gov.scot/binaries/content/documents/govscot/publications/statistics/2020/01/scottish-index-of-multiple-deprivation-2020-indicator-data/documents/simd_2020_indicators/simd_2020_indicators/govscot%3Adocument/SIMD%2B2020v2%2B-%2Bindicators.xlsx'
r = requests.get(url, allow_redirects=True)

open('indicators.xls', 'wb').write(r.content)
tabs = loadxlstabs('indicators.xls')

trace = TransformTrace()
"""
"""
for tab in tabs:
    if tab.name == 'Data':
        table = tab

title = scraper.distributions[0].title + "Indicators"
columns=["Data Zone", "Intermediate Zone", "Council Area", "Indicator Type"]
trace.start(title, table, columns, url)

data_zone = table.filter(contains_string('Data_Zone')).shift(0,1).expand(DOWN).is_not_blank()
trace.Data_Zone("Selected as all non blank values from cell ref A2 down")

intermediate_zone = table.filter(contains_string('Intermediate_Zone')).shift(0,1).expand(DOWN).is_not_blank()
trace.Intermediate_Zone("Selected as all non blank values from cell ref B2 down")

council_area = table.filter(contains_string('Council_area')).shift(0,1).expand(DOWN).is_not_blank()
trace.Council_Area("Selected as all non blank values from cell ref C2 down")

observation = table.filter(contains_string('Total_population')).shift(0,1).expand(DOWN).expand(RIGHT).is_not_blank()

indicator = table.filter(contains_string('Total_population')).expand(RIGHT).is_not_blank()
trace.Council_Area("Selected as all non blank values from cell ref D1 right")


dimensions = [
    HDim(data_zone, 'Data Zone', DIRECTLY, LEFT),
    HDim(intermediate_zone, 'Intermediate Zone', DIRECTLY, LEFT),
    HDim(council_area, 'Council Area', DIRECTLY, LEFT),
    HDim(indicator, 'Indicator Type', DIRECTLY, ABOVE)
]

tidy_sheet = ConversionSegment(table, dimensions, observation)
# savepreviewhtml(tidy_sheet)
trace.with_preview(tidy_sheet)

trace.store("indicators_dataframe", tidy_sheet.topandas())
tidied_sheets["Data"] = tidy_sheet.topandas()
"""



# Sheet names
sn = ['SIMD 2020v2 ranks','Data']
# Output filenames
fn = ['ranks-observations.csv','indicators-observations.csv']
# Comments
co = [
    '2020 Rank data relating to the Scottish Index of Multiple Deprivation - a tool for identifying areas with relatively high levels of deprivation.',
    '2020 Indicator data relating to the Scottish Index of Multiple Deprivation - a tool for identifying areas with relatively high levels of deprivation.'
]
# Description
de = [
    'SIMD ranks data zones from most deprived (ranked 1) to least deprived (ranked 6,976). People using SIMD will often focus on the data zones below a certain rank, for example, the 5%, 10%, 15% or 20% most deprived data zones in Scotland. Link to information booklet: https://www.gov.scot/publications/scottish-index-multiple-deprivation-2020/',
    'SIMD ranks data zones from most deprived (ranked 1) to least deprived (ranked 6,976). People using SIMD will often focus on the data zones below a certain rank, for example, the 5%, 10%, 15% or 20% most deprived data zones in Scotland. Link to information booklet: https://www.gov.scot/publications/scottish-index-multiple-deprivation-2020/'
]
# Title
ti = [
    'Scottish Index of Multiple Deprivation - Ranks',
    'Scottish Index of Multiple Deprivation - Indicators'
]
# Paths
pa = ['/ranks', '/indicators']

try:
    i = 0
    tab = scraper.distributions[0].as_pandas(sheet_name=sn[i])

    tbls = []
    tab.columns = tab.columns.str.replace('_',' ')
    for x in range(5, len(tab.columns)):
        cols = [0,1,2,3,4,x]
        dat = tab.iloc[:, cols]
        dat['Deprivation Rank'] = dat.columns[5]
        dat = dat.rename(columns={dat.columns[5]:'Value'})
        dat = dat[[dat.columns[0],dat.columns[6],dat.columns[3],dat.columns[4],dat.columns[5]]]
        tbls.append(dat)

        k = 0
    for t in tbls:
        if k == 0:
            joined_dat = t
        else:
            joined_dat = pd.concat([joined_dat,t])   
        k = k + 1

    joined_dat['Deprivation Rank'] = joined_dat['Deprivation Rank'].apply(pathify)

    csvName = fn[i]
    out = Path('out')
    out.mkdir(exist_ok=True)
    joined_dat[:5].drop_duplicates().to_csv(out / csvName, index = False)
    joined_dat.drop_duplicates().to_csv(out / (csvName + '.gz'), index = False, compression='gzip')
    
    scraper.dataset.family = 'towns-high-streets'
    scraper.dataset.description = scraper.dataset.description + '\n' + de[i]
    scraper.dataset.comment = co[i]
    scraper.dataset.title = ti[i]

    dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name)).lower() + pa[i]
    scraper.set_base_uri('http://gss-data.org.uk')
    scraper.set_dataset_id(dataset_path)

    csvw_transform = CSVWMapping()
    csvw_transform.set_csv(out / csvName)
    csvw_transform.set_mapping(json.load(open('info.json')))
    csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
    csvw_transform.write(out / f'{csvName}-metadata.json')

    # Remove subset of data
    (out / csvName).unlink()

    with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
        metadata.write(scraper.generate_trig())
except Exception as s:
    print(str(s))

del joined_dat

# +
# need to change the dataURLa to the indicators one
with open("info.json", "r") as jsonFile:
    data = json.load(jsonFile)

data["dataURL"] = "https://www.gov.scot/binaries/content/documents/govscot/publications/statistics/2020/01/scottish-index-of-multiple-deprivation-2020-indicator-data/documents/simd_2020_indicators/simd_2020_indicators/govscot%3Adocument/SIMD%2B2020v2%2B-%2Bindicators.xlsx"
data["transform"]["columns"]["Value"]["unit"] = "http://gss-data.org.uk/def/concept/measurement-units/indicators"

with open("info.json", "w") as jsonFile:
    json.dump(data, jsonFile)
    
trace = TransformTrace()
tidied_sheets = {}
scraper = Scraper(seed="info.json")   
scraper.distributions[0].title = "Scottish Index of Multiple Deprivation 2020"
scraper 

# +
try:
    i = 1
    tab = scraper.distributions[0].as_pandas(sheet_name=sn[i])

    tbls = []
    tab.columns = tab.columns.str.replace('_',' ')
    for x in range(5, len(tab.columns)):
        cols = [0,1,2,3,4,x]
        dat = tab.iloc[:, cols]
        dat['Deprivation Indicator'] = dat.columns[5]
        dat = dat.rename(columns={dat.columns[5]:'Value'})
        dat = dat[[dat.columns[0],dat.columns[6],dat.columns[3],dat.columns[4],dat.columns[5]]]
        tbls.append(dat)

        k = 0
    for t in tbls:
        if k == 0:
            joined_dat = t
        else:
            joined_dat = pd.concat([joined_dat,t])   
        k = k + 1

    joined_dat["Indicator Type"] = joined_dat["Deprivation Indicator"]
    joined_dat["High Level Domain"] = joined_dat["Deprivation Indicator"]

    joined_dat['Indicator Type'] = joined_dat['Indicator Type'].replace({
        'Income rate':'Percentage',
        'Income count':'Count',
        'Employment rate':'Percentage',
        'Employment count':'Count',
        'CIF':'Standardised ratio',
        'ALCOHOL':'Standardised ratio',
        'DRUG':'Standardised ratio',
        'SMR':'Standardised ratio',
        'DEPRESS':'Percentage',
        'LBWT':'Percentage',
        'EMERG':'Standardised ratio',
        'Attendance':'Percentage',
        'Attainment':'Score',
        'no qualifications':'Standardised ratio',
        'not participating':'Percentage',
        'University':'Percentage',
        'drive petrol':'Time (minutes)',
        'drive GP':'Time (minutes)',
        'drive post':'Time (minutes)',
        'drive primary':'Time (minutes)',
        'drive retail':'Time (minutes)',
        'drive secondary':'Time (minutes)',
        'PT GP':'Time (minutes)',
        'PT post':'Time (minutes)',
        'PT retail':'Time (minutes)',
        'Broadband':'Percentage',
        'crime count':'Count',
        'crime rate':'Rate per 10,000 population',
        'overcrowded count':'Count',
        'nocentralheat count':'Count',
        'overcrowded rate':'Percentage',
        'nocentralheat rate':'Percentage'
     })

    joined_dat['High Level Domain'] = joined_dat['High Level Domain'].replace({
        'Income rate':'Income',
        'Income count':'Income',
        'Employment rate':'Employment',
        'Employment count':'Employment',
        'CIF':'Health',
        'ALCOHOL':'Health',
        'DRUG':'Health',
        'SMR':'Health',
        'DEPRESS':'Health',
        'LBWT':'Health',
        'EMERG':'Health',
        'Attendance':'Education, Skills and Training',
        'Attainment':'Education, Skills and Training',
        'no qualifications':'Education, Skills and Training',
        'not participating':'Education, Skills and Training',
        'University':'Education, Skills and Training',
        'drive petrol':'Geographic Access to Services',
        'drive GP':'Geographic Access to Services',
        'drive post':'Geographic Access to Services',
        'drive primary':'Geographic Access to Services',
        'drive retail':'Geographic Access to Services',
        'drive secondary':'Geographic Access to Services',
        'PT GP':'Geographic Access to Services',
        'PT post':'Geographic Access to Services',
        'PT retail':'Geographic Access to Services',
        'Broadband':'Geographic Access to Services',
        'crime count':'Crime',
        'crime rate':'Crime',
        'overcrowded count':'Housing',
        'nocentralheat count':'Housing',
        'overcrowded rate':'Housing',
        'nocentralheat rate':'Housing'
     })
    joined_dat['Deprivation Indicator'] = joined_dat['Deprivation Indicator'].apply(pathify)
    joined_dat['Indicator Type'] = joined_dat['Indicator Type'].apply(pathify)
    joined_dat['High Level Domain'] = joined_dat['High Level Domain'].apply(pathify)

    joined_dat['Marker'] = ''
    joined_dat['Marker'][joined_dat['Value'] == '*'] = 'suppressed-or-population-zero'
    joined_dat['Value'][joined_dat['Value'] == '*'] = 0
    
    joined_dat = joined_dat[['Data Zone','Deprivation Indicator','Indicator Type','High Level Domain','Total population','Working age population','Marker','Value']]
    
    csvName = fn[i]
    out = Path('out')
    out.mkdir(exist_ok=True)
    joined_dat[:5].drop_duplicates().to_csv(out / csvName, index = False)
    joined_dat.drop_duplicates().to_csv(out / (csvName + '.gz'), index = False, compression='gzip')
    
    scraper.dataset.family = 'towns-high-streets'
    scraper.dataset.description = scraper.dataset.description + '\n' + de[i]
    scraper.dataset.comment = co[i]
    scraper.dataset.title = ti[i]

    dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name)).lower() + pa[i]
    scraper.set_base_uri('http://gss-data.org.uk')
    scraper.set_dataset_id(dataset_path)


    csvw_transform = CSVWMapping()
    csvw_transform.set_csv(out / csvName)
    csvw_transform.set_mapping(json.load(open('info.json')))
    csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
    csvw_transform.write(out / f'{csvName}-metadata.json')

    # Remove subset of data
    (out / csvName).unlink()

    with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
        metadata.write(scraper.generate_trig())

except Exception as s:
    print(str(s))

# +
#joined_dat.head(20)

# +
# Need to change the dataURL back to the RANK URL ready for the next run
with open("info.json", "r") as jsonFile:
    data = json.load(jsonFile)

data["dataURL"] = "https://www.gov.scot/binaries/content/documents/govscot/publications/statistics/2020/01/scottish-index-of-multiple-deprivation-2020-ranks-and-domain-ranks/documents/scottish-index-of-multiple-deprivation-2020-ranks-and-domain-ranks/scottish-index-of-multiple-deprivation-2020-ranks-and-domain-ranks/govscot%3Adocument/SIMD%2B2020v2%2B-%2Branks.xlsx"
data["transform"]["columns"]["Value"]["unit"] = "http://gss-data.org.uk/def/concept/measurement-units/ranks"

with open("info.json", "w") as jsonFile:
    json.dump(data, jsonFile)

# +
#scraper.dataset.family = 'towns-high-streets'
#codelistcreation = ['Deprivation Indicator'] 
#df = joined_dat
#codeclass = CSVCodelists()
#for cl in codelistcreation:
#    if cl in df.columns:
#        df[cl] = df[cl].str.replace("-"," ")
#        df[cl] = df[cl].str.capitalize()
#        codeclass.create_codelists(pd.DataFrame(df[cl]), 'codelists', scraper.dataset.family, Path(os.getcwd()).name.lower())
# -








