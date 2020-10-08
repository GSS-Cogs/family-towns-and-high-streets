# -*- coding: utf-8 -*-
from gssutils import * 
import json 
import requests
import os
from urllib.parse import urljoin

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
sn = ['SIMD 2020v2 ranks','']
# Output filenames
fn = ['ranks-observations.csv','']
# Comments
co = [
    '2020 Rank data relating to the Scottish Index of Multiple Deprivation - a tool for identifying areas with relatively high levels of deprivation.',
    ''
]
# Description
de = [
    'SIMD ranks data zones from most deprived (ranked 1) to least deprived (ranked 6,976). People using SIMD will often focus on the data zones below a certain rank, for example, the 5%, 10%, 15% or 20% most deprived data zones in Scotland.',
    ''
]
# Title
ti = [
    'Scottish Index of Multiple Deprivation - Ranks', 'Scottish Index of Multiple Deprivation - Indicators'
]
# Paths
pa = ['/ranks', '/indicators']

#### ONLY OUTPUT RANKS AT THE MOMENT - BREAK AT END OF LOOP
i = 0
for s in sn:
    tab = scraper.distributions[0].as_pandas(sheet_name=s)
    
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
    joined_dat.drop_duplicates().to_csv(out / csvName, index = False)

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

    with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
        metadata.write(scraper.generate_trig())
     
    i = i + 1
    break

joined_dat.head(10)
#joined_dat['Value'].unique()

# +
#codelistcreation = ['Deprivation Rank'] 
#df = joined_dat
#codeclass = CSVCodelists()
#for cl in codelistcreation:
#    if cl in df.columns:
#        df[cl] = df[cl].str.replace("-"," ")
#        df[cl] = df[cl].str.capitalize()
#        codeclass.create_codelists(pd.DataFrame(df[cl]), 'codelists', scraper.dataset.family, Path(os.getcwd()).name.lower() + <change Depending on Dataset>)
# -


