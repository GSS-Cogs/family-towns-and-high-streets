from gssutils import * 
import json 
import requests

trace = TransformTrace()
tidied_sheets = {}
scraper = Scraper(seed="info.json")   
scraper.distributions[0].title = "Scottish Index of Multiple Deprivation 2020"
scraper 

tabs = { tab for tab in scraper.distributions[0].as_databaker() }
len(tabs)


# +
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
# +
url = 'https://www.gov.scot/binaries/content/documents/govscot/publications/statistics/2020/01/scottish-index-of-multiple-deprivation-2020-indicator-data/documents/simd_2020_indicators/simd_2020_indicators/govscot%3Adocument/SIMD%2B2020v2%2B-%2Bindicators.xlsx'
r = requests.get(url, allow_redirects=True)

open('indicators.xls', 'wb').write(r.content)
tabs = loadxlstabs('indicators.xls')

trace = TransformTrace()
# +
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
# -


