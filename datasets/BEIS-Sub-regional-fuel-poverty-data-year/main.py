from gssutils import * 
import json 


scraper = Scraper(seed="info.json")   
scraper 

scraper.select_dataset(title=lambda t: 'data 2020' in t)
scraper


tabs = { tab for tab in scraper.distributions[0].as_databaker() }
# list(tabs)

df = pd.DataFrame()
tidied_sheets = {}

# Place each table in separate variables 
for tab in tabs:
    if tab.name == 'Table 1':
        tab1 = tab
    elif tab.name == 'Table 2':
        tab2 = tab
    elif tab.name == 'Table 2':
        tab2 = tab
    elif tab.name == 'Table 3':
        tab3 = tab
    elif tab.name == 'Table 4':
        tab4 = tab
    elif tab.name == 'Table 5':
        tab5 = tab
    

# +
# Processing each table starting from table 1 
region = tab1.excel_ref('A4').expand(DOWN).is_not_blank()
remove_from_region = tab1.filter("1 Household and fuel poverty numbers at region level come from the national fuel poverty statistics, 2018:").assert_one().expand(DOWN).is_not_blank()
region = region - remove_from_region
# region = tab1.filter(contains_string('Region')).shift(0,1).expand(DOWN).is_not_blank()
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


