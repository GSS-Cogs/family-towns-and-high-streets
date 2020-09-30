from gssutils import * 
import json

info = json.load(open('info.json')) 
#etl_title = info["Name"] 
#etl_publisher = info["Producer"][0] 
#print("Publisher: " + etl_publisher) 
#print("Title: " + etl_title) 

scraper = Scraper(seed="info.json")   
scraper 


# +
#Local authority prepayment electricity meters distribution
datasetTitle1 = 'BEIS Electric Prepayment Meter Statistics'
lapem2017_tabs = { tab.name: tab for tab in scraper.distributions[0].as_databaker() }
wanted_lapem2017_tabs = lapem2017_tabs["2017"]

tidied_sheets = []

trace1 = TransformTrace()
df1 = pd.DataFrame()


lapem2017_columns = ["PERIOD", "REGION", "LOCAL AUTHORITY", "LA CODE", "LAU1", "METERS", "MEASURE TYPES"]
trace1.start(datasetTitle1, wanted_lapem2017_tabs, lapem2017_columns, scraper.distributions[0].downloadURL)

lapem2017_region = wanted_lapem2017_tabs.excel_ref("A4").expand(DOWN).is_not_blank()
trace1.REGION("Selected as all non-blank values from cell ref A4 down.")

lapem2017_local_authority = wanted_lapem2017_tabs.excel_ref("B4").expand(DOWN).is_not_blank()
trace1.LOCAL_AUTHORITY("Selected as all non-blank values from cell ref B4 down.")

lapem2017_la_code = wanted_lapem2017_tabs.excel_ref("C4").expand(DOWN).is_not_blank()
trace1.LA_CODE("Selected as all non-blank values from cell ref C4 down.")

lapem2017_lau1 = wanted_lapem2017_tabs.excel_ref("D4").expand(DOWN).is_not_blank()
trace1.LAU1("Selected as all non-blank values from cell ref D4 down.")

lapem2017_meters = wanted_lapem2017_tabs.excel_ref("E4").expand(DOWN).is_not_blank()
trace1.METERS("Selected as all non-blank values from cell ref E4 down.")

lapem2017_kilowatt_hours = wanted_lapem2017_tabs.excel_ref("F3").expand(RIGHT).is_not_blank()
trace1.MEASURE_TYPES("Selected as all non-blank values from cell ref F3 going right/across.")

lapem2017_period = "2017" #Would be better if this was taken directly from the tab title
trace1.PERIOD("Hardcoded but could have been taken from the dataset title or tab title.")

lapem2017_observations = wanted_lapem2017_tabs.excel_ref("F4").expand(RIGHT).expand(DOWN).is_not_blank()

lapem2017_dimensions = [
    HDimConst('PERIOD', lapem2017_period),
    HDim(lapem2017_region, "REGION", CLOSEST, ABOVE),
    HDim(lapem2017_local_authority, "LOCAL AUTHORITY", CLOSEST, ABOVE),
    HDim(lapem2017_la_code, "LA CODE", CLOSEST, ABOVE),
    HDim(lapem2017_lau1, "LAU1", CLOSEST, ABOVE),
    HDim(lapem2017_meters, "METERS", CLOSEST, ABOVE),
    HDim(lapem2017_kilowatt_hours, "MEASURE TYPES", DIRECTLY, ABOVE)
]

lapem2017_tidy_sheet = ConversionSegment(wanted_lapem2017_tabs, lapem2017_dimensions, lapem2017_observations)
trace1.with_preview(lapem2017_tidy_sheet)
#savepreviewhtml(lapem2017_tidy_sheet)
trace1.store("combined_dataframe", lapem2017_tidy_sheet.topandas())

tidied_sheets.append(lapem2017_tidy_sheet.topandas())

final_sheet = pd.concat(tidied_sheets)
    
final_sheet

# +
#MSOA prepayment electricity meters 2017
datasetTitle2 = "Middle Layer Super Output Area (MSOA) prepayment electricity meter consumption, 2017"
msoa2017_tabs = { tab.name: tab for tab in scraper.distributions[1].as_databaker() }
wanted_msoa2017_tabs = msoa2017_tabs["MSOA Domestic Electricity 2017"]

trace2 = TransformTrace()
df2 = pd.DataFrame()

msoa2017_columns = ["PERIOD", "LA NAME", "LA CODE", "MSOA NAME", "MSOA CODE", "METERS", "MEASURE TYPES"]
trace2.start(datasetTitle2, wanted_msoa2017_tabs, msoa2017_columns, scraper.distributions[1].downloadURL)

msoa2017_la_name = wanted_msoa2017_tabs.excel_ref("A4").expand(DOWN).is_not_blank()
trace2.LA_NAME("Selected as all non-blank values from cell ref A4 down.")

msoa2017_la_code = wanted_msoa2017_tabs.excel_ref("B4").expand(DOWN).is_not_blank()
trace2.LA_CODE("Selected as all non-blank values from cell ref B4 down.")

msoa2017_msoa_name = wanted_msoa2017_tabs.excel_ref("C4").expand(DOWN).is_not_blank()
trace2.MSOA_NAME("Selected as all non-blank values from cell ref C4 down.")

msoa2017_msoa_code = wanted_msoa2017_tabs.excel_ref("D4").expand(DOWN).is_not_blank()
trace2.MSOA_CODE("Selected as all non-blank values from cell ref D4 down.")

msoa2017_meters = wanted_msoa2017_tabs.excel_ref("E4").expand(DOWN).is_not_blank()
trace2.METERS("Selected as all non-blank values from cell ref E4 down.")

msoa2017_kilowatt_hours = wanted_msoa2017_tabs.excel_ref("F3").expand(RIGHT).is_not_blank()
trace2.MEASURE_TYPES("Selected as all non-blank values from cell ref F3 going right/across.")

msoa2017_period = "2017"
trace2.PERIOD("Hardcoded but could have been taken from the dataset title or tab title.")

msoa2017_observations = wanted_msoa2017_tabs.excel_ref("F4").expand(RIGHT).expand(DOWN).is_not_blank()

msoa2017_dimensions = [
    HDimConst('PERIOD', msoa2017_period),
    HDim(msoa2017_la_name, "LA NAME", CLOSEST, ABOVE),
    HDim(msoa2017_la_code, "LA CODE", CLOSEST, ABOVE),
    HDim(msoa2017_msoa_name, "MSOA NAME", CLOSEST, ABOVE),
    HDim(msoa2017_msoa_code, "MSOA CODE", CLOSEST, ABOVE),
    HDim(msoa2017_meters, "METERS", CLOSEST, ABOVE),
    HDim(msoa2017_kilowatt_hours, "MEASURE TYPES", DIRECTLY, ABOVE)
]

msoa2017_tidy_sheet = ConversionSegment(wanted_msoa2017_tabs, msoa2017_dimensions, msoa2017_observations)
trace2.with_preview(msoa2017_tidy_sheet)
#savepreviewhtml(msoa2017_tidy_sheet)
trace2.store("combined_dataframe", msoa2017_tidy_sheet.topandas())

tidied_sheets.append(msoa2017_tidy_sheet.topandas())

final_sheet = pd.concat(tidied_sheets)
    
final_sheet
# +
#LSOA prepayment electricity meters 2017
datasetTitle3 = "Lower Layer Super Output Area (LSOA)  prepayment electricity meter consumption, 2017"
lsoa2017_tabs = { tab.name: tab for tab in scraper.distributions[2].as_databaker() }
wanted_lsoa2017_tabs = lsoa2017_tabs["LSOA Dom Elec 2017"]

trace3 = TransformTrace()
df3 = pd.DataFrame()

lsoa2017_columns = ["PERIOD", "LA NAME", "LA CODE", "MSOA NAME", "MSOA CODE", "LSOA NAME", "LSOA CODE", "METERS", "MEASURE TYPES"]
trace3.start(datasetTitle3, wanted_lsoa2017_tabs, lsoa2017_columns, scraper.distributions[2].downloadURL)

lsoa2017_la_name = wanted_lsoa2017_tabs.excel_ref("A3").expand(DOWN).is_not_blank()
trace3.LA_NAME("Selected as all non-blank values from cell ref A3 down.")

lsoa2017_la_code = wanted_lsoa2017_tabs.excel_ref("B3").expand(DOWN).is_not_blank()
trace3.LA_CODE("Selected as all non-blank values from cell ref B3 down.")

lsoa2017_msoa_name = wanted_lsoa2017_tabs.excel_ref("C3").expand(DOWN).is_not_blank()
trace3.MSOA_NAME("Selected as all non-blank values from cell ref C3 down.")

lsoa2017_msoa_code = wanted_lsoa2017_tabs.excel_ref("D3").expand(DOWN).is_not_blank()
trace3.MSOA_CODE("Selected as all non-blank values from cell ref D3 down.")

lsoa2017_lsoa_name = wanted_lsoa2017_tabs.excel_ref("E3").expand(DOWN).is_not_blank()
trace3.LSOA_NAME("Selected as all non-blank values from cell ref E3 down.")

lsoa2017_lsoa_code = wanted_lsoa2017_tabs.excel_ref("F3").expand(DOWN).is_not_blank()
trace3.LSOA_CODE("Selected as all non-blank values from cell ref F3 down.")

lsoa2017_meters = wanted_lsoa2017_tabs.excel_ref("H3").expand(DOWN).is_not_blank()
trace3.METERS("Selected as all non-blank values from cell ref H3 down.")

lsoa2017_kilowatt_hours = wanted_lsoa2017_tabs.excel_ref("2").filter(contains_string("(kWh)")).is_not_blank()
trace3.MEASURE_TYPES("Selected as all non-blank values from cell ref (row) 2 and filtering for all values containing the string '(kWh)'.")

lsoa2017_period = "2017"
trace3.REGION("Hardcoded but could have been taken from the dataset title or tab title.")

lsoa2017_observations = wanted_lsoa2017_tabs.excel_ref("2").filter(contains_string("(kWh)")).fill(DOWN).is_not_blank()

lsoa2017_dimensions = [
    HDimConst('PERIOD', lsoa2017_period),
    HDim(lsoa2017_la_name, "LA NAME", CLOSEST, ABOVE),
    HDim(lsoa2017_la_code, "LA CODE", CLOSEST, ABOVE),
    HDim(lsoa2017_msoa_name, "MSOA NAME", CLOSEST, ABOVE),
    HDim(lsoa2017_msoa_code, "MSOA CODE", CLOSEST, ABOVE),
    HDim(lsoa2017_lsoa_name, "LSOA NAME", CLOSEST, ABOVE),
    HDim(lsoa2017_lsoa_code, "LSOA CODE", CLOSEST, ABOVE),
    HDim(lsoa2017_meters, "METERS", CLOSEST, ABOVE),
    HDim(lsoa2017_kilowatt_hours, "MEASURE TYPES", DIRECTLY, ABOVE)
]

lsoa2017_tidy_sheet = ConversionSegment(wanted_lsoa2017_tabs, lsoa2017_dimensions, lsoa2017_observations)
trace3.with_preview(lsoa2017_tidy_sheet)
#savepreviewhtml(lsoa2017_tidy_sheet)
trace3.store("combined_dataframe", lsoa2017_tidy_sheet.topandas())

tidied_sheets.append(lsoa2017_tidy_sheet.topandas())

final_sheet = pd.concat(tidied_sheets)
    
final_sheet
# +
#Postcode prepayment electricity meters 2017
datasetTitle4 = "Postcode level prepayment electric meter consumption, 2017"
plpem2017_tabs = { tab.name: tab for tab in scraper.distributions[3].as_databaker() }
wanted_plpem2017_tabs = plpem2017_tabs["Postcode-prepayment-electricity"]

trace4 = TransformTrace()
df4 = pd.DataFrame()

plpem2017_columns = ["PERIOD", "POSTCODES", "METERS", "MEASURE TYPES"]
trace4.start(datasetTitle4, wanted_plpem2017_tabs, plpem2017_columns, scraper.distributions[3].downloadURL)

plpem2017_postcodes = wanted_plpem2017_tabs.excel_ref("A2").expand(DOWN).is_not_blank()
trace4.POSTCODES("Selected as all non-blank values from cell ref A2 down.")

plpem2017_meters = wanted_plpem2017_tabs.excel_ref("B2").expand(DOWN).is_not_blank()
trace4.METERS("Selected as all non-blank values from cell ref B2 down.")

plpem2017_kilowatt_hours = wanted_plpem2017_tabs.excel_ref("C1").expand(RIGHT).is_not_blank()
trace4.MEASURE_TYPES("Selected as all non-blank values from cell ref C1 going right/across.")

plpem2017_period = "2017"
trace4.PERIOD("Hardcoded but could have been taken from the dataset title.")

plpem2017_observations = wanted_plpem2017_tabs.excel_ref("C2").expand(RIGHT).expand(DOWN).is_not_blank()

plpem2017_dimensions = [
    HDimConst('PERIOD', plpem2017_period),
    HDim(plpem2017_postcodes, "POSTCODES", CLOSEST, ABOVE),
    HDim(plpem2017_meters, "METERS", CLOSEST, ABOVE),
    HDim(plpem2017_kilowatt_hours, "MEASURE TYPES", DIRECTLY, ABOVE),
]

plpem2017_tidy_sheet = ConversionSegment(wanted_plpem2017_tabs, plpem2017_dimensions, plpem2017_observations)
trace4.with_preview(plpem2017_tidy_sheet)
#savepreviewhtml(plpem2017_tidy_sheet)
trace4.store("combined_dataframe", plpem2017_tidy_sheet.topandas())

tidied_sheets.append(plpem2017_tidy_sheet.topandas())

final_sheet = pd.concat(tidied_sheets)
    
final_sheet
# -



