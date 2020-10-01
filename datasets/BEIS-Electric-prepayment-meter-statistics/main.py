from gssutils import * 
import json
import math

info = json.load(open('info.json')) 
#etl_title = info["Name"] 
#etl_publisher = info["Producer"][0] 
#print("Publisher: " + etl_publisher) 
#print("Title: " + etl_title) 

scraper = Scraper(seed="info.json")   
scraper 


tidied_sheets = []
trace = TransformTrace()
df = pd.DataFrame()

# +
# #### Distribution 1 : Local authority prepayment electricity meters distribution
#KEY: lapem2017 = Local authority prepayment electricity meters 2017

datasetTitle1 = 'BEIS Electric Prepayment Meter Statistics'
lapem2017_tabs = { tab.name: tab for tab in scraper.distributions[0].as_databaker() }
wanted_lapem2017_tabs = lapem2017_tabs["2017"]


lapem2017_columns = ["PERIOD", "REGION", "LOCAL AUTHORITY", "LA CODE", "LAU1", "METERS", "MEASURE TYPES", "UNIT"]
trace.start(datasetTitle1, wanted_lapem2017_tabs, lapem2017_columns, scraper.distributions[0].downloadURL)

lapem2017_region = wanted_lapem2017_tabs.excel_ref("A4").expand(DOWN).is_not_blank()
trace.REGION("Selected as all non-blank values from cell ref A4 down.")

lapem2017_local_authority = wanted_lapem2017_tabs.excel_ref("B4").expand(DOWN).is_not_blank()
trace.LOCAL_AUTHORITY("Selected as all non-blank values from cell ref B4 down.")

lapem2017_la_code = wanted_lapem2017_tabs.excel_ref("C4").expand(DOWN).is_not_blank()
trace.LA_CODE("Selected as all non-blank values from cell ref C4 down.")

lapem2017_lau1 = wanted_lapem2017_tabs.excel_ref("D4").expand(DOWN).is_not_blank()
trace.LAU1("Selected as all non-blank values from cell ref D4 down.")

lapem2017_meters = wanted_lapem2017_tabs.excel_ref("E4").expand(DOWN).is_not_blank()
trace.METERS("Selected as all non-blank values from cell ref E4 down.")

lapem2017_kilowatt_hours = wanted_lapem2017_tabs.excel_ref("F3").expand(RIGHT).is_not_blank()
trace.MEASURE_TYPES("Selected as all non-blank values from cell ref F3 going right/across.")

lapem2017_period = "2017" #Would be better if this was taken directly from the tab title
trace.PERIOD("Hardcoded but could have been taken from the dataset title or tab title.")

lapem2017_unit = "kWh"
trace.UNIT("Hardcoded but could have been taken from the measure type's heading.")

lapem2017_observations = wanted_lapem2017_tabs.excel_ref("F4").expand(RIGHT).expand(DOWN).is_not_blank()

lapem2017_dimensions = [
    HDimConst("Period", lapem2017_period),
    HDim(lapem2017_region, "Region", CLOSEST, ABOVE),
    HDim(lapem2017_local_authority, "Local Authority", CLOSEST, ABOVE),
    HDim(lapem2017_la_code, "LA Code", CLOSEST, ABOVE),
    HDim(lapem2017_lau1, "LAU1", CLOSEST, ABOVE),
    HDim(lapem2017_meters, "Meters", CLOSEST, ABOVE),
    HDim(lapem2017_kilowatt_hours, "Measure Types", DIRECTLY, ABOVE),
    HDimConst("Unit", lapem2017_unit)
]

lapem2017_tidy_sheet = ConversionSegment(wanted_lapem2017_tabs, lapem2017_dimensions, lapem2017_observations)
trace.with_preview(lapem2017_tidy_sheet)
#savepreviewhtml(lapem2017_tidy_sheet)
trace.store("combined_dataframe_1", lapem2017_tidy_sheet.topandas())

df = trace.combine_and_trace(datasetTitle1, "combined_dataframe_1")
df.rename(columns={'OBS' : 'Value'}, inplace=True)
trace.output()

tidy_d1 = df[["Period", "Region", "Local Authority", "LA Code", "LAU1", "Meters", "Measure Types", "Unit", "Value"]]
tidy_d1


# +
# #### DISTRIBUTION 2 : MSOA prepayment electricity meters 2017
datasetTitle2 = "Middle Layer Super Output Area (MSOA) prepayment electricity meter consumption, 2017"
msoa2017_tabs = { tab.name: tab for tab in scraper.distributions[1].as_databaker() }
wanted_msoa2017_tabs = msoa2017_tabs["MSOA Domestic Electricity 2017"]


msoa2017_columns = ["PERIOD", "LA NAME", "LA CODE", "MSOA NAME", "MSOA CODE", "METERS", "MEASURE TYPES", "UNIT"]
trace.start(datasetTitle2, wanted_msoa2017_tabs, msoa2017_columns, scraper.distributions[1].downloadURL)

tab_length = len(wanted_msoa2017_tabs.excel_ref('A')) # number of rows of data
batch_number = 10 # iterates over this many rows at a time
number_of_iterations = math.ceil(tab_length/batch_number) # databaking will iterate this many times
            
for i in range(0, number_of_iterations):
    Min = str(4 + batch_number * i)  # data starts on row 4
    Max = str(int(Min) + batch_number - 1)
    
    msoa2017_la_name = wanted_msoa2017_tabs.excel_ref("A"+Min+":A"+Max).is_not_blank()
    
    msoa2017_la_code = wanted_msoa2017_tabs.excel_ref("B"+Min+":B"+Max).is_not_blank()
    
    msoa2017_msoa_name = wanted_msoa2017_tabs.excel_ref("C"+Min+":C"+Max).is_not_blank()
    
    msoa2017_msoa_code = wanted_msoa2017_tabs.excel_ref("D"+Min+":D"+Max).is_not_blank()
    
    msoa2017_meters = wanted_msoa2017_tabs.excel_ref("E"+Min+":E"+Max).is_not_blank()
    
    msoa2017_kilowatt_hours = wanted_msoa2017_tabs.excel_ref("F3").expand(RIGHT).is_not_blank()

    msoa2017_period = "2017"
    
    msoa2017_unit = "kWh"    

    msoa2017_observations = wanted_msoa2017_tabs.excel_ref("F4").expand(RIGHT).expand(DOWN).is_not_blank()

    msoa2017_dimensions = [
        HDimConst('Period', msoa2017_period),
        HDim(msoa2017_la_name, "LA Name", CLOSEST, ABOVE),
        HDim(msoa2017_la_code, "LA Code", CLOSEST, ABOVE),
        HDim(msoa2017_msoa_name, "MSOA Name", CLOSEST, ABOVE),
        HDim(msoa2017_msoa_code, "MSOA Code", CLOSEST, ABOVE),
        HDim(msoa2017_meters, "Meters", CLOSEST, ABOVE),
        HDim(msoa2017_kilowatt_hours, "Measure Types", DIRECTLY, ABOVE),
        HDimConst("Unit", msoa2017_unit)
    ]

trace.LA_NAME("Selected as all non-blank values from cell ref A4 down.")
trace.LA_CODE("Selected as all non-blank values from cell ref B4 down.")
trace.MSOA_NAME("Selected as all non-blank values from cell ref C4 down.")
trace.MSOA_CODE("Selected as all non-blank values from cell ref D4 down.")
trace.METERS("Selected as all non-blank values from cell ref E4 down.")
trace.MEASURE_TYPES("Selected as all non-blank values from cell ref F3 going right/across.")
trace.PERIOD("Hardcoded but could have been taken from the dataset title or tab title.")
trace.UNIT("Hardcoded but could have been taken from the measure type's heading.")
    
msoa2017_tidy_sheet = ConversionSegment(wanted_msoa2017_tabs, msoa2017_dimensions, msoa2017_observations)
trace.with_preview(msoa2017_tidy_sheet)
#savepreviewhtml(msoa2017_tidy_sheet)
trace.store("combined_dataframe_2", msoa2017_tidy_sheet.topandas())

df = trace.combine_and_trace(datasetTitle2, "combined_dataframe_2")
df.rename(columns={'OBS' : 'Value'}, inplace=True)
trace.output()

tidy_d2 = df[["Period", "LA Name", "LA Code", "MSOA Name", "MSOA Code", "Meters", "Measure Types", "Unit", "Value"]]
tidy_d2


# +
# #### DISTRIBUTION 3 : LSOA prepayment electricity meters 2017

datasetTitle3 = "Lower Layer Super Output Area (LSOA)  prepayment electricity meter consumption, 2017"
lsoa2017_tabs = { tab.name: tab for tab in scraper.distributions[2].as_databaker() }
wanted_lsoa2017_tabs = lsoa2017_tabs["LSOA Dom Elec 2017"]


lsoa2017_columns = ["PERIOD", "LA NAME", "LA CODE", "MSOA NAME", "MSOA CODE", "LSOA NAME", "LSOA CODE", "METERS", "MEASURE TYPES", "UNIT"]
trace.start(datasetTitle3, wanted_lsoa2017_tabs, lsoa2017_columns, scraper.distributions[2].downloadURL)

tab_length = len(wanted_lsoa2017_tabs.excel_ref('A')) # number of rows of data
batch_number = 10 # iterates over this many rows at a time
number_of_iterations = math.ceil(tab_length/batch_number) # databaking will iterate this many times

for i in range(0, number_of_iterations):
    Min = str(3 + batch_number * i)  # data starts on row 3
    Max = str(int(Min) + batch_number - 1)

    lsoa2017_la_name = wanted_lsoa2017_tabs.excel_ref("A"+Min+":A"+Max).is_not_blank()
    
    lsoa2017_la_code = wanted_lsoa2017_tabs.excel_ref("B"+Min+":B"+Max).is_not_blank()
    
    lsoa2017_msoa_name = wanted_lsoa2017_tabs.excel_ref("C"+Min+":C"+Max).is_not_blank()
    
    lsoa2017_msoa_code = wanted_lsoa2017_tabs.excel_ref("D"+Min+":D"+Max).is_not_blank()
    
    lsoa2017_lsoa_name = wanted_lsoa2017_tabs.excel_ref("E"+Min+":E"+Max).is_not_blank()
    
    lsoa2017_lsoa_code = wanted_lsoa2017_tabs.excel_ref("F"+Min+":F"+Max).is_not_blank()
    
    lsoa2017_meters = wanted_lsoa2017_tabs.excel_ref("H"+Min+":H"+Max).is_not_blank()
    
    lsoa2017_kilowatt_hours = wanted_lsoa2017_tabs.excel_ref("2").filter(contains_string("(kWh)")).is_not_blank()
    
    lsoa2017_period = "2017"
    
    lsoa2017_unit = "kWh"
    
    lsoa2017_observations = wanted_lsoa2017_tabs.excel_ref("2").filter(contains_string("(kWh)")).fill(DOWN).is_not_blank()

    lsoa2017_dimensions = [
        HDimConst("Period", lsoa2017_period),
        HDim(lsoa2017_la_name, "LA Name", CLOSEST, ABOVE),
        HDim(lsoa2017_la_code, "LA Code", CLOSEST, ABOVE),
        HDim(lsoa2017_msoa_name, "MSOA Name", CLOSEST, ABOVE),
        HDim(lsoa2017_msoa_code, "MSOA Code", CLOSEST, ABOVE),
        HDim(lsoa2017_lsoa_name, "LSOA Name", CLOSEST, ABOVE),
        HDim(lsoa2017_lsoa_code, "LSOA Code", CLOSEST, ABOVE),
        HDim(lsoa2017_meters, "Meters", CLOSEST, ABOVE),
        HDim(lsoa2017_kilowatt_hours, "Measure Types", DIRECTLY, ABOVE),
        HDimConst("Unit", lsoa2017_unit)
    ]

trace.LA_NAME("Selected as all non-blank values from cell ref A3 down.")
trace.LA_CODE("Selected as all non-blank values from cell ref B3 down.")
trace.MSOA_NAME("Selected as all non-blank values from cell ref C3 down.")    
trace.MSOA_CODE("Selected as all non-blank values from cell ref D3 down.")
trace.LSOA_NAME("Selected as all non-blank values from cell ref E3 down.")
trace.LSOA_CODE("Selected as all non-blank values from cell ref F3 down.")
trace.METERS("Selected as all non-blank values from cell ref H3 down.")
trace.MEASURE_TYPES("Selected as all non-blank values from cell ref (row) 2 and filtering for all values containing the string '(kWh)'.")
trace.PERIOD("Hardcoded but could have been taken from the dataset title or tab title.")
trace.UNIT("Hardcoded but could have been taken from the measure type's heading.")

lsoa2017_tidy_sheet = ConversionSegment(wanted_lsoa2017_tabs, lsoa2017_dimensions, lsoa2017_observations)
trace.with_preview(lsoa2017_tidy_sheet)
#savepreviewhtml(lsoa2017_tidy_sheet)
trace.store("combined_dataframe_3", lsoa2017_tidy_sheet.topandas())

df = trace.combine_and_trace(datasetTitle3, "combined_dataframe_3")
df.rename(columns={'OBS' : 'Value'}, inplace=True)
trace.output()

tidy_d3 = df[["Period", "LA Name", "LA Code", "MSOA Name", "LSOA Code", "Meters", "Measure Types", "Unit", "Value"]]
tidy_d3

# +
# #### DISTRIBUTION 4 : Postcode prepayment electricity meters 2017
#KEY: plpem2017 = Postcode prepayment electricity meters 2017

datasetTitle4 = "Postcode level prepayment electric meter consumption, 2017"
plpem2017_tabs = { tab.name: tab for tab in scraper.distributions[3].as_databaker() }
wanted_plpem2017_tabs = plpem2017_tabs["Postcode-prepayment-electricity"]


plpem2017_columns = ["PERIOD", "POSTCODES", "METERS", "MEASURE TYPES", "UNIT"]
trace.start(datasetTitle4, wanted_plpem2017_tabs, plpem2017_columns, scraper.distributions[3].downloadURL)

tab_length = len(wanted_plpem2017_tabs.excel_ref('A')) # number of rows of data
batch_number = 10 # iterates over this many rows at a time
number_of_iterations = math.ceil(tab_length/batch_number) # databaking will iterate this many times
            
for i in range(0, number_of_iterations):
    Min = str(2 + batch_number * i)  # data starts on row 2
    Max = str(int(Min) + batch_number - 1)

    plpem2017_postcodes = wanted_plpem2017_tabs.excel_ref("A"+Min+":A"+Max).is_not_blank()
    
    plpem2017_meters = wanted_plpem2017_tabs.excel_ref("B"+Min+":B"+Max).is_not_blank()
    
    plpem2017_kilowatt_hours = wanted_plpem2017_tabs.excel_ref("C1").expand(RIGHT).is_not_blank()
    
    plpem2017_period = "2017"
    
    plpem2017_unit = "kWh"
    
    plpem2017_observations = wanted_plpem2017_tabs.excel_ref("C2").expand(RIGHT).expand(DOWN).is_not_blank()

    plpem2017_dimensions = [
        HDimConst("Period", plpem2017_period),
        HDim(plpem2017_postcodes, "Postcodes", CLOSEST, ABOVE),
        HDim(plpem2017_meters, "Meters", CLOSEST, ABOVE),
        HDim(plpem2017_kilowatt_hours, "Measure Types", DIRECTLY, ABOVE),
        HDimConst("Unit", plpem2017_unit)
    ]

trace.POSTCODES("Selected as all non-blank values from cell ref A2 down.")
trace.METERS("Selected as all non-blank values from cell ref B2 down.")
trace.MEASURE_TYPES("Selected as all non-blank values from cell ref C1 going right/across.")
trace.PERIOD("Hardcoded but could have been taken from the dataset title.")
trace.UNIT("Hardcoded but could have been taken from the measure type's heading.")

plpem2017_tidy_sheet = ConversionSegment(wanted_plpem2017_tabs, plpem2017_dimensions, plpem2017_observations)
trace.with_preview(plpem2017_tidy_sheet)
#savepreviewhtml(plpem2017_tidy_sheet)
trace.store("combined_dataframe_4", plpem2017_tidy_sheet.topandas())

df = trace.combine_and_trace(datasetTitle4, "combined_dataframe_4")
df.rename(columns={'OBS' : 'Value'}, inplace=True)
trace.output()

tidy_d4 = df[["Period", "Postcodes", "Meters", "Measure Types", "Unit", "Value"]]
tidy_d4

# -
#Outputs:
    #tidy_d1 = Local authority prepayment electricity meters 2017
    #tidy_d1 = MSOA prepayment electricity meters 2017
    #tidy_d1 = LSOA prepayment electricity meters 2017
    #tidy_d1 = Postcode prepayment electricity meters 2017


