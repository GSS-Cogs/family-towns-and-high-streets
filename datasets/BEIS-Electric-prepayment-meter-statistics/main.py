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

#lapem2017_columns = ["YEAR", "REGION", "LOCAL AUTHORITY", "GEOGRAPHY CODE", "LAU1", "METERS", "MEASURE TYPE", "UNIT"]
lapem2017_columns = ["YEAR", "GEOGRAPHY LEVEL", "GEOGRAPHY CODE", "METERS", "MEASURE TYPE", "UNIT"]
trace.start(datasetTitle1, wanted_lapem2017_tabs, lapem2017_columns, scraper.distributions[0].downloadURL)

#lapem2017_region = wanted_lapem2017_tabs.excel_ref("A4").expand(DOWN).is_not_blank()
#trace.REGION("Selected as all non-blank values from cell ref A4 down.")

#lapem2017_local_authority = wanted_lapem2017_tabs.excel_ref("B4").expand(DOWN).is_not_blank()
#trace.LOCAL_AUTHORITY("Selected as all non-blank values from cell ref B4 down.")

lapem2017_geography_level = wanted_lapem2017_tabs.excel_ref("B4").expand(DOWN).is_not_blank()
trace.GEOGRAPHY_LEVEL("Selected as all non-blank values from cell ref B4 down.")

lapem2017_geography_code = wanted_lapem2017_tabs.excel_ref("C4").expand(DOWN).is_not_blank()
trace.GEOGRAPHY_CODE("Selected as all non-blank values from cell ref C4 down.")

#lapem2017_lau1 = wanted_lapem2017_tabs.excel_ref("D4").expand(DOWN).is_not_blank()
#trace.LAU1("Selected as all non-blank values from cell ref D4 down.")

lapem2017_meters = wanted_lapem2017_tabs.excel_ref("E4").expand(DOWN).is_not_blank()
trace.METERS("Selected as all non-blank values from cell ref E4 down.")

lapem2017_kilowatt_hours = wanted_lapem2017_tabs.excel_ref("F3").expand(RIGHT).is_not_blank()
trace.MEASURE_TYPE("Selected as all non-blank values from cell ref F3 going right/across.")

lapem2017_year = "2017" #Would be better if this was taken directly from the tab title
trace.YEAR("Hardcoded but could have been taken from the dataset title or tab title.")

lapem2017_unit = "kWh"
trace.UNIT("Hardcoded but could have been taken from the measure type's heading.")

lapem2017_observations = wanted_lapem2017_tabs.excel_ref("F4").expand(RIGHT).expand(DOWN).is_not_blank()

lapem2017_dimensions = [
    HDimConst("Year", lapem2017_year),
    #HDim(lapem2017_region, "Region", CLOSEST, ABOVE),
    #HDim(lapem2017_local_authority, "Local Authority", CLOSEST, ABOVE),
    HDim(lapem2017_geography_level, "Geography Level", CLOSEST, ABOVE),
    HDim(lapem2017_geography_code, "Geography Code", CLOSEST, ABOVE),
    #HDim(lapem2017_lau1, "LAU1", CLOSEST, ABOVE),
    HDim(lapem2017_meters, "Meters", CLOSEST, ABOVE),
    HDim(lapem2017_kilowatt_hours, "Measure Type", DIRECTLY, ABOVE),
    HDimConst("Unit", lapem2017_unit)
]

lapem2017_tidy_sheet = ConversionSegment(wanted_lapem2017_tabs, lapem2017_dimensions, lapem2017_observations)
trace.with_preview(lapem2017_tidy_sheet)
#savepreviewhtml(lapem2017_tidy_sheet)
trace.store("combined_dataframe_1", lapem2017_tidy_sheet.topandas())

df = trace.combine_and_trace(datasetTitle1, "combined_dataframe_1")
df.rename(columns={'OBS' : 'Value'}, inplace=True)
df["Measure Type"] = df["Measure Type"].map({"Sales (kWh)": "Sales", "Mean consumption": "Mean Consumption", "Median consumption": "Median Consumption"})
trace.output()

#Getting rid of the final rows which display "Unallocated" geography.
df.drop(df.index[-3:], inplace=True)

df["Geography Level"] = 'local-authority'

#tidy_d1 = df[["Year", "Region", "Local Authority", "Geography Code", "LAU1", "Meters", "Measure Types", "Unit", "Value"]]
tidy_d1 = df[["Year", "Geography Level", "Geography Code", "Meters", "Measure Type", "Unit", "Value"]]

for i in range(0, 3):
    final_tidy = tidy_d1.iloc[i::3]
    tidied_sheets.append(final_tidy)

#tidy_d1
#tidied_sheets[0:3]


# +
# #### DISTRIBUTION 2 : MSOA prepayment electricity meters 2017
datasetTitle2 = "Middle Layer Super Output Area (MSOA) prepayment electricity meter consumption, 2017"
msoa2017_tabs = { tab.name: tab for tab in scraper.distributions[1].as_databaker() }
wanted_msoa2017_tabs = msoa2017_tabs["MSOA Domestic Electricity 2017"]

msoa2017_tidy_sheet_list = [] # list of dataframes for each iteration
msoa2017_tidy_sheet_iteration = []
msoa2017_cs_list = [] # list of conversionsegments for each iteration

#msoa2017_columns = ["YEAR", "LA NAME", "GEOGRAPHY CODE", "MSOA NAME", "MSOA CODE", "METERS", "MEASURE TYPE", "UNIT"]
msoa2017_columns = ["YEAR", "GEOGRAPHY LEVEL", "GEOGRAPHY CODE", "METERS", "MEASURE TYPE", "UNIT"]
trace.start(datasetTitle2, wanted_msoa2017_tabs, msoa2017_columns, scraper.distributions[1].downloadURL)

tab_length = len(wanted_msoa2017_tabs.excel_ref('A')) # number of rows of data
batch_number = 10 # iterates over this many rows at a time
number_of_iterations = math.ceil(tab_length/batch_number) # databaking will iterate this many times
            
for i in range(0, number_of_iterations):
    Min = str(4 + batch_number * i)  # data starts on row 4
    Max = str(int(Min) + batch_number - 1)
    
    #msoa2017_la_name = wanted_msoa2017_tabs.excel_ref("A"+Min+":A"+Max).is_not_blank()
    
    #msoa2017_la_code = wanted_msoa2017_tabs.excel_ref("B"+Min+":B"+Max).is_not_blank()
    
    msoa2017_geography_level = wanted_msoa2017_tabs.excel_ref("C"+Min+":C"+Max).is_not_blank()
    
    msoa2017_geography_code = wanted_msoa2017_tabs.excel_ref("D"+Min+":D"+Max).is_not_blank()
    
    msoa2017_meters = wanted_msoa2017_tabs.excel_ref("E"+Min+":E"+Max).is_not_blank()
    
    msoa2017_kilowatt_hours = wanted_msoa2017_tabs.excel_ref("F3").expand(RIGHT).is_not_blank()

    msoa2017_period = "2017"
    
    msoa2017_unit = "kWh"    

    msoa2017_observations = wanted_msoa2017_tabs.excel_ref("F"+Min+":H"+Max).is_not_blank()
    #msoa2017_observations = msoa2017_meters.waffle(msoa2017_kilowatt_hours) #This doubles the expected number of returned rows

    msoa2017_dimensions = [
        HDimConst('Year', msoa2017_period),
        #HDim(msoa2017_la_name, "LA Name", CLOSEST, ABOVE),
        #HDim(msoa2017_la_code, "Geography Code", CLOSEST, ABOVE),
        HDim(msoa2017_geography_level, "Geography Level", CLOSEST, ABOVE),
        HDim(msoa2017_geography_code, "Geography Code", CLOSEST, ABOVE),
        HDim(msoa2017_meters, "Meters", CLOSEST, ABOVE),
        HDim(msoa2017_kilowatt_hours, "Measure Type", DIRECTLY, ABOVE),
        HDimConst("Unit", msoa2017_unit)
    ]

    if len(msoa2017_observations) != 0: # only use ConversionSegment if there is data
        msoa2017_cs_iteration = ConversionSegment(wanted_msoa2017_tabs, msoa2017_dimensions, msoa2017_observations) # creating the conversionsegment
        msoa2017_tidy_sheet_iteration = msoa2017_cs_iteration.topandas() # turning conversionsegment into a pandas dataframe
        msoa2017_cs_list.append(msoa2017_cs_iteration) # add to list
        msoa2017_tidy_sheet_list.append(msoa2017_tidy_sheet_iteration) # add to list
                    
msoa2017_tidy_sheet = pd.concat(msoa2017_tidy_sheet_list, sort=False) # dataframe for the whole tab

#trace.LA_NAME("Selected as all non-blank values from cell ref A4 down.")
#trace.GEOGRAPHY_CODE("Selected as all non-blank values from cell ref B4 down.")
trace.GEOGRAPHY_LEVEL("Selected as all non-blank values from cell ref C4 down.")
trace.GEOGRAPHY_CODE("Selected as all non-blank values from cell ref D4 down.")
trace.METERS("Selected as all non-blank values from cell ref E4 down.")
trace.MEASURE_TYPE("Selected as all non-blank values from cell ref F3 going right/across.")
trace.YEAR("Hardcoded but could have been taken from the dataset title or tab title.")
trace.UNIT("Hardcoded but could have been taken from the measure type's heading.")

trace.store("combined_dataframe_2", msoa2017_tidy_sheet)

df = trace.combine_and_trace(datasetTitle2, "combined_dataframe_2")
df.rename(columns={'OBS' : 'Value'}, inplace=True)
df["Measure Type"] = df["Measure Type"].map({"Sales (kWh)": "Sales", "Mean consumption": "Mean Consumption", "Median consumption": "Median Consumption"})
trace.output()

df.drop(df.index[-3:], inplace=True)

df["Geography Level"] = 'middle-layer-super-output-area'

tidy_d2 = df[["Year", "Geography Level", "Geography Code", "Meters", "Measure Type", "Unit", "Value"]]

for i in range(0, 3):
    final_tidy = tidy_d2.iloc[i::3]
    tidied_sheets.append(final_tidy)

#tidy_d2
#tidied_sheets[3:6]


# +
# #### DISTRIBUTION 3 : LSOA prepayment electricity meters 2017

datasetTitle3 = "Lower Layer Super Output Area (LSOA)  prepayment electricity meter consumption, 2017"
lsoa2017_tabs = { tab.name: tab for tab in scraper.distributions[2].as_databaker() }
wanted_lsoa2017_tabs = lsoa2017_tabs["LSOA Dom Elec 2017"]

lsoa2017_tidy_sheet_list = [] # list of dataframes for each iteration
lsoa2017_tidy_sheet_iteration = []
lsoa2017_cs_list = [] # list of conversionsegments for each iteration

lsoa2017_columns = ["YEAR", "MSOA CODE", "GEOGRAPHY LEVEL", "GEOGRAPHY CODE", "METERS", "MEASURE TYPE", "UNIT"]
trace.start(datasetTitle3, wanted_lsoa2017_tabs, lsoa2017_columns, scraper.distributions[2].downloadURL)

tab_length = len(wanted_lsoa2017_tabs.excel_ref('A')) # number of rows of data
batch_number = 10 # iterates over this many rows at a time
number_of_iterations = math.ceil(tab_length/batch_number) # databaking will iterate this many times

for i in range(0, number_of_iterations):
    Min = str(3 + batch_number * i)  # data starts on row 3
    Max = str(int(Min) + batch_number - 1)

    #lsoa2017_la_name = wanted_lsoa2017_tabs.excel_ref("A"+Min+":A"+Max).is_not_blank()
    
    #lsoa2017_la_code = wanted_lsoa2017_tabs.excel_ref("B"+Min+":B"+Max).is_not_blank()
    
    #lsoa2017_msoa_name = wanted_lsoa2017_tabs.excel_ref("C"+Min+":C"+Max).is_not_blank()
    
    lsoa2017_msoa_code = wanted_lsoa2017_tabs.excel_ref("D"+Min+":D"+Max).is_not_blank()
    
    lsoa2017_geography_level = wanted_lsoa2017_tabs.excel_ref("E"+Min+":E"+Max).is_not_blank()
    
    lsoa2017_geography_code = wanted_lsoa2017_tabs.excel_ref("F"+Min+":F"+Max).is_not_blank()
    
    lsoa2017_meters = wanted_lsoa2017_tabs.excel_ref("H"+Min+":H"+Max).is_not_blank()
    
    lsoa2017_kilowatt_hours = wanted_lsoa2017_tabs.excel_ref("2").filter(contains_string("(kWh)")).is_not_blank()
    
    lsoa2017_year = "2017"
    
    lsoa2017_unit = "kWh"
    
    #Note: Alternative observation extraction methods kept for my own benifit to be referred back to later.
    lsoa2017_observations = wanted_lsoa2017_tabs.excel_ref("G"+Min+":G"+Max) | wanted_lsoa2017_tabs.excel_ref("I"+Min+":J"+Max)
    #lsoa2017_observations = wanted_lsoa2017_tabs.excel_ref("G"+Min+":J"+Max) - wanted_lsoa2017_tabs.excel_ref("H") #This doubles the expected number of returned rows
    #lsoa2017_observations = lsoa2017_lsoa_code.waffle(lsoa2017_kilowatt_hours) #This triples the expected number of returned rows
    
    lsoa2017_dimensions = [
        HDimConst("Year", lsoa2017_year),
        #HDim(lsoa2017_la_name, "LA Name", CLOSEST, ABOVE),
        #HDim(lsoa2017_la_code, "LA Code", CLOSEST, ABOVE),
        #HDim(lsoa2017_msoa_name, "MSOA Name", CLOSEST, ABOVE),
        HDim(lsoa2017_msoa_code, "MSOA Code", CLOSEST, ABOVE),
        HDim(lsoa2017_geography_level, "Geography Level", CLOSEST, ABOVE),
        HDim(lsoa2017_geography_code, "Geography Code", CLOSEST, ABOVE),
        HDim(lsoa2017_meters, "Meters", CLOSEST, ABOVE),
        HDim(lsoa2017_kilowatt_hours, "Measure Type", DIRECTLY, ABOVE),
        HDimConst("Unit", lsoa2017_unit)
    ]
    
    if len(lsoa2017_observations) != 0: # only use ConversionSegment if there is data
        lsoa2017_cs_iteration = ConversionSegment(wanted_lsoa2017_tabs, lsoa2017_dimensions, lsoa2017_observations) # creating the conversionsegment
        lsoa2017_tidy_sheet_iteration = lsoa2017_cs_iteration.topandas() # turning conversionsegment into a pandas dataframe
        lsoa2017_cs_list.append(lsoa2017_cs_iteration) # add to list
        lsoa2017_tidy_sheet_list.append(lsoa2017_tidy_sheet_iteration) # add to list

lsoa2017_tidy_sheet = pd.concat(lsoa2017_tidy_sheet_list, sort=False) # dataframe for the whole tab

#trace.LA_NAME("Selected as all non-blank values from cell ref A3 down.")
#trace.LA_CODE("Selected as all non-blank values from cell ref B3 down.")
#trace.MSOA_NAME("Selected as all non-blank values from cell ref C3 down.")    
trace.MSOA_CODE("Selected as all non-blank values from cell ref D3 down.")
trace.GEOGRAPHY_LEVEL("Selected as all non-blank values from cell ref E3 down.")
trace.GEOGRAPHY_CODE("Selected as all non-blank values from cell ref F3 down.")
trace.METERS("Selected as all non-blank values from cell ref H3 down.")
trace.MEASURE_TYPE("Selected as all non-blank values from cell ref (row) 2 and filtering for all values containing the string '(kWh)'.")
trace.YEAR("Hardcoded but could have been taken from the dataset title or tab title.")
trace.UNIT("Hardcoded but could have been taken from the measure type's heading.")

trace.store("combined_dataframe_3", lsoa2017_tidy_sheet)

df = trace.combine_and_trace(datasetTitle3, "combined_dataframe_3")
df.rename(columns={'OBS' : 'Value'}, inplace=True)
df["Measure Type"] = df["Measure Type"].map({"Total consumption (kWh)": "Sales", "Mean consumption (kWh)": "Mean Consumption", "Median consumption \n(kWh)": "Median Consumption"})
trace.output()

df.drop(df.index[-3:], inplace=True)

df["Geography Level"] = 'lower-layer-super-output-area'

tidy_d3 = df[["Year", "Geography Level", "Geography Code", "Meters", "Measure Type", "Unit", "Value"]]

for i in range(0, 3):
    final_tidy = tidy_d3.iloc[i::3]
    tidied_sheets.append(final_tidy)

#tidy_d3
#tidied_sheets

# +
# #### DISTRIBUTION 4 : Postcode prepayment electricity meters 2017
#KEY: plpem2017 = Postcode prepayment electricity meters 2017

datasetTitle4 = "Postcode level prepayment electric meter consumption, 2017"
plpem2017_tabs = { tab.name: tab for tab in scraper.distributions[3].as_databaker() }
wanted_plpem2017_tabs = plpem2017_tabs["Postcode-prepayment-electricity"]

plpem2017_tidy_sheet_list = [] # list of dataframes for each iteration
plpem2017_tidy_sheet_iteration = []
plpem2017_cs_list = [] # list of conversionsegments for each iteration

plpem2017_columns = ["YEAR", "POST CODES", "METERS", "MEASURE TYPE", "UNIT"]
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
    
    plpem2017_year = "2017"
    
    plpem2017_unit = "kWh"
    
    #plpem2017_observations = wanted_plpem2017_tabs.excel_ref("C2").expand(RIGHT).expand(DOWN).is_not_blank()
    plpem2017_observations = wanted_plpem2017_tabs.excel_ref("C"+Min+":E"+Max).is_not_blank()
    #plpem2017_observations = plpem2017_meters.waffle(plpem2017_kilowatt_hours)

    plpem2017_dimensions = [
        HDimConst("Year", plpem2017_year),
        HDim(plpem2017_postcodes, "Post Codes", CLOSEST, ABOVE),
        HDim(plpem2017_meters, "Meters", CLOSEST, ABOVE),
        HDim(plpem2017_kilowatt_hours, "Measure Type", DIRECTLY, ABOVE),
        HDimConst("Unit", plpem2017_unit)
    ]

    if len(plpem2017_observations) != 0: # only use ConversionSegment if there is data
        plpem2017_cs_iteration = ConversionSegment(wanted_plpem2017_tabs, plpem2017_dimensions, plpem2017_observations) # creating the conversionsegment
        plpem2017_tidy_sheet_iteration = plpem2017_cs_iteration.topandas() # turning conversionsegment into a pandas dataframe
        plpem2017_cs_list.append(plpem2017_cs_iteration) # add to list
        plpem2017_tidy_sheet_list.append(plpem2017_tidy_sheet_iteration) # add to list

plpem2017_tidy_sheet = pd.concat(plpem2017_tidy_sheet_list, sort=False) # dataframe for the whole tab

trace.POST_CODES("Selected as all non-blank values from cell ref A2 down.")
trace.METERS("Selected as all non-blank values from cell ref B2 down.")
trace.MEASURE_TYPE("Selected as all non-blank values from cell ref C1 going right/across.")
trace.YEAR("Hardcoded but could have been taken from the dataset title.")
trace.UNIT("Hardcoded but could have been taken from the measure type's heading.")

trace.store("combined_dataframe_4", plpem2017_tidy_sheet)

df = trace.combine_and_trace(datasetTitle4, "combined_dataframe_4")
df.rename(columns={'OBS' : 'Value'}, inplace=True)
df["Measure Type"] = df["Measure Type"].map({"Sales (kWh)": "Sales", "Mean consumption (kWh)": "Mean Consumption", "Median consumption (kWh)": "Median Consumption"})
trace.output()

tidy_d4 = df[["Year", "Post Codes", "Meters", "Measure Type", "Unit", "Value"]]

for i in range(0, 3):
    final_tidy = tidy_d4.iloc[i::3]
    tidied_sheets.append(final_tidy)

#tidy_d4
#tidied_sheets

# +
#Outputs:
    #Stage 1
    #tidy_d1 = Local authority prepayment electricity meters 2017
    #tidy_d2 = MSOA prepayment electricity meters 2017
    #tidy_d3 = LSOA prepayment electricity meters 2017
    #tidy_d4 = Postcode prepayment electricity meters 2017
    
    #After stage 2 spec:
    #tidied_sheets[0] = Local authority prepayment electricity meters 2017 - Sales
    #tidied_sheets[1] = Local authority prepayment electricity meters 2017 - Mean Consumption
    #tidied_sheets[2] = Local authority prepayment electricity meters 2017 - Median Consumption
    
    #tidied_sheets[3] = MSOA prepayment electricity meters 2017 - Sales
    #tidied_sheets[4] = MSOA prepayment electricity meters 2017 - Mean Consumption
    #tidied_sheets[5] = MSOA prepayment electricity meters 2017 - Median Consumption
    
    #tidied_sheets[6] = LSOA prepayment electricity meters 2017 - Sales
    #tidied_sheets[7] = LSOA prepayment electricity meters 2017 - Mean Consumption
    #tidied_sheets[8] = LSOA prepayment electricity meters 2017 - Median Consumption
    
    #tidied_sheets[9] = Postcode prepayment electricity meters 2017 - Sales
    #tidied_sheets[10] = Postcode prepayment electricity meters 2017 - Mean Consumption
    #tidied_sheets[11] = Postcode prepayment electricity meters 2017 - Median Consumption
    
    #Joined sheets
    #tidy_sales = Electric prepayment meter statistics - Sales
    #tidy_mean_consumption = Electric prepayment meter statistics - Mean Consumption
    #tidy_median_consumption = Electric prepayment meter statistics - Median Consumption
    
#Notes:
    #When running each tab, a large number of blank lines will be printed before the completed table.
# +
tidy_sales = pd.concat([tidied_sheets[0], tidied_sheets[3], tidied_sheets[6]])

tidy_mean_consumption = pd.concat([tidied_sheets[1], tidied_sheets[4], tidied_sheets[7]])

tidy_median_consumption = pd.concat([tidied_sheets[2], tidied_sheets[5], tidied_sheets[8]])

# +
# As we only have one Measure and Unit type they are defined within the info.json file so can be deleted from the tables
# Also remove the space from Post Code and rename as to just 'Post Code'
del tidy_sales['Measure Type']
del tidy_sales['Unit']

del tidy_mean_consumption['Measure Type']
del tidy_mean_consumption['Unit']

del tidy_median_consumption['Measure Type']
del tidy_median_consumption['Unit']

del tidied_sheets[9]['Measure Type']
del tidied_sheets[9]['Unit']

tidied_sheets[9]['Post Codes'] = tidied_sheets[9]['Post Codes'].str.replace(' ', '')
tidied_sheets[9] = tidied_sheets[9].rename(columns={'Post Codes': 'Post Code'})


# +
to_output = []
to_output.append([tidy_sales, 
                  "sales", 
                  "Electric prepayment meter statistics - Sales", 
                  "sales", 
                  "/sales", 
                  "Annual prepayment meter electricity sales statistics for Local Authorities, LSOAs, MSOAs in England, Wales and Scotland."])
to_output.append([tidy_mean_consumption, 
                  "mean_consumption", 
                  "Electric prepayment meter statistics - Mean Consumption", 
                  "mean-consumption", 
                  "/mean", 
                 "Annual prepayment meter electricity mean consumption statistics for Local Authorities, LSOAs, MSOAs in England, Wales and Scotland."])
to_output.append([tidy_median_consumption, 
                  "median_consumption", 
                  "Electric prepayment meter statistics - Median Consumption", 
                  "median-consumption", 
                  "/median", 
                  "Annual prepayment meter electricity median consumption statistics for Local Authorities, LSOAs, MSOAs in England, Wales and Scotland."])
to_output.append([tidied_sheets[9], 
                  "post_code_sales", 
                  "Electric prepayment meter statistics by Post Code - Sales", 
                  "sales", 
                  "/sales", 
                  "Annual prepayment meter electricity sales statistics by Post Code in England, Wales and Scotland."])

#tidy_sales
#tidy_mean_consumption
#tidy_median_consumption
# -

tidy_sales.head(10)
#tidy_sales['Geography Level'].unique()


tidy_median_consumption.head(10)

tidied_sheets[9].head(10)



# +
from urllib.parse import urljoin
import os

for i in to_output:
    csvName = i[1] + "_observations.csv"
    out = Path("out")
    out.mkdir(exist_ok=True)
    i[0].drop_duplicates().to_csv(out / csvName, index = False)

    scraper.dataset.family = "towns-high-streets"

    scraper.dataset.description = """Data for prepayment meter electricity consumption, number of meters, mean and median consumption for local
    authority regions across England, Wales & Scotland. This doesn't include smart meters operating in prepayment mode.
    Data excludes:
    Geographies that are disclosive are defined as such if they contain less than 6 meters
    The dataset only contains meters that have consumption between 100 kWh and 100,000 kWh and have a domestic meter profile
    Meters that have not successfully been assigned to a geography due to insufficient address information are counted in the 
    'Unallocated' category but are not included in this data as there is no matching geography code.
    Meters that are deemed to be disclosive at Local Authority level are set as 'Unallocated' but are not included in this 
    data as there is no matching geography code."""

    scraper.dataset.comment = i[5]
    scraper.dataset.title = i[2]

    dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}' + i[4]))
    scraper.set_base_uri('http://gss-data.org.uk')
    scraper.set_dataset_id(dataset_path)

    #Changing the measure in info.json
    temp_info = json.load(open('info.json'))
    temp_info["transform"]["columns"]["Value"]["measure"] = "http://gss-data.org.uk/def/measure/" + i[3]

    csvw_transform = CSVWMapping()
    csvw_transform.set_csv(out / csvName)
    csvw_transform.set_mapping(temp_info)
    csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
    csvw_transform.write(out / f'{csvName}-metadata.json')

    with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
        metadata.write(scraper.generate_trig())


# -


