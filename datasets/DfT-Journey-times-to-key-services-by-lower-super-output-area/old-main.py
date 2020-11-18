from gssutils import * 
import json 

info = json.load(open('info.json')) 
#etl_title = info["Name"] 
#etl_publisher = info["Producer"][0] 
#print("Publisher: " + etl_publisher) 
#print("Title: " + etl_title) 

scraper = Scraper(seed="info.json")   
scraper


scraper.select_dataset(title=lambda t: 'Journey times to key services by lower super output area (JTS05)' in t)
scraper

datasetTitle = "Journey times to key services by lower super output area"
trace = TransformTrace()

# # Distribution: 1 
#
# #### Travel time, destination and origin indicators for Employment centres by mode of travel, Lower Super Output Area (LSOA), England
#
#     Destination : Employment Centres 
#     Population Aged : 16-74 years old
#

scraper.distributions[0]

tabs = { tab.name: tab for tab in scraper.distributions[0].as_databaker() }
list(tabs)

# +

for name, tab in tabs.items():
        columns=["Period", "LSOA Code", "Region of LA", "LA Code", "LA Name","Service users in LSOA",
                "Destination", "Population aged", "Mode of travel", "Destination 15 mins", "Destination 30 mins",
                "Destination 45 mins", "Destination 60 mins", "Percentage 15 mins", "Percentage 30 mins", 
                "Percentage 45 mins", "Percentage 60 mins", "Measure Type", "Unit"]
        trace.start(datasetTitle, tab, columns, scraper.distributions[0].downloadURL)
        
        if 'Metadata' in name:
            continue  
 
        year = name
        trace.Period("Period year taken from sheet name : " + year)
        
        LSOA_code = tab.filter(contains_string('LSOA_code')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LSOA_Code("LSOA Code taken from cell reference A8 down")
        
        region =  tab.filter(contains_string('Region')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Region_of_LA("Region of LA taken from cell reference B8 down")
        
        la_code = tab.filter(contains_string('LA_Code')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LA_Code("LA Code taken from cell reference C8 down")
        
        la_name = tab.filter(contains_string('LA_Name')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LA_Name("LA Name taken from cell reference D8 down")
        
        service_users =  tab.filter(contains_string('Empl_pop')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Service_users_in_LSOA("Service users in LSOA taken from cell reference D8 down")
        
        ## Starting with Travel time in minutes to nearest Employment Centres by PT/walk ##
        mode_of_travel = "PT/Walk"
        trace.Mode_of_travel("Mode of travel hardcoded as PT/Walk, transforming the data data related.")
        
        travel_within_15_mins = tab.filter(contains_string('100EmpPT15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of Employment Centres available by PT/walk within 15 minutes (100EmplPT15n) taken from cell reference G8 down")
        
        travel_within_30_mins = tab.filter(contains_string('100EmpPT30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of Employment Centres available by PT/walk within 30 minutes (100EmpPT30n) taken from cell reference H8 down")

        travel_within_45_mins = tab.filter(contains_string('100EmpPT45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of Employment Centres available by PT/walk within 45 minutes (100EmpPT45n) taken from cell reference I8 down")

        travel_within_60_mins = tab.filter(contains_string('100EmpPT60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_60_mins("Number of Employment Centres available by PT/walk within 60 minutes (100EmpPT60n) taken from cell reference J8 down")

        percentage_users_15_mins = tab.filter(contains_string('100EmpPT15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of Employment Centres available by PT/walk (100EmpPT15pct) taken from cell reference K8 down")
        
        percentage_users_30_mins = tab.filter(contains_string('100EmpPT30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of Employment Centres available by PT/walk (100EmpPT30pct) taken from cell reference L8 down")
        
        percentage_users_45_mins = tab.filter(contains_string('100EmpPT45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of Employment Centres available by PT/walk (100EmpPT45pct) taken from cell reference M8 down")

        percentage_users_60_mins = tab.filter(contains_string('100EmpPT60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of Employment Centres available by PT/walk (100EmpPT60pct) taken from cell reference N8 down")
        
        #Observations = Travel time in minutes to nearest Employment Centres by PT/walk
        observations = tab.filter(contains_string('100EmpPTt')).shift(0,1).expand(DOWN).is_not_blank()
        
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
       # savepreviewhtml(tidy_sheet, fname="walk.html") 
        trace.store("combined_dataframe_1", tidy_sheet.topandas())
            
        ##   Next Travel time in minutes to nearest Employment Centres by cycle  ##
        mode_of_travel = "Cycle"
        trace.Mode_of_travel("Mode of travel hardcoded as Cycle, transforming the data data related.")

        travel_within_15_mins = tab.filter(contains_string('100EmpCyc15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of Employment Centres available by cycle within 15 minutes (100EmplCyc15n) taken from cell reference P8 down")
        
        travel_within_30_mins = tab.filter(contains_string('100EmpCyc30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of Employment Centres available by cycle within 30 minutes (PSCyc30n) taken from cell reference Q8 down")

        travel_within_45_mins = tab.filter(contains_string('100EmpCyc45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of Employment Centres available by cycle within 45 minutes (100EmpCyc45n) taken from cell reference R8 down")

        travel_within_60_mins = tab.filter(contains_string('100EmpCyc60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_60_mins("Number of Employment Centres available by cycle within 60 minutes (100EmpCyc60n) taken from cell reference S8 down")
        
        percentage_users_15_mins = tab.filter(contains_string('100EmpCyc15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of Employment Centres available by cycle (100EmpCyc15pct) taken from cell reference T8 down")

        percentage_users_30_mins = tab.filter(contains_string('100EmpCyc30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of Employment Centres available by cycle (100EmpCyc30pct) taken from cell reference U8 down")

        percentage_users_45_mins = tab.filter(contains_string('100EmpCyc45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of Employment Centres available by cycle (100EmpCyc45pct) taken from cell reference V8 down")
        
        percentage_users_60_mins = tab.filter(contains_string('100EmpCyc60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of Employment Centres available by cycle (100EmpCyc60pct) taken from cell reference W8 down")

        #Observations = Travel time in minutes to nearest Employment Centres by cycle (100EmplCyct down)
        observations = tab.filter(contains_string('100EmpCyct')).shift(0,1).expand(DOWN).is_not_blank()
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),                
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="cycle.html") 
        trace.store("combined_dataframe_1", tidy_sheet.topandas())
            
        ##   Next Travel time in minutes to nearest Employment Centres by Car  ##
        mode_of_travel = "Car"
        trace.Mode_of_travel("Mode of travel hardcoded as Car, transforming the data data related.")

        travel_within_15_mins = tab.filter(contains_string('100EmpCar15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of Employment Centres available by car within 15 minutes (100EmpCar15n) taken from cell reference Y8 down")
 
        travel_within_30_mins = tab.filter(contains_string('100EmpCar30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of Employment Centres available by car within 30 minutes (100EmpCar30n) taken from cell reference Z8 down")
 
        travel_within_45_mins = tab.filter(contains_string('100EmpCar45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of Employment Centres available by car within 45 minutes (100EmpCar45n) taken from cell reference AA8 down")
 
        travel_within_60_mins = tab.filter(contains_string('100EmpCar60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of Employment Centres available by car within 45 minutes (100EmpCar45n) taken from cell reference AB8 down")
 
        percentage_users_15_mins = tab.filter(contains_string('100EmpCar15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of Employment Centres available by car (100EmpCar15pct) taken from cell reference AC8 down")
        
        percentage_users_30_mins = tab.filter(contains_string('100EmpCar30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of Employment Centres available by car (100EmpCar30pct) taken from cell reference AD8 down")

        percentage_users_45_mins = tab.filter(contains_string('100EmpCar45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of Employment Centres available by car (100EmpCar45pct) taken from cell reference AD8 down")

        percentage_users_60_mins = tab.filter(contains_string('100EmpCar60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of Employment Centres available by car (100EmpCar60pct) taken from cell reference AE8 down")

        #Observations = Travel time in minutes to nearest Employment Centres by car
        observations = tab.filter(contains_string('100EmpCart')).shift(0,1).expand(DOWN).is_not_blank()
        
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),                HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),                
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname= year +"car.html") 
        trace.store("combined_dataframe_1", tidy_sheet.topandas())

print("DISTRIBUTION 1 DONE")


# +

df = trace.combine_and_trace(datasetTitle, "combined_dataframe_1")
df.rename(columns={'OBS' : 'Value'}, inplace=True)
df["Destination"] = "Employment Centres"
trace.Destination("Destination hard coded as : Employment Centres")
df["Population aged"] = "16-74 years old"
trace.Population_aged("Population aged hard coded as : 16-74 years old")
df["Measure Type"] = "Travel time"
trace.Measure_Type("Measure Type hard coded as : Travel time")
df["Unit"] = "Minutes"
trace.Unit("Unit hard coded as : Minutes")

# -

#     Output table for distribution 2

# +

tidy_distribution_1 = df[["Period", "LSOA Code", "Region of LA", "LA Code", "LA Name","Service users in LSOA",
                "Destination", "Population aged", "Mode of travel", "Destination avilable by mode of travel within 15 minutes",
                "Destination avilable by mode of travel within 30 minutes", "Destination avilable by mode of travel within 45 minutes",
                "Destination avilable by mode of travel within 60 minutes", "Percentage of users within 15 minutes by mode of transport", 
                "Percentage of users within 30 minutes by mode of transport", "Percentage of users within 45 minutes by mode of transport",
                "Percentage of users within 60 minutes by mode of transport", "Measure Type", "Unit", "Value"]]
tidy_distribution_1

# -

trace.output()

# # Distribution: 2 
#
# #### Travel time, destination and origin indicators for Primary schools by mode of travel, Lower Super Output Area (LSOA), England
#
#     Destination : Primary School
#     Population Aged : 5-10 years

tabs = { tab.name: tab for tab in scraper.distributions[1].as_databaker() }
list(tabs)

# + endofcell="--"

for name, tab in tabs.items():
        columns=["Period", "LSOA Code", "Region of LA", "LA Code", "LA Name","Service users in LSOA",
                "Destination", "Population aged", "Mode of travel", "Destination 15 mins", "Destination 30 mins",
                "Destination 45 mins", "Destination 60 mins", "Percentage 15 mins", "Percentage 30 mins", 
                "Percentage 45 mins", "Percentage 60 mins", "Measure Type", "Unit"]
        trace.start(datasetTitle, tab, columns, scraper.distributions[1].downloadURL)
        
        if 'Metadata' in name:
            continue  
 
        year = name
        trace.Period("Period year taken from sheet name : " + year)
        
        LSOA_code = tab.filter(contains_string('LSOA_code')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LSOA_Code("LSOA Code taken from cell reference A8 down")
        
        region =  tab.filter(contains_string('Region')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Region_of_LA("Region of LA taken from cell reference B8 down")
        
        la_code = tab.filter(contains_string('LA_Code')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LA_Code("LA Code taken from cell reference C8 down")
        
        la_name = tab.filter(contains_string('LA_Name')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LA_Name("LA Name taken from cell reference D8 down")
        
        service_users =  tab.filter(contains_string('PS_pop')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Service_users_in_LSOA("Service users in LSOA taken from cell reference D8 down")
        
        ## Starting with Travel time in minutes to nearest primary schools by PT/walk ##
        mode_of_travel = "PT/Walk"
        trace.Mode_of_travel("Mode of travel hardcoded as PT/Walk, transforming the data data related.")
        
        travel_within_15_mins = tab.filter(contains_string('PSPT15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of primary schools available by PT/walk within 15 minutes (PSPT15n) taken from cell reference G8 down")
        
        travel_within_30_mins = tab.filter(contains_string('PSPT30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of primary schools available by PT/walk within 30 minutes (PSPT30n) taken from cell reference H8 down")

        travel_within_45_mins = tab.filter(contains_string('PSPT45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of primary schools available by PT/walk within 45 minutes (PSPT45n) taken from cell reference I8 down")

        travel_within_60_mins = tab.filter(contains_string('PSPT60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_60_mins("Number of primary schools available by PT/walk within 60 minutes (PSPT60n) taken from cell reference J8 down")

        percentage_users_15_mins = tab.filter(contains_string('PSPT15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of primary schools available by PT/walk (PSPT15pct) taken from cell reference K8 down")
        
        percentage_users_30_mins = tab.filter(contains_string('PSPT30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of primary schools available by PT/walk (PSPT30pct) taken from cell reference L8 down")
        
        percentage_users_45_mins = tab.filter(contains_string('PSPT45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of primary schools available by PT/walk (PSPT45pct) taken from cell reference M8 down")

        percentage_users_60_mins = tab.filter(contains_string('PSPT60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of primary schools available by PT/walk (PSPT60pct) taken from cell reference N8 down")
        
        #Observations = Travel time in minutes to nearest primary schools by PT/walk
        observations = tab.filter(contains_string('PSPTt')).shift(0,1).expand(DOWN).is_not_blank()
        
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        savepreviewhtml(tidy_sheet, fname="walk.html") 
        trace.store("combined_dataframe_2", tidy_sheet.topandas())
            
        ##   Next Travel time in minutes to nearest primary schools by cycle  ##
        mode_of_travel = "Cycle"
        trace.Mode_of_travel("Mode of travel hardcoded as Cycle, transforming the data data related.")

        travel_within_15_mins = tab.filter(contains_string('PSCyc15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of primary schools available by cycle within 15 minutes (PSCyc15n) taken from cell reference P8 down")
        
        travel_within_30_mins = tab.filter(contains_string('PSCyc30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of primary schools available by cycle within 30 minutes (PSCyc30n) taken from cell reference Q8 down")

        travel_within_45_mins = tab.filter(contains_string('PSCyc45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of primary schools available by cycle within 45 minutes (PSCyc45n) taken from cell reference R8 down")

        travel_within_60_mins = tab.filter(contains_string('PSCyc60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_60_mins("Number of primary schools available by cycle within 60 minutes (PSCyc60n) taken from cell reference S8 down")
        
        percentage_users_15_mins = tab.filter(contains_string('PSCyc15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of primary schools available by cycle (PSCyc15pct) taken from cell reference T8 down")

        percentage_users_30_mins = tab.filter(contains_string('PSCyc30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of primary schools available by cycle (PSCyc30pct) taken from cell reference U8 down")

        percentage_users_45_mins = tab.filter(contains_string('PSCyc45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of primary schools available by cycle (PSCyc45pct) taken from cell reference V8 down")
        
        percentage_users_60_mins = tab.filter(contains_string('PSCyc60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of primary schools available by cycle (PSCyc60pct) taken from cell reference W8 down")

        #Observations = Travel time in minutes to nearest primary schools by cycle (PSCyct down)
        observations = tab.filter(contains_string('PSCyct')).shift(0,1).expand(DOWN).is_not_blank()
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),                
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="cycle.html") 
        trace.store("combined_dataframe_2", tidy_sheet.topandas())
            
        ##   Next Travel time in minutes to nearest primary schools by Car  ##
        mode_of_travel = "Car"
        trace.Mode_of_travel("Mode of travel hardcoded as Car, transforming the data data related.")

        travel_within_15_mins = tab.filter(contains_string('PSCar15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of primary schools available by car within 15 minutes (PSCar15n) taken from cell reference Y8 down")
 
        travel_within_30_mins = tab.filter(contains_string('PSCar30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of primary schools available by car within 30 minutes (PSCar30n) taken from cell reference Z8 down")
 
        travel_within_45_mins = tab.filter(contains_string('PSCar45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of primary schools available by car within 45 minutes (PSCar45n) taken from cell reference AA8 down")
 
        travel_within_60_mins = tab.filter(contains_string('PSCar60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of primary schools available by car within 45 minutes (PSCar45n) taken from cell reference AB8 down")
 
        percentage_users_15_mins = tab.filter(contains_string('PSCar15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of primary schools available by car (PSCar15pct) taken from cell reference AC8 down")
        
        percentage_users_30_mins = tab.filter(contains_string('PSCar30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of primary schools available by car (PSCar30pct) taken from cell reference AD8 down")

        percentage_users_45_mins = tab.filter(contains_string('PSCar45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of primary schools available by car (PSCar45pct) taken from cell reference AD8 down")

        percentage_users_60_mins = tab.filter(contains_string('PSCar60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of primary schools available by car (PSCar60pct) taken from cell reference AE8 down")

        #Observations = Travel time in minutes to nearest primary schools by car
        observations = tab.filter(contains_string('PSCart')).shift(0,1).expand(DOWN).is_not_blank()
        
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),                HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),                
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="car.html") 
        trace.store("combined_dataframe_2", tidy_sheet.topandas())

print("DISTRIBUTION 2 DONE")


# # +

df = trace.combine_and_trace(datasetTitle, "combined_dataframe_2")
df.rename(columns={'OBS' : 'Value'}, inplace=True)
df["Destination"] = "Primary School"
trace.Destination("Destination hard coded as : Primary School")
df["Population aged"] = "5-10 years"
trace.Population_aged("Population aged hard coded as : 5-10 years")
df["Measure Type"] = "Travel time"
trace.Measure_Type("Measure Type hard coded as : Travel time")
df["Unit"] = "Minutes"
trace.Unit("Unit hard coded as : Minutes")

# -

#     Output table for distribution 2

# # +

tidy_distribution_2 = df[["Period", "LSOA Code", "Region of LA", "LA Code", "LA Name","Service users in LSOA",
                "Destination", "Population aged", "Mode of travel", "Destination avilable by mode of travel within 15 minutes",
                "Destination avilable by mode of travel within 30 minutes", "Destination avilable by mode of travel within 45 minutes",
                "Destination avilable by mode of travel within 60 minutes", "Percentage of users within 15 minutes by mode of transport", 
                "Percentage of users within 30 minutes by mode of transport", "Percentage of users within 45 minutes by mode of transport",
                "Percentage of users within 60 minutes by mode of transport", "Measure Type", "Unit", "Value"]]
tidy_distribution_2
trace.output()

# -

# ________________________________________________________________________________________________________________

# # Distribution: 3 
#
# #### Travel time, destination and origin indicators for Secondary schools by mode of travel, Lower Super Output Area (LSOA), England
#
#     Destination : Secondary School
#     Population Aged : 11-15 years

tabs = { tab.name: tab for tab in scraper.distributions[2].as_databaker() }
list(tabs)

# # +

for name, tab in tabs.items():
        columns=["Period", "LSOA Code", "Region of LA", "LA Code", "LA Name","Service users in LSOA",
                "Destination", "Population aged", "Mode of travel", "Destination 15 mins", "Destination 30 mins",
                "Destination 45 mins", "Destination 60 mins", "Percentage 15 mins", "Percentage 30 mins", 
                "Percentage 45 mins", "Percentage 60 mins", "Measure Type", "Unit"]
        trace.start(datasetTitle, tab, columns, scraper.distributions[2].downloadURL)
        
        if 'Metadata' in name:
            continue  
 
        year = name
        LSOA_code = tab.filter(contains_string('LSOA_code')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LSOA_Code("LSOA Code taken from cell reference A8 down")
        
        region =  tab.filter(contains_string('Region')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Region_of_LA("Region of LA taken from cell reference B8 down")
        
        la_code = tab.filter(contains_string('LA_Code')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LA_Code("LA Code taken from cell reference C8 down")
        
        la_name = tab.filter(contains_string('LA_Name')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LA_Name("LA Name taken from cell reference D8 down")
        
        service_users =  tab.filter(contains_string('SS_pop')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Service_users_in_LSOA("Service users in LSOA taken from cell reference D8 down")
       
        ## Starting with Travel time in minutes to nearest secondary school by PT/walk ##
        mode_of_travel = "PT/Walk"
        trace.Mode_of_travel("Mode of travel hardcoded as PT/Walk, transforming the data data related.")
        
        travel_within_15_mins = tab.filter(contains_string('SSPT15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of secondary schools available by PT/walk within 15 minutes (PSPT15n) taken from cell reference G8 down")
        
        travel_within_30_mins = tab.filter(contains_string('SSPT30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of secondary schools available by PT/walk within 30 minutes (SSPT30n) taken from cell reference H8 down")

        travel_within_45_mins = tab.filter(contains_string('SSPT45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of secondary schools available by PT/walk within 45 minutes (SSPT45n) taken from cell reference I8 down")

        travel_within_60_mins = tab.filter(contains_string('SSPT60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_60_mins("Number of secondary schools available by PT/walk within 60 minutes (SSPT60n) taken from cell reference J8 down")

        percentage_users_15_mins = tab.filter(contains_string('SSPT15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of secondary schools available by PT/walk (SSPT15pct) taken from cell reference K8 down")
        
        percentage_users_30_mins = tab.filter(contains_string('SSPT30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of secondary schools available by PT/walk (SSPT30pct) taken from cell reference L8 down")
        
        percentage_users_45_mins = tab.filter(contains_string('SSPT45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of secondary schools available by PT/walk (SSPT45pct) taken from cell reference M8 down")

        percentage_users_60_mins = tab.filter(contains_string('SSPT60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of secondary schools available by PT/walk (SSPT60pct) taken from cell reference N8 down")
        
        #Observations = Travel time in minutes to nearest secondary schools by PT/walk       
        observations = tab.filter(contains_string('SSPTt')).shift(0,1).expand(DOWN).is_not_blank()
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="walk.html") 
        trace.store("combined_dataframe_3", tidy_sheet.topandas())
        
                    
        ##   Next Travel time in minutes to nearest secondary schools by cycle  ##
        mode_of_travel = "Cycle"
        trace.Mode_of_travel("Mode of travel hardcoded as Cycle, transforming the data data related.")

        travel_within_15_mins = tab.filter(contains_string('SSCyc15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of secondary schools available by cycle within 15 minutes (SSCyc15n) taken from cell reference P8 down")
        
        travel_within_30_mins = tab.filter(contains_string('SSCyc30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of secondary schools available by cycle within 30 minutes (SSCyc30n) taken from cell reference Q8 down")

        travel_within_45_mins = tab.filter(contains_string('SSCyc45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of secondary schools available by cycle within 45 minutes (SSCyc45n) taken from cell reference R8 down")

        travel_within_60_mins = tab.filter(contains_string('SSCyc60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_60_mins("Number of secondary schools available by cycle within 60 minutes (SSCyc60n) taken from cell reference S8 down")
        
        percentage_users_15_mins = tab.filter(contains_string('SSCyc15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of secondary schools available by cycle (SSCyc15pct) taken from cell reference T8 down")

        percentage_users_30_mins = tab.filter(contains_string('SSCyc30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of secondary schools available by cycle (SSCyc30pct) taken from cell reference U8 down")

        percentage_users_45_mins = tab.filter(contains_string('SSCyc45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of secondary schools available by cycle (SSCyc45pct) taken from cell reference V8 down")
        
        percentage_users_60_mins = tab.filter(contains_string('SSCyc60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of secondary schools available by cycle (SSCyc60pct) taken from cell reference W8 down")

        #Observations = Travel time in minutes to nearest secondary schools by cycle (SSCyct down)   
        observations = tab.filter(contains_string('SSCyct')).shift(0,1).expand(DOWN).is_not_blank()
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),                
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="cycle.html") 
        trace.store("combined_dataframe_3", tidy_sheet.topandas())
        
        ##   Next Travel time in minutes to nearest secondary schools by Car  ##
        mode_of_travel = "Car"
        trace.Mode_of_travel("Mode of travel hardcoded as Car, transforming the data data related.")

        travel_within_15_mins = tab.filter(contains_string('SSCar15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of secondary schools available by car within 15 minutes (SSCar15n) taken from cell reference Y8 down")
 
        travel_within_30_mins = tab.filter(contains_string('SSCar30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of secondary schools available by car within 30 minutes (SSCar30n) taken from cell reference Z8 down")
 
        travel_within_45_mins = tab.filter(contains_string('SSCar45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of secondary schools available by car within 45 minutes (SSCar45n) taken from cell reference AA8 down")
 
        travel_within_60_mins = tab.filter(contains_string('SSCar60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of secondary schools available by car within 45 minutes (SSCar45n) taken from cell reference AB8 down")
 
        percentage_users_15_mins = tab.filter(contains_string('SSCar15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of secondary schools available by car (SSCar15pct) taken from cell reference AC8 down")
        
        percentage_users_30_mins = tab.filter(contains_string('SSCar30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of secondary schools available by car (SSCar30pct) taken from cell reference AD8 down")

        percentage_users_45_mins = tab.filter(contains_string('SSCar45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of secondary schools available by car (SSCar45pct) taken from cell reference AD8 down")

        percentage_users_60_mins = tab.filter(contains_string('SSCar60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of secondary schools available by car (SSCar60pct) taken from cell reference AE8 down")

        #Observations = Travel time in minutes to nearest secondary schools by car        
        observations = tab.filter(contains_string('SSCart')).shift(0,1).expand(DOWN).is_not_blank()
        
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),                HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),                
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="car.html") 
        trace.store("combined_dataframe_3", tidy_sheet.topandas())        
        
print("DISTRIBUTION 3 DONE")


# # +

df = trace.combine_and_trace(datasetTitle, "combined_dataframe_3")
df.rename(columns={'OBS' : 'Value'}, inplace=True)
df["Destination"] = "Secondary School"
trace.Destination("Destination hard coded as : Secondary School")
df["Population aged"] = "11-15 years"
trace.Population_aged("Population aged hard coded as : 11-15 years")
df["Measure Type"] = "Travel time"
trace.Measure_Type("Measure Type hard coded as : Travel time")
df["Unit"] = "Minutes"
trace.Unit("Unit hard coded as : Minutes")

# -

#     Output table for distribution 3 

# # +

tidy_distribution_3 = df[["Period", "LSOA Code", "Region of LA", "LA Code", "LA Name","Service users in LSOA",
                "Destination", "Population aged", "Mode of travel", "Destination avilable by mode of travel within 15 minutes",
                "Destination avilable by mode of travel within 30 minutes", "Destination avilable by mode of travel within 45 minutes",
                "Destination avilable by mode of travel within 60 minutes", "Percentage of users within 15 minutes by mode of transport", 
                "Percentage of users within 30 minutes by mode of transport", "Percentage of users within 45 minutes by mode of transport",
                "Percentage of users within 60 minutes by mode of transport", "Measure Type", "Unit", "Value"]]
tidy_distribution_3
trace.output()

# -

# # Distribution: 4
#
# ####  Travel time, destination and origin indicators for Further education by mode of travel, Lower Super Output Area (LSOA), England 
#     Destination : Further education
#     Population Aged : 16-19 years

tabs = { tab.name: tab for tab in scraper.distributions[3].as_databaker() }
list(tabs)

# # +

for name, tab in tabs.items():
        columns=["Period", "LSOA Code", "Region of LA", "LA Code", "LA Name","Service users in LSOA",
                "Destination", "Population aged", "Mode of travel", "Destination 15 mins", "Destination 30 mins",
                "Destination 45 mins", "Destination 60 mins", "Percentage 15 mins", "Percentage 30 mins", 
                "Percentage 45 mins", "Percentage 60 mins", "Measure Type", "Unit"]
        trace.start(datasetTitle, tab, columns, scraper.distributions[3].downloadURL)
        
        if 'Metadata' in name:
            continue  
 
        year = name
        LSOA_code = tab.filter(contains_string('LSOA_code')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LSOA_Code("LSOA Code taken from cell reference A8 down")
        
        region =  tab.filter(contains_string('Region')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Region_of_LA("Region of LA taken from cell reference B8 down")
        
        la_code = tab.filter(contains_string('LA_Code')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LA_Code("LA Code taken from cell reference C8 down")
        
        la_name = tab.filter(contains_string('LA_Name')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LA_Name("LA Name taken from cell reference D8 down")
        
        service_users =  tab.filter(contains_string('FE_pop')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Service_users_in_LSOA("Service users in LSOA taken from cell reference D8 down")
            
       
        ## Starting with Travel time in minutes to nearest further education colleges by PT/walk ##
        mode_of_travel = "PT/Walk"
        trace.Mode_of_travel("Mode of travel hardcoded as PT/Walk, transforming the data data related.")
        
        travel_within_15_mins = tab.filter(contains_string('FEPT15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of further education colleges available by PT/walk within 15 minutes (FEPT15n) taken from cell reference G8 down")
        
        travel_within_30_mins = tab.filter(contains_string('FEPT30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of further education colleges available by PT/walk within 30 minutes (FEPT30n) taken from cell reference H8 down")

        travel_within_45_mins = tab.filter(contains_string('FEPT45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of further education colleges available by PT/walk within 45 minutes (FEPT45n) taken from cell reference I8 down")

        travel_within_60_mins = tab.filter(contains_string('FEPT60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_60_mins("Number of further education colleges available by PT/walk within 60 minutes (FEPT60n) taken from cell reference J8 down")

        percentage_users_15_mins = tab.filter(contains_string('FEPT15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of further education colleges available by PT/walk (FEPT15pct) taken from cell reference K8 down")
        
        percentage_users_30_mins = tab.filter(contains_string('FEPT30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of further education colleges available by PT/walk (FEPT30pct) taken from cell reference L8 down")
        
        percentage_users_45_mins = tab.filter(contains_string('FEPT45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of further education colleges available by PT/walk (FEPT45pct) taken from cell reference M8 down")

        percentage_users_60_mins = tab.filter(contains_string('FEPT60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of further education colleges available by PT/walk (FEPT60pct) taken from cell reference N8 down")
        
        #Observations = Travel time in minutes to nearest further education colleges by PT/walk       
        observations = tab.filter(contains_string('FEPTt')).shift(0,1).expand(DOWN).is_not_blank()
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="walk.html") 
        trace.store("combined_dataframe_4", tidy_sheet.topandas())
        
                    
        ##   Next Travel time in minutes to nearest further education colleges by cycle  ##
        mode_of_travel = "Cycle"
        trace.Mode_of_travel("Mode of travel hardcoded as Cycle, transforming the data data related.")

        travel_within_15_mins = tab.filter(contains_string('FECyc15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of sfurther education colleges available by cycle within 15 minutes (FECyc15n) taken from cell reference P8 down")
        
        travel_within_30_mins = tab.filter(contains_string('FECyc30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of further education colleges available by cycle within 30 minutes (FECyc30n) taken from cell reference Q8 down")

        travel_within_45_mins = tab.filter(contains_string('FECyc45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of further education colleges available by cycle within 45 minutes (FECyc45n) taken from cell reference R8 down")

        travel_within_60_mins = tab.filter(contains_string('FECyc60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_60_mins("Number of further education colleges available by cycle within 60 minutes (FECyc60n) taken from cell reference S8 down")
        
        percentage_users_15_mins = tab.filter(contains_string('FECyc15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of further education colleges available by cycle (FECyc15pct) taken from cell reference T8 down")

        percentage_users_30_mins = tab.filter(contains_string('FECyc30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of further education colleges available by cycle (FECyc30pct) taken from cell reference U8 down")

        percentage_users_45_mins = tab.filter(contains_string('FECyc45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of further education colleges available by cycle (FECyc45pct) taken from cell reference V8 down")
        
        percentage_users_60_mins = tab.filter(contains_string('FECyc60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of further education colleges available by cycle (FECyc60pct) taken from cell reference W8 down")

        #Observations = Travel time in minutes to nearest further education colleges by cycle (SSCyct down)   
        observations = tab.filter(contains_string('FECyct')).shift(0,1).expand(DOWN).is_not_blank()
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),                
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="cycle.html") 
        trace.store("combined_dataframe_4", tidy_sheet.topandas())
        
        ##   Next Travel time in minutes to nearest further education colleges by Car  ##
        mode_of_travel = "Car"
        trace.Mode_of_travel("Mode of travel hardcoded as Car, transforming the data data related.")

        travel_within_15_mins = tab.filter(contains_string('FECar15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of further education colleges available by car within 15 minutes (FECar15n) taken from cell reference Y8 down")
 
        travel_within_30_mins = tab.filter(contains_string('FECar30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of further education colleges available by car within 30 minutes (FECar30n) taken from cell reference Z8 down")
 
        travel_within_45_mins = tab.filter(contains_string('FECar45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of further education colleges available by car within 45 minutes (FECar45n) taken from cell reference AA8 down")
 
        travel_within_60_mins = tab.filter(contains_string('FECar60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of further education colleges available by car within 45 minutes (FECar45n) taken from cell reference AB8 down")
 
        percentage_users_15_mins = tab.filter(contains_string('FECar15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of further education colleges available by car (FECar15pct) taken from cell reference AC8 down")
        
        percentage_users_30_mins = tab.filter(contains_string('FECar30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of further education colleges available by car (FECar30pct) taken from cell reference AD8 down")

        percentage_users_45_mins = tab.filter(contains_string('FECar45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of further education colleges available by car (FECar45pct) taken from cell reference AD8 down")

        percentage_users_60_mins = tab.filter(contains_string('FECar60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of further education colleges available by car (FECar60pct) taken from cell reference AE8 down")

        #Observations = Travel time in minutes to nearest secondary schools by car        
        observations = tab.filter(contains_string('FECart')).shift(0,1).expand(DOWN).is_not_blank()
        
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),                HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),                
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="car.html") 
        trace.store("combined_dataframe_4", tidy_sheet.topandas())        
        
print("DISTRIBUTION 4 DONE")


# # +

df = trace.combine_and_trace(datasetTitle, "combined_dataframe_4")
df.rename(columns={'OBS' : 'Value'}, inplace=True)
df["Destination"] = "Further education colleges"
trace.Destination("Destination hard coded as : Further education collegesl")
df["Population aged"] = "16-19 years"
trace.Population_aged("Population aged hard coded as : 16-19 years")
df["Measure Type"] = "Travel time"
trace.Measure_Type("Measure Type hard coded as : Travel time")
df["Unit"] = "Minutes"
trace.Unit("Unit hard coded as : Minutes")

# -

#     Output table for distribution 4

# # +

tidy_distribution_4 = df[["Period", "LSOA Code", "Region of LA", "LA Code", "LA Name","Service users in LSOA",
                "Destination", "Population aged", "Mode of travel", "Destination avilable by mode of travel within 15 minutes",
                "Destination avilable by mode of travel within 30 minutes", "Destination avilable by mode of travel within 45 minutes",
                "Destination avilable by mode of travel within 60 minutes", "Percentage of users within 15 minutes by mode of transport", 
                "Percentage of users within 30 minutes by mode of transport", "Percentage of users within 45 minutes by mode of transport",
                "Percentage of users within 60 minutes by mode of transport", "Measure Type", "Unit", "Value"]]
tidy_distribution_4
trace.output()

# -

# # Distribution: 5
#
# #### Travel time, destination and origin indicators for GPs by mode of travel, Lower Super Output Area (LSOA), England       
#     Destination : GPs
#     Population Aged : all households 

tabs = { tab.name: tab for tab in scraper.distributions[4].as_databaker() }
list(tabs)

# # +

for name, tab in tabs.items():
        columns=["Period", "LSOA Code", "Region of LA", "LA Code", "LA Name","Service users in LSOA",
                "Destination", "Population aged", "Mode of travel", "Destination 15 mins", "Destination 30 mins",
                "Destination 45 mins", "Destination 60 mins", "Percentage 15 mins", "Percentage 30 mins", 
                "Percentage 45 mins", "Percentage 60 mins", "Measure Type", "Unit"]
        trace.start(datasetTitle, tab, columns, scraper.distributions[4].downloadURL)
        
        if 'Metadata' in name:
            continue  
 
        year = name
        LSOA_code = tab.filter(contains_string('LSOA_code')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LSOA_Code("LSOA Code taken from cell reference A8 down")
        
        region =  tab.filter(contains_string('Region')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Region_of_LA("Region of LA taken from cell reference B8 down")
        
        la_code = tab.filter(contains_string('LA_Code')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LA_Code("LA Code taken from cell reference C8 down")
        
        la_name = tab.filter(contains_string('LA_Name')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LA_Name("LA Name taken from cell reference D8 down")
        
        service_users =  tab.filter(contains_string('GP_pop')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Service_users_in_LSOA("Service users in LSOA taken from cell reference D8 down")
       
        ## Starting with Travel time in minutes to nearest GP by PT/walk ##
        mode_of_travel = "PT/Walk"
        trace.Mode_of_travel("Mode of travel hardcoded as PT/Walk, transforming the data data related.")
        
        travel_within_15_mins = tab.filter(contains_string('GPPT15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of GPs available by PT/walk within 15 minutes (GPPT15n) taken from cell reference G8 down")
        
        travel_within_30_mins = tab.filter(contains_string('GPPT30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of GPs available by PT/walk within 30 minutes (GPPT30n) taken from cell reference H8 down")

        travel_within_45_mins = tab.filter(contains_string('GPPT45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of GPs available by PT/walk within 45 minutes (FEPT45n) taken from cell reference I8 down")

        travel_within_60_mins = tab.filter(contains_string('GPPT60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_60_mins("Number of GPs available by PT/walk within 60 minutes (GPPT60n) taken from cell reference J8 down")

        percentage_users_15_mins = tab.filter(contains_string('GPPT15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of GPs available by PT/walk (GPPT15pct) taken from cell reference K8 down")
        
        percentage_users_30_mins = tab.filter(contains_string('GPPT30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of GPs available by PT/walk (GPPT30pct) taken from cell reference L8 down")
        
        percentage_users_45_mins = tab.filter(contains_string('GPPT45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of GPs available by PT/walk (GPPT45pct) taken from cell reference M8 down")

        percentage_users_60_mins = tab.filter(contains_string('GPPT60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of GPs available by PT/walk (GPPT60pct) taken from cell reference N8 down")
        
        #Observations = Travel time in minutes to nearest GPs by PT/walk       
        observations = tab.filter(contains_string('GPPTt')).shift(0,1).expand(DOWN).is_not_blank()
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="walk.html") 
        trace.store("combined_dataframe_5", tidy_sheet.topandas())
                    
        ##   Next Travel time in minutes to nearest GPs by cycle  ##
        mode_of_travel = "Cycle"
        trace.Mode_of_travel("Mode of travel hardcoded as Cycle, transforming the data data related.")

        travel_within_15_mins = tab.filter(contains_string('GPCyc15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number oF GPs available by cycle within 15 minutes (GPCyc15n) taken from cell reference P8 down")
        
        travel_within_30_mins = tab.filter(contains_string('GPCyc30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of GPs available by cycle within 30 minutes (GPCyc30n) taken from cell reference Q8 down")

        travel_within_45_mins = tab.filter(contains_string('GPCyc45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of GPs available by cycle within 45 minutes (GPCyc45n) taken from cell reference R8 down")

        travel_within_60_mins = tab.filter(contains_string('GPCyc60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_60_mins("Number of GPs available by cycle within 60 minutes (GPCyc60n) taken from cell reference S8 down")
        
        percentage_users_15_mins = tab.filter(contains_string('GPCyc15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of GPs available by cycle (GPCyc15pct) taken from cell reference T8 down")

        percentage_users_30_mins = tab.filter(contains_string('GPCyc30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of GPs available by cycle (GPCyc30pct) taken from cell reference U8 down")

        percentage_users_45_mins = tab.filter(contains_string('GPCyc45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of GPs available by cycle (GPCyc45pct) taken from cell reference V8 down")
        
        percentage_users_60_mins = tab.filter(contains_string('GPCyc60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of GPs available by cycle (GPCyc60pct) taken from cell reference W8 down")

        #Observations = Travel time in minutes to nearesT GPs by cycle (SSCyct down)   
        observations = tab.filter(contains_string('GPCyct')).shift(0,1).expand(DOWN).is_not_blank()
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),                
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="cycle.html") 
        trace.store("combined_dataframe_4", tidy_sheet.topandas())
        
        ##   Next Travel time in minutes to nearest GPs by Car  ##
        mode_of_travel = "Car"
        trace.Mode_of_travel("Mode of travel hardcoded as Car, transforming the data data related.")

        travel_within_15_mins = tab.filter(contains_string('GPCar15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of GPs available by car within 15 minutes (GPCar15n) taken from cell reference Y8 down")
 
        travel_within_30_mins = tab.filter(contains_string('GPCar30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of GPs available by car within 30 minutes (GPCar30n) taken from cell reference Z8 down")
 
        travel_within_45_mins = tab.filter(contains_string('GPCar45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of GPs available by car within 45 minutes (GPCar45n) taken from cell reference AA8 down")
 
        travel_within_60_mins = tab.filter(contains_string('GPCar60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of GPs available by car within 45 minutes (GPCar45n) taken from cell reference AB8 down")
 
        percentage_users_15_mins = tab.filter(contains_string('GPCar15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of GPs available by car (GPCar15pct) taken from cell reference AC8 down")
        
        percentage_users_30_mins = tab.filter(contains_string('GPCar30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of GPs available by car (GPCar30pct) taken from cell reference AD8 down")

        percentage_users_45_mins = tab.filter(contains_string('GPCar45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of GPs available by car (GPCar45pct) taken from cell reference AD8 down")

        percentage_users_60_mins = tab.filter(contains_string('GPCar60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of GPs available by car (GPCar60pct) taken from cell reference AE8 down")

        #Observations = Travel time in minutes to nearest GPs by car        
        observations = tab.filter(contains_string('GPCart')).shift(0,1).expand(DOWN).is_not_blank()
        
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),                HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),                
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="car.html") 
        trace.store("combined_dataframe_5", tidy_sheet.topandas())        
        
print("DISTRIBUTION 5 DONE")
# -

df = trace.combine_and_trace(datasetTitle, "combined_dataframe_5")
df.rename(columns={'OBS' : 'Value'}, inplace=True)
df["Destination"] = "GPs"
trace.Destination("Destination hard coded as : GPs")
df["Population aged"] = "all households"
trace.Population_aged("Population aged hard coded as : all households")
df["Measure Type"] = "Travel time"
trace.Measure_Type("Measure Type hard coded as : Travel time")
df["Unit"] = "Minutes"
trace.Unit("Unit hard coded as : Minutes")


# # +

tidy_distribution_5 = df[["Period", "LSOA Code", "Region of LA", "LA Code", "LA Name","Service users in LSOA",
                "Destination", "Population aged", "Mode of travel", "Destination avilable by mode of travel within 15 minutes",
                "Destination avilable by mode of travel within 30 minutes", "Destination avilable by mode of travel within 45 minutes",
                "Destination avilable by mode of travel within 60 minutes", "Percentage of users within 15 minutes by mode of transport", 
                "Percentage of users within 30 minutes by mode of transport", "Percentage of users within 45 minutes by mode of transport",
                "Percentage of users within 60 minutes by mode of transport", "Measure Type", "Unit", "Value"]]
tidy_distribution_5
trace.output()

# -

# # Distribution: 6
#
# ####     Travel time, destination and origin indicators for Hospitals by mode of travel, Lower Super Output Area (LSOA), England 
#     Destination : Hospitals
#     Population Aged : number of households in LSOA 

# # +
#tabs = { tab.name: tab for tab in scraper.distributions[5].as_databaker() }
#list(tabs)

# # +

for name, tab in tabs.items():
        columns=["Period", "LSOA Code", "Region of LA", "LA Code", "LA Name","Service users in LSOA",
                "Destination", "Population aged", "Mode of travel", "Destination 15 mins", "Destination 30 mins",
                "Destination 45 mins", "Destination 60 mins", "Percentage 15 mins", "Percentage 30 mins", 
                "Percentage 45 mins", "Percentage 60 mins", "Measure Type", "Unit"]
        trace.start(datasetTitle, tab, columns, scraper.distributions[5].downloadURL)
        
        if 'Metadata' in name:
            continue  
 
        year = name
        LSOA_code = tab.filter(contains_string('LSOA_code')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LSOA_Code("LSOA Code taken from cell reference A8 down")
        
        region =  tab.filter(contains_string('Region')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Region_of_LA("Region of LA taken from cell reference B8 down")
        
        la_code = tab.filter(contains_string('LA_Code')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LA_Code("LA Code taken from cell reference C8 down")
        
        la_name = tab.filter(contains_string('LA_Name')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LA_Name("LA Name taken from cell reference D8 down")
        
        service_users =  tab.filter(contains_string('Hosp_pop')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Service_users_in_LSOA("Service users in LSOA taken from cell reference D8 down")
       
        ## Starting with Travel time in minutes to nearest Hospital by PT/walk ##
        mode_of_travel = "PT/Walk"
        trace.Mode_of_travel("Mode of travel hardcoded as PT/Walk, transforming the data data related.")
        
        travel_within_15_mins = tab.filter(contains_string('HospPT15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of Hospital available by PT/walk within 15 minutes (HospPT15n) taken from cell reference G8 down")

        travel_within_30_mins = tab.filter(contains_string('HospPT30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of Hospital available by PT/walk within 30 minutes (HospPT30n) taken from cell reference H8 down")

        travel_within_45_mins = tab.filter(contains_string('HospPT45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of Hospital available by PT/walk within 45 minutes (HospPT45n) taken from cell reference I8 down")

        travel_within_60_mins = tab.filter(contains_string('HospPT60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_60_mins("Number of Hospital available by PT/walk within 60 minutes (HospPT60n) taken from cell reference J8 down")

        percentage_users_15_mins = tab.filter(contains_string('HospPT15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of Hospital available by PT/walk (HospPT15pct) taken from cell reference K8 down")
        
        percentage_users_30_mins = tab.filter(contains_string('HospPT30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of Hospital available by PT/walk (HospPT30pct) taken from cell reference L8 down")
        
        percentage_users_45_mins = tab.filter(contains_string('HospPT45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of Hospital available by PT/walk (HospPT45pct) taken from cell reference M8 down")

        percentage_users_60_mins = tab.filter(contains_string('HospPT60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of Hospital available by PT/walk (HospPT60pct) taken from cell reference N8 down")
        
        #Observations = Travel time in minutes to nearest Hospital by PT/walk       
        observations = tab.filter(contains_string('HospPTt')).shift(0,1).expand(DOWN).is_not_blank()
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="walk.html") 
        trace.store("combined_dataframe_6", tidy_sheet.topandas())
                    
        ##   Next Travel time in minutes to nearest Hospital by cycle  ##
        mode_of_travel = "Cycle"
        trace.Mode_of_travel("Mode of travel hardcoded as Cycle, transforming the data data related.")

        travel_within_15_mins = tab.filter(contains_string('HospCyc15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of Hospials available by cycle within 15 minutes (HospCyc15n) taken from cell reference P8 down")
        
        travel_within_30_mins = tab.filter(contains_string('HospCyc30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of Hospital available by cycle within 30 minutes (HospCyc30n) taken from cell reference Q8 down")

        travel_within_45_mins = tab.filter(contains_string('HospCyc45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of Hospials available by cycle within 45 minutes (HospCyc45n) taken from cell reference R8 down")

        travel_within_60_mins = tab.filter(contains_string('HospCyc60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_60_mins("Number of Hospials available by cycle within 60 minutes (HospCyc60n) taken from cell reference S8 down")
        
        percentage_users_15_mins = tab.filter(contains_string('HospCyc15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of Hospials available by cycle (HospCyc15pct) taken from cell reference T8 down")

        percentage_users_30_mins = tab.filter(contains_string('HospCyc30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of Hospials available by cycle (HospCyc30pct) taken from cell reference U8 down")

        percentage_users_45_mins = tab.filter(contains_string('HospCyc45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of Hospials available by cycle (HospCyc45pct) taken from cell reference V8 down")
        
        percentage_users_60_mins = tab.filter(contains_string('HospCyc60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of Hospials available by cycle (HospCyc60pct) taken from cell reference W8 down")

        #Observations = Travel time in minutes to nearesT Hosp by cycle (HospCyct down)   
        observations = tab.filter(contains_string('HospCyct')).shift(0,1).expand(DOWN).is_not_blank()
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),                
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="cycle.html") 
        trace.store("combined_dataframe_6", tidy_sheet.topandas())
        
        ##   Next Travel time in minutes to nearest Hospital by Car  ##
        mode_of_travel = "Car"
        trace.Mode_of_travel("Mode of travel hardcoded as Car, transforming the data data related.")

        travel_within_15_mins = tab.filter(contains_string('HospCar15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of Hospital available by car within 15 minutes (HospCar15n) taken from cell reference Y8 down")
 
        travel_within_30_mins = tab.filter(contains_string('HospCar30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of Hospital available by car within 30 minutes (HospCar30n) taken from cell reference Z8 down")
 
        travel_within_45_mins = tab.filter(contains_string('HospCar45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of Hospital available by car within 45 minutes (HospCar45n) taken from cell reference AA8 down")
 
        travel_within_60_mins = tab.filter(contains_string('HospCar60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of Hospital available by car within 45 minutes (HospCar45n) taken from cell reference AB8 down")
 
        percentage_users_15_mins = tab.filter(contains_string('HospCar15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of Hospital available by car (HospCar15pct) taken from cell reference AC8 down")
        
        percentage_users_30_mins = tab.filter(contains_string('HospCar30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of Hospital available by car (HospCar30pct) taken from cell reference AD8 down")

        percentage_users_45_mins = tab.filter(contains_string('HospCar45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of Hospital available by car (HospCar45pct) taken from cell reference AD8 down")

        percentage_users_60_mins = tab.filter(contains_string('HospCar60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of Hospital available by car (HospCar60pct) taken from cell reference AE8 down")

        #Observations = Travel time in minutes to nearest Hospital by car        
        observations = tab.filter(contains_string('HospCart')).shift(0,1).expand(DOWN).is_not_blank()
        
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),                HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),                
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="car.html") 
        trace.store("combined_dataframe_6", tidy_sheet.topandas())        
        
print("DISTRIBUTION 6 DONE")


# # +

df = trace.combine_and_trace(datasetTitle, "combined_dataframe_6")
df.rename(columns={'OBS' : 'Value'}, inplace=True)
df["Destination"] = "Hospital"
trace.Destination("Destination hard coded as : Hospital")
df["Population aged"] = "number of households in LSOA"
trace.Population_aged("Population aged hard coded as : number of households in LSOA")
df["Measure Type"] = "Travel time"
trace.Measure_Type("Measure Type hard coded as : Travel time")
df["Unit"] = "Minutes"
trace.Unit("Unit hard coded as : Minutes")


# # +

tidy_distribution_6 = df[["Period", "LSOA Code", "Region of LA", "LA Code", "LA Name","Service users in LSOA",
                "Destination", "Population aged", "Mode of travel", "Destination avilable by mode of travel within 15 minutes",
                "Destination avilable by mode of travel within 30 minutes", "Destination avilable by mode of travel within 45 minutes",
                "Destination avilable by mode of travel within 60 minutes", "Percentage of users within 15 minutes by mode of transport", 
                "Percentage of users within 30 minutes by mode of transport", "Percentage of users within 45 minutes by mode of transport",
                "Percentage of users within 60 minutes by mode of transport", "Measure Type", "Unit", "Value"]]
tidy_distribution_6
trace.output()

# -

# # Distribution: 7
#
# ####     Travel time, destination and origin indicators for Food stores by mode of travel, Lower Super Output Area (LSOA), England 
#     Destination : Food Stores
#     Population Aged : number of households in LSOA

tabs = { tab.name: tab for tab in scraper.distributions[6].as_databaker() }
list(tabs)

# # +

for name, tab in tabs.items():
        columns=["Period", "LSOA Code", "Region of LA", "LA Code", "LA Name","Service users in LSOA",
                "Destination", "Population aged", "Mode of travel", "Destination 15 mins", "Destination 30 mins",
                "Destination 45 mins", "Destination 60 mins", "Percentage 15 mins", "Percentage 30 mins", 
                "Percentage 45 mins", "Percentage 60 mins", "Measure Type", "Unit"]
        trace.start(datasetTitle, tab, columns, scraper.distributions[6].downloadURL)
        
        if 'Metadata' in name:
            continue  
 
        year = name
        LSOA_code = tab.filter(contains_string('LSOA_code')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LSOA_Code("LSOA Code taken from cell reference A8 down")
        
        region =  tab.filter(contains_string('Region')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Region_of_LA("Region of LA taken from cell reference B8 down")
        
        la_code = tab.filter(contains_string('LA_Code')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LA_Code("LA Code taken from cell reference C8 down")
        
        la_name = tab.filter(contains_string('LA_Name')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LA_Name("LA Name taken from cell reference D8 down")
        
        service_users =  tab.filter(contains_string('Food_pop')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Service_users_in_LSOA("Service users in LSOA taken from cell reference D8 down")
       
        ## Starting with Travel time in minutes to nearest Food Stores by PT/walk ##
        mode_of_travel = "PT/Walk"
        trace.Mode_of_travel("Mode of travel hardcoded as PT/Walk, transforming the data data related.")
        
        travel_within_15_mins = tab.filter(contains_string('FoodPT15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of Food Stores available by PT/walk within 15 minutes (FoodPT15n) taken from cell reference G8 down")

        travel_within_30_mins = tab.filter(contains_string('FoodPT30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of Food Store available by PT/walk within 30 minutes (FoodPT30n) taken from cell reference H8 down")

        travel_within_45_mins = tab.filter(contains_string('FoodPT45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of Food Stores available by PT/walk within 45 minutes (FoodPT45n) taken from cell reference I8 down")

        travel_within_60_mins = tab.filter(contains_string('FoodPT60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_60_mins("Number of Food Stores available by PT/walk within 60 minutes (FoodPT60n) taken from cell reference J8 down")

        percentage_users_15_mins = tab.filter(contains_string('FoodPT15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of Food Stores available by PT/walk (FoodPT15pct) taken from cell reference K8 down")
        
        percentage_users_30_mins = tab.filter(contains_string('FoodPT30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of Food Stores available by PT/walk (FoodPT30pct) taken from cell reference L8 down")
        
        percentage_users_45_mins = tab.filter(contains_string('FoodPT45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of Food Stores available by PT/walk (FoodPT45pct) taken from cell reference M8 down")

        percentage_users_60_mins = tab.filter(contains_string('FoodPT60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of Food Stores available by PT/walk (FoodPT60pct) taken from cell reference N8 down")
        
        #Observations = Travel time in minutes to nearest Food Stores by PT/walk       
        observations = tab.filter(contains_string('FoodPTt')).shift(0,1).expand(DOWN).is_not_blank()
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="walk.html") 
        trace.store("combined_dataframe_7", tidy_sheet.topandas())
                    
        ##   Next Travel time in minutes to nearest Food Stores by cycle  ##
        mode_of_travel = "Cycle"
        trace.Mode_of_travel("Mode of travel hardcoded as Cycle, transforming the data data related.")

        travel_within_15_mins = tab.filter(contains_string('FoodCyc15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of Food Stores available by cycle within 15 minutes (FoodCyc15n) taken from cell reference P8 down")
        
        travel_within_30_mins = tab.filter(contains_string('FoodCyc30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of Food Stores available by cycle within 30 minutes (FoodCyc30n) taken from cell reference Q8 down")

        travel_within_45_mins = tab.filter(contains_string('FoodCyc45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of Food Stores available by cycle within 45 minutes (FoodCyc45n) taken from cell reference R8 down")

        travel_within_60_mins = tab.filter(contains_string('FoodCyc60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_60_mins("Number of Food Stores available by cycle within 60 minutes (FoodCyc60n) taken from cell reference S8 down")
        
        percentage_users_15_mins = tab.filter(contains_string('FoodCyc15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of Food Stores available by cycle (FoodCyc15pct) taken from cell reference T8 down")

        percentage_users_30_mins = tab.filter(contains_string('FoodCyc30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of Food Stores available by cycle (FoodCyc30pct) taken from cell reference U8 down")

        percentage_users_45_mins = tab.filter(contains_string('FoodCyc45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of Food Stores available by cycle (FoodCyc45pct) taken from cell reference V8 down")
        
        percentage_users_60_mins = tab.filter(contains_string('FoodCyc60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of Food Stores available by cycle (FoodCyc60pct) taken from cell reference W8 down")

        #Observations = Travel time in minutes to nearest Food by cycle (FoodCyct down)   
        observations = tab.filter(contains_string('FoodCyct')).shift(0,1).expand(DOWN).is_not_blank()
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),                
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="cycle.html") 
        trace.store("combined_dataframe_7", tidy_sheet.topandas())
        
        ##   Next Travel time in minutes to nearest Food Stores by Car  ##
        mode_of_travel = "Car"
        trace.Mode_of_travel("Mode of travel hardcoded as Car, transforming the data data related.")

        travel_within_15_mins = tab.filter(contains_string('FoodCar15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of Food Stores available by car within 15 minutes (FoodCar15n) taken from cell reference Y8 down")
 
        travel_within_30_mins = tab.filter(contains_string('FoodCar30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of Food Stores available by car within 30 minutes (FoodCar30n) taken from cell reference Z8 down")
 
        travel_within_45_mins = tab.filter(contains_string('FoodCar45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of Food Stores available by car within 45 minutes (FoodCar45n) taken from cell reference AA8 down")
 
        travel_within_60_mins = tab.filter(contains_string('FoodCar60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of Food Stores available by car within 45 minutes (FoodCar45n) taken from cell reference AB8 down")
 
        percentage_users_15_mins = tab.filter(contains_string('FoodCar15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of Food Stores available by car (FoodCar15pct) taken from cell reference AC8 down")
        
        percentage_users_30_mins = tab.filter(contains_string('FoodCar30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of Food Stores available by car (FoodCar30pct) taken from cell reference AD8 down")

        percentage_users_45_mins = tab.filter(contains_string('FoodCar45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of Food Stores available by car (FoodCar45pct) taken from cell reference AD8 down")

        percentage_users_60_mins = tab.filter(contains_string('FoodCar60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of Food Stores available by car (FoodCar60pct) taken from cell reference AE8 down")

        #Observations = Travel time in minutes to nearest Food Store by car        
        observations = tab.filter(contains_string('FoodCart')).shift(0,1).expand(DOWN).is_not_blank()
        
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),                HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),                
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="car.html") 
        trace.store("combined_dataframe_7", tidy_sheet.topandas())        
        
print("DISTRIBUTION 7 DONE")


# # + endofcell="--"

df = trace.combine_and_trace(datasetTitle, "combined_dataframe_7")
df.rename(columns={'OBS' : 'Value'}, inplace=True)
df["Destination"] = "Food Stores"
trace.Destination("Destination hard coded as : Food Stores")
df["Population aged"] = "number of households in LSOA"
trace.Population_aged("Population aged hard coded as : number of households in LSOA")
df["Measure Type"] = "Travel time"
trace.Measure_Type("Measure Type hard coded as : Travel time")
df["Unit"] = "Minutes"
trace.Unit("Unit hard coded as : Minutes")
"""
# --

# +

tidy_distribution_7 = df[["Period", "LSOA Code", "Region of LA", "LA Code", "LA Name","Service users in LSOA",
                "Destination", "Population aged", "Mode of travel", "Destination avilable by mode of travel within 15 minutes",
                "Destination avilable by mode of travel within 30 minutes", "Destination avilable by mode of travel within 45 minutes",
                "Destination avilable by mode of travel within 60 minutes", "Percentage of users within 15 minutes by mode of transport", 
                "Percentage of users within 30 minutes by mode of transport", "Percentage of users within 45 minutes by mode of transport",
                "Percentage of users within 60 minutes by mode of transport", "Measure Type", "Unit", "Value"]]
tidy_distribution_7
trace.output()

# -



# # Distribution: 8 
#
# #### Travel time, destination and origin indicators for town centres by mode of travel, Lower Super Output Area (LSOA), England     
#     Destination : Town centres
#     Population Aged : number of households in LSOA

tabs = { tab.name: tab for tab in scraper.distributions[7].as_databaker() }
list(tabs)

# +

for name, tab in tabs.items():
        columns=["Period", "LSOA Code", "Region of LA", "LA Code", "LA Name","Service users in LSOA",
                "Destination", "Population aged", "Mode of travel", "Destination 15 mins", "Destination 30 mins",
                "Destination 45 mins", "Destination 60 mins", "Percentage 15 mins", "Percentage 30 mins", 
                "Percentage 45 mins", "Percentage 60 mins", "Measure Type", "Unit"]
        trace.start(datasetTitle, tab, columns, scraper.distributions[7].downloadURL)
        
        if 'Metadata' in name:
            continue  
 
        year = name
        LSOA_code = tab.filter(contains_string('LSOA_code')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LSOA_Code("LSOA Code taken from cell reference A8 down")
        
        region =  tab.filter(contains_string('Region')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Region_of_LA("Region of LA taken from cell reference B8 down")
        
        la_code = tab.filter(contains_string('LA_Code')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LA_Code("LA Code taken from cell reference C8 down")
        
        #la_name = tab.filter(contains_string('LA_Name')).shift(0,1).expand(DOWN).is_not_blank()
        #trace.LA_Name("LA Name taken from cell reference D8 down")
        
        #if '2015_REVISED' in name:
            #la_name = "Will need matched up"
            #trace.LA_Name("LA Name will need matched up, not listed in this tab for some reason")
        
        service_users =  tab.filter(contains_string('Town_pop')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Service_users_in_LSOA("Service users in LSOA taken from cell reference D8 down")
       
        ## Starting with Travel time in minutes to nearest Town centres by PT/walk ##
        mode_of_travel = "PT/Walk"
        trace.Mode_of_travel("Mode of travel hardcoded as PT/Walk, transforming the data data related.")
        
        travel_within_15_mins = tab.filter(contains_string('TownPT15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of Town centres available by PT/walk within 15 minutes (FoodPT15n) taken from cell reference G8 down")

        travel_within_30_mins = tab.filter(contains_string('TownPT30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of Town centres available by PT/walk within 30 minutes (TownPT30n) taken from cell reference H8 down")

        travel_within_45_mins = tab.filter(contains_string('TownPT45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of Town centres available by PT/walk within 45 minutes (TownPT45n) taken from cell reference I8 down")

        travel_within_60_mins = tab.filter(contains_string('TownPT60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_60_mins("Number of Town centres available by PT/walk within 60 minutes (TownPT60n) taken from cell reference J8 down")

        percentage_users_15_mins = tab.filter(contains_string('TownPT15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of Town centres available by PT/walk (TownPT15pct) taken from cell reference K8 down")
        
        percentage_users_30_mins = tab.filter(contains_string('TownPT30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of Town centres available by PT/walk (TownPT30pct) taken from cell reference L8 down")
        
        percentage_users_45_mins = tab.filter(contains_string('TownPT45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of Town centres available by PT/walk (TownPT45pct) taken from cell reference M8 down")

        percentage_users_60_mins = tab.filter(contains_string('TownPT60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of Town centres available by PT/walk (TownPT60pct) taken from cell reference N8 down")
        
        #Observations = Travel time in minutes to nearest Town by PT/walk       
        observations = tab.filter(contains_string('TownPTt')).shift(0,1).expand(DOWN).is_not_blank()
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
           #HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="walk.html") 
        trace.store("combined_dataframe_8", tidy_sheet.topandas())
                    
        ##   Next Travel time in minutes to nearest Town centres by cycle  ##
        mode_of_travel = "Cycle"
        trace.Mode_of_travel("Mode of travel hardcoded as Cycle, transforming the data data related.")

        travel_within_15_mins = tab.filter(contains_string('TownCyc15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of Town centres available by cycle within 15 minutes (TownCyc15n) taken from cell reference P8 down")
        
        travel_within_30_mins = tab.filter(contains_string('TownCyc30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of Town centres available by cycle within 30 minutes (TownCyc30n) taken from cell reference Q8 down")

        travel_within_45_mins = tab.filter(contains_string('TownCyc45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of Town centres available by cycle within 45 minutes (TownCyc45n) taken from cell reference R8 down")

        travel_within_60_mins = tab.filter(contains_string('TownCyc60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_60_mins("Number of Town centres available by cycle within 60 minutes (TownCyc60n) taken from cell reference S8 down")
        
        percentage_users_15_mins = tab.filter(contains_string('TownCyc15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of Town centres available by cycle (TownCyc15pct) taken from cell reference T8 down")

        percentage_users_30_mins = tab.filter(contains_string('TownCyc30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of Town centres available by cycle (TownCyc30pct) taken from cell reference U8 down")

        percentage_users_45_mins = tab.filter(contains_string('TownCyc45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of Town centres available by cycle (TownCyc45pct) taken from cell reference V8 down")
        
        percentage_users_60_mins = tab.filter(contains_string('TownCyc60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of Town centres available by cycle (TownCyc60pct) taken from cell reference W8 down")

        #Observations = Travel time in minutes to nearest Town centres by cycle (TownCyct down)   
        observations = tab.filter(contains_string('TownCyct')).shift(0,1).expand(DOWN).is_not_blank()
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            #HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),                
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="cycle.html") 
        trace.store("combined_dataframe_8", tidy_sheet.topandas())
        
        ##   Next Travel time in minutes to nearest Town centres by Car  ##
        mode_of_travel = "Car"
        trace.Mode_of_travel("Mode of travel hardcoded as Car, transforming the data data related.")

        travel_within_15_mins = tab.filter(contains_string('TownCar15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of Town centres available by car within 15 minutes (TownCar15n) taken from cell reference Y8 down")
 
        travel_within_30_mins = tab.filter(contains_string('TownCar30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of Town centres available by car within 30 minutes (TownCar30n) taken from cell reference Z8 down")
 
        travel_within_45_mins = tab.filter(contains_string('TownCar45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of Town centres available by car within 45 minutes (TownCar45n) taken from cell reference AA8 down")
 
        travel_within_60_mins = tab.filter(contains_string('TownCar60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of Town centres available by car within 45 minutes (TownCar45n) taken from cell reference AB8 down")
 
        percentage_users_15_mins = tab.filter(contains_string('TownCar15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of Town centres available by car (TownCar15pct) taken from cell reference AC8 down")
        
        percentage_users_30_mins = tab.filter(contains_string('TownCar30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of Town centres available by car (TownCar30pct) taken from cell reference AD8 down")

        percentage_users_45_mins = tab.filter(contains_string('TownCar45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of Town centres available by car (TownCar45pct) taken from cell reference AD8 down")

        percentage_users_60_mins = tab.filter(contains_string('TownCar60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of Town centres available by car (TownCar60pct) taken from cell reference AE8 down")

        #Observations = Travel time in minutes to nearest Town centre by car        
        observations = tab.filter(contains_string('TownCart')).shift(0,1).expand(DOWN).is_not_blank()
        
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            #HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),                HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),                
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="car.html") 
        trace.store("combined_dataframe_8", tidy_sheet.topandas())        
        
print("DISTRIBUTION 8 DONE")


# +

df = trace.combine_and_trace(datasetTitle, "combined_dataframe_8")
df.rename(columns={'OBS' : 'Value'}, inplace=True)
df["Destination"] = "Town Centre"
trace.Destination("Destination hard coded as : Town Centre")
df["Population aged"] = "number of households in LSOA"
trace.Population_aged("Population aged hard coded as : number of households in LSOA")
df["Measure Type"] = "Travel time"
trace.Measure_Type("Measure Type hard coded as : Travel time")
df["Unit"] = "Minutes"
trace.Unit("Unit hard coded as : Minutes")


# +

tidy_distribution_8 = df[["Period", "LSOA Code", "Region of LA", "LA Code", "LA Name","Service users in LSOA",
                "Destination", "Population aged", "Mode of travel", "Destination avilable by mode of travel within 15 minutes",
                "Destination avilable by mode of travel within 30 minutes", "Destination avilable by mode of travel within 45 minutes",
                "Destination avilable by mode of travel within 60 minutes", "Percentage of users within 15 minutes by mode of transport", 
                "Percentage of users within 30 minutes by mode of transport", "Percentage of users within 45 minutes by mode of transport",
                "Percentage of users within 60 minutes by mode of transport", "Measure Type", "Unit", "Value"]]
tidy_distribution_8

# -

trace.output()

# # Distribution: 9 
#
# ####      Travel time, destination and origin indicators to Pharmacies by cycle and car, Lower Super Output Area (LSOA), England 
#     Destination : Pharmacies
#     Population Aged : all households

tabs = { tab.name: tab for tab in scraper.distributions[8].as_databaker() }
list(tabs)

# +

for name, tab in tabs.items():
        columns=["Period", "LSOA Code", "Region of LA", "LA Code", "LA Name","Service users in LSOA",
                "Destination", "Population aged", "Mode of travel", "Destination 15 mins", "Destination 30 mins",
                "Destination 45 mins", "Destination 60 mins", "Percentage 15 mins", "Percentage 30 mins", 
                "Percentage 45 mins", "Percentage 60 mins", "Measure Type", "Unit"]
        trace.start(datasetTitle, tab, columns, scraper.distributions[8].downloadURL)
        
        if 'Metadata' in name:
            continue  
 
        year = name
        LSOA_code = tab.filter(contains_string('LSOA_code')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LSOA_Code("LSOA Code taken from cell reference A8 down")
        
        region =  tab.filter(contains_string('Region')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Region_of_LA("Region of LA taken from cell reference B8 down")
        
        la_code = tab.filter(contains_string('LA_Code')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LA_Code("LA Code taken from cell reference C8 down")
        
        la_name = tab.filter(contains_string('LA_Name')).shift(0,1).expand(DOWN).is_not_blank()
        trace.LA_Name("LA Name taken from cell reference D8 down")
        
        service_users =  tab.filter(contains_string('Ph_pop')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Service_users_in_LSOA("Service users in LSOA taken from cell reference D8 down")
                    
        ##   starting with Travel time in minutes to nearest Pharmacies centres by cycle  ##
        mode_of_travel = "Cycle"
        trace.Mode_of_travel("Mode of travel hardcoded as Cycle, transforming the data data related.")

        travel_within_15_mins = tab.filter(contains_string('PhCyc15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of Pharmacies available by cycle within 15 minutes (PhCyc15n) taken from cell reference P8 down")
        
        travel_within_30_mins = tab.filter(contains_string('PhCyc30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of Pharmacies available by cycle within 30 minutes (PhCyc30n) taken from cell reference Q8 down")

        travel_within_45_mins = tab.filter(contains_string('PhCyc45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of Pharmacies available by cycle within 45 minutes (PhCyc45n) taken from cell reference R8 down")

        travel_within_60_mins = tab.filter(contains_string('PhCyc60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_60_mins("Number of Pharmacies available by cycle within 60 minutes (PhCyc60n) taken from cell reference S8 down")
        
        percentage_users_15_mins = tab.filter(contains_string('PhCyc15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of Pharmacies available by cycle (PhCyc15pct) taken from cell reference T8 down")

        percentage_users_30_mins = tab.filter(contains_string('PhCyc30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of Pharmacies available by cycle (PhCyc30pct) taken from cell reference U8 down")

        percentage_users_45_mins = tab.filter(contains_string('PhCyc45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of Pharmacies available by cycle (PhCyc45pct) taken from cell reference V8 down")
        
        percentage_users_60_mins = tab.filter(contains_string('PhCyc60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of Pharmacies available by cycle (PhCyc60pct) taken from cell reference W8 down")

        #Observations = Travel time in minutes to nearest Pharmacies by cycle (TownCyct down)   
        observations = tab.filter(contains_string('PhCyct')).shift(0,1).expand(DOWN).is_not_blank()
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),                
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="cycle.html") 
        trace.store("combined_dataframe_9", tidy_sheet.topandas())
        
        ##   Next Travel time in minutes to nearest Pharmacies by Car  ##
        mode_of_travel = "Car"
        trace.Mode_of_travel("Mode of travel hardcoded as Car, transforming the data data related.")

        travel_within_15_mins = tab.filter(contains_string('PhCar15n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_15_mins("Number of Pharmacies available by car within 15 minutes (PhCar15n) taken from cell reference Y8 down")
 
        travel_within_30_mins = tab.filter(contains_string('PhCar30n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_30_mins("Number of Pharmacies available by car within 30 minutes (PhCar30n) taken from cell reference Z8 down")
 
        travel_within_45_mins = tab.filter(contains_string('PhCar45n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of Pharmacies available by car within 45 minutes (PhCar45n) taken from cell reference AA8 down")
 
        travel_within_60_mins = tab.filter(contains_string('PhCar60n')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Destination_45_mins("Number of Pharmacies available by car within 45 minutes (PhCar45n) taken from cell reference AB8 down")
 
        percentage_users_15_mins = tab.filter(contains_string('PhCar15pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_15_mins("% users within 15 minutes of Pharmacies available by car (PhCar15pct) taken from cell reference AC8 down")
        
        percentage_users_30_mins = tab.filter(contains_string('PhCar30pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_30_mins("% users within 30 minutes of Pharmacies available by car (PhCar30pct) taken from cell reference AD8 down")

        percentage_users_45_mins = tab.filter(contains_string('PhCar45pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_45_mins("% users within 45 minutes of Pharmacies available by car (PhCar45pct) taken from cell reference AD8 down")

        percentage_users_60_mins = tab.filter(contains_string('PhCar60pct')).shift(0,1).expand(DOWN).is_not_blank()
        trace.Percentage_60_mins("% users within 60 minutes of Pharmacies available by car (PhCar60pct) taken from cell reference AE8 down")

        #Observations = Travel time in minutes to nearest Pharmacies by car        
        observations = tab.filter(contains_string('PhCart')).shift(0,1).expand(DOWN).is_not_blank()
        
        dimensions = [
            HDimConst('Period', year),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(region, 'Region of LA', DIRECTLY, LEFT),
            HDim(la_code, 'LA Code', DIRECTLY, LEFT),
            HDim(la_name, 'LA Name', DIRECTLY, LEFT),
            HDim(service_users, 'Service users in LSOA', DIRECTLY, LEFT),
            HDimConst('Mode of travel', mode_of_travel),
            ##Attributes
            HDim(travel_within_15_mins, 'Destination avilable by mode of travel within 15 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_30_mins, 'Destination avilable by mode of travel within 30 minutes', DIRECTLY, RIGHT),
            HDim(travel_within_45_mins, 'Destination avilable by mode of travel within 45 minutes', DIRECTLY, RIGHT),                HDim(travel_within_60_mins, 'Destination avilable by mode of travel within 60 minutes', DIRECTLY, RIGHT),                
            HDim(percentage_users_15_mins, 'Percentage of users within 15 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_30_mins, 'Percentage of users within 30 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_45_mins, 'Percentage of users within 45 minutes by mode of transport', DIRECTLY, RIGHT),
            HDim(percentage_users_60_mins, 'Percentage of users within 60 minutes by mode of transport', DIRECTLY, RIGHT),
        ]
        tidy_sheet = ConversionSegment(tab, dimensions, observations)
        #savepreviewhtml(tidy_sheet, fname="car.html") 
        trace.store("combined_dataframe_9", tidy_sheet.topandas())        
        
print("DISTRIBUTION 9 DONE")


# +

df = trace.combine_and_trace(datasetTitle, "combined_dataframe_9")
df.rename(columns={'OBS' : 'Value'}, inplace=True)
df["Destination"] = "Pharmacies"
trace.Destination("Destination hard coded as : Pharmacies")
df["Population aged"] = "all households"
trace.Population_aged("Population aged hard coded as : all households")
df["Measure Type"] = "Travel time"
trace.Measure_Type("Measure Type hard coded as : Travel time")
df["Unit"] = "Minutes"
trace.Unit("Unit hard coded as : Minutes")


# +

tidy_distribution_9 = df[["Period", "LSOA Code", "Region of LA", "LA Code", "LA Name","Service users in LSOA",
                "Destination", "Population aged", "Mode of travel", "Destination avilable by mode of travel within 15 minutes",
                "Destination avilable by mode of travel within 30 minutes", "Destination avilable by mode of travel within 45 minutes",
                "Destination avilable by mode of travel within 60 minutes", "Percentage of users within 15 minutes by mode of transport", 
                "Percentage of users within 30 minutes by mode of transport", "Percentage of users within 45 minutes by mode of transport",
                "Percentage of users within 60 minutes by mode of transport", "Measure Type", "Unit", "Value"]]
tidy_distribution_9
trace.output()


# +
# Each Distirbution is still in a sperate output atm e.g. 
# tidy_distribution_9
# tidy_distribution_8
# tidy_distribution_7
# tidy_distribution_6
# tidy_distribution_5
# tidy_distribution_4
# tidy_distribution_3
# tidy_distribution_2
# tidy_distribution_1
# -
tidy_distribution_1.head(60)









# --
