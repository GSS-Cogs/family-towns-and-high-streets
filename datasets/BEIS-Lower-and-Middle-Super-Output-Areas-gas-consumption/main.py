from gssutils import * 
import json
import re

info = json.load(open('info.json')) 
etl_title = info["title"] 
etl_publisher = info["publisher"][0]
print("Publisher: " + etl_publisher) 
print("Title: " + etl_title) 

scraper = Scraper(seed="info.json")   
scraper 

trace = TransformTrace()

def excelRange(bag):
    min_x = min([cell.x for cell in bag])
    max_x = max([cell.x for cell in bag])
    min_y = min([cell.y for cell in bag])
    max_y = max([cell.y for cell in bag])
    top_left_cell = xypath.contrib.excel.excel_location(bag.filter(lambda x: x.x == min_x and x.y == min_y))
    bottom_right_cell = xypath.contrib.excel.excel_location(bag.filter(lambda x: x.x == max_x and x.y == max_y))
    return f"{top_left_cell}:{bottom_right_cell}"

tidied_sheets = {}

#LSOA domestic gas 2010-18

LSOAdistribution = scraper.distributions[0]
display(LSOAdistribution)

LSOAlink = LSOAdistribution.downloadURL
tabs = [ tab for tab in LSOAdistribution.as_databaker() ]

for tab in tabs:
    if tab.name.lower().strip() in ('title', 'annex sub-national publications'):
        continue
    
    columns = ['Period', 'Local Authority Code', 'MSOA Code', 'LSOA Code', 'Number of Meters', 'Domestic Use', 'Mean Consumption', 'Median Consumption', 'Measure Type', 'Unit']
    trace.start(LSOAdistribution.title, tab, columns, LSOAlink)
    
    pivot = tab.filter(contains_string('Local Authority Name')).shift(DOWN)
    
    local_authority_code = pivot.shift(RIGHT).expand(DOWN).is_not_blank()
    trace.Local_Authority_Code("Values given in range {}", var=excelRange(local_authority_code))
    
    MSOA_code = pivot.shift(3, 0).expand(DOWN).is_not_blank()
    trace.MSOA_Code("Values given in range {}", var=excelRange(MSOA_code))
    
    LSOA_code = pivot.shift(5, 0).expand(DOWN).is_not_blank()
    trace.LSOA_Code("Values given in range {}", var=excelRange(LSOA_code))
    
    number_of_meters = pivot.shift(6, 0).expand(DOWN).is_not_blank()
    trace.Number_of_Meters("Values given in range {}", var=excelRange(number_of_meters))
    
    mean = pivot.shift(8, 0).expand(DOWN).is_not_blank()
    trace.Mean_Consumption("Values given in range {}", var=excelRange(mean))
    
    median = pivot.shift(9, 0).expand(DOWN).is_not_blank()
    trace.Median_Consumption("Values given in range {}", var=excelRange(median))
    
    year = tab.name.replace('r', '')
    trace.Period("Value given in name of tab as {}", var=year)
    
    domestic = 'yes'
    trace.Domestic_Use("Source file only contains Domestic use observations")
    
    observations = pivot.shift(7, 0).expand(DOWN).is_not_blank()
    
    measure_type = 'Gas Consumption'
    trace.Measure_Type('Hardcoded as: {}', var=measure_type)
    
    unit = 'kWh'
    trace.Unit('Hardcoded as: {}', var=unit)
    
    dimensions = [
            HDimConst('Period', year),
            HDim(local_authority_code, 'Local Authority Code', DIRECTLY, LEFT),
            HDim(MSOA_code, 'MSOA Code', DIRECTLY, LEFT),
            HDim(LSOA_code, 'LSOA Code', DIRECTLY, LEFT),
            HDim(number_of_meters, 'Number of Meters', DIRECTLY, LEFT),
            HDimConst('Domestic Use', domestic),
            HDim(mean, 'Mean Consumption', DIRECTLY, RIGHT),
            HDim(median, 'Median Consumption', DIRECTLY, RIGHT),
            HDimConst('Measure Type', measure_type),
            HDimConst('Unit', unit)
            ]
    
    tidy_sheet = ConversionSegment(tab, dimensions, observations) 
    trace.with_preview(tidy_sheet)

    tab_title = pivot.shift(0, -2)

    trace.store(tab.name + '_' + LSOAdistribution.title, tidy_sheet.topandas())
    tidied_sheets[tab.name + '_' + LSOAdistribution.title] = tidy_sheet.topandas()
    
#MSAO domestic gas 2010-18

MSOAdistribution = scraper.distributions[2]
display(MSOAdistribution)

MSOAlink = MSOAdistribution.downloadURL
tabs = [ tab for tab in MSOAdistribution.as_databaker() ]

for tab in tabs:
    if tab.name.lower().strip() in ('title', 'annex sub-national publications'):
        continue
        
    columns = ['Period', 'Local Authority Code', 'MSOA Code', 'Number of Meters', 'Domestic Use', 'Mean Consumption', 'Median Consumption', 'Measure Type', 'Unit']
    trace.start(MSOAdistribution.title, tab, columns, MSOAlink)
    
    pivot = tab.filter(contains_string('Local Authority Name')).shift(DOWN)
    
    local_authority_code = pivot.shift(RIGHT).expand(DOWN).is_not_blank()
    trace.Local_Authority_Code("Values given in range {}", var=excelRange(local_authority_code))
    
    MSOA_code = pivot.shift(3, 0).expand(DOWN).is_not_blank()
    trace.MSOA_Code("Values given in range {}", var=excelRange(MSOA_code))
    
    number_of_meters = pivot.shift(4, 0).expand(DOWN).is_not_blank()
    trace.Number_of_Meters("Values given in range {}", var=excelRange(number_of_meters))
    
    mean = pivot.shift(6, 0).expand(DOWN).is_not_blank()
    trace.Mean_Consumption("Values given in range {}", var=excelRange(mean))
    
    median = pivot.shift(7, 0).expand(DOWN).is_not_blank()
    trace.Median_Consumption("Values given in range {}", var=excelRange(median))
    
    year = tab.name.replace('r', '')
    trace.Period("Value given in name of tab as {}", var=year)
    
    domestic = 'yes'
    trace.Domestic_Use("Source file only contains Domestic use observations")
    
    observations = pivot.shift(5, 0).expand(DOWN).is_not_blank()
    
    measure_type = 'Gas Consumption'
    trace.Measure_Type('Hardcoded as: {}', var=measure_type)
    
    unit = 'kWh'
    trace.Unit('Hardcoded as: {}', var=unit)
    
    dimensions = [
            HDimConst('Period', year),
            HDim(local_authority_code, 'Local Authority Code', DIRECTLY, LEFT),
            HDim(MSOA_code, 'MSOA Code', DIRECTLY, LEFT),
            HDim(number_of_meters, 'Number of Meters', DIRECTLY, LEFT),
            HDimConst('Domestic Use', domestic),
            HDim(mean, 'Mean Consumption', DIRECTLY, RIGHT),
            HDim(median, 'Median Consumption', DIRECTLY, RIGHT),
            HDimConst('Measure Type', measure_type),
            HDimConst('Unit', unit)
            ]
    
    tidy_sheet = ConversionSegment(tab, dimensions, observations)
    trace.with_preview(tidy_sheet)

    tab_title = pivot.shift(0, -1)

    trace.store(tab.name + '_' + MSOAdistribution.title, tidy_sheet.topandas())
    tidied_sheets[tab.name + '_' + MSOAdistribution.title] = tidy_sheet.topandas()
    
#MSAO non-domestic gas 2010-18

MSOANDdistribution = scraper.distributions[4]
display(MSOANDdistribution)

MSOANDlink = MSOANDdistribution.downloadURL
tabs = [ tab for tab in MSOANDdistribution.as_databaker() ]

for tab in tabs:
    if tab.name.lower().strip() in ('title', 'annex sub-national publications'):
        continue
    
    columns = ['Period', 'Local Authority Code', 'MSOA Code', 'Number of Meters', 'Domestic Use', 'Mean Consumption', 'Median Consumption', 'Measure Type', 'Unit']
    trace.start(MSOANDdistribution.title, tab, columns, MSOANDlink)
    
    pivot = tab.filter(contains_string('Local Authority Name')).shift(DOWN)
    
    local_authority_code = pivot.shift(RIGHT).expand(DOWN).is_not_blank()
    trace.Local_Authority_Code("Values given in range {}", var=excelRange(local_authority_code))
    
    MSOA_code = pivot.shift(3, 0).expand(DOWN).is_not_blank()
    trace.MSOA_Code("Values given in range {}", var=excelRange(MSOA_code))
    
    number_of_meters = pivot.shift(4, 0).expand(DOWN).is_not_blank()
    trace.Number_of_Meters("Values given in range {}", var=excelRange(number_of_meters))
    
    mean = pivot.shift(6, 0).expand(DOWN).is_not_blank()
    trace.Mean_Consumption("Values given in range {}", var=excelRange(mean))
    
    median = pivot.shift(7, 0).expand(DOWN).is_not_blank()
    trace.Median_Consumption("Values given in range {}", var=excelRange(median))
    
    year = tab.name.replace('r', '')
    trace.Period("Value given in name of tab as {}", var=year)
    
    domestic = 'no'
    trace.Domestic_Use("Source file only contains Non-Domestic use observations")
    
    observations = pivot.shift(5, 0).expand(DOWN).is_not_blank()
    
    measure_type = 'Gas Consumption'
    trace.Measure_Type('Hardcoded as: {}', var=measure_type)
    
    unit = 'kWh'
    trace.Unit('Hardcoded as: {}', var=unit)
    
    dimensions = [
            HDimConst('Period', year),
            HDim(local_authority_code, 'Local Authority Code', DIRECTLY, LEFT),
            HDim(MSOA_code, 'MSOA Code', DIRECTLY, LEFT),
            HDim(number_of_meters, 'Number of Meters', DIRECTLY, LEFT),
            HDimConst('Domestic Use', domestic),
            HDim(mean, 'Mean Consumption', DIRECTLY, RIGHT),
            HDim(median, 'Median Consumption', DIRECTLY, RIGHT),
            HDimConst('Measure Type', measure_type),
            HDimConst('Unit', unit)
            ]
    
    tidy_sheet = ConversionSegment(tab, dimensions, observations)
    trace.with_preview(tidy_sheet)

    tab_title = pivot.shift(0, -1)

    trace.store(tab.name + '_' + MSOANDdistribution.title, tidy_sheet.topandas())
    tidied_sheets[tab.name + '_' + MSOANDdistribution.title] = tidy_sheet.topandas()
    
out = Path('out')
out.mkdir(exist_ok=True)

for key in tidied_sheets:
    df = tidied_sheets[key]
    df.drop_duplicates().to_csv(out / f'{key}.csv', index=False)
    
trace.render("spec_v1.html")