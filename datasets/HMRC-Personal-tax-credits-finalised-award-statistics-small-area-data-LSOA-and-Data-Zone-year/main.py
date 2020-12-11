from gssutils import * 
import json 

info = json.load(open('info.json')) 
etl_title = info["title"] 
etl_publisher = info["publisher"] 
print("Publisher: " + etl_publisher) 
print("Title: " + etl_title) 

scraper = Scraper(seed="info.json")   
scraper

def excelRange(bag):
    min_x = min([cell.x for cell in bag])
    max_x = max([cell.x for cell in bag])
    min_y = min([cell.y for cell in bag])
    max_y = max([cell.y for cell in bag])
    top_left_cell = xypath.contrib.excel.excel_location(bag.filter(lambda x: x.x == min_x and x.y == min_y))
    bottom_right_cell = xypath.contrib.excel.excel_location(bag.filter(lambda x: x.x == max_x and x.y == max_y))
    return f"{top_left_cell}:{bottom_right_cell}"

def monthToNumber(month):

    return {
            'January' : '01',
            'February' : '02',
            'March' : '03',
            'April' : '04',
            'May' : '05',
            'June' : '06',
            'July' : '07',
            'August' : '08',
            'September' : '09', 
            'October' : '10',
            'November' : '11',
            'December' : '12'
    }[month]


def sanitize_work_situation_family_sheets(value):
    if value == "In-work families":
        return "In Work"
    elif value == "Out-of-work families":
        return "Out of Work"
    else:
        return "All"


def sanitize_work_situation_children_sheets(value):
    if value == "Children within in-work families":
        return "In Work"
    elif value == "Children within out-of-work families":
        return "Out of Work"
    else:
        return "All"


def sanitize_family_type_in_sheets(value):
    if value == None: return 'All'
    value = value.lower() 
    if 'lone parent' in value:
        return 'Lone Parent'
    elif value == 'couples':
        return 'Couples'
    else:
        return 'All'


trace = TransformTrace()
tidied_sheets = {} # dataframes will be stored in here

# latest data
scraper.select_dataset(title=lambda x: 'small area data' in x, latest=True)

for distribution in scraper.distributions:
    
    link = distribution.downloadURL 
    dataset_title = distribution.title # title of dataset
    period = scraper.dataset.title[-12:] # time pulled from dataset title
    print(distribution.title)
    
    if 'LSOA' in distribution.title:
        
        tabs = distribution.as_databaker() # reading in dataset as databaker
        
        for tab in tabs:
            
            tab_name = dataset_title + ' - ' + tab.name # makes a unique name for each tab
            
            footnotes = tab.filter(contains_string('Footnotes')).expand(DOWN).expand(RIGHT) # to be removed
            
            if tab.name.lower() == 'families':
                # tables differ between tabs
                columns = [
                    'Date', 'Local authority', 'Lower Layer Super Output Area', 'Work Situation', 'Family Type', 'Value', 'Measure Type', 'Unit'
                ]
                
                trace.start(dataset_title, tab, columns, link)
                
                date = tab.filter(contains_string('Number of')).value.split(': ')[-1]
                year = date.split(' ')[-1].strip()
                month = date.split(' ')[0].strip()
                month = monthToNumber(month)
                date = year + '/' + month
                
                local_authority_code = tab.filter(contains_string('Local authority code')).fill(DOWN).is_not_number().is_not_blank() - footnotes
                LSOA_code = local_authority_code.shift(2, 0)

                work_situation = tab.filter(contains_string('All Child Benefit recipient families')).expand(RIGHT).is_not_blank()
                family_type = tab.filter(contains_string('All families')).expand(RIGHT).is_not_blank()
                obs = tab.excel_ref("G10").expand(DOWN).expand(RIGHT).is_not_blank()
                
                # tracing dimensions 
                trace.Date("Value taken from dataset title: {}".format(date))
                trace.Local_authority("Values given in range {}", excelRange(local_authority_code))
                trace.Lower_Layer_Super_Output_Area("Values given in range {}", excelRange(LSOA_code))
                trace.Work_Situation("Values given in range {}", excelRange(work_situation))
                trace.Family_Type("Values given in range {}", excelRange(family_type))
                trace.Value("Values given in range {}", excelRange(obs))
                trace.Measure_Type("Hardcoded as Count")
                trace.Unit("Hardcoded as families")
                
                
                dimensions = [
                    HDimConst('Date', date),
                    HDim(local_authority_code, 'Local authority', DIRECTLY, LEFT),
                    HDim(LSOA_code, 'Lower Layer Super Output Area', DIRECTLY, LEFT), 
                    HDim(work_situation, 'Work Situation', CLOSEST, LEFT),
                    HDim(family_type, 'Family Type', CLOSEST, LEFT),
                    HDimConst('Measure Type', 'count'),
                    HDimConst('Unit', 'families')
                    ]
    
                tidy_sheet = ConversionSegment(tab, dimensions, obs)
                trace.with_preview(tidy_sheet)
#                 savepreviewhtml(tidy_sheet)
                
                tidy_sheet_aspandas = tidy_sheet.topandas()
                tidy_sheet_aspandas = tidy_sheet_aspandas.rename(columns={'OBS':'Value'})
            
                tidy_sheet_aspandas["Work Situation"] = tidy_sheet_aspandas["Work Situation"].apply(sanitize_work_situation_family_sheets)
                trace.Work_Situation("Values grouped to In work, Out of work and all")
                
                tidy_sheet_aspandas["Family Type"] = tidy_sheet_aspandas["Family Type"].apply(sanitize_family_type_in_sheets)
                trace.Family_Type("Values grouped to Couples, Lone Parent and all")
                
                tidy_sheet_aspandas = tidy_sheet_aspandas[columns]
                
                trace.store(tab_name, tidy_sheet_aspandas)
                tidied_sheets[tab_name] = tidy_sheet_aspandas
                
                
            elif tab.name.lower() == 'children':
                
                columns = [
                    'Date', 'Local authority', 'Lower Layer Super Output Area',
                    'Work Situation', 'Family Type', 'Value', 'Measure Type', 'Unit'
                ]
                
                trace.start(dataset_title, tab, columns, link)
                
                date = tab.filter(contains_string('Number of')).value.split(': ')[-1]
                year = date.split(' ')[-1].strip()
                month = date.split(' ')[0].strip()
                month = monthToNumber(month)
                date = year + '/' + month
                
                local_authority_code = tab.filter(contains_string('Local authority code')).fill(DOWN).is_not_number().is_not_blank() - footnotes
                LSOA_code = local_authority_code.shift(2, 0)
                work_situation = tab.filter(contains_string('All children within families registered for Child Benefit')).expand(RIGHT).is_not_blank()
                family_type = tab.filter(contains_string('All children within families registered for Child Benefit')).shift(0,1).expand(RIGHT).is_not_blank()

                obs = tab.excel_ref("G9").expand(DOWN).expand(RIGHT).is_not_blank()
                
                # tracing dimensions 
                
                trace.Date("Value taken from dataset title: {}".format(date))
                trace.Local_authority("Values given in range {}", excelRange(local_authority_code))
                trace.Lower_Layer_Super_Output_Area("Values given in range {}", excelRange(LSOA_code))
                trace.Work_Situation("Values given in range {}", excelRange(work_situation))
                trace.Family_Type("Values given in range {}", excelRange(family_type))
                trace.Value("Values given in range {}", excelRange(obs))
                trace.Measure_Type("Hardcoded as Count")
                trace.Unit("Hardcoded as children")
                
                dimensions = [
                    HDimConst('Date', date),
                    HDim(local_authority_code, 'Local authority', DIRECTLY, LEFT),
                    HDim(LSOA_code, 'Lower Layer Super Output Area', DIRECTLY, LEFT),
                    HDim(work_situation, 'Work Situation', CLOSEST, LEFT),
                    HDim(family_type, 'Family Type', CLOSEST, LEFT),
                    HDimConst('Measure Type', 'count'),
                    HDimConst('Unit', 'children')
                    ]
                
                tidy_sheet = ConversionSegment(tab, dimensions, obs)
#                 savepreviewhtml(tidy_sheet)
                trace.with_preview(tidy_sheet)
                
                tidy_sheet_aspandas = tidy_sheet.topandas()
                tidy_sheet_aspandas = tidy_sheet_aspandas.rename(columns={'OBS':'Value'})
                
                tidy_sheet_aspandas["Work Situation"] = tidy_sheet_aspandas["Work Situation"].apply(sanitize_work_situation_children_sheets)
                trace.Work_Situation("Values grouped to In work, Out of work and all")
                
                tidy_sheet_aspandas["Family Type"] = tidy_sheet_aspandas["Family Type"].apply(sanitize_family_type_in_sheets)
                trace.Family_Type("Values grouped to Couples, Lone Parent and all")
                
                tidy_sheet_aspandas = tidy_sheet_aspandas[columns]
                
                trace.store(tab_name, tidy_sheet_aspandas)
                tidied_sheets[tab_name] = tidy_sheet_aspandas
                
    elif 'scottish data zone' in distribution.title.lower():
        # scottish data zone has slightly different format
        
        tabs = distribution.as_databaker() # reading in dataset as databaker
        
        for tab in tabs:
            
            tab_name = dataset_title + ' - ' + tab.name # makes a unique name for each tab
            
            footnotes = tab.filter(contains_string('Footnotes')).expand(DOWN).expand(RIGHT) # to be removed
            
            if tab.name.lower() == 'family': # different tab name to other datasets
                # tables differ between tabs
                columns = [
                    'Date', 'Local authority', 'Data Zone code',
                    'Work Situation', 'Family Type', 'Value', 'Measure Type', 'Unit'
                ]
                
                trace.start(dataset_title, tab, columns, link)
                
                date = tab.filter(contains_string('Number of')).value.split(': ')[-1]
                year = date.split(' ')[-1].strip()
                month = date.split(' ')[0].strip()
                month = monthToNumber(month)
                date = year + '/' + month
                
                
                local_authority_code = tab.filter(contains_string('Local authority code')).fill(DOWN).is_not_number().is_not_blank() - footnotes
                data_zone_code = local_authority_code.shift(2, 0)
                work_situation = tab.filter(contains_string('All Child Benefit recipient families')).expand(RIGHT).is_not_blank()
                family_type = tab.filter(contains_string('All families')).expand(RIGHT).is_not_blank()

                obs = tab.excel_ref("G10").expand(DOWN).expand(RIGHT).is_not_blank()
                
                # tracing dimensions 
                
                trace.Date("Value taken from dataset title: {}".format(date))
                trace.Local_authority("Values given in range {}", excelRange(local_authority_code))
                trace.Data_Zone_code("Values given in range {}", excelRange(data_zone_code))
                trace.Work_Situation("Values given in range {}", excelRange(work_situation))
                trace.Family_Type("Values given in range {}", excelRange(family_type))
                trace.Value("Values given in range {}", excelRange(obs))
                trace.Measure_Type("Hardcoded as Count")
                trace.Unit("Hardcoded as families")
                
                dimensions = [
                    HDimConst('Date', date),
                    HDim(local_authority_code, 'Local authority', DIRECTLY, LEFT),
                    HDim(data_zone_code, 'Data Zone code', DIRECTLY, LEFT),
                    HDim(work_situation, 'Work Situation', CLOSEST, LEFT),
                    HDim(family_type, 'Family Type', CLOSEST, LEFT),
                    HDimConst('Measure Type', 'count'),
                    HDimConst('Unit', 'families')
                    ]
                
                tidy_sheet = ConversionSegment(tab, dimensions, obs)
                trace.with_preview(tidy_sheet)
#                 savepreviewhtml(tidy_sheet)
                
                tidy_sheet_aspandas = tidy_sheet.topandas()
                tidy_sheet_aspandas = tidy_sheet_aspandas.rename(columns={'OBS':'Value'})
            
                tidy_sheet_aspandas["Work Situation"] = tidy_sheet_aspandas["Work Situation"].apply(sanitize_work_situation_family_sheets)
                trace.Work_Situation("Values grouped to In work, Out of work and all")
                
                tidy_sheet_aspandas["Family Type"] = tidy_sheet_aspandas["Family Type"].apply(sanitize_family_type_in_sheets)
                trace.Family_Type("Values grouped to Couples, Lone Parent and all")
                
                tidy_sheet_aspandas = tidy_sheet_aspandas[columns]
                
                trace.store(tab_name, tidy_sheet_aspandas)
                tidied_sheets[tab_name] = tidy_sheet_aspandas
                
            elif tab.name.lower() == 'children':
                columns = [
                    'Date', 'Local authority', 'Data Zone code',
                    'Work Situation', 'Family Type', 'Value', 'Measure Type', 'Unit'
                ]
                
                trace.start(dataset_title, tab, columns, link)
                
                date = tab.filter(contains_string('Number of')).value.split(': ')[-1]
                year = date.split(' ')[-1].strip()
                month = date.split(' ')[0].strip()
                month = monthToNumber(month)
                date = year + '/' + month
                
                local_authority_code = tab.filter(contains_string('Local authority code')).fill(DOWN).is_not_number().is_not_blank() - footnotes
                data_zone_code = local_authority_code.shift(2, 0)
                
                work_situation = tab.filter(contains_string('All children within families registered for Child Benefit')).expand(RIGHT).is_not_blank()
                family_type = tab.filter(contains_string('All children within families registered for Child Benefit')).shift(0,1).expand(RIGHT).is_not_blank()

                obs = tab.excel_ref("G9").expand(DOWN).expand(RIGHT).is_not_blank()
                
                # tracing dimensions 
                
                trace.Date("Value taken from dataset title: {}".format(date))
                trace.Local_authority("Values given in range {}", excelRange(local_authority_code))
                trace.Data_Zone_code("Values given in range {}", excelRange(data_zone_code))
                trace.Work_Situation("Values given in range {}", excelRange(work_situation))
                trace.Family_Type("Values given in range {}", excelRange(family_type))
                trace.Value("Values given in range {}", excelRange(obs))
                trace.Measure_Type("Hardcoded as Count")
                trace.Unit("Hardcoded as families")
                
                dimensions = [
                    HDimConst('Date', date),
                    HDim(local_authority_code, 'Local authority', DIRECTLY, LEFT),
                    HDim(data_zone_code, 'Data Zone code', DIRECTLY, LEFT),
                    HDim(work_situation, 'Work Situation', CLOSEST, LEFT),
                    HDim(family_type, 'Family Type', CLOSEST, LEFT),
                    HDimConst('Measure Type', 'count'),
                    HDimConst('Unit', 'children')
                ]
                
                tidy_sheet = ConversionSegment(tab, dimensions, obs)
                trace.with_preview(tidy_sheet)
                
                tidy_sheet_aspandas = tidy_sheet.topandas()
                tidy_sheet_aspandas = tidy_sheet_aspandas.rename(columns={'OBS':'Value'})
            
                tidy_sheet_aspandas["Work Situation"] = tidy_sheet_aspandas["Work Situation"].apply(sanitize_work_situation_children_sheets)
                trace.Work_Situation("Values grouped to In work, Out of work and all")
                
                tidy_sheet_aspandas["Family Type"] = tidy_sheet_aspandas["Family Type"].apply(sanitize_family_type_in_sheets)
                trace.Family_Type("Values grouped to Couples, Lone Parent and all")
                
                tidy_sheet_aspandas = tidy_sheet_aspandas[columns]
                
                trace.store(tab_name, tidy_sheet_aspandas)
                tidied_sheets[tab_name] = tidy_sheet_aspandas


out = Path('out')
out.mkdir(exist_ok=True)
# tidied_sheets['Scottish Data Zones - Children'].tail(50)


trace.render("spec_v1.html")
