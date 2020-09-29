from gssutils import * 
import json 
import math

info = json.load(open('info.json')) 
etl_title = info["title"] 
etl_publisher = info["publisher"]
print("Publisher: " + etl_publisher) 
print("Title: " + etl_title) 

scraper = Scraper(seed="info.json")   
scraper 

trace = TransformTrace()

scraper.select_dataset(title=lambda x: x.lower().startswith('tracking'))

def excelRange(bag):
    min_x = min([cell.x for cell in bag])
    max_x = max([cell.x for cell in bag])
    min_y = min([cell.y for cell in bag])
    max_y = max([cell.y for cell in bag])
    top_left_cell = xypath.contrib.excel.excel_location(bag.filter(lambda x: x.x == min_x and x.y == min_y))
    bottom_right_cell = xypath.contrib.excel.excel_location(bag.filter(lambda x: x.x == max_x and x.y == max_y))
    return f"{top_left_cell}:{bottom_right_cell}"
    
    
list_of_wanted_datasets = [
    'Economic deprivation index: rank',
    'Economic deprivation index: income deprivation domain score',
    'Economic deprivation index: income deprivation domain rank',
    'Economic deprivation index: employment deprivation domain score',
    'Economic deprivation index: employment deprivation domain rank',
    'Children in income-deprived households index: score',
    'Children in income-deprived households index: rank',
    'Economic deprivation index: income deprivation domain numerator',
    'Economic deprivation index: income deprivation domain denominator',
    'Economic deprivation index: employment deprivation domain numerator',
    'Economic deprivation index: employment deprivation domain denominator',
    'Children in income-deprived households index: numerator',
    'Children in income-deprived households index: denominator',
    'Economic deprivation index: score',
    'Total population used to calculate local authority district and economic deprivation index summary measure'
]
list_of_wanted_datasets = [
    'Economic deprivation index: rank'
]

tidied_sheets = {} # to be filled with each tab of data
for distribution in scraper.distributions:
    
    if distribution.title in list_of_wanted_datasets:
        tabs = distribution.as_databaker()
        tabs = [tab for tab in tabs if 'metadata' not in tab.name.lower()] # unwanted tabs
        
        for tab in tabs:
            # run assertions here
            assert tab.excel_ref('A1').value == 'lsoacode'
            assert tab.excel_ref('B1').value == 'lsoaname'
            assert tab.excel_ref('C1').value == 'lauacode'
            assert tab.excel_ref('D1').value == 'lauaname'
            
            # trace info
            unique_identifier = distribution.title + ' - ' + tab.name # title of dataset + tab name
            link = distribution.downloadURL
            columns = ['Period', 'lsoacode', 'lsoaname', 'lauacode', 'lauaname', 'Value']
            trace.start(scraper.title, unique_identifier, columns, link)
            
            tidy_sheet_list = [] # list of dataframes for each iteration
            cs_list = [] # list of conversionsegments for each iteration
    
            '''Iterating the databaking process'''
            tab_length = len(tab.excel_ref('A')) # number of rows of data
            batch_number = 10 # iterates over this many rows at a time
            number_of_iterations = math.ceil(tab_length/batch_number) # databaking will iterate this many times
            
            for i in range(0, number_of_iterations):
                Min = str(2 + batch_number * i)  # data starts on row 2
                Max = str(int(Min) + batch_number - 1) 
                
                '''
                use "Min" and "Max" to specify a range of cells
                instead of selecting cells using ".expand(DOWN)"                
                '''
                lsoa_code = tab.excel_ref('A'+Min+':A'+Max).is_not_blank()
                lsoa_name = tab.excel_ref('B'+Min+':B'+Max).is_not_blank()

                laua_code = tab.excel_ref('C'+Min+':C'+Max).is_not_blank()
                laua_name = tab.excel_ref('D'+Min+':D'+Max).is_not_blank()

                period = tab.excel_ref('E1').expand(RIGHT).is_not_blank() 
                # will be the same range of cells for each iteration

                obs = period.waffle(lsoa_code)

                dimensions = [
                        HDim(period, 'Period', DIRECTLY, ABOVE),
                        HDim(lsoa_code, 'lsoacode', DIRECTLY, LEFT),
                        HDim(lsoa_name, 'lsoaname', DIRECTLY, LEFT),
                        HDim(laua_code, 'lauacode', DIRECTLY, LEFT),
                        HDim(laua_name, 'lauaname', DIRECTLY, LEFT),
                        ]

                if len(obs) != 0: # only use ConversionSegment if there is data
                    cs_iteration = ConversionSegment(tab, dimensions, obs) # creating the conversionsegment
                    tidy_sheet_iteration = cs_iteration.topandas() # turning conversionsegment into a pandas dataframe
                    cs_list.append(cs_iteration) # add to list
                    tidy_sheet_list.append(tidy_sheet_iteration) # add to list
                    
            tidy_sheet = pd.concat(tidy_sheet_list) # dataframe for the whole tab
            tidied_sheets[unique_identifier] = tidy_sheet 
            
            # trace
            # tracing is more hardcoded
            trace.Period('Values given in range {}', excelRange(period))
            trace.lsoacode('Value taken from column "lsoacode" - A2 expanded down')
            trace.lsoaname('Value taken from column "lsoaname" - B2 expanded down')
            trace.lauacode('Value taken from column "lauacode" - C2 expanded down')
            trace.lauaname('Value taken from column "lsoacode" - D2 expanded down')
            trace.Value('Value taken from period columns - E2:O2 expanded down')
            trace.store(unique_identifier, tidy_sheet)
            trace.with_preview(cs_list[0]) # seems to create the whole preview passing just one iteration
            
            
out = Path('out')
out.mkdir(exist_ok=True)

'''
for key in tidied_sheets:
    df = tidied_sheets[key]
    df.drop_duplicates().to_csv(out / f'{key}.csv', index=False)
'''

trace.render("spec_v1.html") 
