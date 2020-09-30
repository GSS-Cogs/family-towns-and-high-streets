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
    
def PeriodFromColumnName(value):
    # returns just the year from the column name -> which contains the dataset name
    year = value.split(',')[-1].strip()
    return year

def ScoreOrRank(value):
    # tidies up dimension within list_of_transform_type_2 datasets
    if value.lower().startswith('average score'):
        return 'Average Score'
    elif value.lower().startswith('rank of average score'):
        return 'Rank of Average Score'
    elif value.lower().startswith('average rank'):
        return 'Average Rank'
    elif value.lower().startswith('rank of average rank'):
        return 'Rank of Average Rank'
    
    
# there are 2 distinct types of format
list_of_transform_type_1 = [
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

list_of_transform_type_2 = [
    'Local authority district: economic deprivation index and domains average ranks and scores',
    'Local authority district: children in income-deprived households index average ranks and scores'
]

tidied_sheets = {} # to be filled with each tab of data
for distribution in scraper.distributions:
    
    if distribution.title in list_of_transform_type_1:
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
                    
            tidy_sheet = pd.concat(tidy_sheet_list, sort=False) # dataframe for the whole tab
            
            # trace
            # tracing is more hardcoded
            trace.Period('Values given in range {}', excelRange(period))
            trace.lsoacode('Value taken from column "lsoacode" - A2 expanded down')
            trace.lsoaname('Value taken from column "lsoaname" - B2 expanded down')
            trace.lauacode('Value taken from column "lauacode" - C2 expanded down')
            trace.lauaname('Value taken from column "lsoacode" - D2 expanded down')
            trace.Value('Value taken from period columns - E2:O2 expanded down')
            trace.with_preview(cs_list[0])
            
            # some tidying up
            tidy_sheet = tidy_sheet.rename(columns={'OBS':'Value', 'DATAMARKER':'Marker'})
            trace.Value('Renamed "OBS" column as "Value"')
            tidy_sheet['Period'] = tidy_sheet['Period'].apply(PeriodFromColumnName)
            trace.Period('Value changed to only include the year')
            
            if 'Marker' in tidy_sheet.columns:
                trace.add_column('Marker')
                trace.Marker('Value taken from "Value" where a value is surpressed')
                trace.Marker('Renamed "DATAMARKER" column as "Marker"')
                tidy_sheet = tidy_sheet[[
                        'Period', 'lsoacode', 'lsoaname', 'lauacode', 'lauaname', 'Marker', 'Value'
                        ]]
            else:
                tidy_sheet = tidy_sheet[[
                        'Period', 'lsoacode', 'lsoaname', 'lauacode', 'lauaname', 'Value'
                        ]]
            
            trace.store(unique_identifier, tidy_sheet)
            tidied_sheets[unique_identifier] = tidy_sheet
            
            
    elif distribution.title in list_of_transform_type_2:
        tabs = distribution.as_databaker()
        tabs = [tab for tab in tabs if 'metadata' not in tab.name.lower()] # unwanted tabs
        
        for tab in tabs:
            # trace info
            unique_identifier = distribution.title + ' - ' + tab.name
            link = distribution.downloadURL
            columns = ['Period', 'lauacode', 'lauaname', 'Dimension 1', 'Value']
            trace.start(scraper.title, unique_identifier, columns, link)
            
            pivot = tab.filter(contains_string('lauacode'))
            lauacode = pivot.fill(DOWN).is_not_blank()
            lauaname = lauacode.shift(1, 0)
            period = pivot.shift(0, -1).expand(RIGHT).is_not_blank()
            score_rank = pivot.shift(2, 0).expand(RIGHT).is_not_blank()
            obs = lauacode.waffle(score_rank)

            dimensions = [
                    HDim(lauacode, 'lauacode', DIRECTLY, LEFT),
                    HDim(lauaname, 'lauaname', DIRECTLY, LEFT),
                    HDim(period, 'Period', CLOSEST, LEFT),
                    HDim(score_rank, 'Dimension 1', DIRECTLY, ABOVE)
                    ]

            cs = ConversionSegment(tab, dimensions, obs)
            tidy_sheet = cs.topandas()
            trace.with_preview(cs)
            
            # trace
            trace.Period('Values given in range {}', excelRange(period))
            trace.lauacode('Values given in range {}', excelRange(lauacode))
            trace.lauaname('Values given in range {}', excelRange(lauaname))
            trace.Dimension_1('Values given in range {}', excelRange(score_rank))
            trace.Value('Values given in range {}', excelRange(obs))
            
            # some tidying up
            tidy_sheet = tidy_sheet.rename(columns={'OBS':'Value'})
            trace.Value('Renamed "OBS" column as "Value"')
            tidy_sheet['Period'] = tidy_sheet['Period'].apply(lambda x: int(float(x))) # removing the '.0'
            trace.Period('Removed ".0" from year')
            tidy_sheet['Dimension 1'] = tidy_sheet['Dimension 1'].apply(ScoreOrRank)
            trace.Dimension_1('Tidied up dimensions to take one of the values \
                              ["Average Score", "Rank of Average Score", "Average Rank", "Rank of Average Rank"]')
            tidy_sheet = tidy_sheet[[
                    'Period', 'lauacode', 'lauaname', 'Dimension 1', 'Value'
                    ]]

            trace.store(unique_identifier, tidy_sheet)
            tidied_sheets[unique_identifier] = tidy_sheet 
            
            
            
out = Path('out')
out.mkdir(exist_ok=True)

'''
for key in tidied_sheets:
    df = tidied_sheets[key]
    df.drop_duplicates().to_csv(out / f'{key}.csv', index=False)
'''

for key in tidied_sheets:
    print(key)

trace.render("spec_v1.html") 

"""
Was unsure on a dimension name for the score/rank dimension within the datasets:
'Local authority district: economic deprivation index and domains average ranks and scores'
'Local authority district: children in income-deprived households index average ranks and scores'
so dimension is called 'Dimension 1'
"""