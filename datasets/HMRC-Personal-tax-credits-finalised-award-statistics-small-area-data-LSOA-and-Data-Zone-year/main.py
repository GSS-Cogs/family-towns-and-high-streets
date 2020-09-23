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
    
trace = TransformTrace()
tidied_sheets = {} # dataframes will be stored in here

# latest data
scraper.select_dataset(title=lambda x: 'small area data' in x, latest=True)

for distribution in scraper.distributions:
    
    link = distribution.downloadURL 
    dataset_title = distribution.title # title of dataset
    period = scraper.dataset.title[-12:] # time pulled from dataset title
    
    if 'LSOA' in distribution.title:
        
        tabs = distribution.as_databaker() # reading in dataset as databaker
        
        for tab in tabs:
            
            tab_name = dataset_title + ' - ' + tab.name # makes a unique name for each tab
            
            footnotes = tab.filter(contains_string('Footnotes')).expand(DOWN).expand(RIGHT) # to be removed
            
            if tab.name.lower() == 'families':
                # tables differ between tabs
                columns = [
                    'Period', 'Region', 'Local authority code', 'Local authority', 'LSOA code','LSOA name',
                    'All tax credits recipient families', 'WTC and CTC', 'CTC only', 'WTC only', 'Total in work', 
                    'Benefitting from the childcare element', 'National Childcare Indicator', 'Total lone parents', 
                    'Lone parent families Benefitting from the childcare element',
                    'Total out of work', 'Lone parents', 'Couples', 'Value'
                ]
                
                trace.start(dataset_title, tab, columns, link)
                
                local_authority_code = tab.filter(contains_string('Local authority code')).fill(DOWN).is_not_number().is_not_blank() - footnotes
                local_authority_name = local_authority_code.shift(1, 0)
                region = local_authority_code.shift(-1, 0)
                LSOA_code = local_authority_code.shift(2, 0)
                LSOA_name = local_authority_code.shift(3, 0)

                obs = tab.filter(contains_string('All Child Benefit recipient families')).fill(DOWN).is_not_blank()
                obs = obs.same_row(region)
                all_tax_credits_recipient_families = tab.filter(contains_string('All tax credits recipient families')).fill(DOWN).is_not_blank()
                all_tax_credits_recipient_families = all_tax_credits_recipient_families.same_row(region)

                WTC_and_CTC = tab.filter(contains_string('WTC and CTC')).fill(DOWN).is_not_blank()
                WTC_and_CTC = WTC_and_CTC.same_row(region)
                CTC_only = WTC_and_CTC.shift(1, 0)
                WTC_only = WTC_and_CTC.shift(2, 0)
                total_in_work = WTC_and_CTC.shift(3, 0)
                benefitting_from_the_childcare_element = WTC_and_CTC.shift(4, 0)
                national_childcare_indicator = WTC_and_CTC.shift(5, 0)

                total_lone_parents = tab.filter(contains_string('Total lone parents')).fill(DOWN).is_not_blank()
                lone_parent_families_benefitting_from_the_childcare_element = total_lone_parents.shift(1, 0)

                total_out_of_work = tab.filter(contains_string('Total out of work')).fill(DOWN).is_not_blank()
                total_out_of_work = total_out_of_work.same_row(region)
                lone_parents = total_out_of_work.shift(1, 0)
                couples = total_out_of_work.shift(2, 0)
                
                # tracing dimensions
                trace.Period("Value taken from dataset title: {}".format(period))
                trace.Region("Values given in range {}", excelRange(region)) 
                trace.Local_authority_code("Values given in range {}", excelRange(local_authority_code))
                trace.Local_authority("Values given in range {}", excelRange(local_authority_name))
                trace.LSOA_code("Values given in range {}", excelRange(LSOA_code))
                trace.LSOA_name("Values given in range {}", excelRange(LSOA_name))
                trace.All_tax_credits_recipient_families("Values given in range {}", excelRange(all_tax_credits_recipient_families))
                trace.WTC_and_CTC("Values given in range {}", excelRange(WTC_and_CTC))
                trace.CTC_only("Values given in range {}", excelRange(CTC_only))
                trace.WTC_only("Values given in range {}", excelRange(WTC_only))
                trace.Total_in_work("Values given in range {}", excelRange(total_in_work))
                trace.Benefitting_from_the_childcare_element("Values given in range {}", excelRange(benefitting_from_the_childcare_element))
                trace.National_Childcare_Indicator("Values given in range {}", excelRange(national_childcare_indicator))
                trace.Total_lone_parents("Values given in range {}", excelRange(total_lone_parents))
                trace.Lone_parent_families_Benefitting_from_the_childcare_element("Values given in range {}", excelRange(lone_parent_families_benefitting_from_the_childcare_element))
                trace.Total_out_of_work("Values given in range {}", excelRange(total_out_of_work))
                trace.Lone_parents("Values given in range {}", excelRange(lone_parents))
                trace.Couples("Values given in range {}", excelRange(couples))
                trace.Value("Values given in range {}", excelRange(obs))
                
                dimensions = [
                    HDimConst('Period', period),
                    HDim(local_authority_code, 'Local authority code', DIRECTLY, LEFT),
                    HDim(local_authority_name, 'Local authority', DIRECTLY, LEFT),
                    HDim(region, 'Region', DIRECTLY, LEFT),
                    HDim(LSOA_code, 'LSOA code', DIRECTLY, LEFT),
                    HDim(LSOA_name, 'LSOA name', DIRECTLY, LEFT),
                    HDim(all_tax_credits_recipient_families, 'All tax credits recipient families', DIRECTLY, RIGHT),
                    HDim(WTC_and_CTC, 'WTC and CTC', DIRECTLY, RIGHT),
                    HDim(CTC_only, 'CTC only', DIRECTLY, RIGHT),
                    HDim(WTC_only, 'WTC only', DIRECTLY, RIGHT),
                    HDim(total_in_work, 'Total in work', DIRECTLY, RIGHT),
                    HDim(benefitting_from_the_childcare_element, 'Benefitting from the childcare element', DIRECTLY, RIGHT),
                    HDim(national_childcare_indicator, 'National Childcare Indicator', DIRECTLY, RIGHT),
                    HDim(total_lone_parents, 'Total lone parents', DIRECTLY, RIGHT),
                    HDim(lone_parent_families_benefitting_from_the_childcare_element, 'Lone parent families Benefitting from the childcare element', DIRECTLY, RIGHT),
                    HDim(total_out_of_work, 'Total out of work', DIRECTLY, RIGHT),
                    HDim(lone_parents, 'Lone parents', DIRECTLY, RIGHT),
                    HDim(couples, 'Couples', DIRECTLY, RIGHT)                    
                    ]
    
                tidy_sheet = ConversionSegment(tab, dimensions, obs)
                trace.with_preview(tidy_sheet)
                
                tidy_sheet_aspandas = tidy_sheet.topandas()
                tidy_sheet_aspandas = tidy_sheet_aspandas.rename(columns={'OBS':'Value'})
                tidy_sheet_aspandas = tidy_sheet_aspandas[columns]
                
                trace.store(tab_name, tidy_sheet_aspandas)
                tidied_sheets[tab_name] = tidy_sheet_aspandas
                
                
            elif tab.name.lower() == 'children':
                
                columns = [
                    'Period', 'Region', 'Local authority code', 'Local authority', 'LSOA code','LSOA name',
                    'All children within families receiving tax credits',
                    'WTC and CTC', 'CTC only', 'Total children within in work families', 'Lone parents within in work families', 
                    'Total children within out of work families', 'Lone parents within out of work families', 
                    'Couples', 'Value'
                ]
                
                trace.start(dataset_title, tab, columns, link)
                
                local_authority_code = tab.filter(contains_string('Local authority code')).fill(DOWN).is_not_number().is_not_blank() - footnotes
                local_authority_name = local_authority_code.shift(1, 0)
                region = local_authority_code.shift(-1, 0)
                LSOA_code = local_authority_code.shift(2, 0)
                LSOA_name = local_authority_code.shift(3, 0)

                obs = tab.filter(contains_string('All children within families registered for Child Benefit')).fill(DOWN).is_not_blank()
                obs = obs.same_row(region)
                all_children_within_families_receiving_tax_credits = tab.filter(contains_string('All children within families receiving tax credits')).fill(DOWN).is_not_blank()
                all_children_within_families_receiving_tax_credits = all_children_within_families_receiving_tax_credits.same_row(region)

                WTC_and_CTC = tab.filter(contains_string('WTC and CTC')).fill(DOWN).is_not_blank()
                WTC_and_CTC = WTC_and_CTC.same_row(region)
                CTC_only = WTC_and_CTC.shift(1, 0)
                total_children_within_in_work_families = WTC_and_CTC.shift(2, 0)
                lone_parents_within_in_work_families = WTC_and_CTC.shift(3, 0)

                total_children_within_out_of_work_families = tab.filter(contains_string('Children within out-of-work families')).shift(DOWN).fill(DOWN).is_not_blank()
                total_children_within_out_of_work_families = total_children_within_out_of_work_families.same_row(region)
                lone_parents_within_out_of_work_families = total_children_within_out_of_work_families.shift(1, 0)
                couples = total_children_within_out_of_work_families.shift(2, 0)
                
                # tracing dimensions
                trace.Period("Value taken from dataset title: {}".format(period))
                trace.Region("Values given in range {}", excelRange(region)) 
                trace.Local_authority_code("Values given in range {}", excelRange(local_authority_code))
                trace.Local_authority("Values given in range {}", excelRange(local_authority_name))
                trace.LSOA_code("Values given in range {}", excelRange(LSOA_code))
                trace.LSOA_name("Values given in range {}", excelRange(LSOA_name))
                trace.All_children_within_families_receiving_tax_credits("Values given in range {}", excelRange(all_children_within_families_receiving_tax_credits))
                trace.WTC_and_CTC("Values given in range {}", excelRange(WTC_and_CTC))
                trace.CTC_only("Values given in range {}", excelRange(CTC_only))
                trace.Total_children_within_in_work_families("Values given in range {}", excelRange(total_children_within_in_work_families))
                trace.Lone_parents_within_in_work_families("Values given in range {}", excelRange(benefitting_from_the_childcare_element))
                trace.Total_children_within_out_of_work_families("Values given in range {}", excelRange(total_children_within_out_of_work_families))
                trace.Lone_parents_within_out_of_work_families("Values given in range {}", excelRange(lone_parents_within_out_of_work_families))
                trace.Couples("Values given in range {}", excelRange(couples))
                trace.Value("Values given in range {}", excelRange(obs))
                
                dimensions = [
                    HDimConst('Period', period),
                    HDim(local_authority_code, 'Local authority code', DIRECTLY, LEFT),
                    HDim(local_authority_name, 'Local authority', DIRECTLY, LEFT),
                    HDim(region, 'Region', DIRECTLY, LEFT),
                    HDim(LSOA_code, 'LSOA code', DIRECTLY, LEFT),
                    HDim(LSOA_name, 'LSOA name', DIRECTLY, LEFT),
                    HDim(all_children_within_families_receiving_tax_credits, 'All children within families receiving tax credits', DIRECTLY, RIGHT),
                    HDim(WTC_and_CTC, 'WTC and CTC', DIRECTLY, RIGHT),
                    HDim(CTC_only, 'CTC only', DIRECTLY, RIGHT),
                    HDim(total_children_within_in_work_families, 'Total children within in work families', DIRECTLY, RIGHT),
                    HDim(lone_parents_within_in_work_families, 'Lone parents within in work families', DIRECTLY, RIGHT),
                    HDim(total_children_within_out_of_work_families, 'Total children within out of work families', DIRECTLY, RIGHT),
                    HDim(lone_parents_within_out_of_work_families, 'Lone parents within out of work families', DIRECTLY, RIGHT),
                    HDim(couples, 'Couples', DIRECTLY, RIGHT)                    
                    ]
                
                tidy_sheet = ConversionSegment(tab, dimensions, obs)
                trace.with_preview(tidy_sheet)
                
                tidy_sheet_aspandas = tidy_sheet.topandas()
                tidy_sheet_aspandas = tidy_sheet_aspandas.rename(columns={'OBS':'Value'})
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
                    'Period', 'Region', 'Local authority code', 'Local authority', 'Data Zone code','Data Zone name',
                    'All tax credits recipient families',
                    'WTC and CTC', 'CTC only', 'WTC only', 'Total in work', 'Benefitting from the childcare element', 
                    'National Childcare Indicator', 'Total lone parents', 
                    'Lone parent families Benefitting from the childcare element',
                    'Total out of work', 'Lone parents', 'Couples', 'Value'
                ]
                
                trace.start(dataset_title, tab, columns, link)
                
                local_authority_code = tab.filter(contains_string('Local authority code')).fill(DOWN).is_not_number().is_not_blank() - footnotes
                local_authority_name = local_authority_code.shift(1, 0)
                region = local_authority_code.shift(-1, 0)
                data_zone_code = local_authority_code.shift(2, 0)
                data_zone_name = local_authority_code.shift(3, 0)

                obs = tab.filter(contains_string('All Child Benefit recipient families')).fill(DOWN).is_not_blank()
                obs = obs.same_row(region)
                all_tax_credits_recipient_families = tab.filter(contains_string('All tax credits recipient families')).fill(DOWN).is_not_blank()
                all_tax_credits_recipient_families = all_tax_credits_recipient_families.same_row(region)

                WTC_and_CTC = tab.filter(contains_string('WTC and CTC')).fill(DOWN).is_not_blank()
                WTC_and_CTC = WTC_and_CTC.same_row(region)
                CTC_only = WTC_and_CTC.shift(1, 0)
                WTC_only = WTC_and_CTC.shift(2, 0)
                total_in_work = WTC_and_CTC.shift(3, 0)
                benefitting_from_the_childcare_element = WTC_and_CTC.shift(4, 0)
                national_childcare_indicator = WTC_and_CTC.shift(5, 0)

                total_lone_parents = tab.filter(contains_string('Total lone parents')).fill(DOWN).is_not_blank()
                lone_parent_families_benefitting_from_the_childcare_element = total_lone_parents.shift(1, 0)

                total_out_of_work = tab.filter(contains_string('Total out of work')).fill(DOWN).is_not_blank()
                total_out_of_work = total_out_of_work.same_row(region)
                lone_parents = total_out_of_work.shift(1, 0)
                couples = total_out_of_work.shift(2, 0)
                
                # tracing dimensions
                trace.Period("Value taken from dataset title: {}".format(period))
                trace.Region("Values given in range {}", excelRange(region)) 
                trace.Local_authority_code("Values given in range {}", excelRange(local_authority_code))
                trace.Local_authority("Values given in range {}", excelRange(local_authority_name))
                trace.Data_Zone_code("Values given in range {}", excelRange(data_zone_code))
                trace.Data_Zone_name("Values given in range {}", excelRange(data_zone_name))
                trace.All_tax_credits_recipient_families("Values given in range {}", excelRange(all_tax_credits_recipient_families))
                trace.WTC_and_CTC("Values given in range {}", excelRange(WTC_and_CTC))
                trace.CTC_only("Values given in range {}", excelRange(CTC_only))
                trace.WTC_only("Values given in range {}", excelRange(WTC_only))
                trace.Total_in_work("Values given in range {}", excelRange(total_in_work))
                trace.Benefitting_from_the_childcare_element("Values given in range {}", excelRange(benefitting_from_the_childcare_element))
                trace.National_Childcare_Indicator("Values given in range {}", excelRange(national_childcare_indicator))
                trace.Total_lone_parents("Values given in range {}", excelRange(total_lone_parents))
                trace.Lone_parent_families_Benefitting_from_the_childcare_element("Values given in range {}", excelRange(lone_parent_families_benefitting_from_the_childcare_element))
                trace.Total_out_of_work("Values given in range {}", excelRange(total_out_of_work))
                trace.Lone_parents("Values given in range {}", excelRange(lone_parents))
                trace.Couples("Values given in range {}", excelRange(couples))
                trace.Value("Values given in range {}", excelRange(obs))
                
                dimensions = [
                    HDimConst('Period', period),
                    HDim(local_authority_code, 'Local authority code', DIRECTLY, LEFT),
                    HDim(local_authority_name, 'Local authority', DIRECTLY, LEFT),
                    HDim(region, 'Region', DIRECTLY, LEFT),
                    HDim(data_zone_code, 'Data Zone code', DIRECTLY, LEFT),
                    HDim(data_zone_name, 'Data Zone name', DIRECTLY, LEFT),
                    HDim(all_tax_credits_recipient_families, 'All tax credits recipient families', DIRECTLY, RIGHT),
                    HDim(WTC_and_CTC, 'WTC and CTC', DIRECTLY, RIGHT),
                    HDim(CTC_only, 'CTC only', DIRECTLY, RIGHT),
                    HDim(WTC_only, 'WTC only', DIRECTLY, RIGHT),
                    HDim(total_in_work, 'Total in work', DIRECTLY, RIGHT),
                    HDim(benefitting_from_the_childcare_element, 'Benefitting from the childcare element', DIRECTLY, RIGHT),
                    HDim(national_childcare_indicator, 'National Childcare Indicator', DIRECTLY, RIGHT),
                    HDim(total_lone_parents, 'Total lone parents', DIRECTLY, RIGHT),
                    HDim(lone_parent_families_benefitting_from_the_childcare_element, 'Lone parent families Benefitting from the childcare element', DIRECTLY, RIGHT),
                    HDim(total_out_of_work, 'Total out of work', DIRECTLY, RIGHT),
                    HDim(lone_parents, 'Lone parents', DIRECTLY, RIGHT),
                    HDim(couples, 'Couples', DIRECTLY, RIGHT)                    
                    ]
                
                tidy_sheet = ConversionSegment(tab, dimensions, obs)
                trace.with_preview(tidy_sheet)
                
                tidy_sheet_aspandas = tidy_sheet.topandas()
                tidy_sheet_aspandas = tidy_sheet_aspandas.rename(columns={'OBS':'Value'})
                tidy_sheet_aspandas = tidy_sheet_aspandas[columns]
                
                trace.store(tab_name, tidy_sheet_aspandas)
                tidied_sheets[tab_name] = tidy_sheet_aspandas
                
            elif tab.name.lower() == 'children':
                
                columns = [
                    'Period', 'Region', 'Local authority code', 'Local authority', 'Data Zone code','Data Zone name',
                    'All children within families receiving tax credits',
                    'WTC and CTC', 'CTC only', 'Total children within in work families', 'Lone parents within in work families', 
                    'Total children within out of work families', 'Lone parents within out of work families', 
                    'Couples', 'Value'
                ]
                
                trace.start(dataset_title, tab, columns, link)
                
                local_authority_code = tab.filter(contains_string('Local authority code')).fill(DOWN).is_not_number().is_not_blank() - footnotes
                local_authority_name = local_authority_code.shift(1, 0)
                region = local_authority_code.shift(-1, 0)
                data_zone_code = local_authority_code.shift(2, 0)
                data_zone_name = local_authority_code.shift(3, 0)

                obs = tab.filter(contains_string('All children within families registered for Child Benefit')).fill(DOWN).is_not_blank()
                obs = obs.same_row(region)
                all_children_within_families_receiving_tax_credits = tab.filter(contains_string('All children within families receiving tax credits')).fill(DOWN).is_not_blank()
                all_children_within_families_receiving_tax_credits = all_children_within_families_receiving_tax_credits.same_row(region)

                WTC_and_CTC = tab.filter(contains_string('WTC and CTC')).fill(DOWN).is_not_blank()
                WTC_and_CTC = WTC_and_CTC.same_row(region)
                CTC_only = WTC_and_CTC.shift(1, 0)
                total_children_within_in_work_families = WTC_and_CTC.shift(2, 0)
                lone_parents_within_in_work_families = WTC_and_CTC.shift(3, 0)

                total_children_within_out_of_work_families = tab.filter(contains_string('Children within out-of-work families')).shift(DOWN).fill(DOWN).is_not_blank()
                total_children_within_out_of_work_families = total_children_within_out_of_work_families.same_row(region)
                lone_parents_within_out_of_work_families = total_children_within_out_of_work_families.shift(1, 0)
                couples = total_children_within_out_of_work_families.shift(2, 0)
                
                # tracing dimensions
                trace.Period("Value taken from dataset title: {}".format(period))
                trace.Region("Values given in range {}", excelRange(region)) 
                trace.Local_authority_code("Values given in range {}", excelRange(local_authority_code))
                trace.Local_authority("Values given in range {}", excelRange(local_authority_name))
                trace.Data_Zone_code("Values given in range {}", excelRange(data_zone_code))
                trace.Data_Zone_name("Values given in range {}", excelRange(data_zone_name))
                trace.All_children_within_families_receiving_tax_credits("Values given in range {}", excelRange(all_children_within_families_receiving_tax_credits))
                trace.WTC_and_CTC("Values given in range {}", excelRange(WTC_and_CTC))
                trace.CTC_only("Values given in range {}", excelRange(CTC_only))
                trace.Total_children_within_in_work_families("Values given in range {}", excelRange(total_children_within_in_work_families))
                trace.Lone_parents_within_in_work_families("Values given in range {}", excelRange(benefitting_from_the_childcare_element))
                trace.Total_children_within_out_of_work_families("Values given in range {}", excelRange(total_children_within_out_of_work_families))
                trace.Lone_parents_within_out_of_work_families("Values given in range {}", excelRange(lone_parents_within_out_of_work_families))
                trace.Couples("Values given in range {}", excelRange(couples))
                trace.Value("Values given in range {}", excelRange(obs))
                
                dimensions = [
                    HDimConst('Period', period),
                    HDim(local_authority_code, 'Local authority code', DIRECTLY, LEFT),
                    HDim(local_authority_name, 'Local authority', DIRECTLY, LEFT),
                    HDim(region, 'Region', DIRECTLY, LEFT),
                    HDim(data_zone_code, 'Data Zone code', DIRECTLY, LEFT),
                    HDim(data_zone_name, 'Data Zone name', DIRECTLY, LEFT),
                    HDim(all_children_within_families_receiving_tax_credits, 'All children within families receiving tax credits', DIRECTLY, RIGHT),
                    HDim(WTC_and_CTC, 'WTC and CTC', DIRECTLY, RIGHT),
                    HDim(CTC_only, 'CTC only', DIRECTLY, RIGHT),
                    HDim(total_children_within_in_work_families, 'Total children within in work families', DIRECTLY, RIGHT),
                    HDim(lone_parents_within_in_work_families, 'Lone parents within in work families', DIRECTLY, RIGHT),
                    HDim(total_children_within_out_of_work_families, 'Total children within out of work families', DIRECTLY, RIGHT),
                    HDim(lone_parents_within_out_of_work_families, 'Lone parents within out of work families', DIRECTLY, RIGHT),
                    HDim(couples, 'Couples', DIRECTLY, RIGHT)                    
                    ]
                
                tidy_sheet = ConversionSegment(tab, dimensions, obs)
                trace.with_preview(tidy_sheet)
                
                tidy_sheet_aspandas = tidy_sheet.topandas()
                tidy_sheet_aspandas = tidy_sheet_aspandas.rename(columns={'OBS':'Value'})
                tidy_sheet_aspandas = tidy_sheet_aspandas[columns]
                
                trace.store(tab_name, tidy_sheet_aspandas)
                tidied_sheets[tab_name] = tidy_sheet_aspandas
                
                
out = Path('out')
out.mkdir(exist_ok=True)
    
trace.render("spec_v1.html")
                
for key in tidied_sheets:
    print(key) 
    #df = tidied_sheets[key]
    #df.drop_duplicates().to_csv(out / f'{key}.csv', index=False)
    
"""
### Some notes on the transform ###

Transform only pulls in the most recent data 

I have only used LSOA and Scottish zones data, not local authority or higher

It was not obvious to me which column of data should be used as the 'Value' so i have used the 'All Child Benefit recipient families' column for the 'family'/'families' tabs and 'All children within families registered for Child Benefit' for the 'children' tabs

These can all be rectified easily if needed
"""