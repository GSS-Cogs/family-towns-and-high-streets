from gssutils import * 
import json 
import datetime

info = json.load(open('info.json')) 
etl_title = info["title"] 
etl_publisher = info["publisher"]
print("Publisher: " + etl_publisher) 
print("Title: " + etl_title) 

scraper = Scraper(seed="info.json")   
scraper 

trace = TransformTrace()
tidied_data = {} # dataframes will be stored in here

def TimeFormatter(value):
    # function to return time as yyyy-mm-dd
    value_split = value[:2] + '-' + value[2:5].title() + '-' + value[5:]
    value_as_datetime = datetime.datetime.strptime(value_split, '%d-%b-%Y')
    new_value = datetime.datetime.strftime(value_as_datetime, '%Y-%m-%d')
    return new_value 

# hacky way of returning latest data
current_date = datetime.datetime.now().date()
current_month = current_date.strftime('%B')
while scraper.distributions == []:
    scraper.select_dataset(title=lambda x: current_month in x, latest=True)
    current_date = datetime.datetime(current_date.year, current_date.month-1, current_date.day)
    current_month = current_date.strftime('%B')

for dist in scraper.distributions:
    # ignore any non csvs
    if not dist.downloadURL.endswith('.csv'):
        continue
        
    # ignore mapping csv - is just a reference csv, no data
    if 'mapping' in dist.title.lower():
        continue
        
    link = dist.downloadURL
    title = dist.title.split(':')[-1].strip()
    
    if 'males' in dist.title.lower(): # also covers females
        columns = ['Period', 'CCG_CODE', 'ONS_CCG_CODE', 'ORG_CODE', 'POSTCODE', 'SEX', 'AGE', 'Value']
        trace.start(scraper.title, title, columns, link)
        df = dist.as_pandas()
        
        trace.Period('Values taken from "EXTRACT_DATE" column')
        trace.CCG_CODE('Values taken from "CCG_CODE" column')
        trace.ONS_CCG_CODE('Values taken from "ONS_CCG_CODE" column')
        trace.ORG_CODE('Values taken from "ORG_CODE" column')
        trace.POSTCODE('Values taken from "POSTCODE" column')
        trace.SEX('Values taken from "SEX" column')
        trace.AGE('Values taken from "AGE" column')
        trace.Value('Values taken from "NUMBER_OF_PATIENTS" column')
        
        df = df.rename(columns={'EXTRACT_DATE':'Period', 'NUMBER_OF_PATIENTS':'Value'})
        trace.Period('Rename column from "EXTRACT_DATE" to "Period"')
        trace.Value('Rename column from "NUMBER_OF_PATIENTS" to "Value"')
        
        df['Period'] = df['Period'].apply(TimeFormatter)
        trace.Period('Values have been formatted to "yyyy-mm-dd"')
        
        trace.store(title, df)
        tidied_data[title] = df
        
    elif 'age groups' in dist.title.lower():
        columns = ['Period', 'PUBLICATION', 'ORG_TYPE', 'ORG_CODE', 'ONS_CODE', 'POSTCODE', 'SEX', 'AGE_GROUP_5', 'Value']
        trace.start(scraper.title, title, columns, link)
        df = dist.as_pandas()
        
        trace.Period('Values taken from "EXTRACT_DATE" column')
        trace.PUBLICATION('Values taken from "PUBLICATION" column')
        trace.ORG_TYPE('Values taken from "ORG_TYPE" column')
        trace.ORG_CODE('Values taken from "ORG_CODE" column')
        trace.ONS_CODE('Values taken from "ONS_CODE" column')
        trace.POSTCODE('Values taken from "POSTCODE" column')
        trace.SEX('Values taken from "SEX" column')
        trace.AGE_GROUP_5('Values taken from "AGE_GROUP_5" column')
        trace.Value('Values taken from "NUMBER_OF_PATIENTS" column')
        
        df = df.rename(columns={'EXTRACT_DATE':'Period', 'NUMBER_OF_PATIENTS':'Value'})
        trace.Period('Rename column from "EXTRACT_DATE" to "Period"')
        trace.Value('Rename column from "NUMBER_OF_PATIENTS" to "Value"')
        
        df['Period'] = df['Period'].apply(TimeFormatter)
        trace.Period('Values have been formatted to "yyyy-mm-dd"')
        
        # reordered df
        df = df[columns]
        
        trace.store(title, df)
        tidied_data[title] = df
        
    elif 'totals' in dist.title.lower():
        columns = ['Period', 'PUBLICATION', 'TYPE', 'CCG_CODE', 'ONS_CCG_CODE', 'CODE', 'POSTCODE', 'SEX', 'AGE', 'Value']
        trace.start(scraper.title, title, columns, link)
        df = dist.as_pandas()
        
        trace.Period('Values taken from "EXTRACT_DATE" column')
        trace.PUBLICATION('Values taken from "PUBLICATION" column')
        trace.TYPE('Values taken from "TYPE" column')
        trace.CCG_CODE('Values taken from "CCG_CODE" column')
        trace.ONS_CCG_CODE('Values taken from "ONS_CCG_CODE" column')
        trace.CODE('Values taken from "CODE" column')
        trace.POSTCODE('Values taken from "POSTCODE" column')
        trace.SEX('Values taken from "SEX" column')
        trace.AGE('Values taken from "AGE" column')
        trace.Value('Values taken from "NUMBER_OF_PATIENTS" column')
        
        df = df.rename(columns={'EXTRACT_DATE':'Period', 'NUMBER_OF_PATIENTS':'Value'})
        trace.Period('Rename column from "EXTRACT_DATE" to "Period"')
        trace.Value('Rename column from "NUMBER_OF_PATIENTS" to "Value"')
        
        df['Period'] = df['Period'].apply(TimeFormatter)
        trace.Period('Values have been formatted to "yyyy-mm-dd"')
        
        # reordered df
        df = df[columns]
        
        trace.store(title, df)
        tidied_data[title] = df
        
    elif 'region' in dist.title.lower():
        columns = ['Period', 'PUBLICATION', 'ORG_TYPE', 'ORG_CODE', 'ONS_CODE', 'SEX', 'AGE', 'Value']
        trace.start(scraper.title, title, columns, link)
        df = dist.as_pandas()
        
        trace.Period('Values taken from "EXTRACT_DATE" column')
        trace.PUBLICATION('Values taken from "PUBLICATION" column')
        trace.ORG_TYPE('Values taken from "ORG_TYPE" column')
        trace.ORG_CODE('Values taken from "ORG_CODE" column')
        trace.ONS_CODE('Values taken from "ONS_CODE" column')
        trace.SEX('Values taken from "SEX" column')
        trace.AGE('Values taken from "AGE" column')
        trace.Value('Values taken from "NUMBER_OF_PATIENTS" column')
        
        df = df.rename(columns={'EXTRACT_DATE':'Period', 'NUMBER_OF_PATIENTS':'Value'})
        trace.Period('Rename column from "EXTRACT_DATE" to "Period"')
        trace.Value('Rename column from "NUMBER_OF_PATIENTS" to "Value"')
        
        df['Period'] = df['Period'].apply(TimeFormatter)
        trace.Period('Values have been formatted to "yyyy-mm-dd"')
        
        # reordered df
        df = df[columns]
        
        trace.store(title, df)
        tidied_data[title] = df
        
out = Path('out')
out.mkdir(exist_ok=True)

trace.render("spec_v1.html")
for key in tidied_data:
    df = tidied_data[key]
    df.drop_duplicates().to_csv(out / f'{key}.csv', index=False)
    
