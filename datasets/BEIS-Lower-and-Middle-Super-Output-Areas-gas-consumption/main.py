from gssutils import * 
import json
from zipfile import ZipFile
from io import BytesIO

info = json.load(open('info.json')) 
etl_title = info["title"] 
etl_publisher = info["publisher"]
print("Publisher: " + etl_publisher) 
print("Title: " + etl_title) 

scraper = Scraper(seed="info.json")   
scraper

trace = TransformTrace()
tidied_sheets = {} # dataframes will be stored in here

for distribution in scraper.distributions:
    
    # LSOA data first
    if distribution.downloadURL.endswith('zip') and 'LSOA' in distribution.title:
        with ZipFile(BytesIO(scraper.session.get(distribution.downloadURL).content)) as zip:
            for name in zip.namelist()[1:]:
                with zip.open(name, 'r') as file:
                    
                    link = distribution.downloadURL
                    file_name = name.split("/")[-1].split(".")[0] # the name of the file
                    
                    columns = ['Period', 'LA Name', 'LA Code', 'MSOA Name', 'MSOA Code', 'LSOA Name', 'LSOA Code', 'METERS', 'MEAN', 'MEDIAN', 'Value']
                    
                    trace.start(scraper.title, name, columns, link)
                    
                    table = pd.read_csv(file, dtype=str)
                    
                    year = file_name[-4:] # year is last 4 characters of file_name
                    table['Period'] = year
                    
                    trace.Period("Value taken from CSV file name: {}".format(year))
                    trace.LA_Name("Values taken from 'LAName' column")
                    trace.LA_Code("Values taken from 'LACode' column")
                    trace.MSOA_Name("Values taken from 'MSOAName' column")
                    trace.MSOA_Code("Values taken from 'MSOACode' column")
                    trace.LSOA_Name("Values taken from 'LSOAName' column")
                    trace.LSOA_Code("Values taken from 'LSOACode' column")
                    trace.METERS("Values taken from 'METERS' column")
                    trace.MEAN("Values taken from 'MEAN' column")
                    trace.MEDIAN("Values taken from 'MEDIAN' column")
                    trace.Value("Values taken from 'KWH' column")
                    
                    # rename column
                    table = table.rename(columns={'KWH':'Value'}) 
                    trace.Value('Rename column from "KWH" to "Value"')
                    
                    # reordering columns
                    table = table[[
                        'Period', 'LAName', 'LACode', 'MSOAName', 'MSOACode' ,'LSOAName', 'LSOACode', 'METERS', 'MEAN', 'MEDIAN', 'Value'
                    ]] 

                    trace.store(file_name, table)
                    tidied_sheets[file_name] = table
                    
    # MSOA domestic data second
    elif distribution.downloadURL.endswith('zip') and 'MSOA domestic' in distribution.title:
        with ZipFile(BytesIO(scraper.session.get(distribution.downloadURL).content)) as zip:
            for name in zip.namelist()[1:]:
                with zip.open(name, 'r') as file:
                    
                    link = distribution.downloadURL
                    file_name = name.split("/")[-1].split(".")[0] # the name of the file
                    
                    columns = ['Period', 'LA Name', 'LA Code', 'MSOA Name', 'MSOA Code', 'METERS', 'MEAN', 'MEDIAN', 'Value']
                    
                    trace.start(scraper.title, name, columns, link)
                    
                    table = pd.read_csv(file, dtype=str)
                    
                    year = file_name[-4:] # year is last 4 characters of file_name
                    table['Period'] = year
                    
                    trace.Period("Value taken from CSV file name: {}".format(year))
                    trace.LA_Name("Values taken from 'LAName' column")
                    trace.LA_Code("Values taken from 'LACode' column")
                    trace.MSOA_Name("Values taken from 'MSOAName' column")
                    trace.MSOA_Code("Values taken from 'MSOACode' column")
                    trace.METERS("Values taken from 'METERS' column")
                    trace.MEAN("Values taken from 'MEAN' column")
                    trace.MEDIAN("Values taken from 'MEDIAN' column")
                    trace.Value("Values taken from 'KWH' column")
                    
                    # rename column
                    table = table.rename(columns={'KWH':'Value'}) 
                    trace.Value('Rename column from "KWH" to "Value"')
                    
                    # reordering columns
                    table = table[[
                        'Period', 'LAName', 'LACode', 'MSOAName', 'MSOACode', 'METERS', 'MEAN', 'MEDIAN', 'Value'
                    ]] 

                    trace.store(file_name, table)
                    tidied_sheets[file_name] = table
                    
    # MSOA 'Non' domestic data next
    elif distribution.downloadURL.endswith('zip') and 'MSOA non domestic' in distribution.title:
        with ZipFile(BytesIO(scraper.session.get(distribution.downloadURL).content)) as zip:
            for name in zip.namelist()[1:]:
                with zip.open(name, 'r') as file:
                    
                    link = distribution.downloadURL
                    file_name = name.split("/")[-1].split(".")[0] # the name of the file
                    
                    columns = ['Period', 'LA Name', 'LA Code', 'MSOA Name', 'MSOA Code', 'METERS', 'MEAN', 'MEDIAN', 'Value']
                    
                    trace.start(scraper.title, name, columns, link)
                    
                    table = pd.read_csv(file, dtype=str)
                    
                    year = file_name[-4:] # year is last 4 characters of file_name
                    table['Period'] = year
                    
                    trace.Period("Value taken from CSV file name: {}".format(year))
                    trace.LA_Name("Values taken from 'LAName' column")
                    trace.LA_Code("Values taken from 'LACode' column")
                    trace.MSOA_Name("Values taken from 'MSOAName' column")
                    trace.MSOA_Code("Values taken from 'MSOACode' column")
                    trace.METERS("Values taken from 'METERS' column")
                    trace.MEAN("Values taken from 'MEAN' column")
                    trace.MEDIAN("Values taken from 'MEDIAN' column")
                    trace.Value("Values taken from 'KWH' column")
                    
                    # rename column
                    table = table.rename(columns={'KWH':'Value'}) 
                    trace.Value('Rename column from "KWH" to "Value"')
                    
                    # reordering columns
                    table = table[[
                        'Period', 'LAName', 'LACode', 'MSOAName', 'MSOACode', 'METERS', 'MEAN', 'MEDIAN', 'Value'
                    ]] 

                    trace.store(file_name, table)
                    tidied_sheets[file_name] = table
                    

out = Path('out')
out.mkdir(exist_ok=True)

for key in tidied_sheets:
    df = tidied_sheets[key]
    df.drop_duplicates().to_csv(out / f'{key}.csv', index=False)
    
trace.render("spec_v1.html")