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


# +
#for key in tidied_sheets:
#    print(key)
# -

trace.render("spec_v1.html")

# Only output LSOA data for now until PMD4 can handle multiple outputs
lsoa_dat = pd.DataFrame(columns=tidied_sheets['LSOA_GAS_2018'].columns)
for key in tidied_sheets:
    if 'LSOA' in key:
        print('joining: ' + key)
        lsoa_dat = pd.concat([lsoa_dat,pd.DataFrame(tidied_sheets[key])], sort=False)

# Remove attributes for now until we dicide whow we are handling them
del lsoa_dat['LAName']
del lsoa_dat['MSOAName']
del lsoa_dat['LSOAName']
del lsoa_dat['METERS']
del lsoa_dat['MEAN']
del lsoa_dat['MEDIAN']

# +
#Rename the columns to match the Electricity pipeline
'''
lsoa_dat = lsoa_dat.rename(columns=
                           {
                               'METERS': 'Total number of domestic electricity meters', 
                               'MEAN': 'Mean domestic electricity consumption kWh per meter',
                               'MEDIAN': 'Median domestic electricity consumption kWh per meter'
                           })
'''


lsoa_dat = lsoa_dat.rename(columns=
                           {
                               'Period': 'Year',
                               'LACode': 'Local Authority', 
                               'MSOACode': 'Middle Layer Super Output Area',
                               'LSOACode': 'Lower Layer Super Output Area'
                           })
lsoa_dat['Year'] = 'year/' + lsoa_dat['Year'].astype(str)
# -

lsoa_dat.head(10)

# +
import os
from urllib.parse import urljoin

notes = 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/853104/sub-national-methodology-guidance.pdf'

csvName = 'lsoa_observations.csv'
out = Path('out')
out.mkdir(exist_ok=True)
df.drop_duplicates().to_csv(out / csvName, index = False)

scraper.dataset.family = 'towns-high-streets'
scraper.dataset.description = scraper.dataset.description + '\nGuidance documentation can be found here:\n' + notes
#scraper.dataset.comment = 'Total domestic gas consumption, number of meters, mean and median consumption for LSOA regions across England, Wales & Scotland'
scraper.dataset.comment = 'Total domestic gas consumption for LSOA regions across England, Wales & Scotland'
scraper.dataset.title = 'Lower Super Output Areas (LSOA) gas consumption'

dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name)).lower()
scraper.set_base_uri('http://gss-data.org.uk')
scraper.set_dataset_id(dataset_path)


csvw_transform = CSVWMapping()
csvw_transform.set_csv(out / csvName)
csvw_transform.set_mapping(json.load(open('info.json')))
csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
csvw_transform.write(out / f'{csvName}-metadata.json')
# Remove subset of data
#out / csvName).unlink()
with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())
# -


