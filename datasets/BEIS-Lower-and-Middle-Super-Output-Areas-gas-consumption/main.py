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
                    
                    columns = ['Year', 'Local Authority Code', 'MSOA Code', 'LSOA Code', 'Number of Meters', 'Domestic Use', 'Mean Consumption', 'Median Consumption', 'Measure Type', 'Unit', 'Value']
                    trace.start(scraper.title, name, columns, link)
                    
                    table = pd.read_csv(file, dtype=str)
                    
                    year = file_name[-4:] # year is last 4 characters of file_name
                    table["Year"] = "year/" + year
                    
                    domestic = "yes" # data is domestic gas only
                    table["Domestic Use"] = domestic
                    
                    measure_type = "Gas Consumption" 
                    table["Measure Type"] = measure_type
                    
                    unit = "kWh"
                    table["Unit"] = unit
                    
                    trace.Year("Value taken from CSV file name: {}".format(year))
                    trace.Local_Authority_Code("Values taken from 'LACode' column")
                    trace.MSOA_Code("Values taken from 'MSOACode' column")
                    trace.LSOA_Code("Values taken from 'LSOACode' column")
                    trace.Number_of_Meters("Values taken from 'METERS' column")
                    trace.Domestic_Use("Source file only contains Domestic use observations")
                    trace.Mean_Consumption("Values taken from 'MEAN' column")
                    trace.Median_Consumption("Values taken from 'MEDIAN' column")
                    trace.Value("Values taken from 'KWH' column")
                    trace.Measure_Type("Hardcoded as: {}".format(measure_type))
                    trace.Unit('Hardcoded as: {}'.format(unit))
                    
                    table = table.drop(['LAName', 'MSOAName', 'LSOAName'], axis=1) # unwanted columns

                    # renaming columns
                    table = table.rename(columns={
                        'LACode':'Local Authority Code',
                        'MSOACode':'MSOA Code',
                        'LSOACode':'LSOA Code',
                        'METERS':'Number of Meters',
                        'KWH':'Value',
                        'MEAN':'Mean Consumption',
                        'MEDIAN':'Median Consumption'
                        }
                    )

                    trace.Local_Authority_Code("Rename column from 'LACode' to 'Local Authority Code'")
                    trace.MSOA_Code("Rename column from 'MSOACode' to 'MSOA Code'")
                    trace.LSOA_Code("Rename column from 'LSOACode' to 'LSOA Code'")
                    trace.Number_of_Meters("Rename column from 'METERS' to 'Number of Meters'")
                    trace.Mean_Consumption("Rename column from 'MEAN' to 'Mean Consumption'")
                    trace.Median_Consumption("Rename column from 'MEDIAN' to 'Median Consumption'")
                    trace.Value("Rename column from 'KWH' to 'Value'")

                    trace.store(file_name, table)
                    tidied_sheets[file_name] = table
                    
    # MSOA domestic data second
    elif distribution.downloadURL.endswith('zip') and 'MSOA domestic' in distribution.title:
        with ZipFile(BytesIO(scraper.session.get(distribution.downloadURL).content)) as zip:
            for name in zip.namelist()[1:]:
                with zip.open(name, 'r') as file:
                    
                    link = distribution.downloadURL
                    file_name = name.split("/")[-1].split(".")[0] # the name of the file
                    
                    columns = ['Year', 'Local Authority Code', 'MSOA Code', 'Number of Meters', 'Domestic Use', 'Mean Consumption', 'Median Consumption', 'Measure Type', 'Unit', 'Value']
                    trace.start(scraper.title, name, columns, link)
                    
                    table = pd.read_csv(file, dtype=str)
                    
                    year = file_name[-4:] # year is last 4 characters of file_name
                    table["Year"] = "year/" + year
                    
                    domestic = "yes" # data is domestic gas only
                    table["Domestic Use"] = domestic
                    
                    measure_type = "Gas Consumption" 
                    table["Measure Type"] = measure_type
                    
                    unit = "kWh"
                    table["Unit"] = unit
                    
                    trace.Year("Value taken from CSV file name: {}".format(year))
                    trace.Local_Authority_Code("Values taken from 'LACode' column")
                    trace.MSOA_Code("Values taken from 'MSOACode' column")
                    trace.Number_of_Meters("Values taken from 'METERS' column")
                    trace.Domestic_Use("Source file only contains Domestic use observations")
                    trace.Mean_Consumption("Values taken from 'MEAN' column")
                    trace.Median_Consumption("Values taken from 'MEDIAN' column")
                    trace.Value("Values taken from 'KWH' column")
                    trace.Measure_Type("Hardcoded as: {}".format(measure_type))
                    trace.Unit('Hardcoded as: {}'.format(unit))
                    
                    table = table.drop(['LAName', 'MSOAName'], axis=1) # unwanted columns

                    # renaming columns
                    table = table.rename(columns={
                        'LACode':'Local Authority Code',
                        'MSOACode':'MSOA Code',
                        'METERS':'Number of Meters',
                        'KWH':'Value',
                        'MEAN':'Mean Consumption',
                        'MEDIAN':'Median Consumption'
                        }
                    )

                    trace.Local_Authority_Code("Rename column from 'LACode' to 'Local Authority Code'")
                    trace.MSOA_Code("Rename column from 'MSOACode' to 'MSOA Code'")
                    trace.Number_of_Meters("Rename column from 'METERS' to 'Number of Meters'")
                    trace.Mean_Consumption("Rename column from 'MEAN' to 'Mean Consumption'")
                    trace.Median_Consumption("Rename column from 'MEDIAN' to 'Median Consumption'")
                    trace.Value("Rename column from 'KWH' to 'Value'")

                    trace.store(file_name, table)
                    tidied_sheets[file_name] = table
                    
    # MSOA 'Non' domestic data next
    elif distribution.downloadURL.endswith('zip') and 'MSOA non domestic' in distribution.title:
        with ZipFile(BytesIO(scraper.session.get(distribution.downloadURL).content)) as zip:
            for name in zip.namelist()[1:]:
                with zip.open(name, 'r') as file:
                    
                    link = distribution.downloadURL
                    file_name = name.split("/")[-1].split(".")[0] # the name of the file
                    
                    columns = ['Year', 'Local Authority Code', 'MSOA Code', 'Number of Meters', 'Domestic Use', 'Mean Consumption', 'Median Consumption', 'Measure Type', 'Unit', 'Value']
                    trace.start(scraper.title, name, columns, link)
                    
                    table = pd.read_csv(file, dtype=str)
                    
                    year = file_name[-4:] # year is last 4 characters of file_name
                    table["Year"] = "year/" + year
                    
                    domestic = "no" # data is non domestic gas only
                    table["Domestic Use"] = domestic
                    
                    measure_type = "Gas Consumption" 
                    table["Measure Type"] = measure_type
                    
                    unit = "kWh"
                    table["Unit"] = unit
                    
                    trace.Year("Value taken from CSV file name: {}".format(year))
                    trace.Local_Authority_Code("Values taken from 'LACode' column")
                    trace.MSOA_Code("Values taken from 'MSOACode' column")
                    trace.Number_of_Meters("Values taken from 'METERS' column")
                    trace.Domestic_Use("Source file only contains Non-Domestic use observations")
                    trace.Mean_Consumption("Values taken from 'MEAN' column")
                    trace.Median_Consumption("Values taken from 'MEDIAN' column")
                    trace.Value("Values taken from 'KWH' column")
                    trace.Measure_Type("Hardcoded as: {}".format(measure_type))
                    trace.Unit('Hardcoded as: {}'.format(unit))
                    
                    table = table.drop(['LAName', 'MSOAName'], axis=1) # unwanted columns

                    # renaming columns
                    table = table.rename(columns={
                        'LACode':'Local Authority Code',
                        'MSOACode':'MSOA Code',
                        'METERS':'Number of Meters',
                        'KWH':'Value',
                        'MEAN':'Mean Consumption',
                        'MEDIAN':'Median Consumption'
                        }
                    )

                    trace.Local_Authority_Code("Rename column from 'LACode' to 'Local Authority Code'")
                    trace.MSOA_Code("Rename column from 'MSOACode' to 'MSOA Code'")
                    trace.Number_of_Meters("Rename column from 'METERS' to 'Number of Meters'")
                    trace.Mean_Consumption("Rename column from 'MEAN' to 'Mean Consumption'")
                    trace.Median_Consumption("Rename column from 'MEDIAN' to 'Median Consumption'")
                    trace.Value("Rename column from 'KWH' to 'Value'")

                    trace.store(file_name, table)
                    tidied_sheets[file_name] = table
                    

out = Path('out')
out.mkdir(exist_ok=True)

for key in tidied_sheets:
    df = tidied_sheets[key]
    df.drop_duplicates().to_csv(out / f'{key}.csv', index=False)
    
trace.render("spec_v1.html")