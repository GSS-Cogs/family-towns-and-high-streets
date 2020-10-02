# +
from gssutils import *
import json

scraper = Scraper(seed="info.json")  
scraper.select_dataset(title=lambda t: 'Journey times to key services by lower super output area (JTS05)' in t)
scraper

# -

uris = [
    "https://drive.google.com/file/d/1SeekTbw2ShjSws_I5G5bTG8va0hhJ5wg/view?usp=download",
    "https://drive.google.com/file/d/1cPkyBV-YdqaA6z15ew-M_qEW1pL5yDNS/view?usp=download",
    "https://drive.google.com/file/d/1f9TshV-To_t913j6lfMIajmFLpK_X86Q/view?usp=download",
    "https://drive.google.com/file/d/1Y0nMpj9b8rftTT4I7h3IGIvGUzkin4zr/view?usp=download",
    "https://drive.google.com/file/d/15KNDFI7WYcEmfj-6yTAO8LrzTZlC7NUE/view?usp=download",
    "https://drive.google.com/file/d/1sl301ieObzhPiHAIRRVyETH6ucyvjOBC/view?usp=download",
    "https://drive.google.com/file/d/1hwAQuZAazD6xEQcG9OdKiOWIpL7RkRjQ/view?usp=download",
    "https://drive.google.com/file/d/1xmoeawBJ2rB24Nn1GpwvjaOej2lvC6XA/view?usp=download",
    "https://drive.google.com/file/d/1lcFBa3KUsaBd4P0wJdobD5uCjxcXbq4I/view?usp=download"
]
dn = [
    "Employment Centres",
    "Primary Schools",
    "Secondary Schools",
    "Further Education",
    "General Practices",
    "Hospitals",
    "Food Stores",
    "Town Centre",
    "Pharmacies"
]
ag = [
    "(16 to 74 year old)",
    "(5 to 10 year old)",
    "(11 to 15 year old)",
    "(16 to 19 year old)",
    "(All households)",
    "(All households)",
    "(All households)",
    "(All households)",
    "(All households)"
]

# +
import os
from urllib.parse import urljoin
scraper.dataset.family = 'towns-and-high-streets'
scraper.set_base_uri('http://gss-data.org.uk')

out = Path('out')
out.mkdir(exist_ok=True)

notes = """
    2017 journey times have been influenced by changes to the network of walking paths being used for the calculations. The network is more extensive in 2017 reflecting changes to the underlying Ordnance Survey
    Urban Paths data set which is used (this has the effect of reducing the time taken for some trips where a relevant path has been added to the dataset).
    Full details of the datasets for the production of all the estimates are provided in the accompanying guidance note - 
    https://www.gov.uk/government/publications/journey-time-statistics-guidance.
"""

i = 0
for u in uris:
    path = 'https://drive.google.com/uc?export=download&id='+u.split('/')[-2]
    df = pd.read_csv(path)
    df = df.rename(columns={'Field Code': 'Mode of Travel'})
    df['Mode of Travel'] = df['Mode of Travel'].apply(pathify)
    df['Year'] = 'year/' + df['Year'].astype(str)
    #df = df.head(10)
    
    if dn[i] == "Employment Centres":
        df['Employment Centre Size'] = df['Mode of Travel']
        df['Employment Centre Size'] = df['Employment Centre Size'].replace({
            "100empptt": "employment centre with 100 to 499 jobs",
            "100empcyct": "employment centre with 100 to 499 jobs",
            "100empcart": "employment centre with 100 to 499 jobs",
            "500empptt": "employment centre with 500 to 4999 jobs",
            "500empcyct": "employment centre with 500 to 4999 jobs",
            "500empcart": "employment centre with 500 to 4999 jobs",
            "5000empptt": "employment centre with at least 5000 jobs",
            "5000empcyct": "employment centre with at least 5000 jobs",
            "5000empcart": "employment centre with at least 5000 jobs"
            })
        df['Employment Centre Size'] = df['Employment Centre Size'].apply(pathify)
        df = df[['Year','Lower Layer Super Output Area','Local Authority','Employment Centre Size','Mode of Travel','Value']]
    
    csvName = pathify(dn[i]).replace("-","_") + "_observations.csv"
    df.drop_duplicates().to_csv(out / csvName, index = False)

    datasetExtraName = '/' + pathify(dn[i])
    
    if i == 0:
        scraper.dataset.description = scraper.dataset.description + notes
        
    scraper.dataset.comment = f'Travel time, destination and origin indicators for {dn[i]} by mode of travel, Lower Super Output Area (LSOA), England {ag[i]}'
    scraper.dataset.title = f'Journey times to key services by lower super output area: {dn[i]} - JTS050{str(i+1)}'

    dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name)).lower() + datasetExtraName
    scraper.set_dataset_id(dataset_path)

    csvw_transform = CSVWMapping()
    csvw_transform.set_csv(out / csvName)
    csvw_transform.set_mapping(json.load(open('info.json')))
    csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
    csvw_transform.set_registry(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
    csvw_transform.write(out / f'{csvName}-metadata.json')

    with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
        metadata.write(scraper.generate_trig())
    i = i + 1
# +
#info = json.load(open('info.json')) 
#codelistcreation = ['Employment Centre Size'] 

#codeclass = CSVCodelists()
#for cl in codelistcreation:
#    if cl in df.columns:
#        df[cl] = df[cl].str.replace("-"," ")
#        df[cl] = df[cl].str.capitalize()
#        codeclass.create_codelists(pd.DataFrame(df[cl]), 'codelists', scraper.dataset.family, Path(os.getcwd()).name.lower())
# -


