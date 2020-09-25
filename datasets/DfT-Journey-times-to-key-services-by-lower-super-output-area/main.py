# +
from gssutils import *
import json

scraper = Scraper(seed="info.json")  
scraper.select_dataset(title=lambda t: 'Journey times to key services by lower super output area (JTS05)' in t)
scraper

# -

# Observations are alreay transformed on Google drive\n",
url = "https://drive.google.com/file/d/1SeekTbw2ShjSws_I5G5bTG8va0hhJ5wg/view?usp=download"
path = 'https://drive.google.com/uc?export=download&id='+url.split('/')[-2]
df = pd.read_csv(path)


df['Field Code'] = df['Field Code'].apply(pathify)
df['Year'] = 'year/' + df['Year'].astype(str)
df = df.head(10)
df.head(60)

# +
import os
from urllib.parse import urljoin

notes = """
    2017 journey times have been influenced by changes to the network of walking paths being used for the calculations. The network is more extensive in 2017 reflecting changes to the underlying Ordnance Survey
    Urban Paths data set which is used (this has the effect of reducing the time taken for some trips where a relevant path has been added to the dataset).
    Full details of the datasets for the production of all the estimates are provided in the accompanying guidance note - 
    https://www.gov.uk/government/publications/journey-time-statistics-guidance.
"""

csvName = "travel_times_observations.csv"
out = Path('out')
out.mkdir(exist_ok=True)
df.drop_duplicates().to_csv(out / csvName, index = False)

datasetExtraName = '-travel-time'
scraper.dataset.family = 'towns-and-high-streets'
scraper.dataset.description = scraper.dataset.description + notes
scraper.dataset.comment = 'Travel time, destination and origin indicators for Employment centres by mode of travel, Lower Super Output Area (LSOA), England'
scraper.dataset.title = 'Journey times for Employment centres by lower super output area (LSOA) - JTS05'

dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name)).lower() #+ datasetExtraName
scraper.set_base_uri('http://gss-data.org.uk')
scraper.set_dataset_id(dataset_path)

csvw_transform = CSVWMapping()
csvw_transform.set_csv(out / csvName)
csvw_transform.set_mapping(json.load(open('info.json')))
csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
csvw_transform.write(out / f'{csvName}-metadata.json')

with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())
# -


"""
info = json.load(open('info.json')) 
codelistcreation = info['transform']['codelists'] 
print(codelistcreation)
print("-------------------------------------------------------")

codeclass = CSVCodelists()
for cl in codelistcreation:
    if cl in df.columns:
        print(df[cl])
        df[cl] = df[cl].str.replace("-"," ")
        df[cl] = df[cl].str.capitalize()
        codeclass.create_codelists(pd.DataFrame(df[cl]), 'codelists', scraper.dataset.family, Path(os.getcwd()).name.lower() + datasetExtraName)
"""

# +
#df['Field Code'].unique()
# -


