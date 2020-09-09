from gssutils import * 
import json 
import os
from urllib.parse import urljoin
from gssutils.metadata import THEME

info = json.load(open('info.json')) 
etl_title = info["Name"] 
etl_publisher = info["Producer"][0] 
print("Publisher: " + etl_publisher) 
print("Title: " + etl_title) 

scraper = Scraper(seed="info.json")   
scraper 
#########################################################################################################



#########################################################################################################
#### CODE BELOW HAS NOT BEEN TESTED ####
#########################################################################################################
out = Path('out')
out.mkdir(exist_ok=True)
merged.to_csv(out / 'observations.csv', index = False)

scraper.dataset.family = 'towns-and-high-streets'

scraper.dataset.theme = THEME[scraper.dataset.family]

scraper.dataset.description = scraper.dataset.description + 
    """
        \nArea codes implemented in line with GSS Coding and Naming policy
        \nThe figures have been independently rounded to the nearest 5. This can lead to components as shown not summing totals as shown
    """
scraper.dataset.title = 'Child Benefit small area statistics'
scraper.dataset.comment = 'Annual geographical estimates at Lower Super Output Area and Data Zone of the number of families and children claiming Child Benefit'
dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name))
scraper.set_base_uri('http://gss-data.org.uk')
scraper.set_dataset_id(dataset_path)

csvw_transform = CSVWMapping()
csvw_transform.set_csv(out / 'observations.csv')
csvw_transform.set_mapping(json.load(open('info.json')))
csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
csvw_transform.write(out / 'observations.csv-metadata.json')
with open(out / 'observations.csv-metadata.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())

#########################################################################################################