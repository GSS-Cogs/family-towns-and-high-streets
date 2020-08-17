etl_title = 'SG Scottish Index of Multiple Deprivation 2020' 

print(etl_title) 
from gssutils import * 
import json 

info = json.load(open('info.json')) 
scraper = Scraper(seed="info.json")   
scraper 
