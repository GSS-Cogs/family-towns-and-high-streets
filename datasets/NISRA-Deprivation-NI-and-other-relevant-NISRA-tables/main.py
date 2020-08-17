etl_title = 'NISRA Deprivation  NI   and other relevant NISRA tables' 

print(etl_title) 
from gssutils import * 
import json 

info = json.load(open('info.json')) 
scraper = Scraper(seed="info.json")   
scraper 
