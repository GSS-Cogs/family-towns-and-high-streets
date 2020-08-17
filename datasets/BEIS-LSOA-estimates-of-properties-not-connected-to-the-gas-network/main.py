etl_title = 'BEIS LSOA estimates of properties not connected to the gas network' 

print(etl_title) 
from gssutils import * 
import json 

info = json.load(open('info.json')) 
scraper = Scraper(seed="info.json")   
scraper 
