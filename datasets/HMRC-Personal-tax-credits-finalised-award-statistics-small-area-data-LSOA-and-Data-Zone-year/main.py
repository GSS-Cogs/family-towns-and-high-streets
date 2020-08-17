etl_title = 'HMRC Personal tax credits  finalised award statistics - small area data  LSOA and Data Zone   year' 

print(etl_title) 
from gssutils import * 
import json 

info = json.load(open('info.json')) 
scraper = Scraper(seed="info.json")   
scraper 
