#!/usr/bin/env python
# coding: utf-8

# In[41]:



from gssutils import *
import json
import re
from urllib.request import Request, urlopen

def all_same(items):
    if len(items) == 0:
        return False
    return all(x == items[0] for x in items)

info = json.load(open('info.json'))
etl_title = info["title"]
etl_publisher = info["publisher"][0]
print("Publisher: " + etl_publisher)
print("Title: " + etl_title)


# In[42]:


req = Request("https://www.gov.uk/government/statistics/english-indices-of-deprivation-2019", headers={'User-Agent': 'Mozilla/5.0'})
html = urlopen(req).read()
plaintext = html.decode('utf8')
links = re.findall("href=[\"\'](.*?)[\"\']", plaintext)
links = [x for x in links if x.endswith("csv")]
if all_same(links) == True:
    dataURL = links[0]
    with open('info.json', 'r+') as info:
        data = json.load(info)
        data["dataURL"] = dataURL
        info.seek(0)
        json.dump(data, info, indent=4)
        info.truncate()
        print("Data URL found and added to info.json")
else:
    print("Multiple or No files found, investigate or retrieve manually")
    for i in links:
        print(i)

scraper = Scraper(seed = "info.json")
scraper.distributions[0].title = etl_title
scraper


# In[44]:


tab = scraper.distributions[0].as_pandas()
tab

