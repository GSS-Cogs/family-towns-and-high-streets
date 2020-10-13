#!/usr/bin/env python
# coding: utf-8
# %%

# %%
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


# %%


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


# %%
tab = scraper.distributions[0].as_pandas()
#tab


# %%
tbls = []

for x in range(4, len(tab.columns)):
    cols = [0,2,x]
    dat = tab.iloc[:, cols]
    dat['Indices of Deprivation'] = dat.columns[2]
    dat = dat.rename(columns={dat.columns[2]:'Value','LSOA code (2011)':'Lower Layer Super Output Area','Local Authority District code (2019)':'Local Authority'})
    dat = dat[[dat.columns[0],dat.columns[1],dat.columns[3],dat.columns[2]]]
    tbls.append(dat)


# %%
i = 0
for t in tbls:
    if i == 0:
        joined_dat = t
    else:
        joined_dat = pd.concat([joined_dat,t])   
    i = i + 1

# %%
joined_dat['Indices of Deprivation'] = joined_dat['Indices of Deprivation'].apply(pathify)
#joined_dat.head(20)

# %%
import os
from urllib.parse import urljoin

yr = '2019'
notes = f"""
Statistics on relative deprivation in small areas in England, 2019.
These statistics update the English indices of deprivation 2015.
The English indices of deprivation measure relative deprivation in small areas in England called lower-layer super output areas. The index of multiple deprivation is the most widely used of these indices.
The statistical release and FAQ document (above) explain how the Indices of Deprivation {yr} (IoD{yr}) and the Index of Multiple Deprivation (IMD{yr}) can be used and expand on the headline points in the infographic. Both documents also help users navigate the various data files and guidance documents available.
The first data file contains the IMD{yr} ranks and deciles and is usually sufficient for the purposes of most users.
Mapping resources and links to the IoD{yr} explorer and Open Data Communities platform can be found on our IoD{yr} mapping resource page.
Further detail is available in the research report, which gives detailed guidance on how to interpret the data and presents some further findings, and the technical report, which describes the methodology and quality assurance processes underpinning the indices.
\nStatistical release main findings:
https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/835115/IoD{yr}_Statistical_Release.pdf
\nInfographic:
https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/833959/IoD{yr}_Infographic.pdf
\nResearch report:
https://www.gov.uk/government/publications/english-indices-of-deprivation-2019-research-report
\nTechnical report:
https://www.gov.uk/government/publications/english-indices-of-deprivation-2019-technical-report
"""

csvName = 'observations.csv'
out = Path('out')
out.mkdir(exist_ok=True)
#joined_dat.drop_duplicates().to_csv(out / csvName, index = False)
joined_dat.drop_duplicates().to_csv(out / (csvName + '.gz'), index = False, compression='gzip')

scraper.dataset.family = 'towns-high-streets'
scraper.dataset.description = notes
scraper.dataset.comment = 'Statistics on relative deprivation in small areas in England, 2019.'
scraper.dataset.title = 'English indices of deprivation'

dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name)).lower()
scraper.set_base_uri('http://gss-data.org.uk')
scraper.set_dataset_id(dataset_path)

csvw_transform = CSVWMapping()
csvw_transform.set_csv(out / csvName)
csvw_transform.set_mapping(json.load(open('info.json')))
csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
csvw_transform.write(out / f'{csvName}-metadata.json')

with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())

# %%
#codelistcreation = ['Indices of Deprivation'] 
#df = joined_dat
#codeclass = CSVCodelists()
#for cl in codelistcreation:
#    if cl in df.columns:
#        df[cl] = df[cl].str.replace("-"," ")
#        df[cl] = df[cl].str.capitalize()
#        codeclass.create_codelists(pd.DataFrame(df[cl]), 'codelists', scraper.dataset.family, Path(os.getcwd()).name.lower())

# %%

# %%
