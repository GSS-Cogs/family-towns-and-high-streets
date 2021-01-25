#!/usr/bin/env python
# coding: utf-8

# In[113]:


#!/usr/bin/env python
# coding: utf-8


# In[114]:


from gssutils import *
import json
import re
from urllib.request import Request, urlopen

def all_same(items):
    if len(items) == 0:
        return False
    return all(x == items[0] for x in items)


cubes = Cubes("info.json")
info = json.load(open('info.json'))
etl_title = info["title"]
etl_publisher = info["publisher"][0]
print("Publisher: " + etl_publisher)
print("Title: " + etl_title)


# In[115]:


#Commenting out Data URL retrieve to see if its the issue on the pipeline

#req = Request("https://www.gov.uk/government/statistics/english-indices-of-deprivation-2019", headers={'User-Agent': 'Mozilla/5.0'})
#html = urlopen(req).read()
#plaintext = html.decode('utf8')
#links = re.findall("href=[\"\'](.*?)[\"\']", plaintext)
#links = [x for x in links if x.endswith("csv")]
#if all_same(links) == True:
#    dataURL = links[0]
#    with open('info.json', 'r+') as info:
#        data = json.load(info)
#        data["dataURL"] = dataURL
#        info.seek(0)
#        json.dump(data, info, indent=4)
#        info.truncate()
#        print("Data URL found and added to info.json")
#else:
#    print("Multiple or No files found, investigate or retrieve manually")
#    for i in links:
#        print(i)

scraper = Scraper(seed = "info.json")
scraper.distributions[0].title = etl_title
scraper


# In[116]:


tab = scraper.distributions[0].as_pandas()

tab = tab.drop(["LSOA name (2011)", "Local Authority District name (2019)"], axis = 1)

tab


# In[117]:


df = tab.melt(id_vars = ["LSOA code (2011)", "Local Authority District code (2019)"], value_name = 'Value', var_name = "Index of Deprivation")
df = df.rename(columns={'LSOA code (2011)' : 'Lower Layer Super Output Area', 'Local Authority District code (2019)' : 'Local Authority'})

df = df.replace({"Index of Deprivation" : {
    'Index of Multiple Deprivation (IMD) Score' : 'index-of-multiple-deprivation-imd-score',
    'Index of Multiple Deprivation (IMD) Rank (where 1 is most deprived)' : 'index-of-multiple-deprivation-imd-rank-where-1-is-most-deprived',
    'Index of Multiple Deprivation (IMD) Decile (where 1 is most deprived 10% of LSOAs)' : 'index-of-multiple-deprivation-imd-decile-where-1-is-most-deprived-10-of-lsoas',
    'Income Score (rate)' : 'income-score-rate',
    'Income Rank (where 1 is most deprived)' : 'income-rank-where-1-is-most-deprived',
    'Income Decile (where 1 is most deprived 10% of LSOAs)' : 'income-decile-where-1-is-most-deprived-10-of-lsoas',
    'Employment Score (rate)' : 'employment-score-rate',
    'Employment Rank (where 1 is most deprived)' : 'employment-rank-where-1-is-most-deprived',
    'Employment Decile (where 1 is most deprived 10% of LSOAs)' : 'employment-decile-where-1-is-most-deprived-10-of-lsoas',
    'Education, Skills and Training Score' : 'education-skills-and-training-score',
    'Education, Skills and Training Rank (where 1 is most deprived)' : 'education-skills-and-training-rank-where-1-is-most-deprived',
    'Education, Skills and Training Decile (where 1 is most deprived 10% of LSOAs)' : 'education-skills-and-training-decile-where-1-is-most-deprived-10-of-lsoas',
    'Health Deprivation and Disability Score' : 'health-deprivation-and-disability-score',
    'Health Deprivation and Disability Rank (where 1 is most deprived)' : 'health-deprivation-and-disability-rank-where-1-is-most-deprived',
    'Health Deprivation and Disability Decile (where 1 is most deprived 10% of LSOAs)' : 'health-deprivation-and-disability-decile-where-1-is-most-deprived-10-of-lsoas',
    'Crime Score' : 'crime-score',
    'Crime Rank (where 1 is most deprived)' : 'crime-rank-where-1-is-most-deprived',
    'Crime Decile (where 1 is most deprived 10% of LSOAs)' : 'crime-decile-where-1-is-most-deprived-10-of-lsoas',
    'Barriers to Housing and Services Score' : 'barriers-to-housing-and-services-score',
    'Barriers to Housing and Services Rank (where 1 is most deprived)' : 'barriers-to-housing-and-services-rank-where-1-is-most-deprived',
    'Barriers to Housing and Services Decile (where 1 is most deprived 10% of LSOAs)' : 'barriers-to-housing-and-services-decile-where-1-is-most-deprived-10-of-lsoas',
    'Living Environment Score' : 'living-environment-score',
    'Living Environment Rank (where 1 is most deprived)' : 'living-environment-rank-where-1-is-most-deprived',
    'Living Environment Decile (where 1 is most deprived 10% of LSOAs)' : 'living-environment-decile-where-1-is-most-deprived-10-of-lsoas',
    'Income Deprivation Affecting Children Index (IDACI) Score (rate)' : 'income-deprivation-affecting-children-index-idaci-score-rate',
    'Income Deprivation Affecting Children Index (IDACI) Rank (where 1 is most deprived)' : 'income-deprivation-affecting-children-index-idaci-rank-where-1-is-most-deprived',
    'Income Deprivation Affecting Children Index (IDACI) Decile (where 1 is most deprived 10% of LSOAs)' : 'income-deprivation-affecting-children-index-idaci-decile-where-1-is-most-deprived-10-of-lsoas',
    'Income Deprivation Affecting Older People (IDAOPI) Score (rate)' : 'income-deprivation-affecting-older-people-idaopi-score-rate',
    'Income Deprivation Affecting Older People (IDAOPI) Rank (where 1 is most deprived)' : 'income-deprivation-affecting-older-people-idaopi-rank-where-1-is-most-deprived',
    'Income Deprivation Affecting Older People (IDAOPI) Decile (where 1 is most deprived 10% of LSOAs)' : 'income-deprivation-affecting-older-people-idaopi-decile-where-1-is-most-deprived-10-of-lsoas',
    'Children and Young People Sub-domain Score' : 'children-and-young-people-sub-domain-score',
    'Children and Young People Sub-domain Rank (where 1 is most deprived)' : 'children-and-young-people-sub-domain-rank-where-1-is-most-deprived',
    'Children and Young People Sub-domain Decile (where 1 is most deprived 10% of LSOAs)' : 'children-and-young-people-sub-domain-decile-where-1-is-most-deprived-10-of-lsoas',
    'Adult Skills Sub-domain Score' : 'adult-skills-sub-domain-score',
    'Adult Skills Sub-domain Rank (where 1 is most deprived)' : 'adult-skills-sub-domain-rank-where-1-is-most-deprived',
    'Adult Skills Sub-domain Decile (where 1 is most deprived 10% of LSOAs)' : 'adult-skills-sub-domain-decile-where-1-is-most-deprived-10-of-lsoas',
    'Geographical Barriers Sub-domain Score' : 'geographical-barriers-sub-domain-score',
    'Geographical Barriers Sub-domain Rank (where 1 is most deprived)' : 'geographical-barriers-sub-domain-rank-where-1-is-most-deprived',
    'Geographical Barriers Sub-domain Decile (where 1 is most deprived 10% of LSOAs)' : 'geographical-barriers-sub-domain-decile-where-1-is-most-deprived-10-of-lsoas',
    'Wider Barriers Sub-domain Score' : 'wider-barriers-sub-domain-score',
    'Wider Barriers Sub-domain Rank (where 1 is most deprived)' : 'wider-barriers-sub-domain-rank-where-1-is-most-deprived',
    'Wider Barriers Sub-domain Decile (where 1 is most deprived 10% of LSOAs)' : 'wider-barriers-sub-domain-decile-where-1-is-most-deprived-10-of-lsoas',
    'Indoors Sub-domain Score' : 'indoors-sub-domain-score',
    'Indoors Sub-domain Rank (where 1 is most deprived)' : 'indoors-sub-domain-rank-where-1-is-most-deprived',
    'Indoors Sub-domain Decile (where 1 is most deprived 10% of LSOAs)' : 'indoors-sub-domain-decile-where-1-is-most-deprived-10-of-lsoas',
    'Outdoors Sub-domain Score' : 'outdoors-sub-domain-score',
    'Outdoors Sub-domain Rank (where 1 is most deprived)' : 'outdoors-sub-domain-rank-where-1-is-most-deprived',
    'Outdoors Sub-domain Decile (where 1 is most deprived 10% of LSOAs)' : 'outdoors-sub-domain-decile-where-1-is-most-deprived-10-of-lsoas',
    'Total population: mid 2015 (excluding prisoners)' : 'total-population-mid-2015-excluding-prisoners',
    'Dependent Children aged 0-15: mid 2015 (excluding prisoners)' : 'dependent-children-aged-0-15-mid-2015-excluding-prisoners',
    'Population aged 16-59: mid 2015 (excluding prisoners)' : 'population-aged-16-59-mid-2015-excluding-prisoners',
    'Older population aged 60 and over: mid 2015 (excluding prisoners)' : 'older-population-aged-60-and-over-mid-2015-excluding-prisoners',
    'Working age population 18-59/64: for use with Employment Deprivation Domain (excluding prisoners) ' : 'working-age-population-18-59/64-for-use-with-employment-deprivation-domain-excluding-prisoners'
}})

df


# In[118]:


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

scraper.dataset.family = 'towns-high-streets'
scraper.dataset.description = notes
scraper.dataset.comment = 'Statistics on relative deprivation in small areas in England, 2019.'
scraper.dataset.title = 'English indices of deprivation'

cubes.add_cube(scraper, df.drop_duplicates(), csvName)


# In[119]:


cubes.output_all()


# In[120]:


#dataset_path = pathify(os.environ.get('JOB_NAME', f'gss_data/{scraper.dataset.family}/' + Path(os.getcwd()).name)).lower()
#scraper.set_base_uri('http://gss-data.org.uk')
#scraper.set_dataset_id(dataset_path)

#csvw_transform = CSVWMapping()
#csvw_transform.set_csv(out / csvName)
#csvw_transform.set_mapping(json.load(open('info.json')))
#csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
#csvw_transform.write(out / f'{csvName}-metadata.json')

#with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
#    metadata.write(scraper.generate_trig())


# In[121]:


"""codelistcreation = ['Index of Deprivation']
df = joined_dat
codeclass = CSVCodelists()
for cl in codelistcreation:
    if cl in df.columns:
        df[cl] = df[cl].str.replace("-"," ")
        df[cl] = df[cl].str.capitalize()
        codeclass.create_codelists(pd.DataFrame(df[cl]), 'codelists', scraper.dataset.family, Path(os.getcwd()).name.lower())"""


# In[121]:





# In[121]:




