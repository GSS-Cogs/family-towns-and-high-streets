#!/usr/bin/env python
# coding: utf-8

# In[33]:


from gssutils import *
import json
import copy

cubes = Cubes("info.json")

info = json.load(open('info.json'))
scraper = Scraper(seed="info.json")
scraper.select_dataset(title=lambda t: 'Journey times to key services by lower super output area (JTS05)' in t)
scraper


# In[34]:


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
no = [
    "Employment centres: Data used are the number of jobs in a Lower Super Output Area (LSOA). The data tables include results for employment centres of 3 different sizes (100-499 jobs, 500-4,999 jobs and at least 5,000 jobs). For the key services average, the 500-4,999 jobs defnition is used for employment.",
    "Education: Locations of all open Primary schools, Secondary schools, Further Education and Sixth Form Colleges",
    "Education: Locations of all open Primary schools, Secondary schools, Further Education andSixth Form Colleges",
    "Education: Locations of all open Primary schools, Secondary schools, Further Education andSixth Form Colleges",
    "General Practice (GP) surgeries: For 2017 based on the Patients Registered at a GP Practice dataset released by NHS Digital – previously this was based on a fltered dataset of NHS prescribers released by NHS Digital.",
    "Hospitals: Based on hospitals that are registered with the Care Quality Commission (CQC) and are managed by Acute Trusts.",
    "Food stores: Locations of grocery, supermarkets or convenience stores.",
    "Town centres: Locations of Town centres using a central focal point for the town mapped to the nearest road.",
    ""
]


# In[35]:


import os
from urllib.parse import urljoin
scraper.dataset.family = 'towns-high-streets'
scraper.set_base_uri('http://gss-data.org.uk')

out = Path('out')
out.mkdir(exist_ok=True)

notes = """
    2017 journey times have been influenced by changes to the network of walking paths being used for the calculations. The network is more extensive in 2017 reflecting changes to the underlying Ordnance Survey
    Urban Paths data set which is used (this has the effect of reducing the time taken for some trips where a relevant path has been added to the dataset).
    Full details of the datasets for the production of all the estimates are provided in the accompanying guidance note -
    https://www.gov.uk/government/publications/journey-time-statistics-guidance.\n
"""
originalDescription = scraper.dataset.description + notes

i = 0
for u in uris:

    scraper1 = copy.deepcopy(scraper)

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

    csvName = pathify(dn[i]).replace("-","_") + "_observations"
    #df.drop_duplicates().to_csv(out / csvName, index = False)
    #df.drop_duplicates().to_csv(out / (csvName + '.gz'), index = False, compression='gzip')

    datasetExtraName = '/' + pathify(dn[i])

    scraper1.dataset.description = originalDescription + no[i]

    scraper1.dataset.comment = f'Travel time, destination and origin indicators for {dn[i]} by mode of travel, Lower Super Output Area (LSOA), England {ag[i]}'
    scraper1.dataset.title = f'Journey times to key services by lower super output area: {dn[i]} - JTS050{str(i+1)}'

    df = df.drop_duplicates()

    info_json_dataset_id = info.get('id', Path.cwd().name)

    j = 0

    for region in df.iloc[:,3].unique():
        if j == 0:
            # For the first the chunk, create a primary graph graph_uri and csv_name
            graph_uri = f"http://gss-data.org.uk/graph/gss_data/towns-high-streets/dft-journey-times-to-key-services-by-lower-super-output-area/{csvName}"
            csv_name = csvName
            cubes.add_cube(scraper1, df[df.iloc[:,3] == region], csv_name, graph=csvName)
            j += 1
        else:
            # For subsequent chunk to add, create a secondary graph graph_uri and csv_name
            graph_uri = f"http://gss-data.org.uk/graph/gss_data/towns-high-streets/dft-journey-times-to-key-services-by-lower-super-output-area/{csvName}/{region}"
            csv_name = csvName+ f'-{region}'
            cubes.add_cube(scraper1, df[df.iloc[:,3] == region], csv_name, graph=csvName,  override_containing_graph=graph_uri, suppress_catalog_and_dsd_output=True)

    i = i + 1


# In[36]:


print(df.iloc[:,3].unique())


# In[37]:


cubes.output_all()


# In[38]:


#info = json.load(open('info.json'))
#codelistcreation = ['Employment Centre Size']

#codeclass = CSVCodelists()
#for cl in codelistcreation:
#    if cl in df.columns:
#        df[cl] = df[cl].str.replace("-"," ")
#        df[cl] = df[cl].str.capitalize()
#        codeclass.create_codelists(pd.DataFrame(df[cl]), 'codelists', scraper.dataset.family, Path(os.getcwd()).name.lower())
# -




