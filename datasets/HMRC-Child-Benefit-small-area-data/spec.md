# COGS Dataset Specification

[Family Home](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/specmenu.html)

[Family Transform Status](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/index.html)

----------
## HMRC Child Benefit: small area data

[Landing Page](https://www.gov.uk/government/collections/child-benefit-small-area-data)

[Transform Flowchart](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/specflowcharts.html?HMRC-Child-Benefit-small-area-data/flowchart.ttl)

----------
### Stage 1. Spec - DM

#### Datasets: Small Area Data for Child Benefit Statistics (multiple files)

		Regional LSOA
		Electoral Ward
		East Midlands
		East of England
		London
		North East
		North West
		Scottish Data Zone
		South East
		South West
		Wales
		West Midlands
		Yorkshire and the Humber

		All can be joined together into one dataset after adjusting geography columns

		Add a 'Period' column with value taken from sheet Title. Could just put in meta data but might want to join up with previous releases at some point.
		For the two higher level datasets (Regional_LSOA & Electoral_Ward) keep the geography column 'Area Code' and remove the rest. For all the lower level area datasets keep the column 'LSOA name', rename 'Area Code' and remove other geography columns.
		'Age':
			All children = total
			Under 5 = under-5
			5 to 10 = 5-to-10
			11 to 15 = 11-to-15
			16 to 19 = 16-to-19
		'Gender':
			Boys = M
			Girls = F
			Unknown = U
		'Family Size':
			All families = total
			One child = one-child
			Two children = two-children
			Three or more children = three-or-more-children
		'Measure Type' = Count
		'Unit':
			Number of children for whom Child Benefit received = children
			Number of families in receipt of Child Benefit = families
			
		We can only output datasets with one measure and unit type at the moment so you will have to temporarily remove the families data until a solution for multiple measure datasets has been implemented. 

		Some of these Area codes are probably missing from PMD4, if so, a list of the missing Area Code URIs will have to be created in a txt file and fetch_urls.sh in gdp-vocabs repo run:
			example in terminal -> ./fetch_urls.sh < missing.txt >> vocabs/reference-geography.ttl

		Codelists needed for columns 'Age' and 'Family Size'
		info needs to be added to info.json file
		families and children need to be added to measurements-units codelist or checked to see if they already exists in an ontology
		
#### Table Structure

		Period, Area Code, Age, Gender, Family Size, Measure Type, Unit, Value

-------------

### Stage 2. Harmonise

		After discussions with DE have decided to split into 2 datasets, Children and Families.
		Following is based on work already carried out.

		'LSOA name' has been pulled through rather than 'LSOA Code' for one or two of the files
		Rename column 'Area Code' to Geography Code'
		Add column 'Geography Level' with values as per spreadsheet (region, local-authority, lower-layer-super-output-area, etc.)
	Rename Units in both datasets:
		Number of children for whom Child Benefit is received -> children
		Number of families in receipt of Child Benefit -> families

#### Table Structure

		Period, Geography Code, Geography Level, Age, Gender, Family Size, Value
		
#### Children dataset:

	Table Structure
		Period, Geography Code, Geography Level, Age, Gender, Value
		
	Scraper values:
		Title = Child Benefit small area statistics - Children receiving Child benefit
		Comment = Annual geographical estimates at Lower Super Output Area and Data Zone of the number of children claiming Child Benefit
		Description = Scraper.dataset.description + 
			Annual geographical estimates at Lower Super Output Area and Data Zone of the number of children claiming Child Benefit.
			Area codes implemented in line with GSS Coding and Naming policy
			The figures have been independently rounded to the nearest 5. This can lead to components as shown not summing totals as shown
		dataset_id = dataset_id + '/children'

#### Families dataset:

	Table Structure
		Period, Geography Code, Geography Level, Family Size, Value
		
	Scraper values:

		Title = Child Benefit small area statistics - Families in receipt of Child benefit
		Comment = Annual geographical estimates at Lower Super Output Area and Data Zone of the number of families claiming Child Benefit
		Description = Scraper.dataset.description + 
			Annual geographical estimates at Lower Super Output Area and Data Zone of the number of families claiming Child Benefit.
			Area codes implemented in line with GSS Coding and Naming policy
			The figures have been independently rounded to the nearest 5. This can lead to components as shown not summing totals as shown
		dataset_id = dataset_id + '/families'

#### Updating info.json on the fly

	To get the two datasets onto PMD4 we have to cheat (Unless Alex has fixed the multiple measures problem).
	Instead of passing the info.json file to the mapping class, pull it into a dict then change the unit value then pass the Mapping class the updated dict instead.
		Dataset one unit: children
		Dataset two unit: families
