# COGS Dataset Specification

[Family Home](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/specmenu.html)

[Family Transform Status](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/index.html)

## MHCLG Tracking economic and child income deprivation at neighbourhood level in England  1999 to 2009 

[Landing Page](https://www.gov.uk/government/collections/english-indices-of-deprivation)


### ONLY TRANSFORM THE MAIN SCORE AND RANK FILES AT THE MOMENT

	Economic deprivation index: rank
	Economic deprivation index: income deprivation domain rank
	Economic deprivation index: employment deprivation domain rank
	Children in income-deprived households index: rank

	Economic deprivation index: score
	Economic deprivation index: income deprivation domain score
	Economic deprivation index: employment deprivation domain score
	Children in income-deprived households index: score

#### Sheet: Rank files (4)

		Dimensions:
			Rename column 'Period' to 'Year' and format 'year/{yr}'
			Remove columns 'lsoaname' and 'lauaname'
			Rename column 'lsoacode' to 'Lower Layer Super Output Area'
			Rename column 'lauacode' to 'Local Authority'	
			Add column 'Deprivation Domain' with values as per 4 files:
				Overall
				Income
				Employment
				Children in income-deprived households Index
			Add 'marker' column for Values with *
		Measure (info.json):
			Measure Type: deprivation
			Unit: rank
			
		Join the 4 rank files together

		Scraper/Mapping
			Title: Tracking economic and child income deprivation at neighbourhood level in England  1999 to 2009 - Ranks
			Comment: Rank Indices tracking levels of economic deprivation from 1999 to 2009 in small areas in England called lower-layer super output areas.
			Description: This data package contains the underlying data tables that form the basis of the Tracking Neighbourhoods reports submitted to the Department for Communities and Local Government (DCLG) in  October 2012. The project involved creating an Economic Deprivation Index (EDI) at Lower Super Output Area (LSOA) level for each year from 1999 to 2009. The EDI consists of the two component domains of 'income deprivation' and 'employment deprivation'. These two domains are described in more detail in the Tracking Neighbourhoods EDI report. The project also involved creating a Children living in Income Deprived households Index (CIDI) which consists of an age-group relevant subset of the EDI's income deprivation domain. 
			family: towns-high-streets
			dataset_path: dataset_path + '/rank'

#### Sheet: Score files (4)

		Dimensions:
			Rename column 'Period' to 'Year' and format 'year/{yr}'
			Remove columns 'lsoaname' and 'lauaname'
			Rename column 'lsoacode' to 'Lower Layer Super Output Area'
			Rename column 'lauacode' to 'Local Authority'	
			Add column 'Deprivation Domain' with values as per 4 files:
				Overall
				Income
				Employment
				Children in income-deprived households Index
			Add 'marker' column for Values with *
		Measure (info.json):
			Measure Type: deprivation
			Unit: score %
			
		Join the 4 score files together

		Scraper/Mapping:
			Title: Tracking economic and child income deprivation at neighbourhood level in England  1999 to 2009 - Ranks
			Comment: Score Indices tracking levels of economic deprivation from 1999 to 2009 in small areas in England called lower-layer super output areas.
			Description: This data package contains the underlying data tables that form the basis of the Tracking Neighbourhoods reports submitted to the Department for Communities and Local Government (DCLG) in  October 2012. The project involved creating an Economic Deprivation Index (EDI) at Lower Super Output Area (LSOA) level for each year from 1999 to 2009. The EDI consists of the two component domains of 'income deprivation' and 'employment deprivation'. These two domains are described in more detail in the Tracking Neighbourhoods EDI report. The project also involved creating a Children living in Income Deprived households Index (CIDI) which consists of an age-group relevant subset of the EDI's income deprivation domain. 
			family: towns-high-streets
			dataset_path: datset+_path + '/score/
			
#### Table Structure

		Year, Lower Tier Super Output Area, Local Authority, Deprivation Domain, Marker, Value

#### PMD4

	To get the two datasets onto PMD4 we have to cheat (Unless Alex has fixed the multiple measures problem).
	Instead of passing the info.json file to the mapping class, pull it into a dict then change the UNIT value then pass the 		updated dict instead.
    	Dataset one measure: rank
    	Dataset two measure: score
    	Measure will be deprivation for the two datasets  

##### Footnotes

		Need to find out what the value should be for the Marker columns when the Value column has value *. Had to move anto another spec

##### DM Notes

		Need to find out what the value should be for the Marker columns when the Value column has value *. Had to move anto another spec

