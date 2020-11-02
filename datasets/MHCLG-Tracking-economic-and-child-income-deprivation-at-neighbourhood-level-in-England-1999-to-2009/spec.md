# COGS Dataset Specification

[Family Home](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/specmenu.html)

[Family Transform Status](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/index.html)

## MHCLG Tracking economic and child income deprivation at neighbourhood level in England  1999 to 2009 

[Landing Page](https://www.gov.uk/government/collections/english-indices-of-deprivation)


### IGNORE THE AVERAGE SCORE & RANK FILES FOR NOW

	Economic deprivation index: rank - EDI rank
	Economic deprivation index: income deprivation domain score - IDD score
	Economic deprivation index: income deprivation domain rank - IDD rank
	Economic deprivation index: employment deprivation domain score - EDD score
	Economic deprivation index: employment deprivation domain rank - EDD rank
	Children in income-deprived households index: score - CIDI score
	Economic deprivation index: income deprivation domain numerator - IDD numerator
	Economic deprivation index: income deprivation domain denominator - IDD denominator
	Economic deprivation index: employment deprivation domain numerator - EDD numerator
	Economic deprivation index: employment deprivation domain denominator - EDD denominator
	Children in income-deprived households index: numerator - CIDI numerator
	Children in income-deprived households index: denominator - CIDI denominator
	Economic deprivation index: score - EDI score
	Total population used to calculate local authority district and economic deprivation index summary measure - Total Population (all ages)

	JOIN RANKS, SCORES, NUMERATOR, DENOMINATOR AND POPULATION AND PUBLISH AS 5 DATASETS:
		Tracking economic and child income deprivation at neighbourhood level in England: Ranks
		Tracking economic and child income deprivation at neighbourhood level in England: Scores
		Tracking economic and child income deprivation at neighbourhood level in England: Numerators
		Tracking economic and child income deprivation at neighbourhood level in England: Denominators
		Tracking economic and child income deprivation at neighbourhood level in England: Total Population (all ages)

#### Sheet: All files

	Dimensions:
		Rename column 'Period' to 'Year' and format 'year/{yr}'
		Remove columns 'lsoaname' and 'lauaname'
		Rename column 'lsoacode' to 'Lower Layer Super Output Area'
		Rename column 'lauacode' to 'Local Authority'	
		Add 'Economic Deprivation Indicator' column with values: EDI, IDD, EDD, CIDI as per dataset (do not add to population dataset)
		Add 'marker' column with value 'Suppressed' for Values with * and set relevant 'Value' cell to 0
		Measure (info.json):
			Measure Type: deprivation
			Unit: ranks, scores, numerator, denominator and count
			
	Scraper/Mapping
	Titles: 
		1. Tracking economic and child income deprivation at neighbourhood level in England: Ranks
		2. Tracking economic and child income deprivation at neighbourhood level in England: Scores
		3. Tracking economic and child income deprivation at neighbourhood level in England: Numerators
		4. Tracking economic and child income deprivation at neighbourhood level in England: Denominators
		5. Tracking economic and child income deprivation at neighbourhood level in England: Total Population (all ages)
	Comments: 
		1. Ranks tracking levels of economic deprivation in England by Lower Layer Super Output Areas (LSOA).
		2. Scores tracking levels of economic deprivation in England by Lower Layer Super Output Areas (LSOA).
		3. Numerators tracking levels of economic deprivation in England by Lower Layer Super Output Areas (LSOA).
		4. Denominators tracking levels of economic deprivation in England by Lower Layer Super Output Areas (LSOA).
		5. Population counts in England by Lower Layer Super Output Areas (LSOA) used to calculate economic deprivation Indicators.
	Description (Only one description):
		This Statistical Release presents key findings from the Economic Deprivation Index (EDI) and the Children in Income Deprived households Index (CIDI), hereafter referred to collectively as the ‘economic deprivation indices’. These indices track neighbourhood-level deprivation each year on a consistent basis, taking account of changes to the tax and benefit systems over this period. They are produced using the same general methodology as the Income and Employment deprivation domains of the English Indices of Deprivation (with slightly narrower definitions of income and employment deprivation). As such, the economic deprivation indices complement the Indices of Deprivation 2010.
		Report can be found here: 
		https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/36446/Tracking_Neighbourhoods_Stats_Release.pdf
	family: towns-high-streets
	dataset_paths: 
		1. dataset_path + d_path + /ranks
		2. dataset_path + /scores
		3. dataset_path + /numerators
		4. dataset_path + /denominators
		5. dataset_path + /count
			
#### Table Structure

		Year, Economic Deprivation Indicator, Lower Tier Super Output Area, Local Authority, Marker, Value

#### PMD4

	To get the two datasets onto PMD4 we have to cheat (Unless Alex has fixed the multiple measures problem).
	Instead of passing the info.json file to the mapping class, pull it into a dict then change the UNIT value then pass the 		updated dict instead.
    	Dataset one measure: rank
    	Dataset two measure: score
    	Measure will be deprivation for the two datasets  

##### Footnotes


##### DM Notes

		NNeed to find out what the value should be for the Marker columns when the Value column has value *. 
		BAs have got back and said that '*' represents suppressed data*

