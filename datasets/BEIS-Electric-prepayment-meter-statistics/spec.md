# COGS Dataset Specification

[Family Home](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/specmenu.html)

[Family Transform Status](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/index.html)

## BEIS Electric prepayment meter statistics 

[Landing Page](https://www.gov.uk/government/statistics/electric-prepayment-meter-statistics)


#### Output Dataset Name:

		name

#### Sheet: Local Authority (tidy_d1)

	Dimensions
		Rename 'Period' to 'Year' -> 'year/{yr}'
		Remove columns 'Region', 'Local Authority' and 'LAU1'
		Rename column 'LA Code' -> 'Geography Code'
		Add column 'Geography Level' with value 'local-authority' (will probably remove in future but used for reference in short term)
		Spreadsheet rows 385 to 400
			Have these been included?
			if so remove 'Unallocated' values as their is no code for this
	Attributes
		'Meters'
	Measures (info.json)
		Split into 3 datasets (remove kWh)
			Sales 
			Mean Consumption
			Median Consumption

	To be output as 3 datasets and joined with others

#### Sheet: MSOA (tidy_d2)

	Dimensions
		Rename 'Period' to 'Year' -> 'year/{yr}'
		Remove columns 'LA Name', 'LA Code' and 'MSOA Name'
		Rename column 'MSOA Code' -> 'Geography Code'
		Add column 'Geography Level' with value 'middle-layer-super-output-area' (will probably remove in future but used for reference in short term)
		Remove 'Unallocated' values as their is no code for this
	Attributes
		'Meters'
	Measures (info.json)
		Split into 3 datasets (remove kWh)
			Sales
			Mean Consumption
			Median Consumption

	To be output as 3 datasets and joined with others

#### Sheet: LSOA (tidy_d3)

	Dimensions
		Rename 'Period' to 'Year' -> 'year/{yr}'
		Remove columns 'LA Name', 'LA Code' and 'MSOA Name'
		Rename column 'LSOA Code' -> 'Geography Code'
		Add column 'Geography Level' with value 'lower-layer-super-output-area' (will probably remove in future but used for reference in short term)
		Remove 'Unallocated' values as their is no code for this
	Attributes
		'Meters'
	Measures (info.json)
		Split into 3 datasets (remove kWh)
			Sales (change from Total Consumption (kWh))
			Mean Consumption
			Median Consumption

	To be output as 3 datasets and joined with others

#### Join

	Join 3 tables together with following details
		Sales
			Scraper
				Title: Electric prepayment meter statistics - Sales
				Comment: Annual prepayment meter electricity sales statistics for local authorities, LSOAs, MSOAs in England, Wales and Scotland.
				Description: SEE BELOW
				Family: towns-high-streets
				dataset_path: /sales	
		Mean Consumption
			Scraper
				Title: Electric prepayment meter statistics - Mean Consumption
				Comment: Annual prepayment meter electricity mean consumption statistics for local authorities, LSOAs, MSOAs in England, Wales and Scotland.
				Description: SEE BELOW
				Family: towns-high-streets
				dataset_path: /mean
		Median Consumption
			Scraper
				Title: Electric prepayment meter statistics - Median Consumption
				Comment: Annual prepayment meter electricity median consumption statistics for local authorities, LSOAs, MSOAs in England, Wales and Scotland.
				Description: SEE BELOW
				Family: towns-high-streets
				dataset_path: /median
	DESCRIPTION:
	Data for prepayment meter electricity consumption, number of meters, mean and median consumption for local 	
	authority regions across England, Wales & Scotland
	This doesn't include smart meters operating in prepayment mode
	Data excludes:	
	Geographies that are disclosive are defined as such if they contain less than 6 meters		
	The dataset only contains meters that have consumption between 100 kWh and 100,000 kWh and have a domestic meter profile
	Meters that have not successfully been assigned to a geography due to insufficient address information are counted in the 'Unallocated' category but are not included in this data as there is no matching geography code.
	Meters that are deemed to be disclosive at Local Authority level are set as 'Unallocated' but are not included in this data as there is no matching geography code.

	To get the three datasets onto PMD4 we have to cheat (Unless Alex has fixed the multiple measures problem).
	Instead of passing the info.json file to the mapping class, pull it into a dict the change the measure value then pass the updated dict instead.
		Dataset one measure: sales
		Dataset two measure: mean-consumption
		Dataset three measure: median-consumption
		Unit will be kilowatt-hour for all three datasets  

#### Table Structure

		Period, Geography Code, Geography Level, Meters, Value

#### Sheet: Post Code (tidy_d4) Separated out as Post Codes are not part of the ONS Geography Code dataset

	Dimensions
		Rename 'Period' to 'Year' -> 'year/{yr}'
		'Postcodes' -> 'Post Codes
		Remove 'Unallocated' values as their is no code for this
	Attributes
		'Meters'
	Measures (info.json)
		Split into 3 datasets (remove kWh)
			Sales (change from Total Consumption (kWh))
			Mean Consumption
			Median Consumption

	Scraper
	Title: Electric prepayment meter statistics by Post Code - Sales
	Comment: Annual prepayment meter electricity sales statistics for Post Codes in England, Wales and Scotland.
	Description: Data for prepayment meter electricity consumption, number of meters, mean and median consumption for local 	
	authority regions across England, Wales & Scotland
	This doesn't include smart meters operating in prepayment mode
	Data excludes:	
	Geographies that are disclosive are defined as such if they contain less than 6 meters		
	The dataset only contains meters that have consumption between 100 kWh and 100,000 kWh and have a domestic meter profile
	Meters that have not successfully been assigned to a geography due to insufficient address information are counted in the 'Unallocated' category but are not included in this data as there is no matching geography code.
	Meters that are deemed to be disclosive at Local Authority level are set as 'Unallocated' but are not included in this data as there is no matching geography code.
	
	Family: towns-high-streets	
		
	For now just output the SALES data but you will have to set the info.json measure value again.

#### Table Structure

		Period, Post Codes, Meters, Value
		
##### Footnotes

		See Scraper descriptions

##### DM Notes

	Codelist for Post Codes have been created and referenced in info.json. Codelist in ref_common rather than local
	Attribute 'Meters' has been referenced in info.json but will not currently work as Mapping class is not set up to process them.
	
	

