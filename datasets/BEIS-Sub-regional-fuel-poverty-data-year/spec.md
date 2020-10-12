# COGS Dataset Specification

[Family Home](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/specmenu.html)

[Family Transform Status](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/index.html)

## BEIS Sub-regional fuel poverty data  year 

[Landing Page](https://www.gov.uk/government/collections/fuel-poverty-sub-regional-statistics)


### Dataset

		Joined all 5 tables together

#### Sheet: Table 1

		Dimensions:
			Region: rename Geography code and change region name to ONS Geography code
			Geography level = Region
			Household Measure: 
				Number of Households -> Total Households
				Number of households in fuel Poverty -> Households in fuel poverty
		Values:
			Number of Households
			Number of households in fuel Poverty
			ignore proportion as it an be derived from the data
		Measures (info.json):
			Measure Type = Count
			Unit = Households

#### Sheet: Table 2

		Dimensions:
			LA Code: rename Geography code
			Geography level = Local Authority
			Household Measure: 
				Number of Households -> Total Households
				Number of households in fuel Poverty -> Households in fuel poverty
			Remove columns 'La Name' and 'Region'
		Values:
			Number of Households
			Number of households in fuel Poverty
			ignore proportion as it an be derived from the data
		Measures (info.json):
			Measure Type = Count
			Unit = Households

#### Sheet: Table 3

		Dimensions:
			LSOA Code: rename Geography code
			Geography level = Lower layer super output area
			Household Measure: 
				Number of Households -> Total Households
				Number of households in fuel Poverty -> Households in fuel poverty
			Remove columns 'LSOA Name', 'LA Code', 'LA Name' and 'Region'
		Values:
			Number of Households
			Number of households in fuel Poverty
			ignore proportion as it an be derived from the data
		Measures (info.json):
			Measure Type = Count
			Unit = Households

#### Sheet: Table 4

		Dimensions:
			County Code: rename Geography code
			Geography level = County
			Household Measure: 
				Number of Households -> Total Households
				Number of households in fuel Poverty -> Households in fuel poverty
			Remove columns 'County'
		Values:
			Number of Households
			Number of households in fuel Poverty
			ignore proportion as it an be derived from the data
		Measures (info.json):
			Measure Type = Count
			Unit = Households		

#### Sheet: Table 5

		Dimensions:
			Parliamentary Constituency Code: rename Geography code
			Geography level = Parliamentary constituency
			Household Measure: 
				Number of Households -> Total Households
				Number of households in fuel Poverty -> Households in fuel poverty
			Remove columns 'Parliamentary Constituency' and 'Region'
		Values:
			Number of Households
			Number of households in fuel Poverty
			ignore proportion as it an be derived from the data
		Measures (info.json):
			Measure Type = Count
			Unit = Households	

#### Scraper

		Title: Sub-regional fuel poverty data - England
		Comment: Fuel poverty data measured as low income high costs 2020 - Region, Local Authority, LSOA, County and Parliamentary Constituency
		Description: description + '\nReport available here:\n' + 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/882192/fuel-poverty-sub-regional-2020.pdf'
		Family: towns-high-streets
					
#### Table Structure

		Geography Code, Geography Level, Household Measure, Value

##### Footnotes

		To be added to description
		
			Report available here:
			https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/882192/fuel-poverty-sub-regional-2020.pdf
			Household and fuel poverty numbers at region level come from the national fuel poverty statistics, 2018
			Geographies are based on pre-2012 geography codes. More information on geography code changes can be found at the ONS website
			Estimates should only be used to look at general trends and identify areas of particularly high or low fuel poverty. See Sub-regional fuel poverty report, 2020
			Estimates of fuel poverty at Lower Super Output Area (LSOA) should be treated with caution. The estimates should only be used to look at general trends and identify areas of particularly high or low fuel poverty. They should not be used to identify trends over time within an LSOA, or to compare LSOAs with similar fuel poverty levels due to very small sample sizes and consequent instability in estimates at this level. See Sub-regional fuel poverty report, 2020


##### DM Notes

		Codelists have been set up and columns referenced in info.json

