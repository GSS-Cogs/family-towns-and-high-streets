# COGS Dataset Specification

[Family Home](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/specmenu.html)

[Family Transform Status](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/index.html)

## SG Scottish Index of Multiple Deprivation 2020 

[Landing Page](https://www.gov.scot/collections/scottish-index-of-multiple-deprivation-2020/)


### Dataset One

#### File: Ranks

		Geography
			Remove columns 'Intermediate_Zone' and 'Council_area'
			Rename 'Data_Zone' to 'Data Zone'
		Attributes:
			'Total_population' -> 'Total Population'
			'Working_Age_Population' -> 'Working Age Population'
			Cannot show attributes correctly in PMD4 at the moment so might have to remove for now
		Dimensions
			Pivot Columns F to M to create one column with name 'Deprivation Rank' and pathify (codelist)
		Measures (info.json)
			Measure Type = Deprivation
			Unit = Rank

		Scraper:
			Title: Scottish Index of Multiple Deprivation - Ranks
			Comment: Scottish Index of Multiple Deprivation 2020 - Ranks. A tool for identifying areas with relatively high levels of deprivation.
			Description: The Scottish Index of Multiple Deprivation is a relative measure of deprivation across 6,976 small areas (called data zones). If an area is identified as ‘deprived’, this can relate to people having a low income but it can also mean fewer resources or opportunities. SIMD looks at the extent to which an area is deprived across seven domains: income, employment, education, health, access to services, crime and housing.
			Family: towns-high-streets
			Output filename: ranks-observations.csv

#### Table Structure

		Data Zone, Total Population, Working Age Population, Deprivation Rank, Value

#### File: Indicators

		Geography
			Remove columns 'Intermediate_Zone' and 'Council_area'
			Rename 'Data_Zone' to 'Data Zone'
		Attributes:
			'Total_population' -> 'Total Population'
			'Working_Age_Population' -> 'Working Age Population'
			Cannot show attributes correctly in PMD4 at the moment so might have to remove for now
		Dimensions:
			'Deprivation Indicator': see sheet Indicator descriptions column A (codelist)
			'Indicator Type': see sheet Indicator descriptions column C (codelist) (used to get around the problem of multiple measures)
		Measures (info.json)
			Measure Type = Deprivation
			Unit = Indicator

		Scraper:
			Title: Scottish Index of Multiple Deprivation - Indicators
			Comment: Scottish Index of Multiple Deprivation 2020 - Indicators. A tool for identifying areas with relatively high levels of deprivation
			Description: The Scottish Index of Multiple Deprivation is a relative measure of deprivation across 6,976 small areas (called data zones). If an area is identified as ‘deprived’, this can relate to people having a low income but it can also mean fewer resources or opportunities. SIMD looks at the extent to which an area is deprived across seven domains: income, employment, education, health, access to services, crime and housing.
			Family: towns-high-streets
			Output filename: indicators-observations.csv
			
#### Table Structure

		Data Zone, Total Population, Working Age Population, Deprivation Indicator, Indicator Type, Value

##### Footnotes

		footnotes

##### DM Notes

		Have cheated on the Indicators spreadsheet as output has multiple measure types. Have added another dimension to record the measure.We are still measuring Deprivation through Indicators so the Measure Type and Unit stay the same.

