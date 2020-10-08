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
			Pivot Columns F to M to create one column with name 'Deprivation Rank' and pithily
		info.json
			Measure Type = Deprivation
			Unit = Rank

#### Table Structure

		Data Zone, Total Population, Working Age Population, Deprivation Rank, Marker, Value

#### File: Indicators

		Geography
			Remove columns 'Intermediate_Zone' and 'Council_area'
			Rename 'Data_Zone' to 'Data Zone'
		Attributes:
			'Total_population' -> 'Total Population'
			'Working_Age_Population' -> 'Working Age Population'
			Cannot show attributes correctly in PMD4 at the moment so might have to remove for now
		

##### Footnotes

		footnotes

##### DM Notes

		notes

