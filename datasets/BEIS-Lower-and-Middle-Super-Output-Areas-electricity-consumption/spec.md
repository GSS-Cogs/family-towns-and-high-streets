# COGS Dataset Specification


[Family Transform Status](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/index.html)

----------

## BEIS Lower and Middle Super Output Areas electricity consumption

[Landing Page](https://www.gov.uk/government/statistics/lower-and-middle-super-output-areas-electricity-consumption)

[Transform Flowchart](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/specflowcharts.html?BEIS-Lower-and-Middle-Super-Output-Areas-electricity-consumption/flowchart.ttl)

----------

### Stage 2. Harmonise

#### Sheet: 9 sheets = 9 years of data, all the same format (2010 - 2018)

		Add column 'Year' and fill in with relevant year and format as required (year/2011?)
		Remove columns 
			'Local Authority Name'
			'Middle Layer Super Output Area (MSOA) name'
			'Lower Layer Super Output Area (LSOA) Name'
		Keep the 3 geography codes and see if the transform can be published to PMD4. If not keep the LSOA geography code and remove the others. We have had trouble previously where only one column could reference the statistical.geography URI.
		Some geographies might have to be added to gdp-vocabs if not in the ttl file
		Attributes:
			'Total number of domestic electricity meters'
			'Mean Domestic electricity consumption (kWh per meter)'
			'Median domestic electricity consumption (kWh per meter)'
		Value
			'Total domestic electricity consumption (kWh)'

		The following do not need to be added as columns as they have been defined in info.json
		Measure Type
			'kilowatt_hour'
		Unit
			'kilowatt'

----------

#### Table Structure

		Year, Local Authority, Middle Layer Super Output Area, Lower Layer Super Output Area, Total number of domestic electricity meters, Mean Domestic electricity consumption kWh per meter, Median domestic electricity consumption kWh per meter, Value
		
		Or if having the three geography causes problems:
		Year, Lower Layer Super Output Area, Total number of domestic electricity meters, Mean Domestic electricity consumption kWh per meter, Median domestic electricity consumption kWh per meter, Value

--------------

##### Footnotes

		Make sure their is reference to the guidance documentation somewhere in the metadata, if not add it to the description
			https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/853104/sub-national-methodology-guidance.pdf

		If all 3 geographies can be used then add the following comment to scraper.dataset.comment:
			Lower Super Output Area (LSOA), Middle Super Output Area (MSOA) and Intermediate Geography Zone (IGZ) electricity consumption data.
		If only the lowest geography can be used then add:
			Lower Super Output Area (LSOA) electricity consumption data. 

##### DM Notes

		Could not get script to finish running!
		No code lists are needed for this dataset but some geographies might need to be added to gdp-vocabs
		column definitions have been added to info.json
