# COGS Dataset Specification


[Family Transform Status](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/index.html)

----------

## DfT Journey times to key services by lower super output area

[Landing Page](https://www.gov.uk/government/statistical-data-sets/journey-time-statistics-data-tables-jts)

----------

### **Due to the size of the data and how slow the transform takes to run we have processed the data offline and then uploaded the output files to google docs. The Jenkins pipeline then processes these files instead. This will cause problems with future runs but the DE are testing out different methods of speeding things up.**
Dataset one example - employment centres
[observations_01.csv](https://drive.google.com/file/d/1SeekTbw2ShjSws_I5G5bTG8va0hhJ5wg/view?usp=sharing)

### Stage 2. Alignment

#### Dataset: JTS0502: Travel time, destination and origin indicators for Primary schools by mode of travel, Lower Super Output Area (LSOA), England (ODS, 11.7MB)

#### Dataset: JTS0503: Travel time, destination and origin indicators for Secondary schools by mode of travel, Lower Super Output Area (LSOA), England (ODS, 23MB)

#### Dataset: JTS0504: Travel time, destination and origin indicators for Further education by mode of travel, Lower Super Output Area (LSOA), England (ODS, 24MB)

#### Dataset: JTS0505: Travel time, destination and origin indicators for GPs by mode of travel, Lower Super Output Area (LSOA), England (ODS, 22.8MB)

#### Dataset: JTS0506: Travel time, destination and origin indicators for Hospitals by mode of travel, Lower Super Output Area (LSOA), England (ODS, 22.2MB)

#### Dataset: JTS0507: Travel time, destination and origin indicators for Food stores by mode of travel, Lower Super Output Area (LSOA), England (ODS, 21.2MB)

#### Dataset: JTS0508: Travel time, destination and origin indicators for town centres by mode of travel, Lower Super Output Area (LSOA), England (ODS, 27.7MB)

#### Dataset: JTS0509: Travel time, destination and origin indicators to Pharmacies by cycle and car, Lower Super Output Area (LSOA), England (ODS, 3.96MB

		Remove columns 'Region', 'LA_Name'
		Rename columns:
			LSOA_code = Lower Super Output Area
			LA_Code = Local Authority
		Only pull in the travel time columns and flatten into new column 'Field Code':
			columns ending in PTt, Cyct, Cart
		Other columns will have to be processed at a later date
			
		'Field Code' codelist has been created within ref_common and reference made to it in info.json
		Measure Type and Unit have been defined in info.json
			Measure Type = Travel Time
			Unit = Minutes

		Family = 'towns-and-high-streets'
		Comment = 'Travel time, destination and origin indicators for Employment centres by mode of travel, Lower Super Output Area (LSOA), England (16 to 74 year old)'
		Dataset Title = 'Journey times for Employment centres by lower super output area (LSOA) - JTS05'
		Dataset Path = add '/employment-centres' to end

----------

#### Table Structure

		Year, Lower Layer Super Output Area, Local Authority, Field Code, Value

##### Footnotes

	To be added to scraper description
		2017 journey times have been influenced by changes to the network of walking paths being used for the calculations. The 		network is more extensive in 2017 reflecting changes to the underlying Ordnance Survey
    		Urban Paths data set which is used (this has the effect of reducing the time taken for some trips where a relevant path 		has been added to the dataset).
    		Full details of the datasets for the production of all the estimates are provided in the accompanying guidance note - 
    		https://www.gov.uk/government/publications/journey-time-statistics-guidance.

##### DM Notes

		Having major issues with the time it takes to transform this dataset!
		Columns have been referenced in info.json and code lists created (ref_commons)
		Have asked BAs to contact DfT to see if another source of data is available (CSV)
