<!-- #region -->
# COGS Dataset Specification

----------
## LSOA estimates of properties not connected to the gas network 

[Landing Page](https://www.gov.uk/government/statistics/lsoa-estimates-of-households-not-connected-to-the-gas-network)

----------
### Stage 1. Transform
        note all sheets (2015r, 2016r, 2017r, 2018) have the same structure therefore the following stage 1 transform applies to all 4 sheets. 

#### Sheets: 2015r, 2016r, 2017r, 2018
        Period - Taken from sheet name e.g. 2015 
        Local Authority Name - A3:A41733
        Local Authority Code - B3:B41733
        MSOA Name - C3:C41733
        Middle Layer Super Output Area (MSOA) Code - D3:D41733
        LSOA Name - E3:E41733
        Lower Layer Super Output Area (LSOA) Code - F3:F41733
        Number of domestic gas meters - G3:G41733 
        Number of properties - H3:H41733
        Observations : I3:I41733
        Add Measure Type Column with value: Properties not connected to gas network
        Add Unit column with value: Count
        Format DATAMARKER as required 

#### Table Structure
    Period, Local Authority Name, Local Authority Code, MSOA Name, Middle Layer Super Output Area (MSOA) Code, 
    LSOA Name, Lower Layer Super Output Area (LSOA) Code, Number of domestic gas meters, Number of properties,
    Measure Type, Unit, Value, Marker

#####  Notes	
    Please note the stage one transformation has not included the "Estimated percentage of properties not connected to the gas network (gas meters to number of properties)" data due to the fact these percentages can be derived and multiple measure types cannot be handled currently.  

-------------

### Stage 2. Alignment

#### Sheet: LSOA estimates of properties not connected to the gas network 2015-18 


#### Dataset Name: LSOA estimates of properties not connected to the gas network 2015-18 

	Add column with Period with the following dates:
	2015
 	1st October 2014 - 31st September 2015
   	2016
 	15th July 2016 – 15th July 2018
   	2017
 	15th June 2017 – 15th June 2018
   	2018
	15th May 2018 – 15th May 2019
	Distinguish the dates for each year but can be joined into one cube. 
 	

#### Table Structure
	Period, Local Authority Name, Local Authority Code, MSOA Name, Middle Layer Super Output Area (MSOA) Code, 
	LSOA Name, Lower Layer Super Output Area (LSOA) Code, Number of domestic gas meters, Number of properties,
	Measure Type, Unit, Value, Marker

##### Footnotes
	Add https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/920993/sub-national-consumption-methodology-guidance-2020.pdf as link to methodology 
	Add Contacts: 
	Statistician Responsible:
	Adam Bricknell

	Prepared by:
	Oliver Hendey
	EnergyEfficiency.Stats@beis.gov.uk
	020 7215 0222


--------------

##### DM Notes

		

<!-- #endregion -->

```python

```
https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/920993/sub-national-consumption-methodology-guidance-2020.pdfhttps://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/920993/sub-national-consumption-methodology-guidance-2020.pdf
