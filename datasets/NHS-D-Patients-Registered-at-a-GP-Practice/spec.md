# COGS Dataset Specification

[Family Home](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/specmenu.html)

[Family Transform Status](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/index.html)

## NHS-D Patients Registered at a GP Practice 

[Landing Page](https://digital.nhs.uk/data-and-information/publications/statistical/patients-registered-at-a-gp-practice)

[Transform Flowchart](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/specflowcharts.html?NHS-D-Patients-Registered-at-a-GP-Practice/flowchart.ttl)

### 5 Spreadsheets

		Totals (GP practice-all persons)
		Single year of age (Commissioning Regions-STPs-CCGs-PCNs)
		Single year of age (GP practice-females)
		Single year of age (GP practice-males)
		5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice) (NEEDS TO BE SPLIT BETWEEN GO AND THE REST)

#### Totals (GP practice-all persons)

		Delete columns: PUBLICATION, TYPE and CCG_CODE
		Rename columns:
			'ONS_CCG_CODE': 'ONS CCG Code',
    			'CODE': 'Practice Code',
    			'POSTCODE': 'Post Code',
    			'SEX': 'Sex',
    			'AGE': 'Age'
    		Sex: change values to 'T', 'M', 'F', 'U'
    		Age: reformat replace '_' with T and prefix with 'Y' (Y5T10, Y10 etc.)
		
#### Single year of age (GP practice-females)

		Delete column: CCG_CODE
		Rename columns:
			'ONS_CCG_CODE': 'ONS CCG Code',
    			'ORG_CODE': 'Practice Code',
    			'POSTCODE': 'Post Code',
    			'SEX': 'Sex',
    			'AGE': 'Age'
    		Sex: change values to 'T', 'M', 'F', 'U'
    		Age: reformat replace '_' with T and prefix with 'Y' (Y5T10, Y10 etc.)
		
#### Single year of age (GP practice-males)

		Delete column: CCG_CODE
		Rename columns:
		     'ONS_CCG_CODE': 'ONS CCG Code',
   			'ORG_CODE': 'Practice Code',
   			'POSTCODE': 'Post Code',
    			'SEX': 'Sex',
    			'AGE': 'Age'
    		Sex: change values to 'T', 'M', 'F', 'U'
    		Age: reformat replace '_' with T and prefix with 'Y' (Y5T10, Y10 etc.)

#### Single year of age (Commissioning Regions-STPs-CCGs-PCNs) (Split data, 1. STPs & CCGs. 2. PCNs)

		Rename columns:
			'ORG_TYPE': 'ORG Type',
    			'ONS_CODE': 'ONS Code',
    			'ORG_CODE': 'ORG ORG Code',
    			'SEX': 'Sex',
    			'AGE': 'Age'
    		Sex: change values to 'T', 'M', 'F', 'U'
    		Age: reformat replace '_' with T and prefix with 'Y' (Y5T10, Y10 etc.)

#### 5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice) (Split data, 1. STPs & CCGs. 2. PCNs. 3. GP)

		GP data does not have ONS and CCG Codes so you need to map from the other GP data to fill in the columns
		Rename columns:
		 	'ORG_TYPE': 'ORG Type',
    			'ORG_CODE': 'ORG Code',
   			'ONS_CODE': 'ONS ORG Code',
    			'POSTCODE': 'Post Code',
    			'SEX': 'Sex',
    			'AGE_GROUP_5': 'Age'
    		Sex: change values to 'T', 'M', 'F', 'U'
    		Age: reformat replace '_' with T and prefix with 'Y' (Y5T10, Y10 etc.)

### JOIN (Splitting of data above will mean column name swill have to be reconfigured for joining)
		
#### Dataset One: Patients Registered at a GP Practice - GP

		Join:
			Totals (GP practice-all persons)
			Single year of age (GP practice-females)
			Single year of age (GP practice-males)
			5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice) (ONLY GP DATA FROM THIS DATASET)

		Table Structure:
			Period,ONS CCG Code,Post Code,Practice Code,Age,Sex,Value

		Scraper:
			Family = 'towns-high-streets'
			Description = See footnotes below
			Comment = 'Data for this publication are extracted each month as a snapshot in time from the Primary Care Registration database within the NHAIS (National Health Application and Infrastructure Services) system.'
			Title = 'Patients Registered at a GP Practice - GP'


#### Dataset Two: Patients Registered at a GP Practice - CCG, STP, Comm Region

		Join:
			Single year of age (Commissioning Regions-STPs-CCGs-PCNs) (EVERYTHING EXCEPT PCN DATA FROM THIS DATASET)
			5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice) (EVERYTHING EXCEPT GP and PCN DATA FROM THIS DATASET)

		Table Structure:
			Period,ONS ORG Code,ORG Type,Age,Sex,Value

		Scraper:
			Family = 'towns-high-streets'
			Description = See footnotes below
			Comment = 'Data for this publication are extracted each month as a snapshot in time from the Primary Care Registration database within the NHAIS (National Health Application and Infrastructure Services) system.'
			Ttitle = 'Patients Registered at a GP Practice - CCG, STP, Comm Region'


#### Dataset Three: Patients Registered at a GP Practice - PCN

		Join:
			Single year of age (Commissioning Regions-STPs-CCGs-PCNs) (ONLY PCN DATA FROM THIS DATASET)
			5-year age groups (Commissioning Regions-STPs-CCGs-PCNs-GP practice) (ONLY PCN DATA FROM THIS DATASET)

		Table Structure:
			Period,PCN Code,ORG Type,Age,Sex,Value

		Scraper:
			amily = 'towns-high-streets'
			Description = See footnotes below
			Comment = 'Data for this publication are extracted each month as a snapshot in time from the Primary Care Registration database within the NHAIS (National Health Application and Infrastructure Services) system.'
			Title = 'Patients Registered at a GP Practice - PCN'

##### Footnotes

			Data for this publication are extracted each month as a snapshot in time from the Primary Care Registration database within the NHAIS (National Health Application and Infrastructure Services) system.
			GP Practice; Primary Care Network (PCN); Sustainability and transformation partnership (STP); Clinical Commissioning Group (CCG) and NHS England Commissioning Region level data are released in single year of age (SYOA) and 5-year age bands, both of which finish at 95+, split by gender. In addition, organisational mapping data is available to derive STP; PCN; CCG and Commissioning Region associated with a GP practice and is updated each month to give relevant organisational mapping.
			Quarterly publications in January, April, July and October will include Lower Layer Super Output Area (LSOA) populations and a spotlight report.
			The outbreak of Coronavirus (COVID-19) has led to changes in the work of General Practices and subsequently the data within this publication. Until activity in this healthcare setting stabilises, we urge caution in drawing any conclusions from these data without consideration of the country

##### DM Notes

		Data has been transformed first and spec written later. This was due to some of the data having to be split up and joined with other datasets because of geography codes. No ONS Geography code is available for PCN codes and only GP data has Post Codes.

