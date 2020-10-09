# COGS Dataset Specification

[Family Home](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/specmenu.html)

[Family Transform Status](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/index.html)

## MHCLG English indices of deprivation 2019 

[Landing Page](https://www.gov.uk/government/collections/english-indices-of-deprivation)

### File_7_-_All_loD2019_Scores_Ra


#### Sheet: 1

		Overall date of 2019 put in title and comment
		Remove columns 'LSAO name (2011)', 'Local Authority District name (2019)'
		Rename columns:
			'LSOA code (2011) -> 'Lower Layer Super Output Area'
			'Local Authority District code (2019)' -> 'Local Authority'
		Columns E:BE to be pivoted and turned into column 'Indice of Deprivation' (codelist)
		info.json:
			Measure Type: indice
			Unit: deprivation
			Column references have been set up

		Scraper:
			comment: f'Statistics on relative deprivation in small areas in England, {yr}.' (yr set to 2019)
			title: 'English indices of deprivation'
			description: see footnotes.
			family: 'towns-high-streets'

		Codelists:
			'Indice of Deprivation' codelist has been created (csv & json)

#### Table Structure

		Lower Layer Super Output Area, Local Authority, Indice of Deprivation, Value

#### Footnotes
	Notes added to description, yr variable set up to take account of future publications, currently set to 2019

	Statistics on relative deprivation in small areas in England, 2019.
	These statistics update the English indices of deprivation 2015.
	The English indices of deprivation measure relative deprivation in small areas in England called lower-layer super output areas. The index of multiple deprivation is the most widely used of these indices.
	The statistical release and FAQ document (above) explain how the Indices of Deprivation {yr} (IoD{yr}) and the Index of Multiple Deprivation (IMD{yr}) can be used and expand on the headline points in the infographic. Both documents also help users navigate the various data files and guidance documents available.
	The first data file contains the IMD{yr} ranks and deciles and is usually sufficient for the purposes of most users.
	Mapping resources and links to the IoD{yr} explorer and Open Data Communities platform can be found on our IoD{yr} mapping resource page.
	Further detail is available in the research report, which gives detailed guidance on how to interpret the data and presents some further findings, and the technical report, which describes the methodology and quality assurance processes underpinning the indices.
	\nStatistical release main findings:
	https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/835115/IoD{yr}_Statistical_Release.pdf
	\nInfographic:
	https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/833959/IoD{yr}_Infographic.pdf
	\nResearch report:
	https://www.gov.uk/government/publications/english-indices-of-deprivation-2019-research-report
	\nTechnical report:
	https://www.gov.uk/government/publications/english-indices-of-deprivation-2019-technical-report

##### DM Notes

		notes

