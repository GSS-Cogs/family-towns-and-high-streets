# COGS Dataset Specification

[Family Home](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/specmenu.html)

[Family Transform Status](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/index.html)

## ONS Population estimates by output areas, electoral, health and other geographies, England and Wales 

[Landing Page](https://www.nomisweb.co.uk/query/construct/summary.asp?mode=construct&version=0&dataset=2010)


### Specification

		Any age value with a '+' will need to be changed to 'plus'
		Date, dates goes back to 2011, are we taking all this data?
		GEOGRAPHY_NAME - change to Geography, E00000000 etc
		GENDER_NAME: change to 'Gender' and change values to M,F,T,U
		C_AGE_NAME: change to 'Age'
		C_AGE_TYPE: change to 'Age Type' defines from which age codelist the value is from (Labour Market category, Individual age, 5 year age band). Kept in just in case it it important.
		OBS_Status: change to 'Value'

		'Measure Type' has been set to 'count'
		'Unit' has been set to 'persons'

#### Table Structure

		Date, Geography, Gender, Age, Age Type, Value

##### Footnotes

		footnotes

##### DM Notes

		Column definitions have been created in info.json file. Need some descriptions
		Age codelist has been created along with codelist folder

