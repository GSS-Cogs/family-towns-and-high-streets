# COGS Dataset Specification

[Family Home](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/specmenu.html)

[Family Transform Status](https://gss-cogs.github.io/family-towns-and-high-streets/datasets/index.html)

## HMRC Personal tax credits  finalised award statistics - small area data  LSOA and Data Zone   year 

[Landing Page](https://www.gov.uk/government/collections/personal-tax-credits-statistics#other-statistics-information)


--------------------------------------------------

#### Stage 2 Spec

		Files: Several LSOA files split by Regions

#### Sheet: Families

		Format 'Date' column to year/month -> 2020/07
		Remove columns: Region, Local Authority, LSOA name
		Rename columns:
			Local Authority code -> Local Authority
			LSAO code -> Lower Layer Super Output Area
		Remove 'Regional' total rows as these can be derived - although some values are suppressed.
		Work Situation
			In Work (K:P)
			Out of Work (U:W)
			All (G, I)
		Family Type
			All	(K:O, U)
			Lone Parent (R:S, V)
			Couples (W)
		Benefit Type
			All (G, I)
			WTC and CTC (K)
			CTC only (L)
			WTC only (M)
			Childcare element (O, S)
			Child Benefit (G)
			All Tax Credits (I)
		Marker - any value with - replace with 0 and add the following to Marker cell:
			Suppressed
		Remove 'National Childcare Indicator' column as this can be derived

		Measure columns have been set in info.json
			Measure Type -> count
			Unit = families

		Scraper values
			Title: Personal tax credits finalised award statistics, LSOA and Data Zone - Families 
			Comment: These statistics provide detailed geographical estimates of the number of families in receipt of tax credits.
			Description: <should hopefully be sorted>
			
#### Table Structure

		Date, Local Authority, Lower Layer Super Output Area, Marker, Value

#### Sheet: Children

		Format 'Date' column to year/month -> 2020/07
		Remove columns: Region, Local Authority, LSOA name
		Rename columns:
			Local Authority code -> Local Authority
			LSAO code -> Lower Layer Super Output Area
		Remove 'Regional' total rows as these can be derived - although some values are suppressed.
			Work Situation
			In Work Families (K:N)
			Out of Work Families (P:R)
			All (G, I)
		Family Type
			All (K:M, P)
			Lone Parent (N:Q)
			Couples (R)
		Benefit Type
			All (G, I)
			WTC and CTC (K)
			CTC only (L)
			Child Benefit (G)
			All Tax Credits (I)
		Marker - any value with - replace with 0 and add the following to Marker cell:
			Suppressed

		Measure columns have been set in info.json
			Measure Type -> count
			Unit = children
			
		Scraper values
			Title: Personal tax credits finalised award statistics, LSOA and Data Zone - Families 
			Comment: These statistics provide detailed geographical estimates of the number of children in families in receipt of tax credits.
			Description: <should hopefully be sorted>
		
#### Table Structure

		Date, Local Authority, Lower Layer Super Output Area, Marker, Value
		
##### Footnotes

		footnotes

##### DM Notes

		Families tab: Requesting information from BAs about out-of-work families and exactly which benefits this includes (CTC, UC etc.)
		I was not able to run the transform as it was taking so long so have not seen output from the stage 1 Transform.
