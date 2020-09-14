from gssutils import * 
import json 

# +
outputs = {}

trace = TransformTrace()

scraper = Scraper(seed="info.json")
scraper
# -

# ## What's this next bit doing?
#
# The sheets in this one are remarkably similar and where we need to intervene it's just a combination of 2 or 4 pretty standard things. So we're just defining what we want to do here (against which sheet - i.e the key in the following dict).
#
# If the values consists of a blank dictionary it just means it's already flat so we don't need to make any interventions, but keeping those in the dictionary means it'll error for new/unexpected tabs, which is the sort of thing we probably want to know about.
#
# You'll also notice a fair few tables I'm not taking, these are a mix of metadata-esque tables (Contents etc) as well as tables hidden in the outputs and used to power all the macros and what have you they've got going on here.   

# +
tabs_we_want_and_handlers ={
                'LA level at 31 Mar 2020': {},
                'LA level in year 2019-20': {},
                'Provider level at 31 Mar 2020': {"header_row_is": {"row":0}},
                'Provider level in year 2019-20': {"header_row_is": {"row":0}},
                'Providers+places at 31 Mar 2020':{}, 
                'Leavers+Joiners at 31 Mar 2020': {}, 
                'Providers+places six monthly':{"header_row_is": {"row":1}, "remove_footer_from_blank_in": "Date", "melt_to_the_right_of": "All Children's Homes", "remove_blanks_from_column": "value"},
                'Latest OE 31 Mar Time Series':{"header_row_is": {"row": 4}, "fill_down_column": "Provision Type", "remove_columns": [{"without_headers": True}], "melt_to_the_right_of": "Outstanding"}, 
                'In Year Time series': {"header_row_is": {"row":4}, "fill_down_column": "Provision Type", "remove_columns": [{"without_headers": True}]},
                'LA inspections 2009-2020':{"header_row_is": {"row":0}}, 
                }
                
tabs_we_dont_want = ['Cover', 'Contents', 'Notes', 'Meta-data', 'Dates', 'dropdown lookups', 'Pivots_SoN', 'Pivots_InYear']

# -

xls = pd.ExcelFile(scraper.distribution(latest=True).downloadURL) # own cell, cos its slow


# +

def validate_pandas_sheets_by_name(sheet_name):
    """
    Blows up with some details if we get different tabs than we're expecting.
    """
    sheet_name = sheet_name.strip()
    
    if sheet_name in tabs_we_dont_want:
        return False
    
    # Throw an error if we get a tab we were not expecting?
    if sheet_name not in tabs_we_want_and_handlers.keys():
        raise Exception("The tab '{}' was not present at the time this pipleine was " \
                        "originally created.".format(sheet_name))

    return True

# We're just pulling the sheets out into pandas dataframes, as:
# {sheet_name: sheet_as_a_dataframe}
names_and_frames = {}

xls = pd.ExcelFile(scraper.distribution(latest=True).downloadURL)
for sheet in xls.sheet_names:
    if validate_pandas_sheets_by_name(sheet):
        this_excel = pd.read_excel(xls, sheet)
        names_and_frames[sheet] = this_excel



# +

for name, df in names_and_frames.items():
    
    df.fillna("", inplace=True)
    
    # --- IMPORTANT ---
    # Uncomment me to see the before data as well as the after!
    df.to_csv("./out/{}-OLD.csv".format(name), index=False)
    
    try:
        
        print("Processing {}.".format(name))
        # We'll add the trace columns dynamically as it'll keep changing
        trace.start(name, name, [], scraper.distribution(latest=True).downloadURL)
        
        for handler, args in tabs_we_want_and_handlers[name].items():
            
            # Apply whatever things the handler is telling us we need to apply
            # to whichever tab we've iterated too at the moment.

            # Get rid of any bumf above the actual header row
            if handler == "header_row_is":
                old_headers = df.columns.values.tolist()
                for i, row in df.iterrows(): # TODO, dont iterate
                    if i == args["row"]:
                        new_headers = row.values
                        break
                renamed = dict(zip(old_headers, new_headers))
                
                # Trace what we're renaming to what
                for col in new_headers:
                    trace.add_column(col)  
                trace.all('Make row "{}" the new header row'.format(str(i)))
                
                df = df[args["row"]+1:]
                df = df.rename(columns=dict(zip(old_headers, new_headers)))
                
            # Drop any columns we need to drop
            if handler == "remove_columns":
                for item in args:
                    # if its a dict rather than a string column name
                    # it's some sort of globally applied rule
                    if isinstance(item, dict):
                        df.columns = [str(x) for x in df.columns.values.tolist()]
                        df = df.drop("", axis=1)
                        print("Dropped: {}".format(col))
                        
                for col in df.columns.values.tolist():
                    trace.add_column(col)
                trace.all("Dropped all columns with blank headers from dataframe")
                        
            # Fill down a column where they've styled it with the value only at the top
            if handler == "fill_down_column":
                if not isinstance(args, str):
                    raise Exception("Argument to fill down column must be a str type column name")
                
                col = args # easier to read
                if len([x for x in df[col].unique() if x != ""]) != 1:
                    raise Exception("fill_down_column must have a single value in it to fill down. Got {}".format(",".join([str(x) for x in list(df[col].unique())])))
                
                df[col] = list(df[col].unique())[0]
                trace.add_column(col)
                trace.multi([col.replace(" ", "_")], 'Filled down values in column name "{}".'.format(col))
                            
            # Unpivot all columns to the right of a given column
            if handler == "melt_to_the_right_of":
                col = args # easier to read
                i = df.columns.values.tolist().index(col)
                
                dont_melt = df.columns.values.tolist()[:i]
                do_melt = df.columns.values.tolist()[i:]
                df = pd.melt(df, id_vars=dont_melt, value_vars=do_melt)
                trace.add_column(col)
                trace.multi([col.replace(" ", "_")], 'Use melt to unpivot all columns including and to the right of "{}".'.format(col))
                
            # Cur out the footer from the first blank value in the given column
            if handler == "remove_footer_from_blank_in":
                col = args # easier to read
                
                for i,row in df.iterrows():
                    if row[col] == "":
                        break
                df = df[:i-1]
                trace.add_column(col)
                trace.multi([col], 'Remove all rows from the first blank one in column "{}".'.format(col))
            
            # Remove any rows that are blank in the given column 
            if handler == "remove_blanks_from_column":
                col = args # easier to read
                df = df[df[col] != ""]
                trace.add_column(col)
                trace.multi([col], 'Remove all rows with a blank entry in column "{}".'.format(col))
            
        outputs[name] = df
        df.to_csv("./out/{}.csv".format(name), index=False)
            
    except Exception as e:
        raise Exception("With sheet '{}'".format(name)) from e
        
# -
# # Note for future me/somene else
#
# All the sheets are in a dictionary called `outputs` of the structure `{"sheet name":  datframe}`



# leave at the end please
trace.render("spec_v1.html")
