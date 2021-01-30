# edmonton-covid
Python script for parsing Edmonton's COVID statistics

Pushing data from
https://www.alberta.ca/stats/covid-19-alberta-statistics.htm#data-export
was becoming a bit too large to handle so rather than fiddling with
Google's spreadsheets to work around such a large dataset, I wrote this to
look at statistics and trends.

Ultimately this will talk to Google Sheets for the visualiztion since I
don't really want to do all the plotting in Python (maybe I'll do it
eventually).

This is Sunday-afternoon hacking stuff, mostly to keep me sharp as I don't
have the opportunity to do much work in Python lately.  Sharing in case
someone else finds it interesting.

The script will automatically try to update a google sheet as defined in
~/.gsheet.ini (if this file exists).  If the file does not exist, it will
not attempt to update anything.

The sheet needs tabs titled "PIVOT-[YEAR]" (i.e. PIVOT-2020) and will put
the data for that year in the sheet.  What you do with that is up to you, I
use it to build charts and such.  It will _only_ do this when you import,
so it will first update the SQLite database and then attempt to update the
spreadsheet.  Any query operations do not touch the spreadsheet.
