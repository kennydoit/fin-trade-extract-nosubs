

here is a list of crime coedes to be used to pull data.

Data to be stacked in a table.

data are national for "United States Arrests"

target table name: CURATED_FBI.ARRESTS_NATIONAL

Makes sure there is a column added to this table for OFFENSE_CODE

OFFENSE_CODES = [
    310, 110, 50, 60, 101, 330, 290, 260, 150, 158, 157, 160, 159, 156, 153, 152, 155, 154, 151,
    280, 200, 180, 190, 173, 171, 172, 170, 102, 70, 270, 12, 90, 11, 250, 140, 142, 141, 143, 23,
    23, 20, 30, 240, 55, 210, 300, 220, 230
]

Sample API Call, for example, replace OFFENSE_CODE with 310 and so forth using 
the above series:

https://api.usa.gov/crime/fbi/cde/arrest/national/OFFENSE_CODE?type=counts&from=01-1999&to=03-2026&API_KEY=xxxxxx


