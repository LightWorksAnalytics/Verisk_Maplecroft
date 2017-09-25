# Verisk_Maplecroft

Author: Alexander CW Cook
Verison: 1.0
Maintainer: Alexander CW Cook

Provides a method of extracting JSON data from the EONET API please see https://eonet.sci.gsfc.nasa.gov.

The data from the API is stored in a sqllite database called EONET_DB and is saved in the working directory.

This script will create a folder 'TEMP' which is deleted at the end of the script.

Once the data is stored in the EONET_DB, it is then extracted and transformed into 2 pandas dataframes for:
    
    Wildfires
    Severe Storms
    
Both dataframes will be subsetted to the current month period only. The subsetting data is then saved on individual sheets inside a xlsx file. 
which is saved within the temp folder.
As the data contains geometries, the points are plotted on to a chloeropath and saved inside the temp folder.

Any email addresses supplied are parsed and checked for suitability, note this is only a basic check.

Once all the data is ready in the temp folder the files are then placed inside an e-mail for delivery to the intended parties.

On completion the script will remove the TEMP folder.


Potential Modifications on next update;
 1. Global Variables stored inside a configuration JSON file.
 2. Email Passwords encrypted for security purposes.
 
 

