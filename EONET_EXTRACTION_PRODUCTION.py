#!/usr/bin/env python3 

# -*- coding: utf-8 -*-
"""
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
 

"""
import os
import sqlite3
from sqlite3 import Error
from email.utils import parseaddr
import urllib
import urllib.request
import json
import sys
import pandas
import datetime
import dateutil
import shutil
import smtplib
import geopandas as gdp
import matplotlib.pyplot as plt
import warnings
from geopandas import GeoDataFrame
from shapely.geometry import Point
from email import encoders
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from geopy.geocoders import Nominatim
from contextlib import suppress

"""Authorship information"""

__author__ = "Alexander CW Cook"""
__copyright__ = "Copyright 2017, LightWorks Analytics Verisk Maplecroft"
__credits__ = ["Alexander CW Cook"]
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Alexander CW Cook"
__email__ = "acwcook@outlook.com"
__status__ = "Production"

"""Global Variables"""

url = 'https://eonet.sci.gsfc.nasa.gov/api/v2.1/events'
db = 'EONET_DB'
request = urllib.request.Request(url)
response = urllib.request.urlopen(request)
json_obj = json.load(response)
conn = sqlite3.connect(db)
dateformat = "%Y%m%d%H%M"
COMMASPACE = ', '



def fxn():    
    """Turns off any depreciation warnings to the user."""    
    warnings.warn("deprecated", DeprecationWarning)
    
def mail(maddress):
    """Sends emails with attachements to a the inputted email address.
    
    maddress: Supplied email address, which should be seperated by a comma.
    
    
    Note: This function contains hard coded parts; which would not normally exist in
          a production script.
    """  
    sender = 'alex.icsconsulting@gmail.com'
    gmail_password = 'Fran211509!'                                             #Should be encryted
    recipients = [maddress]
    outer = MIMEMultipart()
    outer['Subject'] = 'EONET_DELIVERY'
    outer['To'] = COMMASPACE.join(recipients)
    outer['From'] = sender
    outer.preamble = 'You will not see this in a MIME-aware mail reader.\n'
    inner = MIMEMultipart('alternative')
    text = "Please find attached the latest Spreadsheet and Chart for Severe Storms and Wildfires from EONET "
    html = """\
    <html>
      <head>
         <style>
        *{
            padding:0;
            margin:0 auto;
        }
        #wrapper {
            width:1500px;    
        }
        #header{
            height:80px;
            background-color:blue;
            width:100%;
        }
        #content{
            width:100%;
            background-color:#5DBCD2;
        }
    </style>
      </head>
      <body>
      <div id="wrapper">
    <div id="header"></div>
    <div id="content"><h3>Automated Data Delivery</h3></div>
</div>
        <p>
            Sir/Madam <br/><br/>
        
           Please find attached the latest Spreadsheet and Chart for Severe Storms and Wildfires from EONET
        </p>
        <p>
        <br/><br/>
           <img src = 'https://maplecroft.com/media/maplecroft/img/global/logo_maplecroft_verisk_dark.png'>
        </p>
      </body>
    </html>
    """

    # List of attachments
    attachments = [os.path.join(os.getcwd(), 'TEMP') + '\\' + datetime.datetime.strftime(datetime.datetime.now(),dateformat) + '_' + 'EONET_REPORT.xlsx',os.path.join(os.getcwd(), 'TEMP') + '\\' +  datetime.datetime.strftime(datetime.datetime.now(),dateformat) + '_' + 'chart.png']

    # Add the attachments to the message
    for file in attachments:
        try:
            with open(file, 'rb') as fp:
                msg = MIMEBase('application', "octet-stream")
                msg.set_payload(fp.read())
            encoders.encode_base64(msg)
            msg.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file))
     
            outer.attach(msg)
            
        except:
            print("Unable to open one of the attachments. Error: ", sys.exc_info()[0])
            raise
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')
    inner.attach(part1)
    inner.attach(part2)     
    outer.attach(inner)
    composed = outer.as_string()

    # Send the email
    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as s:
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.login(sender, gmail_password)
            s.sendmail(sender, recipients, composed)
            s.close()
        print("Email sent!")
    except:
        print("Unable to send the email. Error: ", sys.exc_info()[0])
        raise

def validateEmail(eMail):
    """Validates (Basic) the email address supplied.
    
    eMail: Supplied email address.
    
    """      
    if '@' in  parseaddr(eMail)[1]:
        return True
    else:
        return False
     

def db_tables(tbl):
    """Gets a count of tables which match the supplied table name.
    
    tbl: table name
    
    """      
    cur = conn.cursor()
    cur.execute("SELECT count(*) from sqlite_master where name = ?", (tbl,))
    data = cur.fetchone()
    cur.close()
    return data[0]
 

def db_create():
    """Creates of cleans up the SQLite Database.
    
    """      
    if not db_tables('T_S_EVENTS') == 1:
        conn.execute('''CREATE TABLE T_S_EVENTS (EONET_ID, TITLE)''')
    else:
        conn.execute('DELETE FROM T_S_EVENTS')
    if not db_tables('T_S_CATEGORIES') ==1:
        conn.execute('''CREATE TABLE T_S_CATEGORIES (EONET_ID, TITLE)''')
    else:
        conn.execute('DELETE FROM T_S_CATEGORIES') 
    if not db_tables('T_S_SOURCES') == 1:
        conn.execute('''CREATE TABLE T_S_SOURCES (EONET_ID, SOURCES)''')
    else:
        conn.execute('DELETE FROM T_S_SOURCES')
    if not db_tables('T_S_GEOMS') == 1:
        conn.execute('''CREATE TABLE T_S_GEOMS (EONET_ID, DATE, TYPE, GEOM)''')
    else:
        conn.execute('DELETE FROM T_S_GEOMS')
    conn.commit()


def load_data():
    if response.getcode() == 200:
        for elements in json_obj['events']:
            conn.execute("INSERT INTO T_S_EVENTS (EONET_ID, TITLE) VALUES (?,?)", (elements['id'],elements['title']))
            for subelements in elements['categories']:
                conn.execute("INSERT INTO T_S_CATEGORIES (EONET_ID, TITLE) VALUES (?,?)", (elements['id'],subelements['title']))
            for subelements in elements['sources']:
                conn.execute("INSERT INTO T_S_SOURCES (EONET_ID, SOURCES) VALUES (?,?)", (elements['id'],subelements['url']))        
            for subelements in elements['geometries']:
             conn.execute("INSERT INTO T_S_GEOMS (EONET_ID, DATE, TYPE, GEOM) VALUES (?,?,?,?)", (elements['id'], subelements['date'], subelements['type'], str(subelements['coordinates'])  ))
             #print (elements['id'], subelements['date'], subelements['type'], str(subelements['coordinates'])     ) 
        conn.commit()
    else:
        print ('FAILURE TO COONNECT TO API :: CHECK CONNECTION')
def plotmaps(df_WF, df_SS):
    fig, ax = plt.subplots()
    fig.set_size_inches(40, 20)
    world = gdp.read_file(gdp.datasets.get_path('naturalearth_lowres'))
    ax.set_aspect('equal')
    world.plot(ax=ax, color = 'white', edgecolor='grey')
    #world = world[['continent', 'geometry', 'pop_est']]
    geometry = [Point(xy) for xy in zip(df_WF.Longitude.astype(float), df_WF.Latitude.astype(float))]
    geometry_SS = [Point(xy) for xy in zip(df_SS.Longitude.astype(float), df_SS.Latitude.astype(float))]
    crs = {'init': 'WSG:84'}
    geo_df = GeoDataFrame(df_WF, crs=crs, geometry=geometry)
    geo_ss = GeoDataFrame(df_SS, crs=crs, geometry=geometry_SS)
    geo_ss.plot(ax=ax,column='TITLE', marker='o', markersize=5)
    geo_df.plot(ax=ax, color='red',marker='X', markersize=5)
    plt.axis('off')
    ax.legend()
    plt.title('Earth Observations for the Period ' + (datetime.datetime.now() - dateutil.relativedelta.relativedelta(months=1)).strftime('%d %B %Y  %H:%M') + ' untill '+ datetime.datetime.now().strftime('%d %B %Y %H:%M'), fontsize=20)
    plt.savefig(os.path.join(os.getcwd(), 'TEMP') + '\\' +  datetime.datetime.strftime(datetime.datetime.now(),dateformat) + '_' + 'chart.png')
    #plt.show();

def report():
    dateformat = "%Y%m%d%H%M"
    sql = "Select t1.EONET_ID, t1.title Category, t3.title, t2.DATE, TYPE,   case when TYPE = 'Point' THEN substr(t2.GEOM,2,instr(t2.GEOM,',')-2)  end as Longitude, case when TYPE = 'Point' THEN replace(substr(t2.GEOM,instr(t2.GEOM,',')+2,instr(t2.GEOM,']')),']','')  end as Latitude, case when TYPE ='Polygon' THEN replace(replace(replace(replace(t2.GEOM,'[[',''),']]',''),']',''),'[','') end as GEOM from T_S_CATEGORIES t1  inner join T_S_GEOMS t2 on t1.EONET_ID = t2.EONET_ID and t1.title in ('Wildfires','Severe Storms') left join T_S_EVENTS t3 on t1.EONET_ID = t3.EONET_ID order by t1.EONET_ID, DATE" 
    df = pandas.read_sql_query(sql, conn, index_col = None)
    #print (list(df.columns))
    df['DATE'] = pandas.to_datetime(df.DATE, yearfirst = True, errors='coerce')
    #print (df['DATE'].dtype)
    df_WF = df[(df.Category == 'Wildfires') & (df.DATE >= datetime.datetime.now() - dateutil.relativedelta.relativedelta(months=1))]
    df_WF['Longitude'] = pandas.to_numeric(df_WF.Longitude, errors = 'coerce')
    df_WF['Latitude'] = pandas.to_numeric(df_WF.Latitude, errors = 'coerce')
    df_SS = df[(df.Category == 'Severe Storms') & (df.DATE >= datetime.datetime.now() - dateutil.relativedelta.relativedelta(months=1))]
    df_SS['Longitude'] = pandas.to_numeric(df_SS.Longitude, errors = 'coerce')
    df_SS['Latitude'] = pandas.to_numeric(df_SS.Latitude, errors = 'coerce')  
    writer = pandas.ExcelWriter(os.path.join(os.getcwd(), 'TEMP') + '\\' + datetime.datetime.strftime(datetime.datetime.now(),dateformat) + '_' + 'EONET_REPORT.xlsx' ,engine='xlsxwriter')
    df_SS.to_excel(writer,sheet_name = 'Severe Storms', index=False)
    df_WF.to_excel(writer,sheet_name = 'Wildfires', index=False)
    writer.save()
    plotmaps(df_WF, df_SS)
    
def folderCreate():
    try:
        if os.path.isdir(os.path.join(os.getcwd(), 'TEMP')):
            shutil.rmtree(os.path.join(os.getcwd(), 'TEMP'))
        else:
            os.mkdir(os.path.join(os.getcwd(), 'TEMP'))
    except:
        print ('ERROR CREATING TEMP FOLDER CHECK PERMISIONS')
    
def main(eMail):                    #Main is used to control program flow.
    if validateEmail(eMail):
        folderCreate()
        db_create()
        load_data()
        report()
        mail(eMail)
        folderCreate() #Cleanup
        print("Data has been extracted, the program will now quit.")
    else:
        print('E-MAIL VALIDATION ERROR :: ')
    
if __name__ == '__main__':
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fxn()    
        try:
            main(response)
        except:
            print('######################################################')
            print('#                                                    #')                  
            print('#             EONET API DATA EXTRACTOR               #')
            print('#                                                    #')
            print('######################################################')        
            reply = input("Please enter the required e-mail address, if you need help type 'help' ::  " )
            if reply == 'help':
                print('######################################################')
                print('              EONET API DATA EXTRACTOR                ')
                print('                                                      ')
                print('                   HELP FILE                          ')
                print('######################################################')
                print('To revieve the latest monthly report please enter a   ')              
                print('valid email address.')
                reply = input("Please enter the required e-mail address, if you need help type 'help' ::  " )
                main(reply)                                  
            else:
                main(reply)
else:
    print('NOT MAIN')
        