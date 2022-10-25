import dbm
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

### drop the old tables
def droppingFunction_all(dbList, db_source):
    for table in dbList:
        db_source.execute(f'drop table {table}')
        print(f'dropped table {table} succesfully!')
    else:
        print(f'kept table {table}')


load_dotenv()


GCP_MYSQL_HOSTNAME = os.getenv("GCP_MYSQL_HOSTNAME")
GCP_MYSQL_USER = os.getenv("GCP_MYSQL_USER")
GCP_MYSQL_PASSWORD = os.getenv("GCP_MYSQL_PASSWORD")
GCP_MYSQL_DATABASE = os.getenv("GCP_MYSQL_DATABASE")


########


connection_string = f'mysql+pymysql://{GCP_MYSQL_USER}:{GCP_MYSQL_PASSWORD}@{GCP_MYSQL_HOSTNAME}:3306/{GCP_MYSQL_DATABASE}'
db_gcp = create_engine(connection_string)

#### note to self, need to ensure server_paremters => require_secure_transport is OFF in Azure 

### show tables from databases

tableNames_gcp = db_gcp.table_names()

# reoder tables
tableNames_gcp = ['medications','conditions', 'social_determinants','treatments_procedures','patients', 'patient_treatment']

# ### delete everything 
droppingFunction_all(tableNames_gcp, db_gcp)


#### first step below is just creating a basic version of each of the tables,
#### along with the primary keys and default values 


### 


table_medications = """
create table if not exists medications (
    id int auto_increment,
    med_ndc varchar(255) default null unique,
    med_human_name varchar(255) default null,
    med_is_dangerous varchar(255) default null,
    PRIMARY KEY (id)
    
    
); 
"""

table_conditions = """
create table if not exists conditions (
    id int auto_increment,
    icd10_code varchar(255) default null unique,
    icd10_description varchar(255) default null,
    PRIMARY KEY (id) 
    
); 
"""


table_social_determinants = """
create table if not exists social_determinants (
    id int auto_increment,
    loinc_code varchar(255) default null unique,
    loinc_code_description varchar(255) default null,
    PRIMARY KEY (id)

); 
"""


table_treatments_procedures = """
create table if not exists treatments_procedures (
    id int auto_increment,
    cpt_code varchar(255) default null unique,
    cpt_code_description varchar(255) default null,
    PRIMARY KEY (id)
    

); 
"""

table_patients = """
create table if not exists patients(
    id int auto_increment,
    mrn varchar(255) default null unique,
    first_name varchar(255) default null,
    last_name varchar(255) default null,
    dob varchar(255) default null,
    gender varchar(255) default null,
    PRIMARY KEY (id)   
    ); """

table_patient_summary = """
create table if not exists patient_summary (
    id int auto_increment,
    mrn varchar(255) default null,
    conditions varchar(255) default null,
    medication varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (conditions) REFERENCES conditions(icd10_code) ON DELETE CASCADE,
    FOREIGN KEY (medication) REFERENCES medications(med_ndc) ON DELETE CASCADE
); 
"""

db_gcp.execute(table_patients)
db_gcp.execute(table_medications)
db_gcp.execute(table_conditions)
db_gcp.execute(table_treatments_procedures)
db_gcp.execute(table_social_determinants)
db_gcp.execute(table_patient_summary)


# get tables from db_gcp
gcp_tables = db_gcp.table_names()




