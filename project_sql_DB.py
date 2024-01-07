import mysql.connector as mysql
from mysql.connector import Error
import csv
import pandas as pd
import matplotlib.pyplot as plt

# MySQL DB credentials
user = ""
password = ""
file_path = 'database 3.csv'
host = 'localhost'


# create database
def createdb(user:str, passw:str):
    db = mysql.connect(host=host, user=user, passwd=passw)
    curs = db.cursor()

    databasecreation = "CREATE DATABASE homicide_reports"

    curs.execute(databasecreation)


# create tables
def createtables(user: str, passw: str):
    db = mysql.connect(host=host, user=user, passwd=passw, database="homicide_reports")
    curs = db.cursor()

    location_table = """
    CREATE TABLE Location (
        City VARCHAR(100) PRIMARY KEY,
        State VARCHAR(100)
    );
    """

    agency_table = """
    CREATE TABLE Agency (
        Agency_code VARCHAR(50) PRIMARY KEY,
        Agency_name VARCHAR(255),
        Agency_type VARCHAR(255)
    );
    """

    crime_table = """
    CREATE TABLE Crime (
        Record_ID INT PRIMARY KEY,
        Year INT,
        Month VARCHAR(20),
        Weapon VARCHAR(100),
        Record_source VARCHAR(100),
        Victim_count INT,
        Perp_count INT,
        Crime_solved VARCHAR(10),
        Crime_type VARCHAR(100),
        Agency_code VARCHAR(50),
        City VARCHAR(100),
        FOREIGN KEY (City) REFERENCES Location(City),
        FOREIGN KEY (Agency_code) REFERENCES Agency(Agency_code)
    );
    """

    perpetrator_table = """
    CREATE TABLE Perpetrator (
        Record_ID INT PRIMARY KEY,
        Sex VARCHAR(10),
        Age INT,
        Race VARCHAR(50),
        Ethnicity VARCHAR(50),
        FOREIGN KEY (Record_ID) REFERENCES Crime(Record_ID)
    );
    """

    victim_table = """
    CREATE TABLE Victim (
        Record_ID INT PRIMARY KEY,
        Sex VARCHAR(10),
        Age INT,
        Race VARCHAR(50),
        Ethnicity VARCHAR(50),
        Relationship VARCHAR(50),  -- to perpetrator
        FOREIGN KEY (Record_ID) REFERENCES Crime(Record_ID)
    );
    """

    # Execute each table creation command
    curs.execute(location_table)
    curs.execute(agency_table)
    curs.execute(crime_table)
    curs.execute(perpetrator_table)
    curs.execute(victim_table)

    # Close the cursor and the database connection
    curs.close()
    db.close()

def to_int(val):
    try:
        return int(val)
    except ValueError:
        return None


# load and insert data
def dataload(user: str, passw: str, database: str, file_path: str):
    db = mysql.connect(host=host, user=user, password=passw, database=database)
    cursor = db.cursor()

    with open(file_path, 'r', encoding='UTF-8') as f:
        file = csv.reader(f)
        next(file)

        locations = set()
        agencies = set()
        error_log = []

        for index, line in enumerate(file):

            try:
                # Insert data into Location table
                if line[4] not in locations:
                    cursor.execute("INSERT INTO Location (City, State) VALUES (%s, %s)", (line[4], line[5]))
                    locations.add(line[4])
                    db.commit()

                # Insert data into Agency table
                if line[1] not in agencies:
                    cursor.execute("INSERT INTO Agency (Agency_code, Agency_name, Agency_type) VALUES (%s, %s, %s)", (line[1], line[2], line[3]))
                    agencies.add(line[1])
                    db.commit()

                # Insert data into Crime table
                cursor.execute("INSERT INTO Crime (Record_ID, Year, Month, Weapon, Record_source, Victim_count, Perp_count, Crime_solved, Crime_type, Agency_code, City) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (line[0], to_int(line[6]), line[7], line[20], line[23], to_int(line[21]), to_int(line[22]), line[10], line[9], line[1], line[4]))
                db.commit()

                # Insert data into Perpetrator table
                cursor.execute("INSERT INTO Perpetrator (Record_ID, Sex, Age, Race, Ethnicity) VALUES (%s, %s, %s, %s, %s)", (line[0], line[15], to_int(line[16]), line[17], line[18]))
                db.commit()

                # Insert data into Victim table
                cursor.execute("INSERT INTO Victim (Record_ID, Sex, Age, Race, Ethnicity, Relationship) VALUES (%s, %s, %s, %s, %s, %s)", (line[0], line[11], to_int(line[12]), line[13], line[14], line[19]))
                db.commit()

            except Error as e:
                # Log the error and the row number or content that caused it
                error_log.append(f"Error on row {index + 1}: {e}; Data: {line}")

                # Rollback the transaction to undo any changes made before the error occurred in the current iteration
                db.rollback()
            
        if error_log:
            print('Errors occured during data loading: ')
            for error in error_log:
                print(error)
            print('number of errors: ' + str(len(error_log)))




#creating database
try:
    print("creating the database...")
    createdb(user=user, passw=password)
    print("defining tables...")
    createtables(user=user, passw=password)
    print("loading the dataset into the DB...")
    dataload(user=user, passw=password, database='homicide_reports', file_path=file_path)
    print("Database created successfully! Congratulations!")
except Error as e:
    print(f"the database already exists, running queries only...")

# our newly create database
database = 'homicide_reports'

# function to clean up incorrectly labeled missing values in our database:
def clean_database(user, password, database):
    try:
        db = mysql.connect(host=host, user=user, password=password, database=database)
        cursor = db.cursor()

        queries = [
            "update Victim set Sex = NULL where Sex = 'Unknown'",
            "update Victim set age = NULL where age = 99 or age = 998",
            "update Victim set race = NULL where race = 'Unknown'",
            "update Victim set ethnicity = NULL where ethnicity = 'Unknown'",
            "update Victim set Relationship = NULL where Relationship = 'Unknown'",
            "update Perpetrator set Sex = NULL where Sex = 'Unknown'",
            "update Perpetrator set Age = NULL where Age = 0",
            "update Perpetrator set Race = NULL where Race = 'Unknown'",
            "update Perpetrator set Ethnicity = NULL where Ethnicity = 'Unknown'",
            "update Crime set Weapon = NULL where Weapon = 'Unknown'",
            "update Agency set Agency_name = NULL where Agency_name like '%Unknown%'"
        ]

        for query in queries:
            cursor.execute(query)
        db.commit()
    except Error as e:
        print(f"Error occurred: {e}")

    finally:
        if db.is_connected():
            cursor.close()
            db.close()

clean_database(user, password, database)


bug = """

               _ _       \ \ 
    .-'''''-. / \_> /\    |/
   /         \.'`  `',.--//
 -(           I      I  @@\ 
   \         /'.____.'\___|
    '-.....-' __/ | \   (`)
             /   /  /
                 \  \ 
                 
    Oh no! There is a bug in the syntax!             
                 """

# functions for queries
def handgun():
    
    db = mysql.connect(host=host, user=user, password=password, database=database)
    cursor = db.cursor()

    query = """
            with HandgunSolveRate as (
	select 
		year, 
		weapon, 
		count(record_id) as handgun_homicides, 
		count(case when crime_solved = 'Yes' then 1 end) as solved_handgun_homicides,
		count(case when crime_solved = 'Yes' then 1 end) * 1.0 / nullif(count(record_id), 0) * 100 as solved_perc_handgun
	from crime
	where 
		weapon is not null and
		weapon = 'Handgun'
	group by year, weapon
),
OthersSolveRate as (
select
	year,
    count(record_id) as Other_homicides,
    count(case when crime_solved = 'Yes' then 1 end) as solved_other_homicides,
    count(case when crime_solved = 'Yes' then 1 end) * 1.0 / nullif(count(record_id), 0) * 100 as solved_perc_other
from crime
where 
	weapon is not null and
    weapon not like 'Handgun'
group by year
)
select
	o.year,
    h.handgun_homicides,
    o.other_homicides,
    h.solved_handgun_homicides,
	o.solved_other_homicides,
    h.solved_perc_handgun,
	o.solved_perc_other,
	h.solved_perc_handgun - o.solved_perc_other as diff_handgun_to_other
from
	OthersSolveRate o
    left join HandGunSolveRate h on o.year = h.year
order by
	o.year""" 

    cursor.execute(query)
    result = cursor.fetchall()
    return result


def agencies():
    
    db = mysql.connect(host=host, user=user, password=password, database=database)
    cursor = db.cursor()

    query = """
    with murder_stats as (
	select agency_code, count(record_id) as murders, count(case when crime_solved = 'Yes' then 1 end) as solved_murders
	from crime
	group by agency_code
),
overall_stats as(
	select
		sum(m.murders) as total_murders,
        sum(m.solved_murders) as total_solved_murders,
        sum(m.solved_murders) / sum(m.murders) as overall_solve_rate
	from murder_stats m
)
select 
	a.agency_type, 
	count(a.agency_name) as agencies, 
    sum(m.murders) as murders,
    sum(m.solved_murders) as solved_murders,
	(sum(m.murders)/count(a.agency_name)) as murders_per_agency,
    (sum(m.solved_murders)/sum(m.murders)) as agency_type_solve_rate,
	o.overall_solve_rate,
    (sum(m.solved_murders)/sum(m.murders)) - o.overall_solve_rate as solve_rate_difference
from agency a
join murder_stats m on a.agency_code = m.agency_code
cross join overall_stats o
group by a.agency_type, o.overall_solve_rate
order by agencies desc""" 

    cursor.execute(query)
    result = cursor.fetchall()
    return result


def spouses():
    
    db = mysql.connect(host=host, user=user, password=password, database=database)
    cursor = db.cursor()

    query = """
    select 
	l.State,
	count(case when c.Month in ('December', 'January', 'February', 'June', 'July', 'August') then 1 end) as summer_winter_total_murders,
	count(case when c.Month in ('December', 'January', 'February') then 1 end) as winter_murders,
	count(case when c.Month in ('June', 'July', 'August') then 1 end) as summer_murders,
	(count(case when c.Month in ('December', 'January', 'February') then 1 end) / count(case when c.Month in ('December', 'January', 'February', 'June', 'July', 'August') then 1 end)) as winter_percentage,
	(count(case when c.Month in ('June', 'July', 'August') then 1 end) / count(case when c.Month in ('December', 'January', 'February', 'June', 'July', 'August') then 1 end)) as summer_percentage,
    (((count(case when c.Month in ('June', 'July', 'August') then 1 end) / count(case when c.Month in ('December', 'January', 'February', 'June', 'July', 'August') then 1 end))-(count(case when c.Month in ('December', 'January', 'February') then 1 end) / count(case when c.Month in ('December', 'January', 'February', 'June', 'July', 'August') then 1 end)))) as summer_increase
from crime c
inner join victim v on c.Record_ID = v.Record_ID
inner join location l on c.City = l.City
where v.Relationship in ('Wife', 'Girlfriend', 'Husband', 'Boyfriend',
'Common-Law Husband', 'Common-Law Wife', 'Boyfriend/Girlfriend')
group by l.State
order by summer_winter_total_murders desc""" 

    cursor.execute(query)
    result = cursor.fetchall()
    return result


def describe():
    
    db = mysql.connect(host=host, user=user, password=password, database=database)
    cursor = db.cursor()

    describes = [
        "Describe Crime",
        "Describe Perpetrator",
        "Describe Victim",
        "Describe Agency",
        "Describe Location"
    ]

    for table in describes:
        cursor.execute(table)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=['Attribute', 'Type', 'NULL', 'Key', 'Default', 'Extra'])
        print('\n')
        print(str(table))
        print(df)
        print('\n')


def write_query():
    
    db = mysql.connect(host=host, user=user, password=password, database=database)
    cursor = db.cursor()

    while True:
        query = input("Write your own query or type 'exit' to return to main menu:  ")
        
        if query == 'exit':
            break

        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except Error as e:
            print('\n')
            print(bug)
            print(f"{e}")
            print("Please try again!")
            print('\n')
        


welcome_message = """
____    __    ____  _______  __        ______   ______   .___  ___.  _______                 
\   \  /  \  /   / |   ____||  |      /      | /  __  \  |   \/   | |   ____|                
 \   \/    \/   /  |  |__   |  |     |  ,----'|  |  |  | |  \  /  | |  |__                   
  \            /   |   __|  |  |     |  |     |  |  |  | |  |\/|  | |   __|                  
   \    /\    /    |  |____ |  `----.|  `----.|  `--'  | |  |  |  | |  |____                 
    \__/  \__/     |_______||_______| \______| \______/  |__|  |__| |_______|                
                                                                                             
.___________.  ______        ______    __    __  .______                                     
|           | /  __  \      /  __  \  |  |  |  | |   _  \                                    
`---|  |----`|  |  |  |    |  |  |  | |  |  |  | |  |_)  |                                   
    |  |     |  |  |  |    |  |  |  | |  |  |  | |      /                                    
    |  |     |  `--'  |    |  `--'  | |  `--'  | |  |\  \----.                               
    |__|      \______/      \______/   \______/  | _| `._____|                               
                                                                                             
     _______. __  ___  _______ ____    __    ____  __                                        
    /       ||  |/  / |   ____|\   \  /  \  /   / |  |                                       
   |   (----`|  '  /  |  |__    \   \/    \/   /  |  |                                       
    \   \    |    <   |   __|    \            /   |  |                                       
.----)   |   |  .  \  |  |____    \    /\    /    |  `----.                                  
|_______/    |__|\__\ |_______|    \__/  \__/     |_______|                                  
                                                                                             
 _______       ___   .___________.    ___      .______        ___           _______. _______ 
|       \     /   \  |           |   /   \     |   _  \      /   \         /       ||   ____|
|  .--.  |   /  ^  \ `---|  |----`  /  ^  \    |  |_)  |    /  ^  \       |   (----`|  |__   
|  |  |  |  /  /_\  \    |  |      /  /_\  \   |   _  <    /  /_\  \       \   \    |   __|  
|  '--'  | /  _____  \   |  |     /  _____  \  |  |_)  |  /  _____  \  .----)   |   |  |____ 
|_______/ /__/     \__\  |__|    /__/     \__\ |______/  /__/     \__\ |_______/    |_______|
                                                                                             """


goodbye = """
  _______   ______     ______    _______  .______   ____    ____  _______ 
 /  _____| /  __  \   /  __  \  |       \ |   _  \  \   \  /   / |   ____|
|  |  __  |  |  |  | |  |  |  | |  .--.  ||  |_)  |  \   \/   /  |  |__   
|  | |_ | |  |  |  | |  |  |  | |  |  |  ||   _  <    \_    _/   |   __|  
|  |__| | |  `--'  | |  `--'  | |  '--'  ||  |_)  |     |  |     |  |____ 
 \______|  \______/   \______/  |_______/ |______/      |__|     |_______|

⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣠⠴⠒⠊⠉⠉⠉⠒⠲⢤⣀⠀⠀⠀⠀⠀⣀⣤⠤⠶⠒⠶⠤⣄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⡴⠋⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⠦⡤⠖⠋⠁⠀⠀⠀⠀⠀⠀⠀⠙⢦⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⡴⠋⠀⠀⠀⠀⠀⢀⣀⣠⠤⢤⣀⣀⡀⠀⠀⠹⡄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⢣⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⢀⡞⠀⠀⠀⠀⣠⠴⠚⠉⠀⠀⠀⠀⠀⠀⠉⠙⠲⢤⣹⣀⣀⡤⠤⠤⠤⠤⠤⠤⢄⣀⣈⣇⡀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⢀⡞⠀⠀⠀⠀⠀⠁⠀⠀⠀⠀⠀⠀⠀⠀⣀⣀⣀⣀⣀⣙⣧⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠉⠓⢦⡀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⣀⡞⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⡤⠒⣊⡭⠥⠔⠒⠲⠦⠤⢭⣉⣳⣄⣤⣴⣒⣊⡭⠭⠭⠭⠭⠭⣿⣶⣻⣦⣀⠀
⠀⠀⠀⢀⡴⠚⢹⠃⠀⠀⠀⠀⠀⠀⢀⡤⠖⢚⣡⠖⠋⠁⠀⠀⠀⠀⠀⢀⣀⣀⣀⣙⣿⡛⠉⠁⠀⢀⣀⣀⣠⣤⣤⣤⠤⣭⣝⣿⣄
⠀⠀⢠⡞⠁⠀⣾⠀⠀⠀⠀⠀⠀⣾⣛⣛⠋⠉⢀⣀⣀⡠⠤⢶⣶⢿⣿⣿⣤⡀⠀⠀⠈⡷⠒⠚⠉⠉⢠⣿⡿⢿⣿⣿⣦⡀⠀⠉⢻
⠀⢀⡏⠀⠀⠀⠁⠀⠀⠀⠀⠀⠀⠀⠈⠉⠙⠯⣉⠀⠀⠀⢠⣿⣿⣶⣿⠛⢻⣿⡆⠀⣰⠁⠀⠀⠀⠀⣿⣿⠿⣿⣏⣹⣿⣧⢀⣠⡞
⢀⡞⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⠦⢬⣙⠒⠤⢼⠿⢿⡿⠿⠿⠿⠛⠛⢉⡼⠛⠓⠒⠒⠶⠟⠛⠛⠛⠛⠛⠋⢩⡿⠛⠀
⡼⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠉⠉⠒⠒⠒⠒⠒⠒⣲⡾⠉⠉⠀⢀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⡴⠋⠀⠀⠀
⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⡤⠶⠋⠁⠀⠀⠀⠀⠈⠛⠢⢤⣤⠤⠤⠴⠒⢿⡁⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠋⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠁⠀⠀⠀⠀⠀⠙⢦⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⣧⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣀⣤⣤⣤⣀⣀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣹⣄⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡴⠋⠁⡀⠀⣀⡀⠀⠉⠉⠙⠓⠒⠲⠦⠤⠤⣤⣀⣀⣀⣀⣀⣀⣀⣀⣀⣤⠤⠶⠚⠉⢉⣿⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⡅⠀⠀⠉⠉⠉⠉⠉⠓⠒⠶⠤⢤⣤⣀⣀⣀⣀⡀⠀⠀⠉⠉⠉⠉⠁⣀⣀⣀⣀⣠⣴⠟⠁⠀⠀
⡆⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⣆⠀⠙⠒⠒⠒⠒⠒⠲⠦⠤⠤⣀⣀⣀⠀⠀⠀⠉⠉⠉⠉⠉⠉⠉⠉⠉⠉⠉⠉⠀⠀⢀⣿⠀⠀⠀⠀
⠙⣆⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠓⠦⠄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠉⠉⠙⠛⠛⠒⠒⠒⠒⠶⠶⠶⠶⢶⡦⠶⠒⠋⠁⠀⠀⠀⠀
⠟⠿⢿⡶⢤⣀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣠⠔⠋⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠉⠓⠦⣭⣉⠓⠒⠶⠦⠤⢤⣄⣀⣀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣀⣀⡤⠖⠚⠉⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠈⠙⠓⠲⠦⢤⣤⣤⣀⣀⣀⣉⣉⣉⣉⣉⡉⢉⣉⣉⣉⣉⣩⣭⠟⠛⠷⣄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠉⠉⠉⠉⠉⠉⠉⠉⠉⠉⠁⠀⠀⠀⠀⠀⠈⠙⢦⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢿⣄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
"""



# MAIN
if __name__ == "__main__":
    print('\n')
    print(welcome_message)
    print('\n')
    
    valid_choices = ['handgun', 'agencies', 'spouses', 'describe', 'write_query', 'quit']

    while True:
        
        choice = input('''\n\nChoose a query to execute by typing 'handgun', 'agencies', 'spouses', 'describe', 'write_query' or type 'quit' to quit.\n
        'handgun' -> Investigating if there is a difference over time in how many Handgun murders are solved compared to other murders
        'agencies' -> Measuring the performance of different agency types in solving murders
        'spouses' -> Analysing if spouses are more likely to kill each other in the summer vs in the winter
        'describe' -> prints an explanation of all the tables
        'write_query' --> Write your own SQL query
        > ''')

        if choice not in valid_choices:
            print(f"Your choice '{choice}' is not valid. Please retry")
            continue

        if choice == "quit":
            break

        print(f"\nYou chose to execute query {choice}")
        if choice == 'handgun':
            results = handgun()
            df = pd.DataFrame(results, columns=['Year', 'Handgun Homicides', 'Other Homicides', 'Solved Handgun Homicides', 'Solved Other Homicides', 'Solved % Handgun', 'Solved % Other', 'Diff Handgun to Other %'])  # Specify your column names
            print(df)
            print('\n')

            valid_graph_choices = ['Y', 'y']
            graph_choice = input('Would you like to plot this result in a graph? Select Y/N: ')
            
            if graph_choice == 'Y' or graph_choice == 'y':
                years = [row[0] for row in results]
                solved_perc_handgun = [row[5] for row in results]
                solved_perc_other = [row[6] for row in results]

                plt.figure(figsize=(10, 6))
                plt.plot(years, solved_perc_handgun, label='Handgun Homicides Solved %')
                plt.plot(years, solved_perc_other, label='Other Homicides Solved %')
                plt.xlabel('Year')
                plt.ylabel('Percentage Solved')
                plt.title('Difference in solved Handgun homicides')
                plt.legend()
                plt.grid(True)
                plt.show()
            else:
                pass

        elif choice == 'agencies':
            results = agencies()
            df = pd.DataFrame(results, columns = ['Agency Type', 'Agencies', 'Murders', 'Solved Murders', 'Murders per Agency', 'Agency Type Solve Rate', 'Overall Solve Rate', 'Solve Rate Difference'])
            print(df)

        elif choice == 'spouses':
            results = spouses()
            df = pd.DataFrame(results, columns=['State', 'Summer+Winter Total Murders', 'Winter Murders', 'Summer Murders', 'Winter %', 'Summer %', 'Summer % Increase'])
            print(df)

        elif choice =='describe':
            describe()

        elif choice == 'write_query':
            results = write_query()
            df = pd.DataFrame(results)
            print(df)

        else:
            raise Exception("We should never get here!")


    print("\n")
    print(goodbye)
    print('\n')




