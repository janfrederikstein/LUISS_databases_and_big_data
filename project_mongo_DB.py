from pymongo import MongoClient
import pandas as pd

df = pd.read_csv('database 3.csv')

client = MongoClient('mongodb://localhost:27017/')
db = client['homicide_reports']

# Creating or connecting to the collection within our database
collection = db['homicide_data']

if collection.count_documents({}) == 0:
    print("\n")
    print("Creating and populating the database...")
    print("\n")
    print("Here's a joke while you wait:")
    print("3 relational databases walk into a NoSQL bar but left after 5 minutes..")
    print("They couldn't find a table")
    print("\n")
    print("Okay I'll shut up while you wait")
    print("\n")

    # Converting the DataFrame to a dictionary
    data_dict = df.to_dict("records")

    # Inserting the data into the collection
    collection.insert_many(data_dict)

    # Cleaning the collection according to the missing value issues we're aware off from the SQL database
    collection.update_many({'Victim Sex': 'Unknown'}, {'$set': {'Victim Sex': None}})
    collection.update_many({'Victim Age': {'$in': [99, 998]}}, {'$set': {'Victim Age': None}})
    collection.update_many({'Victim Race': 'Unknown'}, {'$set': {'Victim Race': None}})
    collection.update_many({'Victim Ethnicity': 'Unknown'}, {'$set': {'Victim Ethnicity': None}})
    collection.update_many({'Relationship': 'Unknown'}, {'$set': {'Relationship': None}})
    collection.update_many({'Perpetrator Sex': 'Unknown'}, {'$set': {'Perpetrator Sex': None}})
    collection.update_many({'Perpetrator Age': 0}, {'$set': {'Perpetrator Age': None}})
    collection.update_many({'Perpetrator Race': 'Unknown'}, {'$set': {'Perpetrator Race': None}})
    collection.update_many({'Perpetrator Ethnicity': 'Unknown'}, {'$set': {'Perpetrator Ethnicity': None}})
    collection.update_many({'Weapon': 'Unknown'}, {'$set': {'Weapon': None}})
    collection.update_many({'Agency Name': {'$regex': '.*Unknown.*'}}, {'$set': {'Agency Name': None}})

    print("Database created and populated successfully!")

else:
    print("Database already exists with data, proceeding to queries...")


# Just a cosmetic function to format our output a bit nicer :)
def print_header(title):
    print("\n" + title)
    print("-" * len(title))

welcome = """
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
                                                                                             
.__   __.   ______               _______. __  ___  _______ ____    __    ____  __            
|  \ |  |  /  __  \             /       ||  |/  / |   ____|\   \  /  \  /   / |  |           
|   \|  | |  |  |  |           |   (----`|  '  /  |  |__    \   \/    \/   /  |  |           
|  . `  | |  |  |  |            \   \    |    <   |   __|    \            /   |  |           
|  |\   | |  `--'  |        .----)   |   |  .  \  |  |____    \    /\    /    |  `----.      
|__| \__|  \______/   ______|_______/    |__|\__\ |_______|    \__/  \__/     |_______|      
                     |______|                                                                
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

def execute_and_print_query(query_pipeline):
    results = collection.aggregate(query_pipeline)
    for result in results:
        print(result)

# Query 1: Total homicides per State
murders_per_state_pipeline = [
    {
        '$group': {
            '_id': '$State',  # Grouping by state
            'total_homicides': {'$sum': 1}  # Counting the number of homicides per group
        }
    },
    {
        '$sort': {'total_homicides': -1}  # Sorting by count descending
    }
]
# Executinging the aggregation query
murder_per_state_results = collection.aggregate(murders_per_state_pipeline)


# Query 2: The 5 State/Year combinations with the lowest average Victim Age
lowest_avg_victim_age_pipeline = [
    {
        '$group': {
            '_id': {'State': '$State', 'Year': '$Year'},
            'Total Homicides': {'$sum': 1},
            'Average Victim Age': {'$avg': '$Victim Age'}
        }
    },
    {
        '$project': {
            'Total Homicides': 1,
            'Average Victim Age Rounded': {'$round': ['$Average Victim Age', 2]}
        }
    },
    {'$sort': {'Average Victim Age Rounded': 1}},
    {'$limit': 5}
]

lowest_avg_victim_age_results = collection.aggregate(lowest_avg_victim_age_pipeline)


# Query 3a: The Average Age difference between Victim and Perpetrator in every State
age_correlation_pipeline = [
    {'$match': {'Perpetrator Age': {'$gt': 0}, 'Victim Age': {'$gt': 0}}},
    {'$project': {
        'State': 1,
        'Age Difference': {'$abs': {'$subtract': ['$Victim Age', '$Perpetrator Age']}}
    }},
    {'$group': {
        '_id': '$State',
        'Average Age Difference': {'$avg': '$Age Difference'}
    }},
    {'$project': {
        'Average Age Difference': {'$round': ['$Average Age Difference', 2]}
    }},
    {'$sort': {'Average Age Difference': -1}}
]

age_correlation_results = collection.aggregate(age_correlation_pipeline)


# Query 3b: The 5 States with the highest age difference between Victim and Perpetrator
highest_age_diff_pipeline = [
    {'$match': {'Perpetrator Age': {'$gt': 0}, 'Victim Age': {'$gt': 0}}},
    {'$project': {
        'State': 1,
        'Age Difference': {'$abs': {'$subtract': ['$Victim Age', '$Perpetrator Age']}}
    }},
    {'$group': {
        '_id': '$State',
        'Average Age Difference': {'$avg': '$Age Difference'}
    }},
    {'$sort': {'Average Age Difference': -1}},
    {'$limit': 5}
]

highest_age_diff_results = collection.aggregate(highest_age_diff_pipeline)



# Query 3c: The 5 States with the lowest age difference between Victim and Perpetrator 
lowest_age_diff_pipeline = [
    {'$match': {'Perpetrator Age': {'$gt': 0}, 'Victim Age': {'$gt': 0}}},
    {'$project': {
        'State': 1,
        'Age Difference': {'$abs': {'$subtract': ['$Victim Age', '$Perpetrator Age']}}
    }},
    {'$group': {
        '_id': '$State',
        'Average Age Difference': {'$avg': '$Age Difference'}
    }},
    {'$sort': {'Average Age Difference': 1}},
    {'$limit': 5}
]

lowest_age_diff_results = collection.aggregate(lowest_age_diff_pipeline)



# MAIN
if __name__ == "__main__":
    print('\n')
    print(welcome)
    print('\n')
    
    valid_choices = ['state', 'victim_age', 'age_diff', 'age_diff_high', 'age_diff_low', 'quit']

    while True:
        
        choice = input('''\n\nChoose a query to execute by typing 'state', 'victim_age', 'age_diff', 'age_diff_high', 'age_diff_low' or type 'quit' to quit.\n
        'state' -> Total homicides per State
        'victim_age' -> The 5 State/Year combinations with the lowest average Victim Age
        'age_diff' -> The Average Age difference between Victim and Perpetrator in every State
        'age_diff_high' -> The 5 States with the highest age difference between Victim and Perpetrator
        'age_diff_low' --> The 5 States with the lowest age difference between Victim and Perpetrator 
        > ''')

        if choice not in valid_choices:
            print(f"Your choice '{choice}' is not valid. Please retry")
            continue

        if choice == "quit":
            break

        print("\n")
        print(f"\nYou chose to execute query '{choice}'")
        if choice == 'state':
            print_header('The total amount of Homicides per State')
            for result in murder_per_state_results:
                print(f"State: {result['_id']}, Total Homicides: {result['total_homicides']}")
            print("\n")
            print("\n")

        elif choice == 'victim_age':
            print_header('The 5 State/Year combinations with the lowest average Victim Age')
            for result in lowest_avg_victim_age_results:
                print(result)
            print("\n")
            print("\n")

        elif choice == 'age_diff':
            print_header('The Average Age difference between Victim and Perpetrator in every State')
            execute_and_print_query(age_correlation_pipeline)
            print("\n")
            print("\n")

        elif choice =='age_diff_high':
            print_header('The 5 States with the highest age difference between Victim and Perpetrator')
            execute_and_print_query(highest_age_diff_pipeline)
            print("\n")
            print("\n")

        elif choice == 'age_diff_low':
            print_header('The 5 States with the lowest age difference between Victim and Perpetrator')
            execute_and_print_query(lowest_age_diff_pipeline)
            print("\n")
            print("\n")

        else:
            raise Exception("We should never get here!")

    print("\n")
    print(goodbye)
    print('\n')
