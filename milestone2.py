"""
CSC/SER 325: Database Systems - Final Project Fall 2025
Milestone 2: Database Schema Implementation and Data Loading

Author: Samantha Woodburn , Tuana Turhan
Date: November 10, 2025
"""

import pymysql
import json
import csv
import sys

# function to get connection
def getconn():
    """Establishes connection to AWS RDS MySQL database"""
    return pymysql.connect(
        host="lifestyle-db.cmf0qks8a3pr.us-east-1.rds.amazonaws.com",
        port=3306,
        user="root",
        password="Casey9203",
        database=None,
        ssl={"ca": "global-bundle.pem"}
    )

# function to set up our database schema
def setup_database_schema(cur):
    """Creates database and all required tables"""
    print("=" * 60)
    print("TASK 1: Creating Database Schema")
    print("=" * 60)
    
    # create database and tables
    cur.execute('CREATE DATABASE IF NOT EXISTS lifestyle_db')
    cur.execute('USE lifestyle_db')
    print("Database 'lifestyle_db' created/selected")
    cur.execute('SET FOREIGN_KEY_CHECKS=0')
    cur.execute('DROP TABLE IF EXISTS Workout, PersonWorkout, Diet, Health, Person')
    cur.execute('SET FOREIGN_KEY_CHECKS=1')
    
    cur.execute('''
        CREATE TABLE Person (
            personID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            age INT NOT NULL,
            gender VARCHAR(10),
            weight INT,
            height FLOAT
        )
    ''')
    cur.execute('''
        CREATE TABLE Workout (
            workoutID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            session_duration FLOAT,
            calories_burned FLOAT,
            workout_type VARCHAR(30),
            name_of_exercise VARCHAR(50),
            sets FLOAT,
            reps FLOAT,
            body_part VARCHAR(30)
        )
    ''')
    cur.execute('''
        CREATE TABLE PersonWorkout (
            personID INT NOT NULL,
            workoutID INT NOT NULL,
            gender VARCHAR(10),
            PRIMARY KEY (personID, workoutID),
            FOREIGN KEY(personID) REFERENCES Person(personID),
            FOREIGN KEY(workoutID) REFERENCES Workout(workoutID)
        )
    ''')
    cur.execute('''
        CREATE TABLE Diet (
            dietID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            personID INT NOT NULL,
            water_intake FLOAT,
            carbs FLOAT,
            proteins FLOAT,
            fats FLOAT,
            calories INT,
            diet_type VARCHAR(20),
            serving_size FLOAT,
            FOREIGN KEY(personID) REFERENCES Person(personID) 
        )
    ''')
    cur.execute('''
        CREATE TABLE Health (
            healthID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            personID INT NOT NULL,
            max_BPM FLOAT,
            resting_BPM FLOAT,
            fat_percentage FLOAT,
            cholesterol FLOAT,
            FOREIGN KEY(personID) REFERENCES Person(personID)
        )
    ''')
    
    print("\nSchema created successfully!\n")

# parse through our data file
def parse_data_file(file_path):
    """Parses input data file and returns structured data"""
    print("=" * 60)
    print("TASK 2: Parsing Data File")
    print("=" * 60)
    print(f"Reading file: {file_path}")
    
    # all the data we need
    data = []
    COLUMN_MAP = {
        'Age': 'age',
        'Gender': 'gender',
        'Weight (kg)': 'weight',
        'Height (m)': 'height',

        'Session Duration': 'session_duration',
        'Calories Burned': 'calories_burned',
        'Workout Type': 'workout_type',
        'Exercise Name': 'name_of_exercise',
        'Sets': 'sets',
        'Reps': 'reps',
        'Body Part': 'body_part',

        'Calories': 'calories',
        'Protein': 'protein',
        'Carbs': 'carbs',
        'Fats': 'fats',
        'Water_Intake (liters)': 'water_intake',
        'diet_type': 'diet_type',
        'serving_size_g': 'serving_size',

        'Max_BPM': 'max_BPM',
        'Resting_BPM': 'resting_BPM',
        'Fat_Percentage': 'fat_percentage',
        'cholesterol_mg': 'cholesterol'
    }

    # go through and clean up csv
    if file_path.endswith('.csv'):
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cleaned_row = {}
                for key, value in row.items():
                    if value == '' or value is None:
                        cleaned_row[key] = None
                    else:
                        try:
                            if '.' in str(value):
                                cleaned_row[key] = float(value)
                            else:
                                cleaned_row[key] = int(value)
                        except (ValueError, TypeError):
                            cleaned_row[key] = value

                normalized = {}
                for old_key, new_key in COLUMN_MAP.items():
                    if old_key in cleaned_row:
                        normalized[new_key] = cleaned_row[old_key]
                data.append(normalized)
        print("CSV file parsed successfully")
    # parse through json file
    elif file_path.endswith('.json'):
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print("JSON file parsed successfully")
    # if correct file formats couldn't be found
    else:
        raise ValueError("Unsupported file format. Use .csv or .json")
    
    print(f"Total records parsed: {len(data)}")
    print(f"Sample record keys: {list(data[0].keys()) if data else 'No data'}\n")
    return data

# load the data to our database
def bulk_load_data(cur, data, batch_size=500):
    """Enhanced bulk load: always creates Workout and Health rows per Person"""
    print("=" * 60)
    print("TASK 3 (enhanced): Loading Data into Database")
    print("=" * 60)

    # error handling- no data to load
    if not data:
        print("No data to load.")
        return

    cur.execute("USE lifestyle_db;")

    inserted_person = inserted_workout = inserted_diet = inserted_health = 0

    # Simple, safe per-record inserts (fast enough for class-sized datasets)
    for rec in data:
        # Person
        cur.execute("""
            INSERT INTO Person (age, gender, weight, height)
            VALUES (%s, %s, %s, %s)
        """, (
            rec.get("age"),
            rec.get("gender"),
            rec.get("weight"),
            rec.get("height"),
        ))
        person_id = cur.lastrowid
        inserted_person += 1

        # Workout
        cur.execute("""
            INSERT INTO Workout (session_duration, calories_burned, workout_type,
                                 name_of_exercise, sets, reps, body_part)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            rec.get("session_duration", 0.0),
            rec.get("calories_burned", 0.0),
            rec.get("workout_type"),
            rec.get("name_of_exercise"),
            rec.get("sets"),
            rec.get("reps"),
            rec.get("body_part"),
        ))
        workout_id = cur.lastrowid
        inserted_workout += 1

        # Link (junction)
        cur.execute("""
            INSERT INTO PersonWorkout (personID, workoutID)
            VALUES (%s, %s)
        """, (person_id, workout_id))

        # Diet (optional defaults if missing)
        cur.execute("""
            INSERT INTO Diet (personID, water_intake, carbs, proteins, fats, calories,
                              diet_type, serving_size)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            person_id,
            rec.get("water_intake"),
            rec.get("carbs"),
            rec.get("proteins"),
            rec.get("fats"),
            rec.get("calories"),
            rec.get("diet_type"),
            rec.get("serving_size"),
        ))
        inserted_diet += 1

        # Health
        cur.execute("""
            INSERT INTO Health (personID, max_BPM, resting_BPM, fat_percentage, cholesterol)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            person_id,
            rec.get("max_BPM"),
            rec.get("resting_BPM"),
            rec.get("fat_percentage"),
            rec.get("cholesterol"),
        ))
        inserted_health += 1
    # person table
    # person = []
    # for record in data:
    #     person.append((
    #         record.get('age'),
    #         record.get('gender'),
    #         record.get('weight'),
    #         record.get('height')
    #     ))
    # total = len(person)
    # inserted_person_ids = []
    # cur.execute('SET FOREIGN_KEY_CHECKS=0')
    # # insert into person table
    # insert_person_sql = 'INSERT INTO Person (age, gender, weight, height) VALUES (%s, %s, %s, %s)'
    # i = 0
    # while i < total:
    #     batch = person[i:i+batch_size]
    #     cur.executemany(insert_person_sql, batch)
    #     last_id = cur.lastrowid
    #     first_id = last_id - len(batch) + 1
    #     batch_ids = list(range(first_id, last_id + 1))
    #     inserted_person_ids.extend(batch_ids)
    #     i += batch_size
    #     if len(inserted_person_ids) % 5000 == 0 or i >= total:
    #         print(f"  Inserted {len(inserted_person_ids)}/{total} Person rows...")

    # # Prepare default workout, diet, health rows
    # workout_rows = []
    # diet_rows = []
    # health_rows = []

    # for idx, record in enumerate(data):
    #     pid = inserted_person_ids[idx]

    #     # data for the workout table
    #     workout_rows.append((
    #         pid,
    #         record.get('session_duration', 30.0),
    #         record.get('calories_burned', 200.0),
    #         record.get('workout_type', 'General'),
    #         record.get('name_of_exercise', 'Cardio'),
    #         record.get('sets', 3),
    #         record.get('reps', 10),
    #         record.get('body_part', 'Full Body')
    #     ))

    #     # data for the diet table
    #     diet_rows.append((
    #         pid,
    #         record.get('meal_type', 'Meal'),
    #         record.get('calories', 2000),
    #         record.get('protein', 50),
    #         record.get('carbs', 250),
    #         record.get('fats', 70)
    #     ))

    #     # data for the health table
    #     health_rows.append((
    #         pid,
    #         record.get('heart_rate', 75),
    #         record.get('sleep_hours', 7.0),
    #         record.get('stress_level', 'Normal')
    #     ))

    # # Batch inserts
    # cur.executemany('''
    #     INSERT INTO Workout (personID, session_duration, calories_burned, workout_type,
    #                          name_of_exercise, sets, reps, body_part)
    #     VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    # ''', workout_rows)

    # cur.executemany('''
    #     INSERT INTO Diet (personID, meal_type, calories, protein, carbs, fats)
    #     VALUES (%s,%s,%s,%s,%s,%s)
    # ''', diet_rows)

    # cur.executemany('''
    #     INSERT INTO Health (personID, heart_rate, sleep_hours, stress_level)
    #     VALUES (%s,%s,%s,%s)
    # ''', health_rows)

    # cur.execute('SET FOREIGN_KEY_CHECKS=1')

    print("\nData loading completed!")
    print(f"Person: {inserted_person}, Workout: {inserted_workout}, Diet: {inserted_diet}, Health: {inserted_health}\n")

# verfiy data has been loaded
def verify_data_loaded(cur):
    """Verifies that data was loaded correctly"""
    print("=" * 60)
    print("VERIFICATION: Checking Loaded Data")
    print("=" * 60)
    for table in ['Person', 'Workout', 'PersonWorkout' 'Diet', 'Health']:
        cur.execute(f'SELECT COUNT(*) FROM {table}')
        print(f" {table:12} table: {cur.fetchone()[0]} rows")
    print()


def main():
    DATA_FILE = 'lifestyle_data.csv'  
    
    print("\n" + "=" * 60)
    print("CSC 325 - Milestone 2: Database Implementation")
    print("=" * 60 + "\n")
    
    cnx, cur = None, None
    try:
        print("Connecting to database...")
        cnx = getconn()
        cur = cnx.cursor()
        print("Connected successfully\n")
        
        setup_database_schema(cur)
        cnx.commit()
        
        data = parse_data_file(DATA_FILE)
        bulk_load_data(cur, data)
        cnx.commit()
        
        verify_data_loaded(cur)
        print("=" * 60)
        print("ALL TASKS COMPLETED SUCCESSFULLY!")
        print("=" * 60 + "\n")
    # error handeling
    except FileNotFoundError:
        print(f"\nERROR: File '{DATA_FILE}' not found!\n")
        sys.exit(1)
    except Exception as e:
        print(f"\nCRITICAL ERROR: {e}\nRolling back changes...\n")
        if cnx:
            cnx.rollback()
        sys.exit(1)
    finally:
        if cur: cur.close()
        if cnx: cnx.close()
        print("Database connection closed.\n")

if __name__ == "__main__":
    main()
