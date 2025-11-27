
# function to get connection
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import pymysql


def getconn():
    """Establishes connection to AWS RDS MySQL database"""
    return pymysql.connect(
        #host="lifestyle-db.cmf0qks8a3pr.us-east-1.rds.amazonaws.com",
        host="database-1.c9ikwq088lg0.us-east-2.rds.amazonaws.com",
        port=3306,
        user="root",
        password="Casey9203",
        database=None,
        ssl={"ca": "global-bundle.pem"}
    )

''' SCATTER: Height Weight graph'''
# make dataframe
def get_heigh_weight_df():
    conn = getconn()
    try:
        # connect to database
        conn.select_db("lifestyle_db")
        # then do query
        query = """
            SELECT height * 100 AS height_cm, weight
            FROM Person
            WHERE height IS NOT NULL AND weight IS NOT NULL AND age BETWEEN 20 AND 30
        """
        df = pd.read_sql(query, conn)
        return df
    finally:
        # close connection
        conn.close()
# make graph
def plot_height_vs_weight(ax):
    # dataframe
    df = get_heigh_weight_df()
    # sample so it looks cleaner
    if len(df) >400:
        df =df.sample(n=400, random_state=0)
    # graph
    ax.scatter(df["height_cm"],df["weight"], s=15, alpha=0.5, edgecolors="none")
    # labels
    ax.set_title("Height vs. Weight")
    ax.set_xlabel("Height (cm)")
    ax.set_ylabel("Weight (kg)")
    ax.grid(True, alpha=0.3)
    # add a trend line
    z = np.polyfit(df["height_cm"], df["weight"], 1)
    p = np.poly1d(z)
    ax.plot(df["height_cm"], p(df["height_cm"]), color='red', alpha=0.6)

''' LINE: resting bpm vs calories burned in cardio workouts'''
# make dataframe
def get_bpm_calories_df():
    conn = getconn()
    try:
        # connect to database
        conn.select_db("lifestyle_db")
        # then do query
        query = """
            SELECT FLOOR(h.resting_BPM/10)*10 as bpm_group, AVG(w.calories_burned) AS avg_calories
            FROM Health h
            JOIN Person p ON p.personID = h.personID
            JOIN PersonWorkout pw ON pw.personID = h.personID
            JOIN Workout w ON pw.workoutID = w.workoutID
            WHERE h.resting_BPM IS NOT NULL AND w.calories_burned IS NOT NULL AND p.age BETWEEN 20 and 30
            GROUP BY bpm_group
            ORDER BY bpm_group;
        """
        df = pd.read_sql(query, conn)
        return df
    finally:
        # close connection
        conn.close()
# make graph
def plot_bpm_vs_cals(ax):
    # dataframe
    df = get_bpm_calories_df()
    # graph
    ax.plot(df["bpm_group"], df["avg_calories"], marker="o", linestyle="-", linewidth=2, markersize=6, color="#4C72B0")
    # labels
    ax.set_title("Resting BPM vs. Average Calories Burned in a Cardio Workout")
    ax.set_xlabel("Resting BPM")
    ax.set_ylabel("Caloroes Burned")
    # add a grid in the background for better visualization
    ax.grid(True, alpha=0.3)

''' BAR: diet type vs fat percentage'''
# make dataframe
def get_diet_fat_df():
    conn = getconn()
    try:
        # connect to database
        conn.select_db("lifestyle_db")
        # then do query
        query = """
            SELECT d.diet_type, AVG(h.fat_percentage) as avg_fat_percentage
            FROM Diet d
            JOIN Health h ON h.personID = d.personID
            JOIN Person p ON p.personID = h.personID
            WHERE d.diet_type IS NOT NULL AND h.fat_percentage IS NOT NULL AND p.age BETWEEN 20 and 30
            GROUP BY d.diet_type
            ORDER BY d.diet_type
        """
        df = pd.read_sql(query, conn)
        return df
    finally:
        # close connection
        conn.close()
# make graph
def plot_diet_vs_fat(ax):
    # dataframe
    df = get_diet_fat_df()
    # graph
    ax.bar(df["diet_type"], df["avg_fat_percentage"])
    ax.set_ylim(25.7,26.4)
    # labels
    ax.set_title("Diet Type vs. Average Fat Percentage")
    ax.set_xlabel("Resting BPM")
    ax.set_ylabel("Calories Burned")
    # add a grid in the background for better visualization
    ax.grid(True, alpha=0.3)

''' BAR: water intake vs resting BPM'''
# make dataframe
def get_water_rbpm_df():
    conn = getconn()
    try:
        # connect to database
        conn.select_db("lifestyle_db")
        # then do query, group water intake so data is more readable
        query = """
            SELECT ROUND(d.water_intake * 2) / 2 AS water, AVG(h.resting_BPM) AS avg_resting_bpm
            FROM Diet d
            JOIN Health h ON h.personID = d.personID
            JOIN Person p ON p.personID = h.personID
            WHERE d.water_intake IS NOT NULL AND h.resting_BPM IS NOT NULL AND p.age BETWEEN 20 and 30
            GROUP BY ROUND(d.water_intake * 2) / 2
            ORDER BY water;
        """
        df = pd.read_sql(query, conn)
        return df
    finally:
        # close connection
        conn.close()
# make graph
def plot_water_vs_rbpm(ax):
    # dataframe
    df = get_water_rbpm_df()
    # graph
    ax.bar(df["water"].astype(str), df["avg_resting_bpm"])
    ax.set_ylim(61,64)
    # labels
    ax.set_title("Water Intake vs Resting BPM")
    ax.set_xlabel("Daily Water Intake (liters)")
    ax.set_ylabel("Average Resting BPM")
    # add a grid in the background for better visualization
    ax.grid(True, alpha=0.3)

''' BAR: calories burned vs bpm difference'''
# make dataframe
def get_calories_dbpm_df():
    conn = getconn()
    try:
        # connect to database
        conn.select_db("lifestyle_db")
        # then do query, group water intake so data is more readable
        query = """
            SELECT FLOOR(w.calories_burned/200)*200 AS calories, AVG(h.max_BPM-h.resting_BPM) AS dif_bpm
            FROM Workout w
            JOIN PersonWorkout pw ON pw.workoutID=w.workoutID
            JOIN Health h ON h.personID = pw.personID
            JOIN Person p ON p.personID = h.personID
            WHERE w.calories_burned IS NOT NULL AND h.resting_BPM IS NOT NULL AND h.max_BPM IS NOT NULL AND p.age BETWEEN 20 and 30
            GROUP BY calories
            ORDER BY calories
        """
        df = pd.read_sql(query, conn)
        return df
    finally:
        # close connection
        conn.close()
# make graph
def plot_calories_vs_dbpmax(ax):
    # dataframe
    df = get_calories_dbpm_df()
    # graph
    ax.plot(df["calories"], df["dif_bpm"], marker="o", linestyle="-", linewidth=2, markersize=6, color="#4C72B0")
    # labels
    ax.set_title("Calories vs BPM Difference")
    ax.set_xlabel("Calories")
    ax.set_ylabel("BPM Difference")
    # add a grid in the background for better visualization
    ax.grid(True, alpha=0.3)

""" Make the dashboard """
def plot_dashboard():
    fig, axes =  plt.subplots(2,3,figsize=(14,8))
    # fill cells
    plot_height_vs_weight(axes[0,0])
    plot_bpm_vs_cals(axes[0,1])
    plot_diet_vs_fat(axes[0,2])
    plot_water_vs_rbpm(axes[1,0])
    plot_calories_vs_dbpmax(axes[1,2])
    axes[1,1].axis("off")
    # title
    fig.suptitle("Lifestyle Data For Ages 20-30: Dashboard", fontsize=20, y=0.98)
    # fit data
    plt.tight_layout()
    # show
    plt.show()

def main():
   plot_dashboard()
   
    # for testing #
    # conn = getconn()
    # conn.select_db("lifestyle_db")
    # cur = conn.cursor()
    # cur.execute("SELECT DISTINCT ROUND(d.water_intake * 2) / 2 FROM Diet d JOIN Person p ON p.personID = d.personID WHERE p.age BETWEEN 20 AND 25 ORDER BY d.water_intake;")
    # rows = cur.fetchall()  # Get ALL rows
    # for row in rows:
    #     print(row[0])
    # conn.close()
    

if __name__ == "__main__":
    main()
