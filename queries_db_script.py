import mysql.connector
from decimal import Decimal

# Connection details
MYSQL_HOST = 'localhost'
MYSQL_PORT = 3305
MYSQL_USER = 'lazarovici'
MYSQL_PASSWORD = 'lazaro36907'
MYSQL_DATABASE = 'lazarovici'

def sql_connect():
    con = mysql.connector.connect(
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE
    )
    return con

def query_1(director_name):
# Connect to the MySQL database
    try:
        con = sql_connect()
        cursor = con.cursor()
        cursor.execute(f"USE {MYSQL_DATABASE};")
        cursor.execute("SELECT * " 
                       "FROM directors "
                       "WHERE MATCH(name) AGAINST(%s)", (director_name,))
        results = cursor.fetchall()
        for r in results:
            print(r)
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if 'con' in locals() and con.is_connected():
            con.close()
            
def query_2(txt):
# Connect to the MySQL database
    try:
        con = sql_connect()
        cursor = con.cursor()
        cursor.execute(f"USE {MYSQL_DATABASE};")
        cursor.execute("SELECT * "
                       "FROM movies "
                       "WHERE MATCH(overview) AGAINST(%s)", (txt,))
        results = cursor.fetchall()
        for r in results:
            print(r)
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if 'con' in locals() and con.is_connected():
            con.close()
            
def query_3(actor_name, rating):
# Connect to the MySQL database
    try:
        con = sql_connect()
        cursor = con.cursor()
        cursor.execute(f"USE {MYSQL_DATABASE};")
        cursor.execute("SELECT m.id, m.title, m.rating "
                        "FROM movies m, actors a, movie_actors ma "
                        "WHERE MATCH(a.name) AGAINST( %s) "
                        "AND m.rating > %s "
                        "AND m.id = ma.movie_id "
                        "AND a.id = ma.actor_id", (actor_name,rating,))
        results = cursor.fetchall()
        for r in results:
            print(r)
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if 'con' in locals() and con.is_connected():
            con.close()
def query_4():
    # Connect to the MySQL database
    try:
        con = sql_connect()
        cursor = con.cursor()
        cursor.execute(f"USE {MYSQL_DATABASE};")
        cursor.execute(""" SELECT 
                                a.name AS actor_name,
                                d.name AS director_name,
                                m.title AS movie_title,
                                ma.character_name AS character_name
                            FROM 
                                movie_actors ma,
                                actors a,
                                movie_directors md,
                                directors d,
                                movies m
                            WHERE 
                                ma.actor_id = a.id
                                AND ma.movie_id = md.movie_id
                                AND md.director_id = d.id
                                AND ma.movie_id = m.id
                                AND EXISTS (
                                    SELECT 1
                                    FROM movie_actors sub_ma, movie_directors sub_md
                                    WHERE sub_ma.movie_id = sub_md.movie_id
                                    AND sub_ma.actor_id = ma.actor_id
                                    AND sub_md.director_id = md.director_id
                                    GROUP BY sub_ma.actor_id, sub_md.director_id
                                    HAVING COUNT(sub_ma.movie_id) > 1
                                )
                                ORDER BY 
                                    a.name, d.name, m.title;
                                """)
        results = cursor.fetchall()
        for r in results:
            print(r)
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if 'con' in locals() and con.is_connected():
            con.close()
            
def query_5(gender_num, revenue_num):
    try:
        con = sql_connect()
        cursor = con.cursor()
        cursor.execute(f"USE {MYSQL_DATABASE};")
        cursor.execute("SELECT m.id, m.title, m.revenue, d.name, d.gender "
                                "FROM movies m, directors d "
                                "WHERE m.revenue > %s "
                                "AND EXISTS ( "
                                    "SELECT * "
                                    "FROM movie_directors md "
                                    "WHERE md.director_id = d.id "
                                    "AND md.movie_id = m.id "
                                    "AND d.gender = %s)", (revenue_num, gender_num))
        results = cursor.fetchall()
        for r in results:
            print(r)
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if 'con' in locals() and con.is_connected():
            con.close()
            
def query_6():
# Connect to the MySQL database
    try:
        con = sql_connect()
        cursor = con.cursor()
        cursor.execute(f"USE {MYSQL_DATABASE};")
        cursor.execute("SELECT a.place_of_birth, count(*) as actors_in_place "
                                "FROM actors a "
                                "WHERE place_of_birth != 'NA' "
                                "GROUP BY a.place_of_birth "
                                "ORDER BY actors_in_place DESC "
                                "LIMIT 20")
                                
        results = cursor.fetchall()
        for r in results:
            print(r)
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if 'con' in locals() and con.is_connected():
            con.close()

def query_7(actor_name):
# Connect to the MySQL database
    try:
        con = sql_connect()
        cursor = con.cursor()
        cursor.execute(f"USE {MYSQL_DATABASE};")
        cursor.execute("SELECT a.name, m.title, (m.revenue - m.budget) AS profit "
                                "FROM movies m, actors a, movie_actors ma "
                                "WHERE MATCH(a.name) AGAINST( %s)  "
                                "AND m.revenue - m.budget > 0 "
                                "AND ma.movie_id = m.id "
                                "AND a.id = ma.actor_id "
                                "ORDER BY profit DESC", (actor_name,))
        results = cursor.fetchall()
        for r in results:
            print(r)
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if 'con' in locals() and con.is_connected():
            con.close()
            
def query_8():
    # Connect to the MySQL database
    try:
        con = sql_connect()
        cursor = con.cursor()
        cursor.execute(f"USE {MYSQL_DATABASE};")
        cursor.execute(""" SELECT 
                                a.place_of_birth AS place_of_birth,
                                COUNT(DISTINCT m.id) AS movie_count,
                                SUM(m.revenue - m.budget) AS total_profit,
                                AVG(m.rating) AS average_rating,
                                MAX(a.popularity) AS most_popular_actor_popularity,
                                MAX(d.popularity) AS most_popular_director_popularity
                            FROM 
                                movies m, 
                                movie_actors ma, 
                                actors a, 
                                movie_directors md, 
                                directors d
                            WHERE 
                                m.id = ma.movie_id
                                AND ma.actor_id = a.id
                                AND m.id = md.movie_id
                                AND md.director_id = d.id
                                AND a.place_of_birth = d.place_of_birth
                                AND (m.revenue - m.budget) > 0 -- Positive profit
                                AND m.rating > 7.0 -- Filter for high-rated movies
                                AND EXISTS (
                                    SELECT 1
                                    FROM movie_actors sub_ma
                                        WHERE sub_ma.actor_id = a.id
                                        AND sub_ma.movie_id != m.id
                                    )
                            GROUP BY 
                                 a.place_of_birth
                            ORDER BY 
                                total_profit DESC, average_rating DESC;
                                """)
        results = cursor.fetchall()
                # Process results to convert Decimal to float
        processed_results = []
        for row in results:
            processed_row = tuple(float(value) if isinstance(value, Decimal) else value for value in row)
            processed_results.append(processed_row)

        # Print the processed results
        for row in processed_results:
            print(row)
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if 'con' in locals() and con.is_connected():
            con.close()