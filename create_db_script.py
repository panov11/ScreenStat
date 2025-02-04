import mysql.connector

# Connection details
MYSQL_HOST = 'localhost'
MYSQL_PORT = 3305
MYSQL_USER = 'lazarovici'
MYSQL_PASSWORD = 'lazaro36907'
MYSQL_DATABASE = 'lazarovici'

tables = {
    "movies": """
        CREATE TABLE IF NOT EXISTS movies (
            id INT PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            release_date DATE,
            overview TEXT,
            budget BIGINT,
            revenue BIGINT,
            rating FLOAT,
            duration INT,
            genres TEXT,
            production_companies TEXT,
            production_countries TEXT,
            FULLTEXT INDEX idx_title (title),
            FULLTEXT INDEX idx_overview (overview),
            INDEX idx_rating (rating),
            INDEX idx_revenue (revenue),
            INDEX idx_budget (budget)
        );
    """,
    "actors": """
        CREATE TABLE IF NOT EXISTS actors (
            id INT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age INT DEFAULT -1,
            place_of_birth VARCHAR(255),
            gender INT,
            popularity FLOAT,
            FULLTEXT INDEX idx_name (name),
            INDEX idx_place_of_birth (place_of_birth)
        );
    """,
    "directors": """
        CREATE TABLE IF NOT EXISTS directors (
            id INT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age INT DEFAULT -1,
            place_of_birth VARCHAR(255),
            gender INT,
            popularity FLOAT,
            FULLTEXT INDEX idx_name (name)
        );
    """,
    "movie_actors": """
        CREATE TABLE IF NOT EXISTS movie_actors (
            movie_id INT,
            actor_id INT,
            character_name VARCHAR(255),
            PRIMARY KEY (movie_id, actor_id),
            FOREIGN KEY (movie_id) REFERENCES movies(id),
            FOREIGN KEY (actor_id) REFERENCES actors(id)
        );
    """,
    "movie_directors": """
        CREATE TABLE IF NOT EXISTS movie_directors (
            movie_id INT,
            director_id INT,
            PRIMARY KEY (movie_id, director_id),
            FOREIGN KEY (movie_id) REFERENCES movies(id),
            FOREIGN KEY (director_id) REFERENCES directors(id)
        );
    """
}

# Connect to the MySQL database and create tables
try:
    con = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    print("Connection to MySQL was successful!")

    # Create tables
    cursor = con.cursor()
    for table_name, table_sql in tables.items():
        print(f"Creating table: {table_name}")
        cursor.execute(table_sql)
    con.commit()
    print("Tables with updated structure created successfully!")

except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    if 'con' in locals() and con.is_connected():
        con.close()
        print("Connection closed.")
