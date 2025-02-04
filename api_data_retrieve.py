import requests
import mysql.connector
from datetime import datetime
import time

# TMDb API Key and Base URL
API_KEY = 'db8edbd11e9022aad3adf34b04590cdd'
BASE_URL = "https://api.themoviedb.org/3"

# MySQL Connection Details
MYSQL_HOST = 'localhost'
MYSQL_PORT = 3305
MYSQL_USER = 'lazarovici'
MYSQL_PASSWORD = 'lazaro36907'
MYSQL_DATABASE = 'lazarovici'


movies = []
movie_actors = []
movie_directors = []
actors_details = []
directors_details = []


def calculate_age(birth_date):
    if birth_date:
        birth_date = datetime.strptime(birth_date, '%Y-%m-%d')
        today = datetime.today()
        return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
    return -1

def handle_missing_value(value):
    return value if value else "NA"

def handle_numeric_value(value):
    return value if value is not None else -1

# Fetch movies
def fetch_movies():
    global movies
    print("Fetching movies...")
    for page in range(1, 51):  # Adjust range for more pages
        response = requests.get(f"{BASE_URL}/discover/movie", params={
            "api_key": API_KEY,
            "language": "en-US",
            "page": page,
            "include_adult": "false",
            "sort_by": "popularity.desc"
        })
        if response.status_code == 200:
            results = response.json()['results']
            for movie in results:
                movie_id = movie['id']
                details_response = requests.get(f"{BASE_URL}/movie/{movie_id}", params={"api_key": API_KEY})
                if details_response.status_code == 200:
                    details = details_response.json()
                    movies.append({
                        "id": details["id"],
                        "title": handle_missing_value(details.get("title")),
                        "release_date": details.get("release_date") or None,
                        "overview": handle_missing_value(details.get("overview")),
                        "budget": handle_numeric_value(details.get("budget")),
                        "revenue": handle_numeric_value(details.get("revenue")),
                        "rating": handle_numeric_value(details.get("vote_average")),
                        "genres": handle_missing_value(", ".join([g["name"] for g in details.get("genres", [])])),
                        "duration": handle_numeric_value(details.get("runtime")),
                        "production_companies": handle_missing_value(", ".join([p["name"] for p in details.get("production_companies", [])])),
                        "production_countries": handle_missing_value(", ".join([p["name"] for p in details.get("production_countries", [])]))
                    })
    print(f"Fetched {len(movies)} movies.")

# Fetch cast and crew
def fetch_cast_and_crew():
    global movie_actors, movie_directors, movie_executive_producers
    print("Fetching cast and crew...")
    for movie in movies:
        movie_id = movie["id"]
        response = requests.get(f"{BASE_URL}/movie/{movie_id}/credits", params={"api_key": API_KEY})
        if response.status_code == 200:
            credits = response.json()

            # Process top 10 actors
            for actor in sorted(credits["cast"], key=lambda x: x.get("order", 999))[:10]:
                movie_actors.append({
                    "movie_id": movie_id,
                    "actor_id": actor["id"],
                    "character_name": handle_missing_value(actor.get("character")),
                    "order": actor.get("order", 999),
                    "popularity": actor.get("popularity", 0)
                })

            # Process directors
            for director in [crew for crew in credits["crew"] if crew["job"].lower() == "director"]:
                movie_directors.append({
                    "movie_id": movie_id,
                    "director_id": director["id"]
                })

# Fetch person details
def fetch_person_details():
    global actors_details, directors_details, executive_producers_details
    unique_actor_ids = {actor["actor_id"] for actor in movie_actors}
    unique_director_ids = {director["director_id"] for director in movie_directors}

    print("Fetching person details...")
    for person_id in unique_actor_ids | unique_director_ids:
        print(f"Fetching details for person ID {person_id}...")
        response = requests.get(f"{BASE_URL}/person/{person_id}", params={"api_key": API_KEY})
        if response.status_code == 200:
            details = response.json()
            person_data = {
                "id": details["id"],
                "name": handle_missing_value(details.get("name")),
                "age": calculate_age(details.get("birthday")),
                "place_of_birth": handle_missing_value(details.get("place_of_birth")),
                "gender": details.get("gender", -1),
                "popularity": handle_numeric_value(details.get("popularity"))
            }
            if person_id in unique_actor_ids:
                actors_details.append(person_data)
            if person_id in unique_director_ids:
                directors_details.append(person_data)
    print(f"Fetched {len(actors_details)} actors, {len(directors_details)} directors.")

# Insert data into MySQL
def insert_data(cursor):
    # Insert Movies
    for movie in movies:
        try:
            cursor.execute("""
                INSERT INTO movies (id, title, release_date, overview, budget, revenue, rating, duration, genres, production_companies, production_countries)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE title=VALUES(title);
            """, (
                movie["id"], movie["title"], movie["release_date"], movie["overview"], movie["budget"],
                movie["revenue"], movie["rating"], movie["duration"], movie["genres"], movie["production_companies"],
                movie["production_countries"]
            ))
        except Exception as e:
            print(f"Error inserting movie {movie['id']}: {e}")

    # Insert Actors
    for actor in actors_details:
        try:
            cursor.execute("""
                INSERT INTO actors (id, name, age, place_of_birth, gender, popularity)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE name=VALUES(name);
            """, (
                actor["id"], actor["name"], actor["age"], actor["place_of_birth"], actor["gender"], actor["popularity"]
            ))
        except Exception as e:
            print(f"Error inserting actor {actor['id']}: {e}")

    # Insert Directors
    for director in directors_details:
        try:
            cursor.execute("""
                INSERT INTO directors (id, name, age, place_of_birth, gender, popularity)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE name=VALUES(name);
            """, (
                director["id"], director["name"], director["age"], director["place_of_birth"], director["gender"], director["popularity"]
            ))
        except Exception as e:
            print(f"Error inserting director {director['id']}: {e}")


    # Insert Movie_Actors
    for movie_actor in movie_actors:
        try:
            cursor.execute("""
                INSERT INTO movie_actors (movie_id, actor_id, character_name)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE character_name=VALUES(character_name);
            """, (
                movie_actor["movie_id"], movie_actor["actor_id"], movie_actor["character_name"]
            ))
        except Exception as e:
            print(f"Error inserting movie-actor relationship for movie {movie_actor['movie_id']}, actor {movie_actor['actor_id']}: {e}")

    # Insert Movie_Directors
    for movie_director in movie_directors:
        try:
            cursor.execute("""
                INSERT INTO movie_directors (movie_id, director_id)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE movie_id=VALUES(movie_id);
            """, (
                movie_director["movie_id"], movie_director["director_id"]
            ))
        except Exception as e:
            print(f"Error inserting movie-director relationship for movie {movie_director['movie_id']}, director {movie_director['director_id']}: {e}")

# Main function
def main():
    try:
        con = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        cursor = con.cursor()

        # Fetch data
        fetch_movies()
        fetch_cast_and_crew()
        fetch_person_details()

        # Insert data
        insert_data(cursor)

        con.commit()
        print("Data inserted successfully!")

    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
        if 'con' in locals() and con.is_connected():
            con.rollback()

    except Exception as e:
        print(f"Unexpected Error: {e}")

    finally:
        if 'con' in locals() and con.is_connected():
            con.close()
            print("Connection closed.")

if __name__ == "__main__":
    main()
