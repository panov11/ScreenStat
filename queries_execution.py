import queries_db_script as qdb

def main():
    print("Welcome to the ScreenStat Runner!")
    print("""
    Available Queries:
    1: Search for a director by name (requires a name argument).
    2: Search for a movie by overview text (requires a keyword or phrase argument).
    3: Find movies featuring an actor with a minimum rating (requires actor name and rating arguments).
    4: Find actors and directors who collaborated on more than one movie (no arguments required).
    5: Search for movies with a director of a specific gender and minimum revenue (requires gender and revenue arguments).
    6: List the top 20 places of birth with the most actors (no arguments required).
    7: Find movies featuring an actor with positive profit (requires actor name argument).
    8: Group movies by shared actor-director birthplace, showing profitability and popularity (no arguments required).
    """)

    while True:
        try:
            # Prompt user to choose a query
            query_number = input("Enter the query number you want to run (1-8), or type 'exit' to quit: ")

            if query_number.lower() == 'exit':
                print("Thank you for using the ScreenStat Runner! Goodbye!")
                break

            # Convert input to integer
            query_number = int(query_number)

            # Validate query number
            if query_number not in range(1, 9):
                print("Invalid query number. Please enter a number between 1 and 8.")
                continue

            # Handle each query
            if query_number == 1:
                director_name = input("Enter the director's name: ")
                qdb.query_1(director_name)

            elif query_number == 2:
                keyword = input("Enter a keyword or phrase to search in movie overviews: ")
                qdb.query_2(keyword)

            elif query_number == 3:
                actor_name = input("Enter the actor's name: ")
                rating = float(input("Enter the minimum rating: "))
                qdb.query_3(actor_name, rating)

            elif query_number == 4:
                print("Running query 4...")
                qdb.query_4()

            elif query_number == 5:
                gender = int(input("Enter the gender (1 for Female, 2 for Male): "))
                revenue = float(input("Enter the minimum revenue: "))
                qdb.query_5(gender, revenue)

            elif query_number == 6:
                print("Running query 6...")
                qdb.query_6()

            elif query_number == 7:
                actor_name = input("Enter the actor's name: ")
                qdb.query_7(actor_name)

            elif query_number == 8:
                print("Running query 8...")
                qdb.query_8()


        except ValueError:
            print("Invalid input. Please provide the correct type of arguments for the query.")
        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
