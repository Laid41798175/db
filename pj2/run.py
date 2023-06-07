import pymysql
import pandas

connection = pymysql.connect(
    host = 'astronaut.snu.ac.kr',
    port = 7000,
    user = 'DB2018_19857',
    password = 'DB2018_19857',
    db = 'DB2018_19857',
    charset = 'utf8'
)

cursor = connection.cursor()

def create_tables():
    create_movies_table_query = '''
        CREATE TABLE movies (
            id INT PRIMARY KEY AUTO_INCREMENT,
            title VARCHAR(255) UNIQUE,
            director VARCHAR(255),
            price INT CHECK (price BETWEEN 0 AND 100000)
        )
    '''
    cursor.execute(create_movies_table_query)

    create_customers_table_query = '''
        CREATE TABLE customers (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255),
            age INT CHECK (age BETWEEN 12 AND 110),
            class VARCHAR(7) CHECK (class IN ('Basic', 'Premium', 'Vip')),
            UNIQUE (name, age)
        )
    '''
    cursor.execute(create_customers_table_query)

    create_bookings_table_query = '''
        CREATE TABLE bookings (
            movie_id INT,
            customer_id INT,
            PRIMARY KEY (movie_id, customer_id),
            FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
            FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
        )
    '''
    cursor.execute(create_bookings_table_query)

    create_ratings_table_query = '''
        CREATE TABLE ratings (
            movie_id INT,
            customer_id INT,
            rating INT CHECK (rating BETWEEN 1 AND 5),
            PRIMARY KEY (movie_id, customer_id),
            FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
            FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
        )
    '''
    cursor.execute(create_ratings_table_query)

def tables_exist() -> bool :
    show_query = "SHOW TABLES"
    cursor.execute(show_query)
    tables = cursor.fetchall()
    return len(tables) > 0

def put_movie(title : str, director : str, price : int) -> int :
    movie_id = get_movie_id(title)
    if movie_id == -1:
        insert_query = f'''INSERT INTO movies (title, director, price) VALUES("{title}", "{director}", {price})'''
        cursor.execute(insert_query)
        movie_id = cursor.lastrowid
    return movie_id

def put_customer(name : str, age : int, class_t : int) -> int :
    customer_id = get_customer_id(name, age)
    if customer_id == -1:
        insert_query = f'''INSERT INTO customers (name, age, class) VALUES("{name}", {age}, "{class_t}")'''
        cursor.execute(insert_query)
        customer_id = cursor.lastrowid
    return customer_id

def get_movie_id(title : str) -> int :
    select_query = f'''SELECT id
                    FROM movies
                    WHERE title = "{title}"'''
    cursor.execute(select_query)
    result = cursor.fetchall()
    if len(result) == 0:
        return -1
    else:
        return result[0][0]

def get_customer_id(name : str, age : int) -> int :
    select_query = f'''SELECT id
                    FROM customers
                    WHERE name = "{name}" AND age = {age}'''
    cursor.execute(select_query)
    result = cursor.fetchall()
    if len(result) == 0:
        return -1
    else:
        return result[0][0]

def read_data_csv():
    # read csv with one header line
    df = pandas.read_csv('data.csv', header=0)
    for line in df.values.tolist():
        title : str = line[0]
        director : str = line[1]
        price : int = int(line[2])
        name : str = line[3]
        age : int = int(line[4])
        class_t : str = line[5]

        movie_id : int = put_movie(title, director, price)
        customer_id : int = put_customer(name, age, class_t)

        insert_query = f'''INSERT INTO bookings VALUES({movie_id}, {customer_id})'''
        cursor.execute(insert_query)

# Problem 1 (5 pt.)
def initialize_database():    
    if not tables_exist():
        create_tables()

    read_data_csv()

    # success message
    print('Database successfully initialized')

def delete_table(table : str):
    delete_query = f"DELETE FROM {table}"
    cursor.execute(delete_query)

def drop_table(table : str):
    drop_query = f"DROP TABLE {table}"
    cursor.execute(drop_query)

# Problem 15 (5 pt.)
def reset(check : bool):
    if check:
        reset = str(input('Do you want to reset the database? (y/n): '))
        if reset != 'y':
            return

    if tables_exist():
        # reset the database
        drop_table("bookings")
        drop_table("ratings")
        drop_table("movies")
        drop_table("customers")

    print('Database successfully reset')
    initialize_database()

# Problem 2 (4 pt.)
def print_movies():
    select_query = f'''SELECT id, title, director, price
                    FROM movies'''
    cursor.execute(select_query)
    records = cursor.fetchall()
    print("---------------------------------------------------------------------------------------------------------------------------------------------------")
    print("id".ljust(10) + "title".ljust(70) + "director".ljust(35) + "price".ljust(8) + "avg. price".ljust(15) + "reservation".ljust(15) + "avg. rating")
    print("---------------------------------------------------------------------------------------------------------------------------------------------------")
    for record in records:
        id : int = record[0]
        title : str = record[1]
        director : str = record[2]
        price : int = record[3]
        bookings_select_query = f'''SELECT DISTINCT class
                                FROM customers JOIN bookings ON id = customer_id
                                WHERE movie_id = {id}'''
        cursor.execute(bookings_select_query)
        bookings_records = cursor.fetchall()
        reservation : int = len(bookings_records)
        sum : int = 0
        for bookings_record in bookings_records:
            class_t : str = bookings_record[0]
            if class_t == "basic":
                sum += price
            elif class_t == "premium":
                sum += int(price * 0.75)
            elif class_t == "vip":
                sum += int(price * 0.5)
        if reservation == 0:
            average_price = None
        else:
            average_price = int(sum / reservation)
        ratings_select_query = f'''SELECT avg(rating)
                                FROM ratings
                                WHERE movie_id = {id}'''
        cursor.execute(ratings_select_query)
        ratings_record = cursor.fetchall()
        if ratings_record is None:
            average_rating = None
        else:
            average_rating = ratings_record[0][0]
        print(str(id).ljust(10) + title.ljust(70) + director.ljust(35) + str(price).ljust(8) + str(average_price).ljust(15) + str(reservation).ljust(15) + str(average_rating))
    print("---------------------------------------------------------------------------------------------------------------------------------------------------")

# Problem 3 (3 pt.)
def print_users():
    select_query = f'''SELECT *
                    FROM customers'''
    cursor.execute(select_query)
    records = cursor.fetchall()
    print("--------------------------------------------------------------------------------")
    print("id".ljust(10) + "name".ljust(30) + "age".ljust(15) + "class")
    print("--------------------------------------------------------------------------------")
    for record in records:
        id : int = record[0]
        name : str = record[1]
        age : int = record[2]
        class_t : str = record[3]
        print(str(id).ljust(10) + name.ljust(30) + str(age).ljust(15) + class_t)
    print("--------------------------------------------------------------------------------")

# Problem 4 (4 pt.)
def insert_movie():
    title = input('Movie title: ')
    director = input('Movie director: ')
    price = int(input('Movie price: '))

    price_constraint = 0 <= price and price <= 100000
    if not price_constraint:
        print("Movie price should be from 0 to 100000")
        return

    movie_id = get_movie_id(title)
    if movie_id != -1:
        print(f"The movie {title} already exists")

    put_movie(title, director, price)

    # success message
    print('One movie successfully inserted')

# Problem 6 (4 pt.)
def remove_movie():
    movie_id = int(input('Movie ID: '))
    
    delete_query = f"DELETE FROM movies WHERE id = {movie_id}"
    deleted_row = cursor.execute(delete_query)
    if deleted_row == 0:
        # error message
        print(f'Movie {movie_id} does not exist')
    else:
        # success message
        print('One movie successfully removed')

# Problem 5 (4 pt.)
def insert_user():
    name = input('User name: ')
    age = int(input('User age: '))
    class_t = input('User class: ')

    age_constraint = 12 <= age and age <= 110
    class_constraint = class_t in ["basic", "premium", "vip"]

    if not age_constraint:
        print("User age should be from 12 to 110")
        return
    if not class_constraint:
        print("User class should be basic, premium or vip")
        return

    customer_id = get_customer_id(name, age)
    if customer_id != -1:
        print(f'The user ({name}, {age}) already exists')
        return
    
    put_customer(name, age, class_t)
    
    # success message
    print('One user successfully inserted')

# Problem 7 (4 pt.)
def remove_user():
    user_id = input('User ID: ')

    delete_query = f"DELETE FROM customers WHERE id = {user_id}"
    deleted_row = cursor.execute(delete_query)
    if deleted_row == 0:
        # error message
        print(f'User {user_id} does not exist')
    else:
        # success message
        print('One user successfully removed')

def movie_exists(movie_id : int) -> bool :
    select_query = f'''SELECT *
                    FROM movies
                    WHERE id = {movie_id}'''
    cursor.execute(select_query)
    result = cursor.fetchall()
    return len(result) > 0

def customer_exists(customer_id : int) -> bool :
    select_query = f'''SELECT *
                    FROM customers
                    WHERE id = {customer_id}'''
    cursor.execute(select_query)
    result = cursor.fetchall()
    return len(result) > 0

def booking_exists(movie_id : int, customer_id : int) -> bool:
    select_query = f'''SELECT *
                    FROM bookings
                    WHERE movie_id = {movie_id} AND customer_id = {customer_id}'''
    cursor.execute(select_query)
    result = cursor.fetchall()
    return len(result) > 0

def rating_exists(movie_id : int, customer_id : int) -> bool:
    select_query = f'''SELECT *
                    FROM ratings
                    WHERE movie_id = {movie_id} AND customer_id = {customer_id}'''
    cursor.execute(select_query)
    result = cursor.fetchall()
    return len(result) > 0

def is_movie_fully_booked(movie_id : int) -> bool:
    select_query = f'''SELECT *
                    FROM bookings
                    WHERE movie_id = {movie_id}'''
    cursor.execute(select_query)
    records = cursor.fetchall()
    return len(records) >= 10

# Problem 8 (5 pt.)
def book_movie():
    movie_id = int(input('Movie ID: '))
    user_id = int(input('User ID: '))

    if not movie_exists(movie_id):
        print(f'Movie {movie_id} does not exist')
        return
    if not customer_exists(user_id):
        print(f'User {user_id} does not exist')
        return
    if booking_exists(movie_id, user_id):
        print(f'User {user_id} already booked movie {movie_id}')
        return
    if is_movie_fully_booked(movie_id):
        print(f'Movie {movie_id} has already been fully booked')
        return

    insert_query = f"INSERT INTO bookings VALUES({movie_id}, {user_id})"
    cursor.execute(insert_query)

    # success message
    print('Movie successfully booked')

# Problem 9 (5 pt.)
def rate_movie():
    movie_id = int(input('Movie ID: '))
    user_id = int(input('User ID: '))
    rating = int(input('Ratings (1~5): '))

    rating_constraint = 1 <= rating and rating <= 5

    if not movie_exists(movie_id):
        print(f'Movie {movie_id} does not exist')
        return
    if not customer_exists(user_id):
        print(f'User {user_id} does not exist')
        return
    if not rating_constraint:
        print(f'Wrong value for a rating')
        return
    if not booking_exists(movie_id, user_id):
        print(f'User {user_id} has not booked movie {movie_id} yet')
        return        
    if rating_exists(movie_id, user_id):
        print(f'User {user_id} has already rated movie {movie_id}')
        return
    
    insert_query = f"INSERT INTO ratings VALUES({movie_id}, {user_id}, {rating})"
    cursor.execute(insert_query)

    # success message
    print('Movie successfully rated')

# Problem 10 (5 pt.)
def print_users_for_movie():
    movie_id = int(input('Movie ID: '))
    
    if not movie_exists(movie_id):
        print(f'Movie {movie_id} does not exist')
        return
    
    select_query = f'''SELECT price
                    FROM movies
                    WHERE id = {movie_id}'''
    cursor.execute(select_query)
    result = cursor.fetchall()
    price : int = result[0][0]
    
    select_query = f'''SELECT id, name, age, class, rating
                    FROM customers LEFT JOIN bookings ON id = customer_id NATURAL LEFT JOIN ratings
                    WHERE movie_id = {movie_id}'''
    cursor.execute(select_query)
    records = cursor.fetchall()
    print("--------------------------------------------------------------------------------------------------------")
    print("id".ljust(10) + "name".ljust(30) + "age".ljust(10) + "res. price".ljust(15) + "rating")
    print("--------------------------------------------------------------------------------------------------------")
    for record in records:
        id : int = record[0]
        name : str = record[1]
        age : int = record[2]
        class_t : str = record[3]
        rating : int = record[4]
        if class_t == "basic":
            reserve_price = price
        elif class_t == "premium":
            reserve_price = int(price * 0.75)
        elif class_t == "vip":
            reserve_price = int(price * 0.5)
        print(str(id).ljust(10) + name.ljust(30) + str(age).ljust(10) + str(reserve_price).ljust(15) + str(rating))
    print("--------------------------------------------------------------------------------------------------------")

# Problem 11 (5 pt.)
def print_movies_for_user():
    user_id = int(input('User ID: '))

    if not customer_exists(user_id):
        print(f'User {user_id} does not exist')
        return

    select_query = f"SELECT class FROM customers WHERE id = {user_id}"
    cursor.execute(select_query)
    results = cursor.fetchall()
    class_t = results[0][0]

    select_query = f'''SELECT id, title, director, price, rating
                    FROM movies LEFT JOIN bookings ON id = movie_id NATURAL LEFT JOIN ratings
                    WHERE customer_id = {user_id}'''
    cursor.execute(select_query)
    records = cursor.fetchall()
    print("------------------------------------------------------------------------------------------------------------------------------------------------------------")
    print("id".ljust(10) + "title".ljust(70) + "director".ljust(35) + "res. price".ljust(15) + "rating")
    print("------------------------------------------------------------------------------------------------------------------------------------------------------------")
    for record in records:
        id : int = record[0]
        title : str = record[1]
        director : str = record[2]
        price : int = record[3]
        rating : int = record[4]
        if class_t == "basic":
            reserve_price = price
        elif class_t == "premium":
            reserve_price = int(price * 0.75)
        elif class_t == "vip":
            reserve_price = int(price * 0.5)
        print(str(id).ljust(10) + title.ljust(70) + director.ljust(35) + str(reserve_price).ljust(15) + str(rating))
    print("------------------------------------------------------------------------------------------------------------------------------------------------------------")

# Problem 12 (6 pt.)
def recommend_popularity():
    # YOUR CODE GOES HERE
    user_id = int(input('User ID: '))

    if not customer_exists(user_id):
        # error message
        print(f'User {user_id} does not exist')
        return
    
    customer_query = f'''SELECT class
                    FROM customers
                    WHERE id = {user_id}'''
    cursor.execute(customer_query)
    result = cursor.fetchall()
    class_t : str = result[0][0]

    select_query = f'''SELECT id, title, price, avg(rating)
                    FROM movies LEFT JOIN ratings ON id = movie_id
                    GROUP BY id
                    ORDER BY avg(rating) DESC, id'''
    cursor.execute(select_query)
    records = cursor.fetchall()
    print("---------------------------------------------------------------------------------------------------------------------------------------")
    print("Rating-based")
    print("id".ljust(10) + "title".ljust(70) + "res. price".ljust(15) + "reservation".ljust(15) + "avg. rating")
    print("---------------------------------------------------------------------------------------------------------------------------------------")
    for record in records:
        movie_id : int = record[0]
        booking_query = f'''SELECT * FROM bookings
                        WHERE movie_id = {movie_id} AND customer_id = {user_id}'''
        cursor.execute(booking_query)
        result = cursor.fetchall()
        if len(result) == 0:
            # recommend this movie
            title : str = record[1]
            price : int = record[2]

            if class_t == "basic":
                reserve_price = price
            elif class_t == "premium":
                reserve_price = int(price * 0.75)
            elif class_t == "vip":
                reserve_price = int(price * 0.5)

            reservation_query = f'''SELECT * FROM bookings
                                WHERE movie_id = {movie_id}'''
            cursor.execute(reservation_query)
            result = cursor.fetchall()
            reservation : int = len(result)

            average_rating : float | None = record[3]
            
            print(str(movie_id).ljust(10) + title.ljust(70) + str(reserve_price).ljust(15) + str(reservation).ljust(15) + str(average_rating))
            break
        else:
            # User already booked the movie
            continue
    
    select_query = f'''SELECT id, title, price, count(bookings.movie_id) AS reservation
                    FROM movies LEFT JOIN bookings ON id = movie_id
                    GROUP BY id, title, price
                    ORDER BY reservation DESC, id'''
    cursor.execute(select_query)
    records = cursor.fetchall()
    print("---------------------------------------------------------------------------------------------------------------------------------------")
    print("Popularity-based")
    print("id".ljust(10) + "title".ljust(70) + "res. price".ljust(15) + "reservation".ljust(15) + "avg. rating")
    print("---------------------------------------------------------------------------------------------------------------------------------------")
    for record in records:
        movie_id : int = record[0]
        booking_query = f'''SELECT * FROM bookings
                        WHERE movie_id = {movie_id} AND customer_id = {user_id}'''
        cursor.execute(booking_query)
        result = cursor.fetchall()
        if len(result) == 0:
            # recommend this movie
            title : str = record[1]
            price : int = record[2]

            if class_t == "basic":
                reserve_price = price
            elif class_t == "premium":
                reserve_price = int(price * 0.75)
            elif class_t == "vip":
                reserve_price = int(price * 0.5)
            
            reservation : int = record[3]

            rating_query = f'''SELECT avg(rating)
                            FROM ratings
                            WHERE movie_id = {movie_id}'''
            cursor.execute(rating_query)
            result = cursor.fetchall()
            average_rating : float = result[0][0]

            print(str(movie_id).ljust(10) + title.ljust(70) + str(reserve_price).ljust(15) + str(reservation).ljust(15) + str(average_rating))
            break
        else:
            # User already booked the movie
            continue
    print("---------------------------------------------------------------------------------------------------------------------------------------")

# Problem 13 (10 pt.)
def recommend_item_based():
    # YOUR CODE GOES HERE
    user_id = input('User ID: ')


    # error message
    print(f'User {user_id} does not exist')
    print('Rating does not exist')
    # YOUR CODE GOES HERE
    pass

def sql():
    query = input("sql: ")
    cursor.execute(query)
    result = cursor.fetchall()
    for row in result:
        print(row)

# Total of 70 pt.
def main():

    # initialize database
    reset(False)

    while True:
        print('============================================================')
        print('1. initialize database')
        print('2. print all movies')
        print('3. print all users')
        print('4. insert a new movie')
        print('5. remove a movie')
        print('6. insert a new user')
        print('7. remove an user')
        print('8. book a movie')
        print('9. rate a movie')
        print('10. print all users who booked for a movie')
        print('11. print all movies rated by an user')
        print('12. recommend a movie for a user using popularity-based method')
        print('13. recommend a movie for a user using item-based collaborative filtering')
        print('14. exit')
        print('15. reset database')
        print('============================================================')
        menu = int(input('Select your action: '))

        if menu == 1:
            initialize_database()
        elif menu == 2:
            print_movies()
        elif menu == 3:
            print_users()
        elif menu == 4:
            insert_movie()
        elif menu == 5:
            remove_movie()
        elif menu == 6:
            insert_user()
        elif menu == 7:
            remove_user()
        elif menu == 8:
            book_movie()
        elif menu == 9:
            rate_movie()
        elif menu == 10:
            print_users_for_movie()
        elif menu == 11:
            print_movies_for_user()
        elif menu == 12:
            recommend_popularity()
        elif menu == 13:
            recommend_item_based()
        elif menu == 14:
            print('Bye!')
            break
        elif menu == 15:
            reset(True)
        elif menu == 16:
            sql()
        else:
            print('Invalid action')


if __name__ == "__main__":
    main()