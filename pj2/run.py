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
    # create table 'movies'
    create_movies_table_query = '''
        CREATE TABLE movies (
            id INT PRIMARY KEY AUTO_INCREMENT,
            title VARCHAR(255) UNIQUE,
            director VARCHAR(255),
            price INT CHECK (price BETWEEN 0 AND 100000)
        )
    '''
    cursor.execute(create_movies_table_query)

    # create table 'customers'
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

    # create table 'bookings'
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

    # create table 'ratings'
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

# put_movie and put_customer functions are used in reading data.csv
# if the movie or customer exists, return the id (primary key)
# else, insert into tables and return the id
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
        # if there is no such id, return -1
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
        # if there is no such id, return -1
        return -1
    else:
        return result[0][0]

def read_data_csv():
    # read csv with one header line
    df = pandas.read_csv('data.csv', header=0)
    for line in df.values.tolist():
        # assume that data in data.csv don't violate any constraint including integrity
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
        # create tables if tables not exist
        create_tables()

    try:
        # read data in data.csv
        read_data_csv()
    except pymysql.IntegrityError:
        # if read_data_csv() fails with IntegerityError,
        # user is initializing more than once or data.csv has invalid data
        print('Initializing database must be executed only once')
        print('If you want to read additional data in data.csv, please reset instead')
        print('Or, data.csv may have invalid data')
        return

    # success message
    print('Database successfully initialized')

def delete_table(table : str):
    delete_query = f"DELETE FROM {table}"
    cursor.execute(delete_query)

def drop_table(table : str):
    drop_query = f"DROP TABLE {table}"
    cursor.execute(drop_query)

# Problem 15 (5 pt.)
def reset():
    # give a warning message
    reset = str(input('Do you want to reset the database? (y/n): '))
    if reset not in ['y', 'Y', 'Yes', 'yes', 'YES']:
        return

    if tables_exist():
        # delete all the database including schemas
        drop_table("bookings")
        drop_table("ratings")
        drop_table("movies")
        drop_table("customers")

    print('Database successfully reset')
    # call initialize_database
    initialize_database()

# this function is for truncating data before the submission
def truncate_data():
    drop_table("bookings")
    drop_table("ratings")
    drop_table("movies")
    drop_table("customers")

    create_tables()

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

        bookings_select_query = f'''SELECT class
                                FROM customers JOIN bookings ON id = customer_id
                                WHERE movie_id = {id}'''
        cursor.execute(bookings_select_query)
        bookings_records = cursor.fetchall()
        # the number of records is reservation
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

        # if there is no booking for the movie, average_price is None
        if reservation == 0:
            average_price = None
        else:
            average_price = int(sum / reservation)

        ratings_select_query = f'''SELECT avg(rating)
                                FROM ratings
                                WHERE movie_id = {id}'''
        cursor.execute(ratings_select_query)
        ratings_record = cursor.fetchall()

        # if there is no rating for the movie, average_rating is None
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

    # constraints are pre-checked before inserting into database
    price_constraint = 0 <= price and price <= 100000
    if not price_constraint:
        print("Movie price should be from 0 to 100000")
        return

    movie_id = get_movie_id(title)
    if movie_id != -1:
        print(f"Movie {title} already exists")

    put_movie(title, director, price)

    # success message
    print('One movie successfully inserted')

# Problem 6 (4 pt.)
def remove_movie():
    movie_id = int(input('Movie ID: '))
    
    delete_query = f"DELETE FROM movies WHERE id = {movie_id}"
    # cursor.execute returns the number of affected rows
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

    # constraints are pre-checked before inserting into database
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
    user_id = int(input('User ID: '))

    delete_query = f"DELETE FROM customers WHERE id = {user_id}"
    # cursor.execute returns the number of affected rows
    deleted_row = cursor.execute(delete_query)
    if deleted_row == 0:
        # error message
        print(f'User {user_id} does not exist')
    else:
        # success message
        print('One user successfully removed')

# belows are helper functions for checking constraints
# movie_exists, customer_exists, booking_exists, rating_exists, is_movie_fully_booked
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

    # check constraints one by one
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

    # check constraints one by one
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
    # id is a primary key thus price is result[0][0]
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

    select_query = f'''SELECT class
                    FROM customers
                    WHERE id = {user_id}'''
    cursor.execute(select_query)
    results = cursor.fetchall()
    # id is a primary key thus class (class_t) is result[0][0]
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
    # id is a primary key thus class (class_t) is result[0][0]
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
        # ORDER BY avg(rating) DESC, id
        movie_id : int = record[0]

        booking_query = f'''SELECT * FROM bookings
                        WHERE movie_id = {movie_id} AND customer_id = {user_id}'''
        cursor.execute(booking_query)
        result = cursor.fetchall()
        # check if the user already watched the movie via booking_query
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
        # ORDER BY reservation DESC, id
        movie_id : int = record[0]

        booking_query = f'''SELECT * FROM bookings
                        WHERE movie_id = {movie_id} AND customer_id = {user_id}'''
        cursor.execute(booking_query)
        result = cursor.fetchall()
        # check if the user already watched the movie via booking_query
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
            # id is a primary key thus avg(rating) is result[0][0]
            average_rating : float = result[0][0]

            print(str(movie_id).ljust(10) + title.ljust(70) + str(reserve_price).ljust(15) + str(reservation).ljust(15) + str(average_rating))
            break
        else:
            # User already booked the movie
            continue
    print("---------------------------------------------------------------------------------------------------------------------------------------")

# Problem 13 (10 pt.)
def recommend_item_based():
    user_id = int(input('User ID: '))
    rec_count = int(input('Recommend Count: '))

    if not customer_exists(user_id):
        print(f'User {user_id} does not exist')
        return

    ratings_query = f'''SELECT * FROM ratings
                    WHERE customer_id = {user_id}'''
    cursor.execute(ratings_query)
    result = cursor.fetchall()
    if len(result) == 0:
        # There is no ratings from the user
        print('Rating does not exist')
        return

    customer_query = f'''SELECT class
                    FROM customers
                    WHERE id = {user_id}'''
    cursor.execute(customer_query)
    result = cursor.fetchall()
    # id is a primary key thus class (class_t) is result[0][0]
    class_t : str = result[0][0]

    movie_id_list = []
    movie_id_query = f'''SELECT id
                    FROM movies
                    ORDER BY id'''
    cursor.execute(movie_id_query)
    records = cursor.fetchall()
    for record in records:
        movie_id : int = record[0]
        movie_id_list.append(movie_id)
    # store max_movid_id with last movie_id in records
    max_movie_id : int = movie_id

    customer_id_list = []
    customer_id_query = f'''SELECT distinct customer_id
                        FROM ratings
                        ORDER BY customer_id'''
    cursor.execute(customer_id_query)
    records = cursor.fetchall()
    for record in records:
        customer_id : int = record[0]
        customer_id_list.append(customer_id)
    # store max_customer_id with last customer_id in records
    max_customer_id : int = customer_id

    rating_table = []

    for movie_id in range(max_movie_id + 1):
        rating_list = [0 for _ in range(max_customer_id + 1)]
        
        if movie_id not in movie_id_list:
            # no such movie_id
            rating_table.append(rating_list)
            continue

        rating_query = f'''SELECT customer_id, rating
                        FROM ratings
                        WHERE movie_id = {movie_id}'''
        cursor.execute(rating_query)
        records = cursor.fetchall()
        
        if len(records) == 0:
            # movie doesn't have any rating
            # rating_list is initialized with 0s and appended to rating_table 
            rating_table.append(rating_list)
            continue
        
        sum : int = 0
        for record in records:
            customer_id : int = record[0]
            rating : int = record[1]
            rating_list[customer_id] = rating
            sum += rating
        # rating is rounded with 2 digits
        average_rating : float = round(float(sum) / len(records), 2)

        for customer_id in range(max_customer_id + 1):
            if customer_id not in customer_id_list:
                # no such customer_id or customer doesn't have any rating
                continue

            if rating_list[customer_id] == 0:
                # update rating with average_rating if there was no rating
                rating_list[customer_id] = average_rating
                
        rating_table.append(rating_list)

    # obtain total_average
    total : float = 0
    for movie_id in range(max_movie_id + 1):
        if movie_id not in movie_id_list:
            continue
        
        for customer_id in range(max_customer_id + 1):
            if customer_id not in customer_id_list:
                continue
            total += rating_table[movie_id][customer_id]
    total_average : float = round(total / (len(movie_id_list) * len(customer_id_list)), 4)

    # create cf matrix
    cf = [[0 for _ in range(max_movie_id + 1)] for _ in range(max_movie_id + 1)]
    for i in range(max_movie_id + 1):
        if i not in movie_id_list:
            continue

        # tip 1 (cf[i][i] = 1)
        cf[i][i] = 1

        # tip 2 (cf is symmetric, i.e. cf[i][j] = cf[j][i])
        for j in range(i + 1, max_movie_id + 1):
            if j not in movie_id_list:
                continue
            
            sigma_AB : float = 0
            sigma_A2 : float = 0
            sigma_B2 : float = 0

            for customer_id in range(max_customer_id + 1):
                if customer_id not in customer_id_list:
                    continue

                value_A : float = rating_table[i][customer_id] - total_average
                value_B : float = rating_table[j][customer_id] - total_average
                
                sigma_AB += value_A * value_B
                sigma_A2 += value_A ** 2 # value_A ^ 2
                sigma_B2 += value_B ** 2 # value_B ^ 2
            
            # cf[i][j] = sigma_AB / (sqrt(sigma_A2) * sqrt(sigma_B2)) = cf[j][i]
            cf[i][j] = round(sigma_AB / ((sigma_A2 * sigma_B2) ** (1/2)), 4)
            cf[j][i] = cf[i][j]

    # create expected rating list for the user
    expected_rating_list = []
    for i in range(max_movie_id + 1):
        if i not in movie_id_list:
            continue
        
        sigma_CF_R : float = 0
        sigma_CF : float = 0

        for j in range(max_movie_id + 1):
            if j not in movie_id_list or i == j:
                continue
            
            value_CF : float = cf[i][j]
            value_R : float = rating_table[j][user_id]

            sigma_CF_R += value_CF * value_R
            sigma_CF += value_CF

        # create a tuple (movie_id, expected rating)
        expected_rating_list.append((i, round(sigma_CF_R / sigma_CF, 2)))

    # sort with temporary lambda function
    # key is x[1] (expected rating), reverse is True (DESC order)
    sorted_list = sorted(expected_rating_list, key=lambda x: x[1], reverse=True)

    print("---------------------------------------------------------------------------------------------------------------------------------------")
    print("id".ljust(10) + "title".ljust(70) + "res. price".ljust(15) + "reservation".ljust(15) + "avg. rating")
    print("---------------------------------------------------------------------------------------------------------------------------------------")
    for (movie_id, _) in sorted_list:
        check_query = f'''SELECT * FROM bookings
                        WHERE movie_id = {movie_id} AND customer_id = {user_id}'''
        cursor.execute(check_query)
        result = cursor.fetchall()
        if len(result) != 0:
            # customer already booked the movie
            continue

        select_query = f'''SELECT title, price
                        FROM movies
                        WHERE id = {movie_id}'''
        cursor.execute(select_query)
        result = cursor.fetchall()
        # id is a primary key thus title is result[0][0] and price is result[0][1]
        title : str = result[0][0]
        price : int = result[0][1]

        if class_t == "basic":
            reserve_price = price
        elif class_t == "premium":
            reserve_price = int(price * 0.75)
        elif class_t == "vip":
            reserve_price = int(price * 0.5)

        # obtain reservation
        reservation_query = f'''SELECT * FROM bookings
                            WHERE movie_id = {movie_id}'''
        cursor.execute(reservation_query)
        result = cursor.fetchall()
        reservation : int = len(result)

        # obtain average_rating (expected rating will not be included)
        rating_query = f'''SELECT avg(rating)
                        FROM ratings
                        WHERE movie_id = {movie_id}'''
        cursor.execute(rating_query)
        result = cursor.fetchall()
        average_rating : float = result[0][0]
        
        print(str(movie_id).ljust(10) + title.ljust(70) + str(reserve_price).ljust(15) + str(reservation).ljust(15) + str(average_rating))

        rec_count -= 1
        if rec_count == 0:
            # recommended initial rec_count movies
            break
    print("---------------------------------------------------------------------------------------------------------------------------------------")

# sql() is for the debugging and truncating the data
def sql():
    query = input("sql: ")
    cursor.execute(query)
    result = cursor.fetchall()
    for row in result:
        print(row)

# Total of 70 pt.
def main():
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
            reset()
        elif menu == 9998:
            sql()
        elif menu == 9999:
            truncate_data()
        else:
            print('Invalid action')

        # after processing the query, commit to connection
        connection.commit()

if __name__ == "__main__":
    main()