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
    create_movie_table_query = '''
        CREATE TABLE movie (
            id INT PRIMARY KEY AUTO_INCREMENT,
            title VARCHAR(255) UNIQUE,
            director VARCHAR(255),
            price INT CHECK (price BETWEEN 0 AND 100000)
        )
    '''
    cursor.execute(create_movie_table_query)

    create_customer_table_query = '''
        CREATE TABLE customer (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255),
            age INT CHECK (age BETWEEN 12 AND 110),
            class VARCHAR(7) CHECK (class IN ('Basic', 'Premium', 'Vip')),
            UNIQUE (name, age)
        )
    '''
    cursor.execute(create_customer_table_query)

    create_booking_table_query = '''
        CREATE TABLE booking (
            movie_id INT,
            customer_id INT,
            PRIMARY KEY (movie_id, customer_id),
            FOREIGN KEY (movie_id) REFERENCES movie(id) ON DELETE CASCADE,
            FOREIGN KEY (customer_id) REFERENCES customer(id) ON DELETE CASCADE
        )
    '''
    cursor.execute(create_booking_table_query)

    create_review_table_query = '''
        CREATE TABLE review (
            movie_id INT,
            customer_id INT,
            rating INT CHECK (rating BETWEEN 1 AND 5),
            PRIMARY KEY (movie_id, customer_id),
            FOREIGN KEY (movie_id) REFERENCES movie(id) ON DELETE CASCADE,
            FOREIGN KEY (customer_id) REFERENCES customer(id) ON DELETE CASCADE
        )
    '''
    cursor.execute(create_review_table_query)

def tables_exist() -> bool :
    show_query = "SHOW TABLES"
    cursor.execute(show_query)
    tables = cursor.fetchall()
    return len(tables) > 0

def put_movie(title : str, director : str, price : int) -> int :
    movie_id = get_movie_id(title)
    if movie_id == -1:
        movie_query = f'''INSERT INTO movie (title, director, price) VALUES("{title}", "{director}", {price})'''
        cursor.execute(movie_query)
        movie_id = cursor.lastrowid
    return movie_id

def put_customer(name : str, age : int, class_t : int) -> int :
    customer_id = get_customer_id(name, age)
    if customer_id == -1:
        customer_query = f'''INSERT INTO customer (name, age, class) VALUES("{name}", {age}, "{class_t}")'''
        cursor.execute(customer_query)
        customer_id = cursor.lastrowid
    return customer_id

def get_movie_id(title : str) -> int :
    select_query = f'''SELECT id FROM movie WHERE title = "{title}"'''
    cursor.execute(select_query)
    result = cursor.fetchall()
    if len(result) == 0:
        return -1
    else:
        return result[0]

def get_customer_id(name : str, age : int) -> int :
    select_query = f'''SELECT id FROM customer WHERE name = "{name}" AND age = {age}'''
    cursor.execute(select_query)
    result = cursor.fetchall()
    if len(result) == 0:
        return -1
    else:
        return result[0]

# TODO
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
        drop_table("booking")
        drop_table("review")
        drop_table("movie")
        drop_table("customer")

    print('Database successfully reset')
    initialize_database()

# Problem 2 (4 pt.)
def print_movies():
    # YOUR CODE GOES HERE

    
    # YOUR CODE GOES HERE
    pass

# Problem 3 (3 pt.)
def print_users():
    select_query = "SELECT * FROM customer"
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
    # YOUR CODE GOES HERE
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
    # YOUR CODE GOES HERE
    movie_id = input('Movie ID: ')
    
    delete_query = f"DELETE FROM movie WHERE id = {movie_id}"
    deleted_row = cursor.execute(delete_query)
    if deleted_row == 0:
        # error message
        print(f'Movie {movie_id} does not exist')
    else:
        # success message
        print('One movie successfully removed')

# Problem 5 (4 pt.)
def insert_user():
    # YOUR CODE GOES HERE
    name = input('User name: ')
    age = input('User age: ')
    class_t = input('User class: ')

    age_constraint = 12 <= age and age <= 110
    class_constraint = class_t in ["basic", "premium", "vip"]

    if (not age_constraint) and class_constraint:
        print("User age should be from 12 to 110")
        return
    elif age_constraint and (not class_constraint):
        print("User class should basic, premium or vip")
        return
    elif (not age_constraint) and (not class_constraint):
        print("User age should be from 12 to 110, User class should basic, premium or vip")
        return
    else: # age_constraint and class_constraint
        pass

    customer_id = get_customer_id(name, age)
    if customer_id != -1:
        print(f'The user ({name}, {age}) already exists')
        return
    
    put_customer(name, age, class_t)
    
    # success message
    print('One user successfully inserted')

# Problem 7 (4 pt.)
def remove_user():
    # YOUR CODE GOES HERE
    user_id = input('User ID: ')

    delete_query = f"DELETE FROM customer WHERE id = {user_id}"
    deleted_row = cursor.execute(delete_query)
    if deleted_row == 0:
        # error message
        print(f'User {user_id} does not exist')
    else:
        # success message
        print('One user successfully removed')

def movie_exists(movie_id : int) -> bool :
    select_query = f"SELECT * FROM movie WHERE id = {movie_id}"
    cursor.execute(select_query)
    result = cursor.fetchall()
    return len(result) > 0

def customer_exists(user_id : int) -> bool :
    select_query = f"SELECT * FROM movie WHERE id = {user_id}"
    cursor.execute(select_query)
    result = cursor.fetchall()
    return len(result) > 0



# Problem 8 (5 pt.)
def book_movie():
    # YOUR CODE GOES HERE
    movie_id = input('Movie ID: ')
    user_id = input('User ID: ')

    

    # error message
    print(f'Movie {movie_id} does not exist')
    print(f'User {user_id} does not exist')
    print(f'User {user_id} already booked movie {movie_id}')
    print(f'Movie {movie_id} has already been fully booked')

    # success message
    print('Movie successfully booked')
    # YOUR CODE GOES HERE
    pass

# Problem 9 (5 pt.)
def rate_movie():
    # YOUR CODE GOES HERE
    movie_id = input('Movie ID: ')
    user_id = input('User ID: ')
    rating = input('Ratings (1~5): ')


    # error message
    print(f'Movie {movie_id} does not exist')
    print(f'User {user_id} does not exist')
    print(f'Wrong value for a rating')
    print(f'User {user_id} has not booked movie {movie_id} yet')
    print(f'User {user_id} has already rated movie {movie_id}')

    # success message
    print('Movie successfully rated')
    # YOUR CODE GOES HERE
    pass

# Problem 10 (5 pt.)
def print_users_for_movie():
    # YOUR CODE GOES HERE
    user_id = input('User ID: ')

    
    # error message
    print(f'User {user_id} does not exist')
    # YOUR CODE GOES HERE
    pass


# Problem 11 (5 pt.)
def print_movies_for_user():
    # YOUR CODE GOES HERE
    user_id = input('User ID: ')


    # error message
    print(f'User {user_id} does not exist')
    # YOUR CODE GOES HERE
    pass

# Problem 12 (6 pt.)
def recommend_popularity():
    # YOUR CODE GOES HERE
    user_id = input('User ID: ')


    # error message
    print(f'User {user_id} does not exist')
    # YOUR CODE GOES HERE
    pass


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