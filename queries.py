import datetime
from psycopg2.extras import RealDictCursor # Now each entry will be a dictionary instead of a list
import psycopg2


def create_tables(con):
    create_user_table_query = """
    CREATE TABLE IF NOT EXISTS users(
        id SERIAL PRIMARY KEY,
        username VARCHAR(50) UNIQUE
    )
    """

    create_genre_table_query = """
    CREATE TABLE IF NOT EXISTS genres(
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) UNIQUE
    )
    """

    create_movies_table_query = """
    CREATE TABLE IF NOT EXISTS movies(
        id SERIAL PRIMARY KEY,
        title VARCHAR(200) UNIQUE,
        release_date DATE,
        genre_id INT REFERENCES genres(id)
    )
    """

    watchlist = """
    CREATE TABLE IF NOT EXISTS watchlist(
        user_id INT REFERENCES users(id),
        movie_id INT REFERENCES movies(id),
        added_date DATE DEFAULT CURRENT_DATE,
        PRIMARY KEY(user_id, movie_id)
    )
    """

    reviews = """
    CREATE TABLE IF NOT EXISTS reviews(
        id SERIAL PRIMARY KEY,
        user_id INT REFERENCES users(id),
        movie_id INT REFERENCES movies(id),
        rating INT,
        review_text TEXT,
        review_date DATE DEFAULT CURRENT_DATE
    )
    """

    with con:
        with con.cursor() as cursor:
            cursor.execute(create_user_table_query)
            cursor.execute(create_genre_table_query)
            cursor.execute(create_movies_table_query)
            cursor.execute(reviews)
            cursor.execute(watchlist)


def populate_tables(con):
    # Queries to insert dummy data
    insert_users_query = """
    INSERT INTO users(username) VALUES('user1'), ('user2'), ('user3')
    ON CONFLICT (username) DO NOTHING
    """

    insert_genres_query = """
    INSERT INTO genres(name) VALUES('Comedy'), ('Drama'), ('Action')
    ON CONFLICT (name) DO NOTHING
    """

    insert_movies_query = """
    INSERT INTO movies (title, release_date, genre_id) VALUES 
    ('Movie 1', %s, 1), 
    ('Movie 2', %s, 2), 
    ('Movie 3', %s, 3)
    ON CONFLICT (title) DO NOTHING
    """

    # Execute the queries
    with con:
        with con.cursor() as cursor:
            cursor.execute(insert_users_query)
            cursor.execute(insert_genres_query)
    # Execute the queries
    with con:
        with con.cursor() as cursor:
            current_date = datetime.datetime.now().date()
            cursor.execute(insert_movies_query,
                           (current_date, current_date, current_date))


def list_all_movies_db(con):
    list_movies_query = """
    SELECT * FROM movies
    """
    with con:
        with con.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(list_movies_query)
            return cursor.fetchall()

def get_movie_db(con, id):
    detail_movie_query = """
    SELECT * FROM movies
    WHERE id = %s
    """
    with con:
        with con.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(detail_movie_query, (id,))
            return cursor.fetchone()

def add_user_db(con, username):
    add_user_query = """
   INSERT INTO users(username)
   VALUES(%s)
   """
    with con:
        with con.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(add_user_query, (username,))


def list_users_db(con):
    list_users_query = """
    SELECT * FROM users
    """
    with con:
        with con.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(list_users_query)
            return cursor.fetchall()

def get_user_db(con, id):
    detail_user_query = """
    SELECT * FROM users
    WHERE id = %s
    """
    with con:
        with con.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(detail_user_query, (id,))
            return cursor.fetchone()

def add_movie_genre_db(con, genre):
    add_movie_genre_query = """
    INSERT INTO genres(name)
    VALUES(%s)
    
    """
    with con:
        with con.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(add_movie_genre_query, (genre,))


def add_movie_db(con, title, release_date, genre):
    add_movie_query = """
    INSERT INTO movies(title, release_date, genre_id)
    VALUES(%s, %s, (SELECT id FROM genres WHERE name = %s))
    RETURNING id, title, release_date, genre_id
    """
    with con:
        with con.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(add_movie_query, (title, release_date, genre))
            return cursor.fetchone()


def list_movies_db(con, limit, release_date):
    list_movies_query = """
    SELECT * FROM movies
    WHERE release_date >= %s
    LIMIT %s
    """
    with con:
        with con.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(list_movies_query, (release_date, limit))
            return cursor.fetchall()


def update_movie_db(con, title, new_movie_title, release_date, genre):
    update_movie_query = """
    UPDATE movies
    SET title = %s, release_date = %s, genre_id = (SELECT id FROM genres WHERE name = %s)
    WHERE title = %s
    RETURNING id
    """
    with con:
        with con.cursor() as cursor:
            cursor.execute(update_movie_query,
                           (new_movie_title, release_date, genre, title))
            return cursor.fetchone()


def create_movie_review_db(con, username,movie_title, rating, review_text):
    create_review_query = """
    INSERT INTO reviews(user_id, movie_id, rating, review_text)
    VALUES((SELECT id FROM users WHERE username = %s), (SELECT id FROM movies WHERE title = %s),
    %s, %s)
    RETURNING id
    """
    with con:
        with con.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(create_review_query, (username,
                           movie_title, rating, review_text))
            return cursor.fetchone()


def list_user_reviews_db(con, username):
    list_user_reviews_query = """
    SELECT username, title, review_text, release_date, rating FROM reviews
    JOIN users ON users.id = reviews.user_id
    JOIN movies ON movies.id = reviews.movie_id
    WHERE users.username = %s
    """
    with con:
        with con.cursor() as cursor:
            cursor.execute(list_user_reviews_query, (username, ))
            return cursor.fetchall()