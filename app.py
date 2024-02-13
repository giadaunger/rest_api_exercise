from crypt import methods
from flask import Flask, request
from dotenv import load_dotenv
import os
import psycopg2
import queries

app = Flask(__name__)
load_dotenv()
pw = os.environ.get("DB")

def get_connection():
    connection = psycopg2.connect(host="localhost", dbname="flaskrest", user="postgres", password=pw, port=5433)
    return connection

con = get_connection()

@app.route("/tables")
def tables():
    try:
        queries.create_tables(con)
        return {"message": "ok"}, 200
    except psycopg2.Error:
        return {"message": "Create tables failed"}, 500

@app.route("/populate")
def populate():
    try:
        queries.populate_tables(con)
        return {"message": "ok"}, 200
    except psycopg2.Error:
        return {"message": "Populate table failed"}, 500

@app.route("/movies", methods=["GET"])
def list_movies():
    movies = queries.list_all_movies_db(con=con)
    return movies

@app.route("/movies/<movie_id>")
def movie_detail(movie_id):
    movie = queries.get_movie_db(con=con, id=movie_id)
    if movie:
        return movie, 200
    return {"message":"Could not find movie"}, 404

@app.route("/users")
def list_users():
    users = queries.list_users_db(con=con)
    return users, 200

@app.route("/users/<user_id>")
def get_user_db(user_id):
    user = queries.get_user_db(con=con, id=user_id)
    if user:
        return user, 200
    return {"message":"Could not find user"}, 404

@app.route("/movies", methods=["POST"])
def create_movie():
    data = request.get_json()
    title = data["title"]
    genre = data["genre"]
    try: 
        release_date = data["release_date"]
        movie = queries.add_movie_db(con=con, title=title, genre=genre, release_date=release_date)
        return movie, 200
    except psycopg2.DatabaseError:
        return {"message": "Unable to add movie"}, 400