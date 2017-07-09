import sqlite3
import sys, getopt

from app import createTable, addRow

import mysql.connector

config = {  
    'user': 'root',
    'password': 'smoothie42',
    'host': 'localhost',
    'database': 'mediahorde'
}

db = mysql.connector.connect(**config)
c = db.cursor()
c.callproc('spCreateTable',('games','''
    `name` BLOB,
    `type` TEXT,
    `required_age` INT,
    `num_dlc` INT,
    `detailed_description` BLOB,
    `about_the_game` BLOB,
    `supported_languages` TEXT,
    `header_image` TEXT,
    `website` TEXT,
    `min_req` TEXT,
    `rec_req` TEXT,
    `developers` TEXT,
    `publishers` TEXT,
    `demo_id` INT,
    `platflorms` TEXT,
    `metacritic` INT,
    `metaurl` TEXT,
    `release_date` TEXT,
    '''))
c.execute('''CREATE TABLE IF NOT EXISTS categories (
    game_id INT,
    id INT,
    description TEXT,
    PRIMARY KEY(game_id, id))''')
c.execute('''CREATE TABLE IF NOT EXISTS genres (
    game_id INT,
    id INT,
    description TEXT,
    PRIMARY KEY(game_id, id))''')
c.execute('''CREATE TABLE IF NOT EXISTS movies (
    game_id INT,
    id INT,
    name TEXT,
    thumbnail TEXT,
    webm480 TEXT,
    webm TEXT,
    PRIMARY KEY(game_id, id))''')
c.execute('''CREATE TABLE IF NOT EXISTS screenshots (
    game_id INT,
    id INT,
    path_thumbnail TEXT,
    path_full TEXT,
    PRIMARY KEY(game_id, id))''')
db.commit()

liteDb = sqlite3.connect('games.db')
lc = liteDb.cursor()

games = lc.execute('''SELECT * FROM games''')
data = games.fetchall()

gamesids = {}
for game in data:
    if (game[2] == 'game'):
        gamesids[game[0]] = 1
        c.execute('''REPLACE INTO games VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', game)
db.commit()

cats = lc.execute('''SELECT * FROM categories''')
data = cats.fetchall()

for cat in data:
    if gamesids.get(cat[0]):
        c.execute('''REPLACE INTO categories VALUES(%s,%s,%s)''', cat)
db.commit()

gens = lc.execute('''SELECT * FROM categories''')
data = gens.fetchall()

for gen in data:
    if gamesids.get(gen[0]):
        c.execute('''REPLACE INTO genres VALUES(%s,%s,%s)''', gen)
db.commit()

movs = lc.execute('''SELECT * FROM movies''')
data = movs.fetchall()

for mov in data:
    if gamesids.get(mov[0]):
        c.execute('''REPLACE INTO movies VALUES(%s,%s,%s,%s,%s,%s)''', mov)
db.commit()

pics = lc.execute('''SELECT * FROM screenshots''')
data = pics.fetchall()

for pic in data:
    if gamesids.get(pic[0]):
        c.execute('''REPLACE INTO screenshots VALUES(%s,%s,%s,%s)''', pic)
db.commit()