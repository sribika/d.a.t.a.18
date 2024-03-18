import sqlite3

# DATABASE creator

# Function to create database tables
def create_tables():
  conn = sqlite3.connect('data18.db')
  c = conn.cursor()

  # Create Scenes table
  c.execute('''CREATE TABLE IF NOT EXISTS Scenes (
                id INTEGER PRIMARY KEY,
                scene_key TEXT,
                date TEXT,
                title TEXT,
                page_title TEXT,
                url TEXT,
                thumbnail TEXT,
                trailer TEXT,
                duration TEXT,
                description TEXT,
                scene_number TEXT,
                episode_number TEXT,
                output_link TEXT,
                movie_title TEXT,
                movie_link TEXT,
                movie_id INTEGER,
                FOREIGN KEY (movie_id) REFERENCES Movies(id)
                )''')

  # Create Movies table
  c.execute('''CREATE TABLE IF NOT EXISTS Movies (
                id INTEGER PRIMARY KEY,
                title TEXT,
                description TEXT,
                movie_link TEXT,
                total_episodes TEXT,
                release_date TEXT,
                production_date TEXT,
                back_cover TEXT,
                front_cover TEXT,
                studio TEXT,
                movie_serie TEXT,
                movie_length TEXT
                )''')

  # Create a new table for the movie series
  c.execute('''CREATE TABLE IF NOT EXISTS Series (
                id INTEGER,
                serie_title TEXT,
                serie_link TEXT,
                total_movies TEXT,
                total_scenes TEXT
                )''')

  # Create a new table for the relationship between series and movies
  c.execute('''CREATE TABLE IF NOT EXISTS MovieSeries (
                    id INTEGER,
                    serie_id INTEGER,
                    FOREIGN KEY (id) REFERENCES Series(id),
                    FOREIGN KEY (serie_id) REFERENCES Movies(id)
                    )''')
  print("Table MovieSeries created successfully.")

  # Create Tags table
  c.execute('''CREATE TABLE IF NOT EXISTS Tags (
                id INTEGER PRIMARY KEY,
                tag_name TEXT UNIQUE
                )''')

  # Create Cast table
  c.execute('''CREATE TABLE IF NOT EXISTS Cast (
                id INTEGER PRIMARY KEY,
                actor_name TEXT UNIQUE
                )''')

  # Create the SceneTags table
  c.execute('''
        CREATE TABLE IF NOT EXISTS SceneTags (
            scene_id INTEGER,
            tag_id INTEGER,
            FOREIGN KEY (scene_id) REFERENCES Scenes(id),
            FOREIGN KEY (tag_id) REFERENCES Tags(id),
            PRIMARY KEY (scene_id, tag_id)
        )
    ''')

  # Create the SceneCast table
  c.execute('''
        CREATE TABLE IF NOT EXISTS SceneCast (
            scene_id INTEGER,
            cast_id INTEGER,
            FOREIGN KEY (scene_id) REFERENCES Scenes(id),
            FOREIGN KEY (cast_id) REFERENCES Cast(id),
            PRIMARY KEY (scene_id, cast_id)
        )
    ''')
  # Create a new table for the relationship between scenes and movies
  c.execute('''CREATE TABLE IF NOT EXISTS SceneMovieRelation (
                  scene_id INTEGER,
                  movie_id INTEGER,
                  FOREIGN KEY (scene_id) REFERENCES Scenes(id),
                  FOREIGN KEY (movie_id) REFERENCES Movies(id)
               )''')
  print("Table SceneMovieRelation created successfully.")

  # Create relational tables for categories
  categories = [
    'studios', 'groups', 'networks', 'sites', 'webseries', 'miniseries'
  ]
  for category in categories:
    # Create category table
    c.execute(f'''CREATE TABLE IF NOT EXISTS {category.capitalize()} (
                    id INTEGER PRIMARY KEY,
                    {category}_name TEXT UNIQUE
                    )''')

    # Create relational table for Scenes and current category
    c.execute(f'''CREATE TABLE IF NOT EXISTS Scene{category.capitalize()} (
                    scene_id INTEGER,
                    {category}_id INTEGER,
                    FOREIGN KEY (scene_id) REFERENCES Scenes(id),
                    FOREIGN KEY ({category}_id) REFERENCES {category.capitalize()}(id),
                    PRIMARY KEY (scene_id, {category}_id)
                    )''')

  conn.commit()
  conn.close()


# Run the function to create tables
create_tables()
print('Tables created successfully.')
