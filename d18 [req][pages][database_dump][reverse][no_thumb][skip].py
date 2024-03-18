import requests
from bs4 import BeautifulSoup
import sqlite3

# d18 [req][pages][database_dump][reverse][no_thumb][skip]
#
# Scrape data18 all scenes (title, url, movie, image, scene key, cast, trailer, studio, group, network, site, web)
# steps = loop = extract data + insert scene data /for not existing/ + insert cast + insert cateogry data
# functions = create_connection + extract_data + insert_scene + insert_categories + insert_cast + link_scene_cast
###### w/out THUMBNAIL extraction + SKIP if already exist ######


# Function to establish a database connection
def create_connection(db_file):
  conn = None
  try:
    conn = sqlite3.connect(db_file)
  except sqlite3.Error as e:
    print(e)
  return conn


# Function to insert scene data into the Scenes table
def insert_scene(conn, scene):
  scene_key = scene['scene_key']

  # Check if the scene with the same key already exists
  cur = conn.cursor()
  cur.execute("SELECT id FROM Scenes WHERE scene_key = ?", (scene_key, ))
  existing_scene = cur.fetchone()
  if existing_scene:
    print(f"Scene with key {scene_key} already exists. Skipping insertion.")
    return existing_scene[0]  # Return the ID of the existing scene

  # If the scene does not exist, insert it into the Scenes table
  sql = ''' INSERT INTO Scenes(scene_key, date, title, url, trailer, duration, description, scene_number, movie_id)
              VALUES(?,?,?,?,?,?,?,?,?) '''
  cur.execute(sql,
              (scene['scene_key'], scene['date'], scene['scene_title'],
               scene['scene_url'], scene['scene_trailer'],
               scene.get('duration', None), scene.get('description', None),
               scene.get('scene_number', None), scene.get('movie_id', None)))
  return cur.lastrowid


# Function to insert categories into their respective tables and link them to scenes
def insert_categories(conn, scene_id, categories):
  cur = conn.cursor()
  for category, value in categories.items():
    if value:
      # Check the category name and insert into the appropriate table
      if category == 'studio':
        # Insert into Studios table if not already present
        studio_name = value['title']
        cur.execute("SELECT id FROM Studios WHERE studios_name = ?",
                    (studio_name, ))
        row = cur.fetchone()
        if row:
          studio_id = row[0]
        else:
          cur.execute("INSERT OR IGNORE INTO Studios(studios_name) VALUES(?)",
                      (studio_name, ))
          studio_id = cur.lastrowid

        # Check if the link between scene and studio already exists
        cur.execute(
          "SELECT * FROM SceneStudios WHERE scene_id = ? AND studios_id = ?",
          (scene_id, studio_id))
        existing_link = cur.fetchone()
        if not existing_link:
          cur.execute(
            "INSERT INTO SceneStudios(scene_id, studios_id) VALUES(?, ?)",
            (scene_id, studio_id))

      elif category == 'group':
        # Insert into Groups table if not already present
        group_name = value['title']
        cur.execute("SELECT id FROM Groups WHERE groups_name = ?",
                    (group_name, ))
        row = cur.fetchone()
        if row:
          group_id = row[0]
        else:
          cur.execute("INSERT OR IGNORE INTO Groups(groups_name) VALUES(?)",
                      (group_name, ))
          group_id = cur.lastrowid

        # Check if the link between scene and group already exists
        cur.execute(
          "SELECT * FROM SceneGroups WHERE scene_id = ? AND groups_id = ?",
          (scene_id, group_id))
        existing_link = cur.fetchone()
        if not existing_link:
          cur.execute(
            "INSERT INTO SceneGroups(scene_id, groups_id) VALUES(?, ?)",
            (scene_id, group_id))

      elif category == 'network':
        # Insert into Networks table if not already present
        network_name = value['title']
        cur.execute("SELECT id FROM Networks WHERE networks_name = ?",
                    (network_name, ))
        row = cur.fetchone()
        if row:
          network_id = row[0]
        else:
          cur.execute(
            "INSERT OR IGNORE INTO Networks(networks_name) VALUES(?)",
            (network_name, ))
          network_id = cur.lastrowid

        # Check if the link between scene and network already exists
        cur.execute(
          "SELECT * FROM SceneNetworks WHERE scene_id = ? AND networks_id = ?",
          (scene_id, network_id))
        existing_link = cur.fetchone()
        if not existing_link:
          cur.execute(
            "INSERT INTO SceneNetworks(scene_id, networks_id) VALUES(?, ?)",
            (scene_id, network_id))

      elif category == 'site':
        # Insert into Sites table if not already present
        site_name = value['title']
        cur.execute("SELECT id FROM Sites WHERE sites_name = ?", (site_name, ))
        row = cur.fetchone()
        if row:
          site_id = row[0]
        else:
          cur.execute("INSERT OR IGNORE INTO Sites(sites_name) VALUES(?)",
                      (site_name, ))
          site_id = cur.lastrowid

        # Check if the link between scene and site already exists
        cur.execute(
          "SELECT * FROM SceneSites WHERE scene_id = ? AND sites_id = ?",
          (scene_id, site_id))
        existing_link = cur.fetchone()
        if not existing_link:
          cur.execute(
            "INSERT INTO SceneSites(scene_id, sites_id) VALUES(?, ?)",
            (scene_id, site_id))

      elif category == 'webserie':
        # Insert into Webseries table if not already present
        webserie_name = value['title']
        cur.execute("SELECT id FROM Webseries WHERE webseries_name = ?",
                    (webserie_name, ))
        row = cur.fetchone()
        if row:
          webserie_id = row[0]
        else:
          cur.execute(
            "INSERT OR IGNORE INTO Webseries(webseries_name) VALUES(?)",
            (webserie_name, ))
          webserie_id = cur.lastrowid

        # Check if the link between scene and webserie already exists
        cur.execute(
          "SELECT * FROM SceneWebseries WHERE scene_id = ? AND webseries_id = ?",
          (scene_id, webserie_id))
        existing_link = cur.fetchone()
        if not existing_link:
          cur.execute(
            "INSERT INTO SceneWebseries(scene_id, webseries_id) VALUES(?, ?)",
            (scene_id, webserie_id))

  conn.commit()


def insert_cast(conn, cast):
  inserted_cast_ids = []
  cur = conn.cursor()
  for actor_name in cast:
    # Check if the cast member already exists in the Cast table
    cur.execute("SELECT id FROM Cast WHERE actor_name = ?", (actor_name, ))
    existing_cast = cur.fetchone()
    if existing_cast:
      cast_id = existing_cast[0]
    else:
      # If the cast member does not exist, insert it into the Cast table
      cur.execute("INSERT INTO Cast(actor_name) VALUES (?)", (actor_name, ))
      cast_id = cur.lastrowid
    inserted_cast_ids.append(cast_id)
  conn.commit()
  return inserted_cast_ids


def link_scene_cast(conn, scene_id, cast_ids):
  cur = conn.cursor()
  for cast_id in cast_ids:
    # Check if the link already exists in the SceneCast table
    cur.execute("SELECT * FROM SceneCast WHERE scene_id = ? AND cast_id = ?",
                (scene_id, cast_id))
    existing_link = cur.fetchone()
    if not existing_link:
      # If the link does not exist, insert it into the SceneCast table
      cur.execute("INSERT INTO SceneCast(scene_id, cast_id) VALUES (?, ?)",
                  (scene_id, cast_id))
  conn.commit()


# Function to extract data from HTML content
def extract_data(html_content):
  soup = BeautifulSoup(html_content, 'html.parser')
  extracted_data = []

  # Find all div elements with class "boxep1"
  scenes_container = soup.find_all('div', class_='boxep1')

  for container in scenes_container:
    # Find all div elements within the container representing each scene
    scenes = container.find_all(
      'div', id=lambda value: value and value.startswith('item'))

    for scene in scenes:
      scene_info = {}
      # Extract the date from the div, excluding the <b> tag
      date_div = scene.find('div', class_='genmed')
      date = ''.join(date_div.find_all(string=True, recursive=False)).strip()
      scene_info['date'] = date

      scene_key = scene.find('b').text.strip('#')
      scene_info['scene_key'] = scene_key

      # thumbnail = scene.find('img', class_='yborder')['src']
      # scene_info['thumbnail'] = thumbnail

      scene_title = scene.find('a', class_='gen12 bold').text.strip()
      scene_info['scene_title'] = scene_title

      scene_trailer = scene.find('a', title=True)['href']
      scene_info['scene_trailer'] = scene_trailer

      scene_url = scene.find('a', class_='gen12 bold')['href']
      scene_info['scene_url'] = scene_url

      cast = ', '.join(a.text for a in scene.find_all(
        'a', href=lambda value: value and '/name/' in value))
      scene_info['cast'] = cast

      # Extract the categories using the provided logic
      categories_info = {}
      for p in scene.find_all('p'):
        text = p.get_text(strip=True)
        if text.startswith('Site:') or text.startswith(
            'Studio:') or text.startswith('Webserie:') or text.startswith(
              'Group:') or text.startswith('Network:'):
          key = text.split(':')[0]
          link = p.find('a')
          if link:
            title = link.text.strip()
            href = link.get('href')
            categories_info[key.lower()] = {'title': title, 'link': href}
          else:
            categories_info[key.lower()] = None

      scene_info['categories'] = categories_info

      extracted_data.append(scene_info)

  return extracted_data


# Establish database connection
conn = create_connection('data18.db')

base_url = "https://www.data18.com/sys/page.php?t=1&b=2&o=0&html=index&html2=&total=460650&doquery=1&spage={}&dopage=1"

start_page = 1
end_page = 50

# Loop through pages in reverse order
for page in range(end_page, start_page - 1, -1):
  url = base_url.format(page)
  headers = {
    'Accept': 'text/html, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Cookie':
    'data_user=MA-en-1; data_user_nav_pages=t%3D1%26b%3D6%26html%3Dindex; _ga_S3JKNGV0BY=GS1.1.1705258250.2.0.1705258250.0.0.0; _ga_JHZZQZ7GBL=GS1.1.1705258251.2.0.1705258251.0.0.0; data_user_navigation=1; data_user_functions=0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-1-0-0-0-0-0-0-0-0; data_user_nav_custom=t%3D1%26b%3D2%26html%3Dindex%26FILTER%26spage%3D3%26; data_user_functions=0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-1-0-0-0-0-0-0-0-0; data_user_nav_custom=t%3D1%26b%3D2%26html%3Dindex; data_user_navigation=1',
    'Pragma': 'no-cache',
    'Referer': 'https://www.data18.com/scenes',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua':
    '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"'
  }

  response = requests.get(url, headers=headers)

  if response.status_code == 200:
    html_content = response.text
    extracted_data = extract_data(html_content)
    # Reverse the order of scenes within each page
    extracted_data.reverse()
    for scene in extracted_data:
      # Check if the scene URL already exists in the database
      cur = conn.cursor()
      cur.execute("SELECT id FROM Scenes WHERE url = ?",
                  (scene['scene_url'], ))
      existing_scene = cur.fetchone()
      if existing_scene:
        print(f"Skipping: {scene['scene_url']}")
      else:
        scene_id = insert_scene(conn, scene)
        insert_categories(conn, scene_id, scene['categories'])
        # Extract and insert cast
        cast = scene.get('cast', '').split(', ')
        cast_ids = insert_cast(conn, cast)
        link_scene_cast(conn, scene_id, cast_ids)
        # print("Inserted scene with URL", scene['scene_url'])
    print("Inserted data from page", page)
  else:
    print("Failed to fetch data from the URL:", url)

# Close the database connection
if conn:
  conn.close()
