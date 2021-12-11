# DROP TABLES
from typing import List, Tuple, Union

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES


def create_sql_str_create_table(table_name:str, column_names: List, column_datatypes: List) -> str:
    """This function allows to generate sql query to create a table."""
    if len(column_names) != len(column_datatypes):
        raise ValueError("The number of columns should be the same as the number of datatypes for a given table.")
    column_to_datatype = [
        f"{column_name} {column_type}"
        for column_name, column_type in zip(column_names,column_datatypes)
    ]
    output_query = f"""CREATE TABLE IF NOT EXISTS {table_name}
                    ({r", ".join(column_to_datatype)});"""
    return output_query


columns_songplay = "songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent".split(", ")
types = ["SERIAL PRIMARY KEY", "BIGINT", "INT", "VARCHAR", "VARCHAR", "VARCHAR", "INT", "VARCHAR", "VARCHAR"]
songplay_table_create = create_sql_str_create_table("songplays", columns_songplay, types)

columns_users = "user_id, first_name, last_name, gender, level".split(", ")
types = ["SERIAL PRIMARY KEY", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"]
user_table_create = create_sql_str_create_table("users", columns_users, types)

columns_songs = "song_id, title, artist_id, year, duration".split(", ")
types = ["VARCHAR PRIMARY KEY", "VARCHAR", "VARCHAR", "INT", "DECIMAL(10,6)"]
song_table_create = create_sql_str_create_table("songs", columns_songs, types)

columns_artists = "artist_id, name, location, latitude, longitude".split(", ")
types = ["VARCHAR PRIMARY KEY", "VARCHAR", "VARCHAR", "DECIMAL(10,6)", "DECIMAL(10,6)"]
artist_table_create = create_sql_str_create_table("artists", columns_artists, types)

columns_time = "start_time, hour, day, week, month, year, weekday".split(", ")
types = ["TIMESTAMP  PRIMARY KEY", "INT", "INT", "INT", "INT", "INT", "INT"]
time_table_create = create_sql_str_create_table("time", columns_time, types)

# INSERT RECORDS


def create_sql_insert_query(table_name: str, column_info: Union[dict, List], have_column_values = False) -> Tuple[str, Tuple]:
    """This function generates sql query for inserting new values into a table."""
    placeholders = ", ".join(["%s"]*len(column_info))
    if have_column_values:
        column_names = ", ".join(column_info.keys())
        column_values = tuple(column_info.values())
    else:
        column_names = ", ".join(column_info)
        column_values = None

    output_insert_query = f"""
        INSERT INTO {table_name}
        ({column_names})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING;;
    """
    return output_insert_query, column_values


songplay_table_insert, _ = create_sql_insert_query("songplays", columns_songplay)

user_table_insert, _ = create_sql_insert_query("users", columns_users)

song_table_insert, _ = create_sql_insert_query("songs", columns_songs)

artist_table_insert, _ = create_sql_insert_query("artists", columns_artists)

time_table_insert, _ = create_sql_insert_query("time", columns_time)


# FIND SONGS

song_select = ("""SELECT s.song_id, s.artist_id
    FROM songs s
    JOIN artists a ON s.artist_id = a.artist_id
    WHERE s.title = %s AND a.name = %s AND s.duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create,
                        artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
