# DROP TABLES
from typing import List, Tuple

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


columns = "songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent".split(", ")
types = ["SERIAL PRIMARY KEY", "INT", "INT", "VARCHAR", "INT", "INT", "INT", "VARCHAR", "VARCHAR"]
songplay_table_create = create_sql_str_create_table("songplays", columns, types)

columns = "user_id, first_name, last_name, gender, level".split(", ")
types = ["SERIAL PRIMARY KEY", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"]
user_table_create = create_sql_str_create_table("users", columns, types)

columns = "song_id, title, artist_id, year, duration".split(", ")
types = ["SERIAL PRIMARY KEY", "VARCHAR", "INT", "INT", "INT"]
song_table_create = create_sql_str_create_table("songs", columns, types)

columns = "artist_id, name, location, latitude, longitude".split(", ")
types = ["SERIAL PRIMARY KEY", "VARCHAR", "VARCHAR", "DECIMAL(10,6)", "DECIMAL(10,6)"]
artist_table_create = create_sql_str_create_table("artists", columns, types)

columns = "start_time, hour, day, week, month, year, weekday".split(", ")
types = ["TIMESTAMP  PRIMARY KEY", "INT", "INT", "INT", "INT", "INT", "INT"]
time_table_create = create_sql_str_create_table("time", columns, types)

# INSERT RECORDS


def create_sql_insert_query(table_name: str, column_to_value_data:dict) -> Tuple[str, Tuple]:
    """This function generates sql query for inserting new values into a table."""
    placeholders = ", ".join("?"*len(column_to_value_data))
    column_names = ", ".join(column_to_value_data.keys())
    column_values = tuple(column_to_value_data.values())
    output_insert_query = f"""
        INSERT INTO {table_name}
        ({column_names})
        VALUES ({placeholders});
    """
    return output_insert_query, column_values

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")


time_table_insert = ("""
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create,
                        artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
