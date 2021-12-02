import os
import json

import database
import movies_data

APP_DIR = os.path.dirname(os.path.abspath(__file__))


def main():
    with open(os.path.join(APP_DIR, 'config.json')) as f:
        config = json.load(f)

    database.build_database(config)

    movies_data.retrieve_dataset(config['data_url'], path=os.path.join(APP_DIR, 'data'))
    meta_file = os.path.join(APP_DIR, 'data', 'movies_metadata.csv')

    df = movies_data.read_and_clean_dataset(filename=meta_file)

    database.write_df_to_sql(config, movies_data.make_movies_df(df), 'movies_consolidated')
    database.write_df_to_sql(config, movies_data.make_studios_df(df), 'production_companies')
    database.write_df_to_sql(config, movies_data.make_genres_df(df), 'genres')


if __name__ == "__main__":
    main()