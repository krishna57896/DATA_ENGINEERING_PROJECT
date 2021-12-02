import os
import sys
import requests
import logging
import zipfile
import ast
import numpy as np
import pandas as pd


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
USE_CNTLM = False
APP_DIR = os.path.dirname(os.path.abspath(__file__))


proxies = {}
if USE_CNTLM:
    proxies = {
        'http': 'http://127.0.0.1:3128',
        'https': 'http://127.0.0.1:3128'
    }


class BadFileTypeException(Exception):
    pass


def stream_download_with_progress(outfile, response):
    length_received = 0
    total_length = int(response.headers.get('content-length'))
    for d in response.iter_content(chunk_size=4096):
        length_received += len(d)
        outfile.write(d)
        done = int(50 * length_received / total_length)
        sys.stdout.write(f'\r[{"="*done}{" "*(50-done)}]')
        sys.stdout.flush()


def retrieve_dataset(uri, path='data'):
    '''
    default behavior is to download the dataset and extract to a 'data'
    directory on the pwd
    '''
    def unzip(p, of):
        with zipfile.ZipFile(of) as zf:
            zf.extractall(os.path.join(APP_DIR, p))

    _, src_file = os.path.split(uri)
    _, ext = os.path.splitext(src_file)
    out_file = os.path.join(path, src_file)

    if os.path.isfile(out_file):
        unzip(path, out_file)
        return

    if not os.path.isdir(path):
        os.mkdir(path)

    if ext != '.zip':
        logging.error('movies_data.py is only configured for *.zip files')
        raise BadFileTypeException('requested non *.zip data')

    with open(out_file, 'wb') as zip_file:
        zip_response = requests.get(uri, proxies=proxies, stream=True)
        total_length = zip_response.headers.get('content-length')

        if total_length is None: 
            # for data without content-length headers...
            zip_file.write(zip_response.content)
        else:
            stream_download_with_progress(zip_file, zip_response)

    unzip(path, out_file)


def read_and_clean_dataset(filename='movies_metadata.csv'):
    def safe_json_eval(s):
        try:
            return ast.literal_eval(s)
        except:
            return []

    df = pd.read_csv(os.path.join(filename))
    # drop movies without title
    df.dropna(subset=["title"], inplace=True)
    # convert movielens id to integer, nonconforming to nan
    df["id"] = pd.to_numeric(df["id"], errors="coerce", downcast="integer")
    # convert release date to datetime and drop movies which don't have release date
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
    df.dropna(subset=["release_date"], inplace=True)
    # convert popularity to numeric
    df["popularity"] = pd.to_numeric(df["popularity"], errors='coerce')
    # convert budget to numeric, set 0 values to nan
    df["budget"] = pd.to_numeric(df["budget"], errors='coerce')
    df["budget"] = df["budget"].replace(0, np.nan)
    # convert budget to numeric, set 0 values to nan
    df["revenue"] = pd.to_numeric(df["revenue"], errors='coerce')
    df["revenue"] = df["revenue"].replace(0, np.nan)
    # handle null production companies and convert to list of dict
    df["production_companies"] = df["production_companies"].fillna("[]").apply(safe_json_eval)
    # handle null genres and convertdf.head to list of dicts
    df["genres"] = df["genres"].fillna("[]").apply(safe_json_eval)
    return df


def make_movies_df(df):
    movies_df = pd.DataFrame()
    movies_df["id"] = df["id"]
    movies_df["title"] = df["title"]
    movies_df["released"] = df["release_date"]
    movies_df["popularity"] = df["popularity"]
    movies_df["budget"] = df["budget"]
    movies_df["revenue"] = df["revenue"]
    movies_df["vote_average"] = df["vote_average"]
    movies_df["vote_count"] = df["vote_count"]
    return movies_df


def _process_json_data(jsonarray, field):
    # NB: jsonarray has already been processed to a list by ast
    if not isinstance(jsonarray, list):
        return []
    return [doc[field] for doc in jsonarray]


def _explode_column(df, col):
    return df.apply(lambda x: pd.Series(x[col]), axis=1) \
        .stack() \
        .reset_index(level=1, drop=True)


def make_studios_df(df):
    df['studios'] = df['production_companies'] \
        .apply(lambda x: _process_json_data(x, 'name'))
    df['studio_ids'] = df['production_companies'] \
        .apply(lambda x: _process_json_data(x, 'id'))

    studios_df = pd.DataFrame()
    studios_df['company_id'] = pd.to_numeric(_explode_column(df, 'studio_ids'), downcast="integer")
    studios_df = studios_df.join(df['id'])
    studios_df['company_name'] = _explode_column(df, 'studios')
    studios_df = studios_df.rename(columns={'id': 'movie_id'})

    return studios_df


def make_genres_df(df):
    df['genre_names'] = df['genres'] \
        .apply(lambda x: _process_json_data(x, 'name'))
    df['genre_ids'] = df['genres'] \
        .apply(lambda x: _process_json_data(x, 'id'))

    genres_df = pd.DataFrame()
    genres_df['genre_id'] = pd.to_numeric(_explode_column(df, 'genre_ids'), downcast="integer")
    genres_df = genres_df.join(df['id'])
    genres_df['genre'] = _explode_column(df, 'genre_names')
    genres_df = genres_df.rename(columns={'id': 'movie_id'})

    return genres_df


if __name__ == "__main__":
    uri = 'https://s3-us-west-2.amazonaws.com/com.guild.us-west-2.public-data/' \
        + 'project-data/the-movies-dataset.zip'

    logging.info(f'retrieving data from {uri}')

    retrieve_dataset(uri)
