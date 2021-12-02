CREATE TABLE IF NOT EXISTS movies_consolidated (
  movie_id integer PRIMARY KEY,
  title varchar(100),
  released date,
  popularity float,
  budget integer,
  revenue integer,
  vote_average float,
  vote_count integer
);

CREATE TABLE IF NOT EXISTS  genres (
  genre_id integer PRIMARY KEY,
  movie_id integer,
  genre varchar(50)
);

CREATE TABLE IF NOT EXISTS  production_companies (
  company_id integer PRIMARY KEY,
  movie_id integer,
  company_name varchar(50)
);

ALTER TABLE movie_genres ADD FOREIGN KEY (movie_id) REFERENCES movies_consolidated (movie_id);

ALTER TABLE production_companies ADD FOREIGN KEY (movie_id) REFERENCES movies_consolidated (movie_id);