class ProductionHouse:
  def __init__(self):
    self.aggregate_production_studios()
 
  def aggregate_production_studios(self):
        # select distinct production studios cfrom credits  csv
        # cleanse names, insert into production studios table

  def _join_production_studio_to_movies(self, query_builder):
        # mutate query builder with join to Production tsudio from movie table
        $ return query builder

  def get_popularity(self, query_builder, group_by_expression=["year(movies.release_date)"]):
        group_by_expression.append("movies.production_studio_uid")
        query_builder = self._join_production_studio_to_movies(query_builder)
        # mutate query builder with addtional group by expressions (yearly)
        # mutate query builder by adding average of ratings column to select list
        # return query builder

  def get_revenue(self, group_by_expression=["year(movies.release_date)"]):
        group_by_expression.append("movies.production_studio_uid")
        query_builder = self._join_production_studio_to_movies(query_builder)
        # mutate query builder with addtional group by expressions (yearly)
        # mutate query builder by adding sum of revenue to select list
        # return query builder

class Genre:
  def __init__(self):
    self.aggregate_genres()
  #  normalizes genre dimension in case genre becomes multi level hierarchy in aggregations
  #  tracks it as an SCD
  def aggregate_genres(self):
        # select distinct genres from movies csv
        # cleanse genres, insert into Genres table

  def _join_genre_to_movies(self, query_builder):
        # mutate query builder with join to Genre from movie table
        # return query builder


  def get_popularity(self, group_by_expression=["year(movies.release_date)"]):
        group_by_expression.append("movies.genre")
        query_builder = self._join_genre_to_movies(query_builder)
        # return query builder with addtional group by expressions
        # add average of ratings column to select list

  def get_revenue(self, group_by_expression=["year(movies.release_date)"]):
        group_by_expression.append("movies.genre")
        query_builder = self._join_genre_to_movies(query_builder)
        # return query builder with addtional group by expressions
        # add sum of revenue to select list

class Movie:
  def __init__(self, name, budget, production_house, genre):
    self.name = name // natural key
        self.budget = budget
        self.production_house = production_house // natural key
        self.revenue = 0
        self.genre = genre
        # insert  movie into movies table based on natural key, or get UID if it exists

  def released(self, date):
        self.release_date = date

  def revenue_generated(self, revenue):
        self.revenue += revenue

  def rated(self, rating):
        # insert ratings into ratings table linked by movie ID

  def persist(self, sqldb):
        # update sql row with values

 
class Rating:
  def __init__(self,  reviewer, rating, updated_dt, movie_uid, ratings_scale_uid):
      # insert rating into ratings table
 
class RatingScale:
  def __init__(self, min, max):
      # insert rating scale into ratings scale table
~                                                                                                                                                                       
~                                                   
