# GUILD DATA ENGINEERING PRE-HIRE PROJECT
This is a prehire project performed by krishna for the role Senior Data Engineer at Guild Education.<br>
The data used for this project will be The Movies Dataset (pulled from https://www.kaggle.com/rounakbanik/the-movies-dataset) provided at https://s3-us-west-2.amazonaws.com/com.guild.us-west-2.public-data/project-data/the-movies-dataset.zip

### Deliverables
#### Data model that can be used to answer below questions
Production Company Details:budget per year<br>
Production Company Details:revenue per year<br>
Production Company Details:profit per year<br>
Production Company Details:releases by genre per year<br>
Production Company Details:average popularity of produced movies per year<br>
Movie Genre Details:most popular genre by year<br>
Movie Genre Details:budget by genre by year<br>
Movie Genre Details:revenue by genre by year<br>
Movie Genre Details:profit by genre by year<br>

#### Code
Python program that transforms the input data into a form usable by the data model<br>

#### Discussion on scaing decisons



### Execution
Data Modeling Decisons<br>

The idea is to consolidate the movies_metadata.csv file into multiple sub tables as there are many to many relatioships between movies&genre and movies&production_companies.
I used id(movie_id) acting as the foriegn key to establish the relationships. Below is the simple model faciliitates for running the given aggregation queries easily and for the requested reports.

![simple_data_model](https://user-images.githubusercontent.com/95439131/144490646-a423101f-63e6-43d6-aa04-21ee8e8eaeb5.PNG)<br>

For the production company aggregation the following query should be used:<br>

SELECT<br> 
    EXTRACT(year FROM movie.released) AS yyyy,<br>
    company.company_name,<br>
    SUM(movie.budget) AS total_budget,<br>
    SUM(movie.revenue) AS total_revenue,<br>
    total_revenue - total_budget as profit,<br>
    AVG(movie.popularity)<br>
FROM production_companies AS company<br> 
LEFT JOIN movies_consolidated AS movie ON company.movie_id = movie.movie_id<br>
GROUP BY yyyy, company.company_name<br>
ORDER BY company.company_name ASC, yyyy ASC;<br>

SELECT<br>
    EXTRACT(year from movie.released) as yyyy,<br>
    company.company_name,<br>
    genre.genre_name,<br>
    COUNT(movie.released)<br>
FROM production_companies AS company<br>
LEFT JOIN movies_consolidated AS movie on company.movie_id = movie.movie_id<br>
LEFT JOIN genres AS genre ON company.movie_id = genre.id<br>
GROUP BY yyyy, company.company_name, genre.genre_name<br>
ORDER BY company.company ASC, yyyy ASC, genre.genre_name ASC<br>

The aggregation of the genre information is similar to the first query taking the form of:<br>

SELECT<br> 
    EXTRACT(year FROM movie.released) AS yyyy,<br>
    genre.genre_name,<br>
    AVG(movie.popularity) AS genre_popularity,<br>
    SUM(movie.budget) AS total_budget,<br>
    SUM(movie.revenue) AS total_revenue,<br>
    total_revenue - total_budget as profit<br>
FROM genres AS genre<br>
LEFT JOIN movies_metadata AS movie ON genre.movie_id = movie.movie_id<br>
GROUP BY yyyy, genre.genre_name<br>
ORDER BY yyyy ASC, genre_popularity DESC;<br>



### Implementation
The ingestion process I have implemented for testing purposes is simply loading the csv directly into pandas. This is convenient for a number of purposes. In a real world example it is the basis for performing the Exploratory Data Analysis (EDA) of our data to understand what quirks we are putting into our database. It also has a really robust read_csv function. Finally, the sqlalchemy library provides a very simple and easy to use to_sql() method allowing for us to directly deliver the data to our postgres instance.
<br>
That said, there are some challenges to the dataset. For many movies the recorded budget/revenue. There are challenges with parsing the csv - some records have special characters which break the recod. And there is also the occasional GATORADE to handle - fortunately some of us are somewhat thirsty. For now, a minimal data cleaning approach is taken which handles a number of these issues.
<br>
The postgres instance is stood up on the localhost using docker. In practice, there is no reason to bind it specifically to localhost - but docker makes it (mostly) infinitely portable.
<br>
To run have docker and python3 installed; then<br>
> sudo chmod 754 launch_postgres.sh<br>
> launch_postgres.sh<br>
> cd python<br>
> source ./.venv/bin/activate<br>
> python main.py<br>


### Design
The above architecutre is the simple solution of implementing the project, however assuming the following factors I'll go with more of a modern architecture with the microservice domain model.
Propose solutions for an 100x increase in data volume, and an hourly update cadence
Propose ideas for data reprocessing:
How would you go about backfilling 1 year worth of data?
How would you avoid impact on the production flow (e.g. concurrent job runs)?
What kind of error handling would you put in place?


![Screenshot 2021-12-02 at 00-10-21 The Visual Workspace Whimsical(1)](https://user-images.githubusercontent.com/95439131/144493656-130db051-4ebf-4158-b832-7f30e722ef8f.png)

* Propose solutions for an 100x increase in data volume, and an hourly update cadence<br>
    Linear scalability/elastic scaling afforded by event sourcing design   
    Hourly aggregate built into domain model, could be shortened or made real time due to evnet sourcing domain model<br>

* Propose ideas for data reprocessing:<br>
    Event sourcing uses kappa architecture for reprocessing data with new transformations from an immutable audit log<br>
    
* How would you go about backfilling 1 year worth of data?<br>
    Kappa architecture would read an event streamed immutable audit log form a list of batch files, using the unified batch/streaming model exemplified in todays streaming model architectures<br>
The third and current generation data platforms are more or less similar to the previous generation, with a modern twist towards<br>
(a) streaming for real-time data availability with architectures such as Kappa,<br> 
(b) unifying the batch and stream processing for data transformation with frameworks such as Apache Beam, as well as<br>
(c) fully embracing cloud based managed services for storage, data pipeline execution engines and machine learning platforms.<br> 

* How would you avoid impact on the production flow (e.g. concurrent job runs)?<br>
    Concurrency is natural to an event sourcing pipeline due to the data model segregating commands and queries in the domain driven design based domain model<br>
    The same concurrency with keyed data streams in Kafka or partitions/shards/prefixes in a database/object store can be applied to make data pipelines distributed and fault tolerant, relying on Pub/Sub queue like Kafka for message delivery semantics and data/object store for correctness guarantees<br>

* What kind of error handling would you put in place?<br>
    Data quality validation exception handling, deserialization exception handling, etc.<br>

