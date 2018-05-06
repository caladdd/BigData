movie = LOAD '/user/jcaladh/datasets/peliculas.csv' using PigStorage(',') AS (user_id:int, movie_id:int, genero:chararray, rating:float, date:chararray);
g_movie = GROUP movie by date;
movieV =  foreach g_movie generate group, COUNT(movie.movie_id) AS (cont);
gruall = GROUP movieV ALL;
max_dia = FOREACH gruall GENERATE movieV, MAX(movieV.cont);
DUMP max_dia;