movie = LOAD '/user/jcaladh/datasets/peliculas.csv' using PigStorage(',') AS (user_id:int, movie_id:int, rating:float, genero:chararray, date:chararray);
g_movie = GROUP movie by date;
movieV =  foreach g_movie generate group, COUNT(movie.movie_id) AS (cont);
gruall = GROUP movieV ALL;
max_dia = FOREACH gruall GENERATE movieV, MAX(movieV.cont);
DUMP max_dia;
STORE max_dia INTO '/user/jcaladh/datasets/pig/output/pel2' USING PigStorage(',');