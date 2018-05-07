movie = LOAD '/user/jcaladh/datasets/peliculas.csv' using PigStorage(',') AS (user_id:int, movie_id:int, rating:float, genero:chararray, date:chararray);
g_movie = GROUP movie by genero;
movieV =  foreach g_movie generate group, AVG(movie.rating) AS (avgm);
gruall = GROUP movieV ALL;
min_max = FOREACH gruall GENERATE movieV, ( MIN(movieV.avgm), MAX(movieV.avgm));
DUMP min_max;
STORE min_max INTO '/user/jcaladh/datasets/pig/output/pel7' USING PigStorage(',');