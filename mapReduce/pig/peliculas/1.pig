movie = LOAD '/user/jcaladh/datasets/peliculas.csv' using PigStorage(',') AS (user_id:int, movie_id:int, rating:float, genero:chararray, date:chararray);
g_movie = GROUP movie by user_id;
movieV =  foreach g_movie generate group, (COUNT(movie.movie_id), AVG(movie.rating));
DUMP movieV;
STORE movieV INTO '/user/jcaladh/datasets/pig/output/pel1' USING PigStorage(',');