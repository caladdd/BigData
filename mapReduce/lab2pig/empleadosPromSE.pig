emplead = LOAD '/user/jcaladh/datasets/empleados.csv' using PigStorage(',') AS (SE:int,Id_empleado:int,salario:float,año:chararray);
group = GROUP empleados by empleado.SE;
avg = FOREACH group generate AVG(empleado.salario);
DUMP avg