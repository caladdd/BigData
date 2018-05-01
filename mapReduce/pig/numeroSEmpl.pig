emplead = LOAD '/user/jcaladh/datasets/empleados.csv' using PigStorage(',') AS (SE:int, Id_empleado:int, salario:float, anho:chararray);
generat = FOREACH emplead GENERATE Id_empleado, SE;
group_emplead = GROUP generat by Id_empleado;
dump group_emplead
avgempl =  foreach group_emplead generate Count(emplead.SE);
DUMP avgempl;
