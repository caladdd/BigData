emplead = LOAD '/user/jcaladh/datasets/empleados.csv' using PigStorage(',') AS (SE:int, Id_empleado:int, salario:float, anho:chararray);
group_emplead = GROUP emplead by Id_empleado;
dump group_emplead
avgempl =  foreach group_emplead { A = Distinct emplead.SE;
	   	   		   generate group, COUNT(A);
				   };
DUMP avgempl;
