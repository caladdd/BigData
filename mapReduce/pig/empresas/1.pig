empresas = LOAD '/user/jcaladh/datasets/empresas.csv' using PigStorage(',') AS (empresa:chararray, valor:float, fecha:chararray);
g_empresa = GROUP empresas by empresa;
empresaV =  foreach g_empresa generate group, (MIN(empresas.valor),MAX(empresas.valor));
DUMP empresaV;