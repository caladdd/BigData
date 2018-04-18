from mrjob.job import MRJob
import numpy as np

class Acciones(MRJob):

    def mapper(self, _, line):

	empresas = line.split(",")
	temp = [empresas[2],float(empresas[1])]
	yield empresas[0], temp

    def reducer(self, key, values):
	lista = list(values)
        lista.sort()
        maxi = 0
        boo = True
        for c in lista:
            if float(maxi) <= c[1]:
                maxi = c[1]
            else:
                boo = False
        if boo == True:
	    yield "accion que ha subido o se mantiene ",key

if __name__ == '__main__':
    Acciones.run()


