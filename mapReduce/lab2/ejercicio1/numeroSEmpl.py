from mrjob.job import MRJob
import numpy as np

class EmpleadosPromSE(MRJob):

    def mapper(self, _, line):

	empleado = line.split(",")
	yield empleado[1], int(empleado[0])

    def reducer(self, key, values):
	lista = list(values)
        mset = set(lista)
	yield "Empleado " + key, "numeros de SE " + str(len(mset))

if __name__ == '__main__':
    EmpleadosPromSE.run()

