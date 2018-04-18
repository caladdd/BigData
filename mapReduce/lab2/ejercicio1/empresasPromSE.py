from mrjob.job import MRJob
import numpy as np

class EmpleadosPromSE(MRJob):

    def mapper(self, _, line):

        empleado = line.split(",")
        yield empleado[0], int(empleado[2])

    def reducer(self, key, values):
        lista = list(values)
        #print(np.mean(lista))
        yield key, sum(lista)/len(lista)

if __name__ == '__main__':
    EmpleadosPromSE.run()

