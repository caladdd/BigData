from mrjob.job import MRJob
import numpy as np

class EmpleadosPromEmpl(MRJob):

    def mapper(self, _, line):

        empleado = line.split(",")
        yield empleado[1], int(empleado[2])

    def reducer(self, key, values):
        lista = list(values)
        #print(np.mean(lista))                                                                                                                        
        yield key, sum(lista)/len(lista)

if __name__ == '__main__':
    EmpleadosPromEmpl.run()


