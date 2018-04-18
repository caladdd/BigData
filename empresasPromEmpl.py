from mrjob.job import MRJob
import numpy as np

class EmpresasPromEmpl(MRJob):

    def mapper(self, _, line):

        empresas = line.split(",")
        yield empresas[1], int(empresas[2])

    def reducer(self, key, values):
        cont = 0
        suma = 0
        for c in values:
            suma+=c
            cont+=1
	#print(np.mean(values))                                                                                                                       
        yield key, suma/cont

if __name__ == '__main__':
    EmpresasPromEmpl.run()


