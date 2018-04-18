from mrjob.job import MRJob
import numpy as np

class MayorMenor(MRJob):

    def mapper(self, _, line):

	empresas = line.split(",")
	yield empresas[0], float(empresas[1])

    def reducer(self, key, values):
        cont =0
	mset = set(values)
	for v in mset:
            cont+=1
	yield key, cont

if __name__ == '__main__':
    MayorMenor.run()
