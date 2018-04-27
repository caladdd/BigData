from mrjob.job import MRJob
import numpy as np

class BlackDay(MRJob):

    def mapper(self, _, line):

        empresas = line.split(",")
        temp = [float(empresas[1]),empresas[2]]
        yield empresas[0], temp

    def reducer(self, key, values):
        lista = list(values)
        print(lista)
        maxi = max(lista)
        mini = min(lista)
        yield key, "max= "+str(maxi[1])+" min= "+str(mini[1])


if __name__ == '__main__':
    BlackDay.run()
