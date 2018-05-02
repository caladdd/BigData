from mrjob.job import MRJob
import numpy as np

class BlackDay(MRJob):

    def mapper(self, _, line):

        empresas = line.split(",")
        temp = [float(empresas[1]),empresas[2]]
        yield empresas[0], temp

    def reducer(self, key, values):
        fecha = ""
        anterior = 0
        for v in values:
            if anterior > 0 and v[0] < anterior:
                break
            anterior = v[0]
            fecha = v[1]

        yield key, "fecha "+ fecha


if __name__ == '__main__':
    BlackDay.run()
