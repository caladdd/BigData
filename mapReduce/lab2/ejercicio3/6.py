from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np

class MejorEval(MRJob):
    def steps(self):
        return[
            MRStep(mapper = self.mapper1,
                   reducer = self.reducer1),
            MRStep(reducer = self.reducer2)
        ]
    
    def mapper1(self, _, line):
        pelicula = line.split(",")
        yield pelicula[4], float(pelicula[2])
        
    def reducer1(self, key, values):
        acum = 0
        cont = 0
        for v in values:
            acum = acum + v
            cont = cont + 1
        yield  None, (key, acum / cont)

    def reducer2(self, key, values):
        maxi = 0
        key = " "
        for v in values:
            if maxi < v[1]:
                maxi = v[1]
                key = v[0]
        yield key, maxi


if __name__ == '__main__':
    MejorEval.run()

