from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np


class MejorPeorMovie(MRJob):
    def steps(self):
        return[
            MRStep(mapper = self.mapper1,
                   reducer = self.reducer1),
            MRStep(reducer = self.reducer2)
        ]
    
    def mapper1(self, _, line):
        pelicula = line.split(",")
        yield pelicula[0], (float(pelicula[2]), pelicula[3])
        
    def reducer1(self, key, values):
        acum = 0
        genero = ''
        cont = 0
        for va in values:
            cont = cont + 1
            acum = acum + va[0]
            genero = va[1]

        yield genero, (acum/cont) 

    def reducer2(self, key, values):
        mini = float("inf")
        maxi = 0
        for va in values:
            if mini > va:
                mini = va
            if maxi < va:
                maxi = va
                
        yield key, "maximo rating "+str(maxi)+" minimo rating "+str(mini)


if __name__ == '__main__':
    MejorPeorMovie.run()
