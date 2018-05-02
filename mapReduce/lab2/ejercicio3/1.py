from mrjob.job import MRJob
import numpy as np

class PelVista(MRJob):

    def mapper(self, _, line):

        pelicula = line.split(",")
        temp = [pelicula[1], float(pelicula[2])]
        yield pelicula[0], temp

    def reducer(self, key, values):
        calificacion = 0
        numpel = 0
        for va in values:
            calificacion = calificacion + va[1] 
            numpel = numpel + 1
        yield key, "numero peliculas= "+str(numpel)+" prom rating= "+ str(calificacion/numpel)


if __name__ == '__main__':
    PelVista.run()
