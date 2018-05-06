from mrjob.job import MRJob
from mrjob.step import MRStep
#import numpy as np

class NUserEM(MRJob):
    def steps(self):
        return[
            MRStep(mapper = self.mapper1,
                   reducer = self.reducer1)
        ]
    
    def mapper1(self, _, line):
        pelicula = line.split(",")
        yield pelicula[1], (pelicula[0], float(pelicula[2]))
        
    def reducer1(self, key, values):
        userM = list(values)
        acum = 0
        for a in userM:
            acum = acum + a[1]
        #print userM
        yield key, "numero de usuarios "+str(len(userM))+" rating "+str(acum/len(userM))

if __name__ == '__main__':
    NUserEM.run()
