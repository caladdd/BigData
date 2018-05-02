from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np


class MinPelvisDay(MRJob):
    def steps(self):
        return[
            MRStep(mapper = self.mapper1,
                   combiner = self.combiner1,
                   reducer = self.reducer1),
            MRStep(reducer = self.reducer2)
        ]
    
    def mapper1(self, _, line):
        pelicula = line.split(",")
        yield pelicula[4], 1

    def combiner1(self, key, values):
        yield key, sum(values)
        
    def reducer1(self, key, values):
        yield None, (key, sum(values))

    def reducer2(self, key, values):
        mini = float("inf")
        date = ''
        for va in values:
            if mini > va[1]:
                mini = va[1]
                date = va[0]
                
        yield date, mini


if __name__ == '__main__':
    MinPelvisDay.run()
