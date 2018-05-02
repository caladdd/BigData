from mrjob.job import MRJob


class EmpleadosPromEmpl(MRJob):

    def mapper(self, _, line):
        empleado = line.split(",")
        yield empleado[1], int(empleado[2])

    def reducer(self, key, values):
        suma = 0
        cont = 0
        for val in values:
            suma = suma + val
            cont = cont + 1
        yield key, suma/cont


if __name__ == '__main__':
    EmpleadosPromEmpl.run()
