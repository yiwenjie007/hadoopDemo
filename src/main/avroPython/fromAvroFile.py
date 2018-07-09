# coding=utf-8
from avro.io import DatumReader
from avro.datafile import DataFileReader

if __name__ == '__main__':
    reader = DataFileReader(open('data_python.avro', 'rb'), DatumReader())
    for data in reader:
        print('left is: %s' % data.get('left'))
        print('right is: %s' % data.get('right'))

    reader_java = DataFileReader(open('data.avro', 'rb'), DatumReader())
    for data in reader_java:
        print('left from java is: %s' % data.get('left'))
        print('right from java is: %s' % data.get('right'))