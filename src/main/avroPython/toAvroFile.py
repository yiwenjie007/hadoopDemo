# coding=utf-8
import avro.schema
from avro.io import DatumWriter
from avro.datafile import DataFileWriter

if __name__ == '__main__':
    schema = avro.schema.Parse(open('user.avsc', 'rb').read())
    writer = DataFileWriter(open('data_python.avro', 'wb'), DatumWriter(), schema)
    writer.append(
        {
            'left': 'L-python',
            'right': 'R-python'
        }
    )
    writer.close()