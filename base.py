import argparse
import logging
import apache_beam as beam
import re
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='',
                        default='',
                        help='')
    parser.add_argument('--output',
                        dest='',
                        default='',
                        help='')
    known_args, pipeline_args = parser.parse_known_args()

    def process_csv(row):
        string = str(row)
        string = string + 'nunu'
        return string

    def get_value(element):
        line = element.split('"')
        return line[0]

    def parse_method(string_input):
        row = {'a': string_input}
        return row
