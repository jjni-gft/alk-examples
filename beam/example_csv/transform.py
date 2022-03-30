#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A word-counting workflow."""

# pytype: skip-file

# beam-playground:
#   name: WordCount
#   description: An example that counts words in Shakespeare's works.
#   multifile: false
#   pipeline_options: --output output.txt
#   context_line: 44
#   categories:
#     - Combiners
#     - Options
#     - Quickstart

import argparse
import logging
import typing

import apache_beam as beam
from apache_beam.io.fileio import MatchFiles, ReadMatches
from apache_beam.io import WriteToText, ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import csv

class User(typing.NamedTuple):
    id: int
    name: str
    last_name:str
    email: str
    email2: str
    profession: str

def read_csv(file):
    for line in csv.reader([file]):
        yield line

class MapProfession(beam.DoFn):

    def process(self, element, mapping):
        profession, count = element
        yield beam.pvalue.TaggedOutput("original", element)
        if profession in mapping:
            yield mapping[profession], count
        else:
            yield element


def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://dataflow-samples/shakespeare/kinglear.txt',
      help='Input file to process.')
  parser.add_argument(
      '--input2',
      dest='input2',
      default='gs://dataflow-samples/shakespeare/kinglear.txt',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:

    csv_1 = (p | 'Read' >> ReadFromText(known_args.input, skip_header_lines=1)
            | 'Parse CSV' >> beam.FlatMap(read_csv)
            # | 'Get content' >> beam.FlatMap(lambda x: x.read_utf8().split('\r\n'))
            | 'Convert to user' >> beam.Map(lambda x: User(*x)).with_output_types(User))

    csv_2 = (p | 'Read2' >> ReadFromText(known_args.input2, skip_header_lines=1)
            | 'Parse CSV2' >> beam.FlatMap(read_csv)
            # | 'Get content' >> beam.FlatMap(lambda x: x.read_utf8().split('\r\n'))
            | 'Convert to user2' >> beam.Map(lambda x: User(*x)).with_output_types(User))

    # side input - mapowanie kluczy
    mapping = p | beam.Create({"police officer": "policjant", "developer" : "programista"})

    counts, original = (
        (csv_1, csv_2)
        | 'Flatten' >> beam.Flatten()
        | 'CreateKey' >> beam.Map(lambda x: (x.profession, x))
        | 'Count' >> beam.combiners.Count.PerKey()
        | 'Map names' >> beam.ParDo(MapProfession(), mapping = beam.pvalue.AsDict(mapping))
        .with_outputs("original", main="mapped"))

    # Format the counts into a PCollection of strings.
    def format_result(elem):
       profession, count = elem
       return '%s: %d' % (profession, count)

    output = counts | 'Format' >> beam.Map(format_result)
    output_original = original | 'Format original' >> beam.Map(format_result)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | 'Write' >> WriteToText(known_args.output)
    output_original | 'Write original' >> WriteToText(known_args.output + "original.txt")

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
