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
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class CustomOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
      '--input',
      dest='input',
      default='gs://dataflow-samples/shakespeare/kinglear.txt',
      help='Input file to process.')
    parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
    parser.add_value_provider_argument(
      '--output_bq',
      dest='output_bq',
      required=True,
      help='Output BQ to write results to in format project_id:dataset.table_name')

class WordExtractingDoFn(beam.DoFn):
  """Parse each line of input text into words."""
  def process(self, element):
    """Returns an iterator over the words of this element.

    The element is a line of text.  If the line is blank, note that, too.

    Args:
      element: the element being processed

    Returns:
      The processed element.
    """
    return re.findall(r'[\w\']+', element, re.UNICODE)


def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  _, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  custom_options = pipeline_options.view_as(CustomOptions)  
  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:

    # Read the text file[pattern] into a PCollection.
    lines = p | 'Read' >> ReadFromText(custom_options.input)    

    counts = (
        lines
        | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
        | 'To Upper' >> beam.Map(lambda word: word.upper())
        | 'Create tupple' >> beam.Map(lambda x: (x, x))
        # alternatywne podejście (oryginalne)
        # | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.combiners.Count.PerKey()
        # alternatywne podejście (oryginalne)
        # | 'GroupAndSum' >> beam.CombinePerKey(sum)
        | 'Filter single occurence words' >> beam.Filter(lambda v: v[1] > 1))

    # Format the counts into a PCollection of strings.
    def format_result(word, count):
      return "%s: %d" % (word, count)
    
    def to_json(word, count):
      return {
          "word": word, 
          "count": count,
      }

    output_csv = counts | 'Format csv' >> beam.MapTuple(format_result)
    output_json = counts | 'Format json' >> beam.MapTuple(to_json)
    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output_csv | 'Write' >> WriteToText(custom_options.output)
    
    schema = ",".join({
        'word:STRING',
        'count:INTEGER'
    })
    output_json | 'Write to BQ' >> WriteToBigQuery(custom_options.output_bq, schema=schema, method="STREAMING_INSERTS")


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
