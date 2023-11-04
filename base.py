import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Set up pipeline options
options = PipelineOptions()
# options.view_as(StandardOptions).runner = 'DataflowRunner'  # If running on Dataflow

# Define your pipeline
with beam.Pipeline(options=options) as p:
    # Read data from three CSV files in GCS
    input_files = [
        "gs://your-bucket/input-data/file1.csv",
        "gs://your-bucket/input-data/file2.csv",
        "gs://your-bucket/input-data/file3.csv"
    ]

    # Read multiple files and combine them into a single PCollection
    data = p | "ReadFiles" >> beam.io.ReadFromText(input_files)

    # Transformation: Split lines and combine fields
    transformed_data = (
        data
        # Split CSV by comma
        | "SplitLines" >> beam.Map(lambda line: line.split(','))
        # Combine fields back into CSV
        | "CombineFields" >> beam.Map(lambda fields: ','.join(fields))
    )

    # Write transformed data to GCS
    transformed_data | "WriteOutput" >> beam.io.WriteToText(
        "gs://your-bucket/output-data/transformed_output.txt")
