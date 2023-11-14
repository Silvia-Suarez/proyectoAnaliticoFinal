import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd
import json

# Function to convert JSON lines to CSV string


def json_to_csv(line):
    # Parse each JSON line
    data = json.loads(line)
    # Convert JSON data to CSV string (example, modify as per your JSON structure)
    csv_data = ','.join([str(value) for value in data.values()])
    return csv_data

# Function to create Pandas DataFrame from PCollection


def create_dataframe(data):
    # Convert PCollection to a list
    data_list = data | "Collect as List" >> beam.combiners.ToList()

    # Create a Pandas DataFrame from the list
    df = pd.DataFrame(data_list)

    return df


# Set up pipeline options
options = PipelineOptions()
# options.view_as(StandardOptions).runner = 'DataflowRunner'  # If running on Dataflow

# Define your pipeline
with beam.Pipeline(options=options) as p:
    # Read data from three CSV files in GCS
    input_files = [
        "gs://transporte_grupo_4/CineColombia_peliculas.csv",
        "gs://transporte_grupo_4/CineColombia_usuarios.csv",
        "gs://transporte_grupo_4/CineColombia_visualizaciones.csv",
        "gs://transporte_grupo_4/CineMark_peliculas.csv",
        "gs://transporte_grupo_4/CineMark_usuarios.csv",
        "gs://transporte_grupo_4/CineMark_visualizaciones.csv",
        "gs://transporte_grupo_4/Procinal_peliculas.csv",
        "gs://transporte_grupo_4/Procinal_usuarios.csv",
        "gs://transporte_grupo_4/Procinal_visualizaciones.csv",
        "gs://transporte_grupo_4/UniCine_peliculas.json",
        "gs://transporte_grupo_4/UniCine_usuarios.json",
        "gs://transporte_grupo_4/UniCine_visualizaciones.json"
    ]

    # Read each CSV file and create a Pandas DataFrame
    dataframes = {}
    for file in input_files:
        if file.endswith('.json'):
            # Read the JSON file and transform it to CSV
            json_data = p | f"Read {file}" >> beam.io.ReadFromText(file)
            transformed_json = json_data | f"JSONToCSV {file}" >> beam.Map(
                json_to_csv)
            dataframes[file] = transformed_json | f"Create DataFrame {file}" >> beam.Map(
                create_dataframe)
        else:
            # Read each CSV file and create a Pandas DataFrame
            data = p | f"Read {file}" >> beam.io.ReadFromText(file)
            transformed_data = (
                data
                | f"SplitLines {file}" >> beam.Map(lambda line: line.split(','))
                | f"CombineFields {file}" >> beam.Map(lambda fields: ','.join(fields))
            )
            dataframes[file] = transformed_data | f"Create DataFrame {file}" >> beam.Map(
                create_dataframe)

    for file, dataframe in dataframes.items():
        if file.endswith('.json'):
            # For JSON file, write the DataFrame to CSV in GCS
            output_file = f"gs://cruda_grupo_4/{file.split('/')[-1].split('.')[0]}_output.csv"
        else:
            output_file = f"gs://cruda_grupo_4/{file.split('/')[-1]}_output.csv"

# python etl_pipeline_transport_raw_data.py ^
#   --project datalake-dev-399421 ^
#   --job_name etl-pipeline-transporte-1 ^
#   --temp_location gs://transporte_grupo_4/temp ^
#   --staging_location gs://transporte_grupo_4/staging ^
#   --runner DataflowRunner ^
#   --region us-east1
