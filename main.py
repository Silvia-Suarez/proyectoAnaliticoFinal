# import apache_beam as beam
# import argparse
# import csv
# import json
# import re
# import os

# import pandas as pd

# from apache_beam.options.pipeline_options import PipelineOptions

# def main():
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--input")
#     parser.add_argument("--output")

#     our_args, beam_args = parser.parse_known_args()
    
#     run_pipeline(our_args, beam_args)

# def preprocess_json(json_string):
#     # Corrige el formato del JSON reemplazando comillas simples con comillas dobles
#     corrected_json = re.sub(r"'(.*?)':", r'"\1":', json_string)
#     return corrected_json

# def json_to_csv(row):
#     # Convierte un diccionario JSON en una fila CSV
#     header = list(row.keys())
#     return [str(row.get(column, '')) for column in header]

# def run_pipeline(custom_args, beam_args):
#     entrada = custom_args.input
#     salida = custom_args.output

#     if entrada is None:
#         raise ValueError("El argumento --input debe especificarse correctamente")

#     opts = PipelineOptions(beam_args)

#     with beam.Pipeline(options=opts) as p:
#         lines = p | beam.io.ReadFromText(entrada)

#         # Corrige el formato del JSON
#         corrected_json = lines | beam.Map(preprocess_json)

#         # Convierte los datos JSON en filas CSV
#         csv_data = corrected_json | beam.Map(lambda x: ','.join(json_to_csv(json.loads(x))))

#         # Escribe los datos CSV en un archivo de salida
#         csv_data | beam.io.WriteToText(salida, file_name_suffix=".csv")

# if __name__ == "__main__":
#     main()

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import os

# Function to convert JSON lines to CSV string
def json_to_csv(line):
    data = json.loads(line)
    # Convert JSON data to CSV string (example, modify as per your JSON structure)
    csv_data = ','.join([str(value) for value in data.values()])
    return csv_data

def main(input_folder, output_folder):
    options = PipelineOptions()

    with beam.Pipeline(options=options) as p:
        for filename in os.listdir(input_folder):
            if filename.endswith('.json'):
                json_file = os.path.join(input_folder, filename)
                json_data = p | f"Read {json_file}" >> beam.io.ReadFromText(json_file)
                transformed_json = json_data | f"JSONToCSV-{os.path.basename(json_file)}" >> beam.Map(json_to_csv)

                # Define the output CSV file path
                output_file = os.path.join(output_folder, f"{os.path.basename(json_file).split('.')[0]}.csv")

                # Write the CSV data to the output file
                transformed_json | f"WriteToCSV-{os.path.basename(json_file)}" >> beam.io.WriteToText(output_file)

if __name__ == "__main__":
    input_folder = "data-prueba"  # Replace with the path to your local folder
    output_folder = "resultados_prueba"  # Replace with the path to your desired output folder
    main(input_folder, output_folder)
