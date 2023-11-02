import random

def generate_random_coordinates(min_latitude, max_latitude, min_longitude, max_longitude):
  """Genera coordenadas aleatorias dentro de un rango especificado.

  Args:
    min_latitude: Latitud mínima.
    max_latitude: Latitud máxima.
    min_longitude: Longitud mínima.
    max_longitude: Longitud máxima.

  Returns:
    Una tupla de coordenadas (latitud, longitud).
  """

  latitude = random.uniform(min_latitude, max_latitude)
  longitude = random.uniform(min_longitude, max_longitude)
  return latitude, longitude


if __name__ == "main":
  # Generar coordenadas aleatorias en el mundo.
  latitude, longitude = generate_random_coordinates(-90, 90, -180, 180)
  print(f"Latitud: {latitude}, longitud: {longitude}")

  # Generar coordenadas aleatorias en Colombia.
  latitude, longitude = generate_random_coordinates(-4.609943, 12.616667, -75.269943, -66.930057)
  print(f"Latitud: {latitude}, longitud: {longitude}")