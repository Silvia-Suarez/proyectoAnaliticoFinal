cd C:\SilviaSuarez\Proyectos\Nube\proyectoAnaliticoFinal
.\env\Scripts\activate
python metodos_data.py
python upload_data.py
gsutil cp C:\SilviaSuarez\Proyectos\Nube\proyectoAnaliticoFinal\data\* gs://transporte_grupo_4
