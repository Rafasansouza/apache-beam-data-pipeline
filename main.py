import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

colunas_dengue: list = [  "id",
                    "data_iniSE",
                    "casos",
                    "ibge_code",
                    "cidade",
                    "uf",
                    "cep",
                    "latitude",
                    "longitude"]


def lista_para_dicionario(elemento, colunas):
    """
    Recebe duas listas
    Retorna um dicionario
    """
    return dict(zip(colunas, elemento))


def texto_para_lista(elemento, delimitador='|'):
    """
    Recebe um texto e um delimitador
    Retorna uma lista de elementos pelo delimitador
    """
    return elemento.split(delimitador)

def trata_data(elemento):
    """
    Recebe dicionario e cria novo campo com ANO-MES
    Retorna o mesmo dicionario com o novo campo
    """
    elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2])
    return elemento

def chave_uf(elemento):
    """
    Recebe um dicionario
    Retorna uma tupla com o estado e o elemento (UF, dicionario)
    """
    chave = elemento['uf']
    return (chave, elemento)

def casos_dengue(elemento):
    """
    Recebe uma tupla ('RS', [{},{}])
    Retorna uma tupla ('RS-2024-12', 8.0)
    """
    uf, registros = elemento
    for registro in registros:
        yield (f"{uf}-{registro['ano-mes']}", float(registro['casos']))
    


dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> 
        ReadFromText('bases_dados/casos_dengue.txt', skip_header_lines=1)
    | "De texto para lista" >>
        beam.Map(texto_para_lista)
    | "De lista para dicionario" >>
        beam.Map(lista_para_dicionario, colunas_dengue)
    | "Criar campo ano_mes" >> 
        beam.Map(trata_data)
    | "Criar chave pelo estado" >> 
        beam.Map(chave_uf)
    | "Agrupar pelo estado" >>
        beam.GroupByKey()
    | "Descompactar casos de dengue" >> 
        beam.FlatMap(casos_dengue)
    | "Soma dos casos pela chave" >>
        beam.CombinePerKey(sum)
    | "Mostrar resultados" >> beam.Map(print)
)

pipeline.run()