import re
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
        if bool(re.search(r'\d', registro['casos'])):
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))
        else:
            yield (f"{uf}-{registro['ano_mes']}", 0.0)

def chave_uf_ano_mes_de_lista(elemento):
    """
    Recebe uma lista de elementos ['2016-01-24', '4.2', 'TO']
    Retorna uma tupla contendo uma chave e o valor de chuva em mm ('UF-ANO-MES', 1.3)
    """
    data, mm, uf = elemento
    ano_mes = '-'.join(data.split('-')[:2])
    chave = f'{uf}-{ano_mes}'
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return (chave, mm)

def arredonda(elemento):
    """
    Recebe uma tupla
    Retorna uma tupla com o valor arredondardo
    """
    chave, mm = elemento
    return (chave, round(mm, 1))


dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> 
        ReadFromText('bases_dados/sample_casos_dengue.txt', skip_header_lines=1)
    | "De texto para lista (dengues)" >>
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
    | "Soma dos casos de dengue pela chave" >>
        beam.CombinePerKey(sum)
    # | "Mostrar resultados de dengues" >> beam.Map(print)
)


chuvas = (
    pipeline
    | "Leitura do datasset de chuvas" >>
        ReadFromText('bases_dados/sample_chuvas.csv', skip_header_lines=1)
    | "De texto para lista (chuvas)" >>
        beam.Map(texto_para_lista, delimitador=',')
    | "Criando a chave UF-ANO-MES" >>
        beam.Map(chave_uf_ano_mes_de_lista)
    | "Soma dos casos de chuvas pela chave" >>
        beam.CombinePerKey(sum)
    | "Arredondar resultados de chuvas" >> 
        beam.Map(arredonda)
    # | "Mostrar resultados de chuvas" >> beam.Map(print)
)


resultado = (
    # (chuvas, dengue})
    # | "Unir as pcols" >> beam.Flatten()
    # | "Agrupar as pcols" >> beam.GroupByKey()
    ({'Chuvas': chuvas, 'dengue': dengue})
    | "Mesclar pcols" >> beam.CoGroupByKey()
    | "Mostrar resultados" >> beam.Map(print)
)

pipeline.run()