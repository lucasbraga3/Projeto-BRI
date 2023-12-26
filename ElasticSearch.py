from typing import Dict, List, Sequence
from elasticsearch import Elasticsearch
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class SearchEngine:

    def __init__(self, esquema):
        self.esquema = esquema
        self.index_name = 'my_index'
        self.es = Elasticsearch(
            ["https://localhost:9200"],
            basic_auth=("username", "senha"),
            verify_certs=False
        )
        if not self.es.indices.exists(index=self.index_name, ):
            self.es.indices.create(index=self.index_name, body={"mappings": {"properties": self.esquema}})

    def indexar_documentos(self, documentos: Sequence):
        for documento in documentos:
            self.es.index(index=self.index_name, body=documento)

    def obter_tamanho_do_indice(self) -> int:
        response = self.es.cat.count(index=self.index_name, h='count')
        return int(response.strip())

    def consulta(self, q: str, campos: Sequence, destaque: bool = True) -> List[Dict]:
        resultados_da_pesquisa = []
        body = {
            "query": {
                "multi_match": {
                    "query": q,
                    "fields": campos
                }
            }
        }

        results = self.es.search(index=self.index_name, body=body)['hits']['hits']
        for resultado in results:
            d = resultado['_source']
            if destaque and 'highlight' in resultado:
                for campo in campos:
                    if campo in resultado['highlight'] and isinstance(resultado['highlight'][campo], list):
                        d[campo] = ' '.join(resultado['highlight'][campo])
            resultados_da_pesquisa.append(d)

        return resultados_da_pesquisa

def ler_colecao(caminho):
    with open(caminho, 'r') as arquivo:
        linhas = arquivo.readlines()
        doc = {'id': None, 'title': '', 'author': '', 'bibliography': '', 'content': ''}
        colecao = []
        campo_atual = None
        for linha in linhas:
            linha = linha.strip()
            if linha.startswith('.I'):
                if doc['id'] is not None:
                    colecao.append(doc)
                doc = {'id': int(linha[3:].strip()), 'title': '', 'author': '', 'bibliography': '', 'content': ''}
            elif linha.startswith('.T'):
                campo_atual = 'title'
                doc['title'] += ' ' + linha[3:].strip()
            elif linha.startswith('.A'):
                campo_atual = 'author'
                doc['author'] += ' ' + linha[3:].strip()
            elif linha.startswith('.B'):
                campo_atual = 'bibliography'
                doc['bibliography'] += ' ' + linha[3:].strip()
            elif linha.startswith('.W'):
                campo_atual = 'content'
                doc['content'] += ' ' + linha[3:].strip()
            elif campo_atual and linha:
                doc[campo_atual] += ' ' + linha.strip()

        if doc['id'] is not None:
            colecao.append(doc)

    return colecao


def ler_queries(caminho):
    with open(caminho, 'r') as arquivo:
        queries = arquivo.read().split('.I')[1:]

    query_docs = []
    current_query = {}
    for query in queries:
        lines = query.strip().split('\n')
        current_query['id'] = lines[0].strip()
        current_query['query'] = ' '.join(lines[2:]).strip()
        query_docs.append(current_query.copy())

    return query_docs


if __name__ == '__main__':
    esquema = {
        'id': {'type': 'integer'},
        'title': {'type': 'text'},
        'author': {'type': 'text'},
        'bibliography': {'type': 'text'},
        'content': {'type': 'text'},
    }

    mecanismo = SearchEngine(esquema)
    caminho_dataset = 'cran.all.1400'
    colecao = ler_colecao(caminho_dataset)
    mecanismo.indexar_documentos(colecao)

    print(f"Indexed {mecanismo.obter_tamanho_do_indice()} documents")

    campos_a_pesquisar = ["title", "author", "bibliography", "content"]

    # Ler as queries do arquivo cran.qry
    queries = ler_queries('cran.qry')
    i = 0
    # Utilizar as queries do arquivo cran.qry para as consultas
    for q in queries:
        print(f"Query:: {q['query']}")
        print("\t", mecanismo.consulta(q['query'], campos_a_pesquisar, destaque=True))
        print("-" * 70)