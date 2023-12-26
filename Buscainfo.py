from typing import Dict, List, Sequence
from whoosh.index import create_in
from whoosh.fields import *
from whoosh.qparser import MultifieldParser
from whoosh.filedb.filestore import RamStorage
from whoosh.analysis import StemmingAnalyzer
import json

class SearchEngine:
    def __init__(self, schema):
        self.schema = schema
        schema.add('raw', TEXT(stored=True))
        self.ix = RamStorage().create_index(self.schema)

    def index_documents(self, docs: Sequence):
        writer = self.ix.writer()
        for doc in docs:
            d = {k: v for k,v in doc.items() if k in self.schema.stored_names()}
            d['raw'] = json.dumps(doc) # raw version of all of doc
            writer.add_document(**d)
        writer.commit(optimize=True)

    def get_index_size(self) -> int:
        return self.ix.doc_count_all()

    def query(self, q: str, fields: Sequence, highlight: bool=True) -> List[Dict]:
        search_results = []
        with self.ix.searcher() as searcher:
            results = searcher.search(MultifieldParser(fields, schema=self.schema).parse(q))
            for r in results:
                d = json.loads(r['raw'])
                if highlight:
                    for f in fields:
                        if r[f] and isinstance(r[f], str):
                            d[f] = r.highlights(f) or r[f]

                search_results.append(d)

        return search_results

if __name__ == '__main__':
    # Ler o conteúdo do arquivo cran.qry
    with open('cran.qry', 'r') as f:
        queries = f.read().split('.I')[1:]

    # Processar as queries e criar uma lista de dicionários
    query_docs = []
    current_query = {}
    for query in queries:
        lines = query.strip().split('\n')
        current_query['id'] = lines[0].strip()
        current_query['query'] = ' '.join(lines[2:]).strip()
        query_docs.append(current_query.copy())

    schema = Schema(
        id=ID(stored=True),
        query=TEXT(stored=True, analyzer=StemmingAnalyzer())
    )

    engine = SearchEngine(schema)
    engine.index_documents(query_docs)

    print(f"indexed {engine.get_index_size()} queries")

    fields_to_search = ["query"]

    # Utilizar as queries do arquivo cran.qry para as consultas
    for q in query_docs:
        print(f"Query:: {q['query']}")
        print("\t", engine.query(q['query'], fields_to_search, highlight=True))
        print("-" * 70)
