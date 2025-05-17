import os
from collections import defaultdict
from query_index import stem, load_index, docs_with_all_terms

PARTIAL_MATCH_BONUS = 5 
EXACT_PHRASE_BONUS = 10

DATASETS_DIR = "datasets"

def score_documents(index_path: str, query_terms: list[str]) -> tuple[dict[str,int], list[str]]:
    index = load_index(index_path)
    stems = [stem(t) for t in query_terms]
    literal_phrase = " ".join(query_terms).lower()

    scores: dict[str,int] = defaultdict(int)
    for s in stems:
        for doc, cnt in index.get(s, {}).items():
            scores[doc] += cnt 

    if not scores:
        return {}, []

    exact_phrase_docs: list[str] = []
    for doc in list(scores):
        doc_has_all_words = all(stem in index and doc in index[stem] for stem in stems)
        if doc_has_all_words:
            scores[doc] += PARTIAL_MATCH_BONUS

        try:
            raw = open(os.path.join(DATASETS_DIR, doc), encoding="utf-8").read().lower()
            if literal_phrase in raw:
                scores[doc] += EXACT_PHRASE_BONUS
                exact_phrase_docs.append(doc)
        except FileNotFoundError:
            pass

    return dict(scores), exact_phrase_docs
