# document_scorer.py
import os
from collections import defaultdict
from query_index import stem, load_index, docs_with_all_terms

# tweak these constants to taste
PARTIAL_MATCH_BONUS = 5        # all stemmed terms present (order/distances irrelevant)
EXACT_PHRASE_BONUS = 10        # literal “…query …” appears in raw text (case-insensitive)

DATASETS_DIR = "datasets"

def score_documents(index_path: str, query_terms: list[str]) -> tuple[dict[str,int], list[str]]:
    """
    1. Count **stemmed** occurrences of every query word. (+1 each)
    2. Add PARTIAL_MATCH_BONUS once if document contains *all* stemmed query words.
    3. Add EXACT_PHRASE_BONUS once if document contains the literal phrase (no stemming).
    Returns (scores, docs_with_exact_phrase)
    """
    index = load_index(index_path)
    stems = [stem(t) for t in query_terms]
    literal_phrase = " ".join(query_terms).lower()

    # Pass 1 – term-frequency score
    scores: dict[str,int] = defaultdict(int)
    for s in stems:
        for doc, cnt in index.get(s, {}).items():
            scores[doc] += cnt          # +1 per occurrence (cnt already = #occurrences)

    if not scores:
        return {}, []

    # Pass 2 – bonuses
    exact_phrase_docs: list[str] = []
    for doc in list(scores):            # iterate over a copy – we mutate scores
        doc_has_all_words = all(stem in index and doc in index[stem] for stem in stems)
        if doc_has_all_words:
            scores[doc] += PARTIAL_MATCH_BONUS

        # read raw text once per doc only if needed
        try:
            raw = open(os.path.join(DATASETS_DIR, doc), encoding="utf-8").read().lower()
            if literal_phrase in raw:
                scores[doc] += EXACT_PHRASE_BONUS
                exact_phrase_docs.append(doc)
        except FileNotFoundError:
            # silently ignore missing dataset file
            pass

    return dict(scores), exact_phrase_docs
