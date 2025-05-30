import os
from collections import defaultdict
from query_index import stem, load_index, STOP_WORDS

PARTIAL_MATCH_BONUS = 5
EXACT_PHRASE_BONUS = 10
DATASETS_DIR = "datasets"

def score_documents(index_path: str, query_terms: list[str]) -> tuple[dict[str, int], list[str]]:
    index = load_index(index_path)

    # Filter stop-words once, then stem
    stems = [stem(t) for t in query_terms if t.lower() not in STOP_WORDS and stem(t)]
    literal_phrase = " ".join(query_terms).lower()

    scores: dict[str, int] = defaultdict(int)
    for s in stems:
        for doc, cnt in index.get(s, {}).items():
            scores[doc] += cnt

    if not scores:
        return {}, []

    exact_phrase_docs: list[str] = []
    for doc in list(scores):
        # Bonus when all stems are present
        if all(doc in index.get(s, {}) for s in stems):
            scores[doc] += PARTIAL_MATCH_BONUS

        try:
            with open(os.path.join(DATASETS_DIR, doc), encoding="utf-8") as f:
                if literal_phrase in f.read().lower():
                    scores[doc] += EXACT_PHRASE_BONUS
                    exact_phrase_docs.append(doc)
        except FileNotFoundError:
            pass

    return dict(scores), exact_phrase_docs
