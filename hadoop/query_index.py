# query_index.py  (also imported by document_scorer.py)
from pathlib import Path
from collections import defaultdict
from nltk.stem import PorterStemmer

stemmer = PorterStemmer()

def stem(word: str) -> str:
    return stemmer.stem(word.lower())

def _parse_index_line(line: str) -> tuple[str, dict[str, int]]:
    """
    Accepts either TAB- or SPACE-separated inverted-index lines like:
        cloud<TAB>doc1.txt:3<TAB>doc2.txt:1
        cloud doc1.txt:3 doc2.txt:1
    Returns (stemmed_term, {doc:count, …})
    """
    parts = line.strip().split()          # works for both delimiters
    if not parts:
        return "", {}

    term = stem(parts[0])
    postings = {}
    for item in parts[1:]:
        if ':' in item:
            doc, cnt = item.split(':', 1)
            try:
                postings[doc] = int(cnt)
            except ValueError:
                # silently skip malformed counts
                pass
    return term, postings

def load_index(filepath: str) -> dict[str, dict[str, int]]:
    index = defaultdict(dict)
    with open(filepath, encoding="utf-8") as f:
        for line in f:
            term, postings = _parse_index_line(line)
            for doc, cnt in postings.items():
                index[term][doc] = cnt + index[term].get(doc, 0)
    return index

def docs_with_all_terms(index: dict[str, dict[str, int]], terms: list[str]) -> set[str]:
    """
    Return the set of documents that contain **every** (stemmed) query word.
    """
    result: set[str] | None = None
    for t in terms:
        postings = set(index.get(stem(t), {}))
        result = postings if result is None else result & postings
        if not result:
            return set()          # early exit – no AND-match possible
    return result or set()
