from collections import defaultdict
from nltk.stem import PorterStemmer

stemmer = PorterStemmer()

def stem(word: str) -> str:
    return stemmer.stem(word.lower())

def _parse_index_line(line: str) -> tuple[str, dict[str, int]]:
    parts = line.strip().split()
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
    result: set[str] | None = None
    for t in terms:
        postings = set(index.get(stem(t), {}))
        result = postings if result is None else result & postings
        if not result:
            return set()
    return result or set()
