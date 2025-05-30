from pathlib import Path
from collections import defaultdict
from nltk.stem import PorterStemmer

STOP_WORDS = {
    "a","about","above","after","again","against","all","am","an","and","any","are","as","at","be","because","been","before",
    "being","below","between","both","but","by","could","did","do","does","doing","down","during","each","few","for","from",
    "further","had","has","have","having","he","her","here","hers","herself","him","himself","his","how","i","if","in",
    "into","is","it","its","itself","just","me","more","most","my","myself","no","nor","not","now","of","off","on","once",
    "only","or","other","our","ours","ourselves","out","over","own","same","she","should","so","some","such","than","that",
    "the","their","theirs","them","themselves","then","there","these","they","this","those","through","to","too","under",
    "until","up","very","was","we","were","what","when","where","which","while","who","whom","why","with","you","your",
    "yours","yourself","yourselves"
}

stemmer = PorterStemmer()

def stem(word: str) -> str:
    w = word.lower()
    if w in STOP_WORDS:
        return ""
    return stemmer.stem(w)

def _parse_index_line(line: str) -> tuple[str, dict[str, int]]:
    parts = line.strip().split()
    if not parts:
        return "", {}
    term = stem(parts[0])
    if not term:
        return "", {}
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
            if not term:
                continue
            for doc, cnt in postings.items():
                index[term][doc] = cnt + index[term].get(doc, 0)
    return index

def docs_with_all_terms(index: dict[str, dict[str, int]], terms: list[str]) -> set[str]:
    result = None
    for t in terms:
        s = stem(t)
        if not s:
            continue
        postings = set(index.get(s, {}))
        result = postings if result is None else result & postings
        if not result:
            return set()
    return result or set()