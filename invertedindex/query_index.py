from collections import defaultdict

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

class PorterStemmer:
    def __init__(self):
        self.vowels = frozenset("aeiou")
    def cons(self, w, i):
        if w[i] in self.vowels: return False
        if w[i] == "y": return i>0 and not self.cons(w, i-1)
        return True
    def stem(self, w):
        w = w.lower()
        if len(w) <= 2: return w
        if w.endswith("sses"): return w[:-2]
        if w.endswith("ies"): return w[:-2]
        if w.endswith("ss"): return w
        if w.endswith("s"): return w[:-1]
        return w

_stemmer = PorterStemmer()

def stem(word: str) -> str:
    w = word.lower()
    if w in STOP_WORDS: return ""
    return _stemmer.stem(w) or ""

def _parse_index_line(line: str) -> tuple[str, dict[str, int]]:
    parts = line.split()
    if not parts: return "", {}
    term = stem(parts[0])
    if not term: return "", {}
    postings = {}
    for item in parts[1:]:
        if ':' in item:
            d,c = item.split(':',1)
            try: postings[d] = int(c)
            except: pass
    return term, postings

def load_index(fp: str) -> dict[str, dict[str, int]]:
    idx = defaultdict(dict)
    with open(fp, encoding="utf-8") as f:
        for line in f:
            t,p = _parse_index_line(line)
            if not t: continue
            for d,c in p.items():
                idx[t][d] = idx[t].get(d,0) + c
    return idx

def docs_with_all_terms(index: dict[str, dict[str, int]], terms: list[str]) -> set[str]:
    res = None
    for t in terms:
        s = stem(t)
        if not s: continue
        p = set(index.get(s, {}))
        res = p if res is None else (res & p)
        if not res: return set()
    return res or set()
