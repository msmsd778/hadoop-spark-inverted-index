from flask import Flask, render_template, request, redirect, url_for
from query_index      import load_index, docs_with_all_terms
from document_scorer  import score_documents
import os, subprocess
from pathlib import Path

app = Flask(__name__)

INDEX_DIR   = "output"
DATASET_DIR = "datasets"

# --------------------------------------------------  default index
current_index_path = os.path.join(INDEX_DIR, "inverted_index.txt")
inverted_index     = load_index(current_index_path) if os.path.exists(current_index_path) else {}

# --------------------------------------------------  helpers
def list_dataset_files():
    files = []
    for root, _, fs in os.walk(DATASET_DIR):
        files.extend(
            os.path.relpath(os.path.join(root, f), DATASET_DIR)
            for f in fs if f.endswith((".txt", ".xml"))
        )
    return files

def list_index_files():
    return [f for f in os.listdir(INDEX_DIR) if f.endswith(".txt")]

# --------------------------------------------------  home page
@app.route("/", methods=["GET"])
def index():
    return render_template(
        "index.html",
        indexes        = list_index_files(),
        datasets       = list_dataset_files(),
        selected_index = os.path.basename(current_index_path) if current_index_path else None,
        query          = None,
        results        = None,
        exact_matches  = None,
    )

# --------------------------------------------------  build new index
@app.route("/build", methods=["POST"])
def build_index():
    selected_files = request.form.getlist("datasets")
    if not selected_files:
        return redirect(url_for("index"))

    # ← NEW: read reducers (default 2 if empty or invalid)
    reducers_raw = request.form.get("reducers", "2").strip()
    reducers     = reducers_raw if reducers_raw.isdigit() and int(reducers_raw) > 0 else "2"

    full_paths = [os.path.join(DATASET_DIR, f) for f in selected_files]
    base_name  = "_".join(Path(p).stem for p in full_paths)
    output_fn  = f"inverted_index_{base_name}.txt"
    output_fp  = os.path.join(INDEX_DIR, output_fn)

    # Pass the reducer count as the final arg to the shell script
    subprocess.run(["bash", "build_index.sh", *full_paths, output_fp, reducers], check=True)

    return redirect(url_for("index"))

# --------------------------------------------------  switch current index
@app.route("/select_index", methods=["POST"])
def select_index():
    global current_index_path
    index_file = request.form["index_file"]
    current_index_path = os.path.join(INDEX_DIR, index_file)
    # DO NOT load index yet — defer until /search
    return redirect(url_for("index"))

# --------------------------------------------------  search
@app.route("/search", methods=["POST"])
def search():
    query = request.form["query"].strip()
    if not query:
        return redirect(url_for("index"))

    terms  = query.split()
    idx_fp = current_index_path

    # Load the selected index file here
    scores, phrase_hits = score_documents(idx_fp, terms)

    all_term_docs   = docs_with_all_terms(load_index(idx_fp), terms)
    top5            = sorted(scores.items(), key=lambda x: -x[1])[:5]

    return render_template(
        "index.html",
        indexes        = list_index_files(),
        datasets       = list_dataset_files(),
        selected_index = os.path.basename(current_index_path),
        query          = query,
        results        = top5,
        exact_matches  = sorted(all_term_docs),
    )

# --------------------------------------------------  raw index view
@app.route("/output/<filename>")
def view_output(filename):
    path = os.path.join(INDEX_DIR, filename)
    if not os.path.exists(path):
        return "File not found", 404
    with open(path, encoding="utf-8") as f:
        return f"<pre>{f.read()}</pre>"

# --------------------------------------------------  entry point
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
