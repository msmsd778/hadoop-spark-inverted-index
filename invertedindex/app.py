from flask import Flask, render_template, request, redirect, url_for
from query_index import load_index, docs_with_all_terms, STOP_WORDS
from document_scorer import score_documents
import os, subprocess
from pathlib import Path
from datetime import datetime

app = Flask(__name__)
INDEX_DIR = "output"
DATASET_DIR = "datasets"
current_index_path = None

def list_dataset_files():
    files = []
    for root, _, fs in os.walk(DATASET_DIR):
        files.extend(
            os.path.relpath(os.path.join(root, f), DATASET_DIR)
            for f in fs if f.endswith((".txt", ".xml"))
        )
    return files

def list_index_files():
    files = [f for f in os.listdir(INDEX_DIR) if f.endswith(".txt")]
    files.sort(key=lambda f: os.path.getmtime(os.path.join(INDEX_DIR, f)), reverse=True)
    return files

@app.route("/", methods=["GET"])
def index():
    return render_template(
        "index.html",
        indexes=list_index_files(),
        datasets=list_dataset_files(),
        selected_index=os.path.basename(current_index_path) if current_index_path else None,
        query=None,
        results=None,
        exact_matches=None,
    )

@app.route("/build", methods=["POST"])
def build_index():
    selected_files = request.form.getlist("datasets")
    if not selected_files:
        return redirect(url_for("index"))
    reducers_raw = request.form.get("reducers", "2").strip()
    reducers = reducers_raw if reducers_raw.isdigit() and int(reducers_raw) > 0 else "2"
    engine = request.form.get("engine", "hadoop")
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    full_paths = [os.path.join(DATASET_DIR, f) for f in selected_files]
    base_name = "_".join(Path(p).stem for p in full_paths)
    if engine == "spark":
        output_fn = f"{timestamp}_spark_{base_name}_{reducers}.txt"
        output_fp = os.path.join(INDEX_DIR, output_fn)
        cmd = ["bash", "build_index_spark.sh", *full_paths, output_fp, reducers]
    else:
        variant = request.form.get("variant", "combiner")
        output_fn = f"{timestamp}_{variant}_{base_name}_{reducers}.txt"
        output_fp = os.path.join(INDEX_DIR, output_fn)
        cmd = ["bash", "build_index.sh", *full_paths, output_fp, reducers, variant]
    subprocess.run(cmd, check=True)
    return redirect(url_for("index"))

@app.route("/select_index", methods=["POST"])
def select_index():
    global current_index_path
    current_index_path = os.path.join(INDEX_DIR, request.form["index_file"])
    return redirect(url_for("index"))

@app.route("/search", methods=["POST"])
def search():
    query = request.form["query"].strip()
    if not query:
        return redirect(url_for("index"))
    terms = [t for t in query.split() if t.lower() not in STOP_WORDS]
    if not terms:
        return redirect(url_for("index"))
    scores, _ = score_documents(current_index_path, terms)
    all_term_docs = docs_with_all_terms(load_index(current_index_path), terms)
    top5 = sorted(scores.items(), key=lambda x: -x[1])[:5]
    return render_template(
        "index.html",
        indexes=list_index_files(),
        datasets=list_dataset_files(),
        selected_index=os.path.basename(current_index_path),
        query=query,
        results=top5,
        exact_matches=sorted(all_term_docs),
    )

@app.route("/output/<filename>")
def view_output(filename):
    path = os.path.join(INDEX_DIR, filename)
    if not os.path.exists(path):
        return "File not found", 404
    with open(path, encoding="utf-8") as f:
        return f"<pre>{f.read()}</pre>"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
