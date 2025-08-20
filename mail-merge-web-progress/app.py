import os
import io
import zipfile
from flask import Flask, request, send_file, render_template
import pandas as pd
from docx import Document

app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")

def fmt(v):
    """POC formatting: empty -> '', ints without .0, dates -> YYYY-MM-DD."""
    if pd.isna(v):
        return ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    if isinstance(v, pd.Timestamp):
        return v.strftime("%Y-%m-%d")
    return str(v)

def replace_in_doc(doc: Document, mapping: dict):
    """Replace placeholders in paragraphs and tables (POC: no split-run handling)."""
    # Paragraphs
    for p in doc.paragraphs:
        if not p.text:
            continue
        txt = p.text
        for k, v in mapping.items():
            ph = f"<<{k}>>"
            if ph in txt:
                txt = txt.replace(ph, v)
        p.text = txt

    # Tables
    for tbl in doc.tables:
        for row in tbl.rows:
            for cell in row.cells:
                for p in cell.paragraphs:
                    if not p.text:
                        continue
                    txt = p.text
                    for k, v in mapping.items():
                        ph = f"<<{k}>>"
                        if ph in txt:
                            txt = txt.replace(ph, v)
                    p.text = txt

@app.route("/merge", methods=["POST"])
def merge():
    # ---- Inputs ----
    excel_file = request.files.get("excel")
    templates = request.files.getlist("word_templates")
    try:
        row_start = int(request.form["row_start"])
        row_end = int(request.form["row_end"])
    except Exception:
        return "Invalid row numbers.", 400

    # ---- Validate row range (headers are row 3; data starts at row 4) ----
    if row_start < 4 or row_end < 4:
        return "❌ Invalid range: you must choose rows 4 or higher. Row 3 is headers.", 400
    if row_start > row_end:
        return "Row start cannot be greater than row end.", 400
    if not excel_file or not templates:
        return "Please upload one Excel file and at least one Word template.", 400

    # ---- Read Excel (headers at row 3 -> header=2) ----
    df = pd.read_excel(excel_file, header=2, engine="openpyxl")
    # Excel row 4 corresponds to iloc index 0. We want inclusive [row_start, row_end].
    start_iloc = row_start - 4
    end_exclusive = (row_end - 4) + 1  # iloc end is exclusive
    window = df.iloc[start_iloc:end_exclusive]

    # ---- Build ZIP in memory ----
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as z:
        for excel_row_num, record in zip(range(row_start, row_end + 1), window.to_dict(orient="records")):
            mapping = {k: fmt(v) for k, v in record.items()}

            for t in templates:
                # Reset the uploaded file stream before each use
                t.stream.seek(0)
                doc = Document(t)
                replace_in_doc(doc, mapping)

                out_name = f"{os.path.splitext(t.filename)[0]}_row{excel_row_num}.docx"
                out_buf = io.BytesIO()
                doc.save(out_buf)
                out_buf.seek(0)
                z.writestr(out_name, out_buf.read())

    zip_buffer.seek(0)
    return send_file(zip_buffer, as_attachment=True, download_name="merged_docs.zip")

if __name__ == "__main__":
    # Works on Render and locally
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
