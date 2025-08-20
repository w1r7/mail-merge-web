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

@app.route("/merge", methods=["POST"])
def merge():
    excel = request.files["excel"]
    word = request.files.getlist("word_templates")
    row_start = int(request.form["row_start"])
    row_end = int(request.form["row_end"])

    df = pd.read_excel(excel, header=2)  # headers start at row 3
    df = df.iloc[row_start-4:row_end-3]  # adjust index since header is row 3

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zipf:
        for idx, row in df.iterrows():
            for template in word:
                doc = Document(template)
                for p in doc.paragraphs:
                    for k, v in row.items():
                        if f"<<{k}>>" in p.text:
                            p.text = p.text.replace(f"<<{k}>>", str(v))
                out_name = f"{os.path.splitext(template.filename)[0]}_row{idx+4}.docx"
                buf = io.BytesIO()
                doc.save(buf)
                buf.seek(0)
                zipf.writestr(out_name, buf.read())

    zip_buffer.seek(0)
    return send_file(zip_buffer, as_attachment=True, download_name="merged_docs.zip")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
