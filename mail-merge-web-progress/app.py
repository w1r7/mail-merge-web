import os, re, time, uuid, zipfile, tempfile, gc
from threading import Thread
from datetime import datetime

import pytz
from flask import Flask, render_template, request, jsonify, send_file
from openpyxl import load_workbook
from docx import Document
from docx.oxml import OxmlElement
from docx.oxml.ns import qn

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200 MB upload cap

# In-memory job store (safe on Render because Procfile uses ONE worker)
JOBS = {}

# Soft cap to avoid OOM on free instances; override via Render env var if needed
MAX_ROWS_PER_RUN = int(os.environ.get("MAX_ROWS_PER_RUN", "35"))

# Map placeholders in Word → Excel header names (headers are on row 3)
MERGE_FIELDS = {
    "<<DATE>>": "DATE",
    "<<JOB #>>": "JOB #",
    "<<NAME>>": "NAME",
    "<<VEHICLE MAKE>>": "VEHICLE MAKE",
    "<<VEHICLE MODEL>>": "VEHICLE MODEL",
    "<<KCML SERIAL #>>": "KCML SERIAL #",
    "<<VOLTAGE USED>>": "VOLTAGE USED",
    "<<CAN OR FREQUENCY>>": "CAN OR FREQUENCY",
    "<<PPK>>": "PPK",
    "<<SAP FILE>>": "SAP FILE",
    "<<ECU SERIAL #>>": "ECU SERIAL #",
}

# ---------------- helpers ----------------

def today_yymmdd_brisbane():
    tz = pytz.timezone("Australia/Brisbane")
    return datetime.now(tz).strftime("%y%m%d")

def format_value(v):
    if v is None:
        return ""
    # Excel dates as datetime via openpyxl
    if hasattr(v, "strftime"):
        return v.strftime("%Y-%m-%d")
    try:
        if isinstance(v, float) and float(v).is_integer():
            return str(int(v))
    except Exception:
        pass
    return str(v)

def replace_placeholders_across_runs_preserve_style(doc: Document, mapping: dict):
    """Replace placeholders even when split across runs, preserving styling."""
    def do_paragraph(par):
        runs = par.runs
        if not runs:
            return
        text = ''.join(r.text for r in runs)
        for ph, val in mapping.items():
            start = 0
            while True:
                idx = text.find(ph, start)
                if idx == -1:
                    break
                # find start & end run indices spanning the placeholder
                char_count = 0
                s = e = None
                for i, r in enumerate(runs):
                    rl = len(r.text)
                    if rl == 0:
                        continue
                    seg_s, seg_e = char_count, char_count + rl - 1
                    if s is None and seg_s <= idx <= seg_e:
                        s = i
                    if seg_s <= idx + len(ph) - 1 <= seg_e:
                        e = i
                        break
                    char_count += rl
                if s is not None and e is not None:
                    combined = ''.join(runs[k].text for k in range(s, e + 1))
                    replaced = combined.replace(ph, val)
                    for k in range(s, e + 1):
                        runs[k].text = ''
                    runs[s].text = replaced
                    text = ''.join(r.text for r in runs)  # refresh
                    start = idx + len(val)
                else:
                    start = idx + len(ph)

    for p in doc.paragraphs:
        do_paragraph(p)
    for tbl in doc.tables:
        for row in tbl.rows:
            for cell in row.cells:
                for p in cell.paragraphs:
                    do_paragraph(p)
    for section in doc.sections:
        for p in section.header.paragraphs:
            do_paragraph(p)
        for p in section.footer.paragraphs:
            do_paragraph(p)

def insert_section_break(doc: Document):
    p = OxmlElement('w:p')
    pPr = OxmlElement('w:pPr')
    sectPr = OxmlElement('w:sectPr')
    t = OxmlElement('w:type')
    t.set(qn('w:val'), 'nextPage')
    sectPr.append(t)
    pPr.append(sectPr)
    p.append(pPr)
    doc.element.body.append(p)

# --------------- Excel streaming (low memory) ---------------

def stream_rows(excel_path: str, row_start: int, row_end: int):
    """Yield {placeholder: value} for each requested row using read_only workbook."""
    wb = load_workbook(excel_path, data_only=True, read_only=True)
    try:
        ws = wb.active
        # headers on row 3
        headers = [c.value if c.value is not None else '' for c in ws[3]]
        # build column index for MERGE_FIELDS
        col_index = {}
        for ph, header in MERGE_FIELDS.items():
            try:
                col_index[ph] = headers.index(header) + 1  # 1-based
            except ValueError:
                col_index[ph] = None
        for r in range(row_start, row_end + 1):
            mapping = {}
            for ph, col in col_index.items():
                mapping[ph] = "" if col is None else format_value(ws.cell(row=r, column=col).value)
            yield r, mapping
    finally:
        wb.close()

# --------------- Background worker ---------------

def worker(job_id, excel_path, template_paths, row_start, row_end):
    job = JOBS[job_id]
    job.update(status='running', message='Reading Excel…', start=time.time(), done=0, total=0)
    session = tempfile.mkdtemp(prefix=f"merge_{job_id}_")
    try:
        if row_start < 4 or row_end < 4:
            raise ValueError('❌ Invalid range: You must choose rows 4 or higher. Row 3 is for headers.')
        if row_end < row_start:
            raise ValueError('Row start cannot be greater than row end.')
        if (row_end - row_start + 1) > MAX_ROWS_PER_RUN:
            raise ValueError(f'Requested {row_end-row_start+1} rows. Server limit is {MAX_ROWS_PER_RUN} per run.')

        total_docs = (row_end - row_start + 1) * len(template_paths) + len(template_paths)
        job['total'] = total_docs

        outputs = []
        yymmdd = today_yymmdd_brisbane()

        for ti, tpath in enumerate(template_paths, start=1):
            job['message'] = f'Processing template {ti}/{len(template_paths)}…'
            part_paths = []
            for row_no, mapping in stream_rows(excel_path, row_start, row_end):
                doc = Document(tpath)
                replace_placeholders_across_runs_preserve_style(doc, mapping)
                part = os.path.join(session, f'Row_{row_no}.docx')
                doc.save(part)
                part_paths.append(part)
                del doc
                if len(part_paths) % 5 == 0:
                    gc.collect()
                job['done'] += 1
                elapsed = max(0.001, time.time() - job['start'])
                job['eta'] = int(elapsed / job['done'] * (job['total'] - job['done']))

            # merge parts with section breaks
            merged = Document(part_paths[0])
            for p in part_paths[1:]:
                insert_section_break(merged)
                sub = Document(p)
                for el in list(sub.element.body):
                    merged.element.body.append(el)

            base = os.path.splitext(os.path.basename(tpath))[0]
            m = re.search(r'(PRF\s?\d{3}-\d)', base.upper())
            short = m.group(1).replace(' ', '') if m else 'Template'
            final_path = os.path.join(session, f'{yymmdd}_row{row_start}-{row_end}_of-{short}.docx')
            merged.save(final_path)
            outputs.append(final_path)

            for p in part_paths:
                try:
                    os.remove(p)
                except Exception:
                    pass
            del merged, part_paths
            gc.collect()
            job['done'] += 1

        if len(outputs) == 1:
            job.update(status='done', message='✅ Done', result_path=outputs[0], result_name=os.path.basename(outputs[0]))
        else:
            zip_name = f"{yymmdd}_row{row_start}-{row_end}_of_{len(outputs)}_files.zip"
            zip_path = os.path.join(session, zip_name)
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as z:
                for f in outputs:
                    z.write(f, os.path.basename(f))
            job.update(status='done', message='✅ Done', result_path=zip_path, result_name=os.path.basename(zip_path))

    except Exception as e:
        job.update(status='error', message=str(e))
    finally:
        try:
            os.remove(excel_path)
        except Exception:
            pass

# ---------------- routes ----------------

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/start", methods=["POST"])
def start():
    try:
        rs = int(request.form["row_start"])
        re_ = int(request.form["row_end"])
        if rs < 4 or re_ < 4:
            return jsonify({"error": "❌ Invalid range: rows must be ≥ 4."}), 400
        if rs > re_:
            return jsonify({"error": "Row start cannot be greater than row end."}), 400
        if (re_ - rs + 1) > MAX_ROWS_PER_RUN:
            return jsonify({"error": f"Requested {re_-rs+1} rows. Server limit is {MAX_ROWS_PER_RUN} per run."}), 400

        excel = request.files.get("excel")
        temps = request.files.getlist("templates")
        if not excel or not temps:
            return jsonify({"error": "Please upload one Excel and at least one Word template."}), 400

        work = tempfile.mkdtemp(prefix="up_")
        excel_path = os.path.join(work, excel.filename)
        excel.save(excel_path)
        tpaths = []
        for f in temps:
            p = os.path.join(work, f.filename)
            f.save(p)
            tpaths.append(p)

        jid = uuid.uuid4().hex[:12]
        JOBS[jid] = {
            "status": "queued",
            "message": "Queued…",
            "done": 0,
            "total": 0,
            "eta": None,
            "result_path": None,
            "result_name": None,
            "start": time.time(),
        }
        Thread(target=worker, args=(jid, excel_path, tpaths, rs, re_), daemon=True).start()
        return jsonify({"job_id": jid})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/progress/<jid>")
def progress(jid):
    j = JOBS.get(jid)
    if not j:
        return jsonify({"error": "Invalid job id"}), 404
    total = max(1, j.get("total", 1))
    pct = int(j.get("done", 0) / total * 100)
    return jsonify({
        "status": j["status"],
        "message": j.get("message", ""),
        "progress": pct,
        "eta_seconds": j.get("eta"),
        "result_ready": j["status"] == "done",
        "result_name": j.get("result_name"),
    })

@app.route("/download/<jid>")
def download(jid):
    j = JOBS.get(jid)
    if not j or j["status"] != "done" or not j.get("result_path"):
        return "Result not ready", 400
    return send_file(j["result_path"], as_attachment=True, download_name=j["result_name"])

# ---- IMPORTANT for Render: bind to the provided PORT ----
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
