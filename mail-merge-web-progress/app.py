import os, re, time, uuid, zipfile, tempfile, gc
for f in outputs:
z.write(f, os.path.basename(f))
job.update(status='done', message='✅ Done', result_path=zip_path, result_name=os.path.basename(zip_path))


except Exception as e:
job.update(status='error', message=str(e))
finally:
try: os.remove(excel_path)
except Exception: pass




# ---------------- routes ----------------


@app.route('/')
def index():
return render_template('index.html')




@app.route('/start', methods=['POST'])
def start():
try:
rs = int(request.form['row_start'])
re_ = int(request.form['row_end'])
if rs < 4 or re_ < 4:
return jsonify({'error': '❌ Invalid range: rows must be ≥ 4.'}), 400
if rs > re_:
return jsonify({'error': 'Row start cannot be greater than row end.'}), 400
if (re_ - rs + 1) > MAX_ROWS_PER_RUN:
return jsonify({'error': f'Requested {re_-rs+1} rows. Server limit is {MAX_ROWS_PER_RUN} per run.'}), 400


excel = request.files.get('excel')
temps = request.files.getlist('templates')
if not excel or not temps:
return jsonify({'error': 'Please upload one Excel and at least one Word template.'}), 400


work = tempfile.mkdtemp(prefix='up_')
excel_path = os.path.join(work, excel.filename)
excel.save(excel_path)
tpaths = []
for f in temps:
p = os.path.join(work, f.filename)
f.save(p)
tpaths.append(p)


jid = uuid.uuid4().hex[:12]
JOBS[jid] = { 'status':'queued','message':'Queued…','done':0,'total':0,'eta':None,'result_path':None,'result_name':None,'start':time.time() }
Thread(target=worker, args=(jid, excel_path, tpaths, rs, re_), daemon=True).start()
return jsonify({'job_id': jid})
except Exception as e:
return jsonify({'error': str(e)}), 400




@app.route('/progress/<jid>')
def progress(jid):
j = JOBS.get(jid)
if not j:
return jsonify({'error':'Invalid job id'}), 404
total = max(1, j.get('total', 1))
pct = int(j.get('done',0) / total * 100)
return jsonify({
'status': j['status'],
'message': j.get('message',''),
'progress': pct,
'eta_seconds': j.get('eta'),
'result_ready': j['status']=='done',
'result_name': j.get('result_name')
})




@app.route('/download/<jid>')
def download(jid):
j = JOBS.get(jid)
if not j or j['status'] != 'done' or not j.get('result_path'):
return 'Result not ready', 400
return send_file(j['result_path'], as_attachment=True, download_name=j['result_name'])




if __name__ == '__main__':
app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)
