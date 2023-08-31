from flask import Flask, request, jsonify
import labreport_methods, med_methods
import os, sqlite3
from datetime import datetime
from threading import Thread
import json

app = Flask(__name__)
path=os.path.dirname(os.path.abspath(__file__))

@app.route('/')
def rootfunc():
    return "<h1>Model Deployed Successfully !!</h1>"

@app.route('/testapi')
def testapi():
    return jsonify({"This is the test page":"Model has deployed Successfully and is running"})

@app.route('/status', methods=['POST', 'GET'])
def getstatus():
    path = os.path.dirname(os.path.abspath(__file__))
    try:
        jobid=request.form.get('id')
        conn = sqlite3.connect(os.path.join(path, "status.sqlite"))
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM jobs WHERE jobid = ?", (jobid,))
        result=cursor.fetchone()
        if result:
            data={"Error":"Couldnt Fetch the output."}
            filename=jobid+".json"
            path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "outputs", filename)
            if result[3]=='Ready':
                with open(path, "r+") as output:
                    data=json.load(output)
            else:
                data={"jobid":result[0], "userid":result[1], "datetime":result[2], "status":result[3], "details":result[4]}
            if result[3] in ('Ready', 'ERROR(S)', 'ERROR(U)'):
                cursor.execute("DELETE FROM jobs where jobid=?", (jobid,))
                os.remove(path)
                conn.commit()
            return jsonify(data)
        else:return jsonify({"Error":"Job corresponding to jobid doesnt exist"})
    except Exception as e:
        return(jsonify({"Error":str(e)}))

@app.route('/labreport', methods=['POST', 'GET'])
def labreport():
    path=os.path.dirname(os.path.abspath(__file__))
    statusdb=os.path.join(path, "status.sqlite")
    dbjson=os.path.join(path, "db.json")
    outjson=os.path.join(path, "outputs")
    threshold=75
    try:
        id=request.form.get('id')
        source=request.files.get('source')
        type=os.path.splitext(source.filename)[-1].lower()
        jobid = assign_job(id)
        if not (source and id):
            raise ValueError("source and id are required fields")
        if type in ['.pdf', '.png', '.jpg', '.jpeg']:
            source = source.read()
        else:
            return jsonify({"error": "Unsupported file format"}), 400
        outjson=request.form.get('output', outjson)
        dbjson=request.form.get('dbjson', dbjson)
        statusdb=request.form.get('statusdb', statusdb)
        threshold=request.form.get('threshold', threshold)
        labthread = Thread(target = labreport_methods.generateJson, args=(source, type, id, jobid,), kwargs={"json_path":outjson, "dbjson":dbjson, "status":statusdb, "threshold":threshold})
        labthread.start()
        return jsonify({"jobid":jobid})
    except Exception as e:
        return jsonify({"Unknown Error":str(e)})
    
@app.route('/medicine', methods=['POST', 'GET'])
def medicine():
    path=os.path.dirname(os.path.abspath(__file__))
    outjson=os.path.join(path, "outputs")
    datapath=os.path.join(path, "meddata.csv")
    statusdb=os.path.join(path, "status.sqlite")
    vect_data=os.path.join(path, "vect_data.csv")
    vect_path=os.path.join(path, "vect.pkl")

    dsearchtop=6
    dresultnum=5

    try:
        id=request.form.get('id')
        source=request.files.get('source')
        type=os.path.splitext(source.filename)[-1].lower()
        jobid = assign_job(id)
        if not (source and id):
            raise ValueError("source and id are required fields")
        if type in ['.png', '.jpg', '.jpeg']:
            source = source.read()
        else:
            return jsonify({"error": "Unsupported file format"}), 400
        outjson=request.form.get('output', outjson)
        statusdb=request.form.get('statusdb', statusdb)
        searchtop=request.form.get('searchtop', dsearchtop)
        resultnum=request.form.get('resultnum', dresultnum)
        vect_path=request.form.get('vectorised_file', vect_path)
        vect_data=request.form.get('vectorise_data', vect_data)
        data=request.form.get('dbjson', datapath)
        medthread = Thread(target=med_methods.generateJson, 
                           args=(source, id, jobid,), 
                           kwargs={"searchtop":int(searchtop), "resultnum":int(resultnum), "vect_data":vect_data, 
                                   "vect_path":vect_path, "outpath":outjson, "datapath":data, 
                                   "status":statusdb})
        medthread.start()
        return jsonify({"jobid":jobid})
    except Exception as e:
        return jsonify({"Error":str(e)})

def assign_job(user_id):
    try:
        conn = sqlite3.connect(os.path.join(path, "status.sqlite"))
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM jobs")
        result=cursor.fetchone()[0]+1
        jobid = f"J{result:05}"
        query = "INSERT INTO JOBS VALUES(?, ?, ?, \"Job assigned\", \"\")"
        cursor.execute(query, (jobid, user_id, datetime.now()))
        conn.commit()
        return jobid
    except sqlite3.OperationalError:
        errormsg="No valid Status database found"
        return errormsg

#if __name__ == '__main__':
    #from waitress import serve
    #serve(app, host="0.0.0.0", port=5000)
    #5app.run(host='0.0.0.0', port=5000)