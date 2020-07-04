from flask import Flask,jsonify,request
from flask import render_template
import ast

app = Flask(__name__)

labels = []
values = []

positive = {}
@app.route("/")
def chart():
    global labels,values,positive
    labels = []
    values = []
    positive = {}
    return render_template('chart.html', values=values, labels=labels)


@app.route('/refreshData')
def refresh_graph_data():
    global labels, values,positive
    print("labels now: " + str(labels))
    print("data now: " + str(values))
    print(positive)
    a=[]
    b=[]
    for lab,val in positive.items():
        a.append(lab)
        b.append(val)
    return jsonify(sLabel=labels, sData=values , sPosL = a , sPosv = b )


@app.route('/updateData', methods=['POST'])
def update_data_post():
    global labels, values
    if not request.form or 'data' not in request.form:
        return "error",400
    labels = ast.literal_eval(request.form['label'])
    values = ast.literal_eval(request.form['data'])

    #print(request.form['label'].decode("utf-8"))
    #print(request.form['data'].decode("utf-8"))
    #print("labels received: " + str(labels))
    #print("data received: " + str(values))
    return "success",201



@app.route('/updateDataPos', methods=['POST'])
def update_dataPos_post():
    global positive
    if not request.form or 'data' not in request.form:
        return "error",400
    lab=[]
    val=[]
    positive={}
    lab = ast.literal_eval(request.form['label'])
    val = ast.literal_eval(request.form['data'])
    for i in range(len(lab)):
        positive[lab[i]]=val[i]
    #print(request.form['label'].decode("utf-8"))
    #print(request.form['data'].decode("utf-8"))
    #print("labels received: " + str(labels))
    #print("data received: " + str(values))
    return "success",201


if __name__ == "__main__":
    app.run(host='localhost', port=5001)