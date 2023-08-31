import csv
import os
import pickle
import json
import time
import sqlite3

try:
    import cv2
    import numpy as np
    from sklearn.feature_extraction.text import HashingVectorizer
    from sklearn.neighbors import NearestNeighbors
    from paddleocr import PaddleOCR
except ModuleNotFoundError as e:
    print("Error(S):" + str(e) + "\nUse \"pip install -r requirements.txt\"")
    exit(0)

processing_status="Processing"
complete_status="Ready"
system_error='ERROR(S)'
user_error='ERROR(U)'

path=os.path.dirname(os.path.abspath(__file__))
display_csv = os.path.join(path, "meddata.csv")
vect_data=os.path.join(path, "med.csv")
vect_path=os.path.join(path, "vect.pkl")
outjson=os.path.join(path, "outputs")
status=os.path.join(path, "status.sqlite")

searchtop=10
resultnum=5
uid='a12'

def ocr(img):
    ocr = PaddleOCR(lang='en', use_angle_cls=True, show_log=False)
    res=ocr.ocr(img)[0]
    return [[i[0], i[1][0]] for i in res]

def sortbysize(wordlist):
    def size(col):
        l = abs(col[0][1][1]-col[0][2][1])
        b = abs(col[0][0][0]-col[0][1][0])
        return (l*b)/len(col[1])
    if len(wordlist) <= 1:
        return wordlist
    pivot = wordlist[0]
    left = []
    right = []
    for i in range(1, len(wordlist)):
        if size(wordlist[i]) >= size(pivot):
            left.append(wordlist[i])
        else:
            right.append(wordlist[i])
    return sortbysize(left) + [pivot] + sortbysize(right) 

def preprocess(image):
    image=cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)  #grayscale
    image=cv2.medianBlur(image,1)  # noise removal
    image = cv2.adaptiveThreshold(image, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 85, 11)   #adaptive thresholding
    #image=cv2.threshold(image, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]  #thresholding
    #image=cv2.dilate(image, np.ones((2,1),np.uint8), iterations = 1)  #dilation
    #image=cv2.erode(image, np.ones((2,1),np.uint8), iterations = 1)  #erosion
    image=cv2.morphologyEx(image, cv2.MORPH_OPEN, np.ones((2,1),np.uint8))  #opening - erosion followed by dilation
    #image=cv2.Canny(image, 50, 100)  #canny edge detection
    #image=cv2.matchTemplate(image, template, cv2.TM_CCOEFF_NORMED)  #template matching
    #display(image, 'Preprocessed')
    return image

def extract(searchlist, conn, jid, resultnum = resultnum,vect_path=vect_path, vect_data=vect_data):
    if not searchlist:
        return None
    vect = HashingVectorizer(norm=None, alternate_sign=False)
    try:
        with open(vect_path, 'rb') as f:
            vectorised = pickle.load(f)
    except FileNotFoundError:
        try:
            with open(vect_data, 'r', encoding='utf8') as dat:
                newdat=[]
                data = csv.reader(dat)
                for row in data:
                    newarr=[i.strip() for i in row if i not in ['Nil', '']]
                    newdat.append(' '.join(newarr))
                vectorised=vect.fit_transform(newdat)
        except FileNotFoundError:
            errormsg="No valid Vectorizer file or pre Vectorized file found"
            updateStatus(conn, jid, system_error, errormsg)
            return None
        with open(vect_path, 'wb') as f:
            pickle.dump(vectorised, f)
    nn_model = NearestNeighbors(n_neighbors=resultnum, algorithm='brute', metric='manhattan')
    nn_model.fit(vectorised)
    query = ' '.join([i.strip() for i in searchlist])
    query_vect = vect.transform([query])
    distances, indices = nn_model.kneighbors(query_vect)
    return indices[0]

def updateStatus(conn, jid, status, details):
    conn.execute("UPDATE jobs SET status=?, details=? WHERE jobid=?", (status, details, jid,))
    conn.commit()

def generateJson(source, uid, jid, searchtop=searchtop, resultnum=resultnum,
                  outpath=outjson, datapath=display_csv, vect_path=vect_path, vect_data=vect_data, status=status):
    start_time=time.time()
    try:
        conn = sqlite3.connect(status)
        updateStatus(conn, jid, processing_status, "")
    except sqlite3.OperationalError:
        errormsg="No valid Status database found"
        updateStatus(conn, jid, system_error, errormsg)
        return
    try:
        img=cv2.imdecode(np.frombuffer(source, np.uint8), cv2.IMREAD_COLOR)
        img=preprocess(img)
        wordlist=ocr(img)
    except cv2.error:
        errormsg="Invalid File(s) or Filepath(s)"
        updateStatus(conn, jid, user_error, errormsg)
        return
    wordlist=sortbysize(wordlist)
    wordlist=wordlist if len(wordlist)<searchtop else wordlist[:searchtop]
    searchlist = [word[1] for word in wordlist]
    indices = extract(searchlist, conn, jid, resultnum=resultnum, vect_path=vect_path, vect_data=vect_data)
    dictionary={'Jobid':jid, "Userid":uid}
    try:
        with open(datapath, 'r', encoding='utf8') as dcsv:
            data = list(csv.reader(dcsv))
            result={}
            for n, index in enumerate(indices):
                med_details = data[index]
                result[n+1] = {data[0][0]:med_details[0], data[0][1]:med_details[1], data[0][2]:med_details[2],
                                data[0][3]:med_details[3], data[0][4]:med_details[4], data[0][5]:med_details[5], 
                                data[0][6]:med_details[6], data[0][7]:med_details[7], data[0][8]:med_details[8]}
            dictionary['result']=result
    except FileNotFoundError:
        errormsg="Error(S): No valid medicine dataset found"
        updateStatus(conn, jid, system_error, errormsg)
        return
    except TypeError:
        dictionary['result']=None
    dictionary["Time Elapsed"] = time.time()-start_time
    try:
        if not os.path.exists(outpath):
            os.mkdir(outpath)
        with open(os.path.join(outpath, f"{jid}.json"), 'w') as j:
            json.dump(dictionary, j)
    except Exception as e:
        errormsg="Error(S): Unable to generate output file > " + str(e)
        updateStatus(conn, jid, system_error, errormsg)
        return
    updateStatus(conn, jid, complete_status, "Json Generated Successfully")
    conn.close()
    return
        
