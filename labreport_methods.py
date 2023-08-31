import json, os
import sqlite3
import re
import warnings
import time
warnings.filterwarnings('ignore')

try:
    import numpy as np
    import fitz, cv2
    from paddleocr import PaddleOCR
    import dateutil.parser as dparser
    from fuzzywuzzy import fuzz
except ModuleNotFoundError as e:
    errormsg=str(e) + "\nUse \"pip install -r requirements.txt\""
    exit(-1)

path=os.path.dirname(os.path.abspath(__file__))

processing_status="Processing"
complete_status="Ready"
system_error='ERROR(S)'
user_error='ERROR(U)'
threshold=75   #thrshold for validate


results = ['result', 'value']
testname = ['test name', 'test',  'sample' , 'description', 'compound', 'component']
dates = ['date', 'reported', 'date of report', 'released on', 'billed at']
units = ['units']
THRESHOLD=85  #threshold for extract

status=os.path.join(path, "status.sqlite")
dbjson=os.path.join(path, "db.json")
outjson=os.path.join(path, "outputs")

#OCR Functions
def ocr(img):
    ocr = PaddleOCR(lang='en', use_angle_cls=True, show_log=False)
    res=ocr.ocr(img)[0]
    return [[i[0], i[1][0]] for i in res]

#Extract Functions
def sortbyrow(wordlist):
    ylength=[]
    for words in wordlist:
        ylength.append(abs(words[0][0][1]-words[0][2][1]))
    offset = 0.70*max(set(ylength), key = ylength.count) if ylength else 0
    sortbyy = []
    for i in wordlist:
        y = i[0][0][1]
        if len(sortbyy) == 0:
            sortbyy.append([i])
        else:
            flag=False
            for j in sortbyy:
                if abs(j[0][0][0][1]-y)<=offset:
                    flag=True
                    j.append(i)
                    break
            if not flag:
                sortbyy.append([i])
    return sortbyy

def getHeader(wordlist, searchlist):  #Get a header match from the document wordlist by searching from a predefined searchlist
    for i in searchlist:
        for rows in wordlist:
            for cellnum, cell in enumerate(rows):
                if matcher(cell[1].lower(), i)>=THRESHOLD:
                    return rows, cell, cellnum
    return None, None, None

def getColumn(rowlist, headerlist, shape, thin=False, oneperrow=False):
    rows, head,cellnum = getHeader(rowlist, headerlist)
    if rows:
        offleft, offright = findoff(rows, cellnum, shape, thin)
        col = []
        for row in rowlist:
            temp=[]
            for cell in row:
                if cell!=head and cell[0][3][1]>=head[0][3][1] and cell[0][0][0]>=offleft and cell[0][1][0]<=offright:
                    temp.append(cell) 
            if oneperrow and len(temp)>1:
                mincell=[]
                min=1000
                headstart, headend=head[0][0][0], head[0][1][0]
                for i in temp:
                    commonarea=abs(headstart-i[0][1][0]) if headstart>i[0][0][0] else abs(headend-i[0][0][0])
                    if commonarea<min:
                        min=commonarea
                        mincell=i
                col.append(mincell)
            else:
                col = col+temp
        return list(col)
    return []

def findoff(row, location, shape, thin=False):
    leftoff=20
    if len(row)==1:
        return 0, shape[1]
    elif location==0:
        if thin:
            return row[location][0][0][0]-leftoff, (row[location][0][0][0]+row[location+1][0][0][0])/2
        else:
            return 0, row[location+1][0][0][0]
    elif location==len(row)-1:
        if thin:
            return row[location][0][0][0]-leftoff, shape[1]
        else:
            return row[location-1][0][0][0], shape[1]
    else:
        if thin:
            return row[location][0][0][0]-leftoff, (row[location][0][1][0]+row[location+1][0][0][0]+leftoff)/2
        else:
            return row[location-1][0][1][0], row[location+1][0][0][0]

def findDate(wordlist, depth=10):
    d = re.compile(r'\d{1,4}[.\-/]\d{1,2}[.\-/]\d{1,4}')
    for i in dates:
        for num, rows in enumerate(wordlist):
            if matcher(rows[1].lower(), i)>=THRESHOLD:
                    for k in range(1, depth+1):
                        try:
                            docdate=dparser.parse(wordlist[num+k][1], fuzzy=True)
                        except:
                            docdate=''
                        if not docdate:
                            try:
                                docdate=re.search(d, wordlist[num+k][1])
                            except IndexError:
                                continue
                            if docdate:docdate=docdate.group()
                        if docdate:return docdate
    from datetime import date
    return date.today()

def data_clean(data, unitcol=True, startcount=1):
    dic={}
    valextract = re.compile(r'\d*\s{0,2}[-.,_"]?\s{0,2}\d+')
    for i in data:
        if len(i)<=1 or len(i)>=4:
            continue
        result=i[1][1]
        found=re.findall(valextract, result)
        if not unitcol:   #if separate column for units used doesnt exist
            if found:tfound=found[0].strip()
            else:continue
            unit = result.replace(tfound, '').strip()
            if len(unit)<=0:
                unit=''
        else:unit=i[-1][1]
        if found:
            found=found[0].strip()
            try:
                result=float(found)
            except ValueError:    
                corrected=False
                while not corrected:
                    try:
                        result=float(found)
                        corrected=True
                    except(ValueError, TypeError):
                        found = found.replace(',', '.')
                        found = found.replace(' ', '.')
                        found = found.replace('-', '.')
                        dec = found.find('.')   #remove extra decimals 
                        found = found[:dec+1] + found[dec+1:].replace('.', '')
        dic[str(startcount)] = {i[0][1] : {'value' : result, 'unit' : unit}}
        startcount+=1
    return dic

def extract(doclist):
    startcount=1
    dictionary={'Date':'', 'Readings':{}}
    for doc in doclist:
        wordlist, shape=doc 
        wordsbyrow = sortbyrow(wordlist)
        tnamecolumn = getColumn(wordsbyrow, testname, shape)
        resultcolumn = getColumn(wordsbyrow, results, shape, True)
        unitcol = getColumn(wordsbyrow, units, shape, oneperrow=True)
        totallist=tnamecolumn+resultcolumn+unitcol
        unitcolempty=True if unitcol else False
        extracted = data_clean(sortbyrow(totallist), unitcolempty, startcount=startcount)
        if not dictionary['Date']:
            date = findDate(wordlist)
            dictionary['Date'] = str(date)
        dictionary['Readings'] = {**dictionary['Readings'], **extracted}
        startcount=len(extracted)+1
    return dictionary

#Validate Functions
def matcher(a, b):
    a, b=a.lower(), b.lower()
    w1, w2, w3,w4 =0.41830895, 0.3991425, 0.07020547, 0.11234309   #Optimal Calculated Weights
    p1=fuzz.ratio(a, b)
    p2=fuzz.partial_ratio(a, b)
    p3=fuzz.partial_token_sort_ratio(a, b)
    p4=fuzz.WRatio(a, b)
    return (p1*w1+p2*w2+p3*w3+p4*w4)

def getothernames(dbtest, dbtestname):
    otherlist=dbtest[dbtestname]['Other Names']
    otherlist.append(dbtestname)
    return otherlist

def cleandictwithref(d, reference, threshold=75):
    newdat, discard={}, {} 
    newdatcounter, discardcounter=0, 0
    referencelist=list(reference.values())
    for doctestname in d:
        curr=d[doctestname]
        doctestname=list(curr.keys())[0]
        docvalue=curr[doctestname]['value']
        docunit=curr[doctestname]['unit']
        if reference:
            namemax, namemaxindex, namemaxval=0, 0, ''
            for m, dbtest in enumerate(referencelist):
                dbtestname=list(dbtest.keys())[0]
                nametest=[matcher(doctestname, i) for i in getothernames(dbtest, dbtestname)]
                eval=max(nametest)
                if eval>namemax:
                    namemax, namemaxindex, namemaxval=eval, m, dbtestname  #Information of dbname with max match
                if namemax==100:break
                
            if namemax>=threshold: 
                unitmax,unitmaxval=0,''
                unitlist=referencelist[namemaxindex][namemaxval]["Units"]
                if len(docunit.strip())==0:docunit=unitlist[0]  #if no units are present, take first unit from the list as unit
                
                for units in unitlist:
                    unitscore=matcher(units,docunit)
                    if unitscore>unitmax:
                        unitmax,unitmaxval=unitscore, units
                    if unitmax==100:break
                if unitmax==0:docunit=unitlist[0]   #if all units return 0 match, take first unit from the list as unit
                if unitmax>=threshold*0.4:      #case with both name hit and unit hit
                    newdat[str(newdatcounter+1)] = {namemaxval:{'value':docvalue, 'unit':unitmaxval}}
                else:       #case with name hit and nut no unit hit
                    newdat[str(newdatcounter+1)] = {namemaxval:{'value':docvalue, 'unit':docunit}}
                newdatcounter+=1
            else:
                discard[str(discardcounter+1)] = {doctestname:{'value':docvalue, 'unit':docunit}}
                discardcounter+=1
        else:
            return d, {}       
    return newdat, discard

#Misc functions
def pdftoimg(pdfdata):
    imglist=[]
    pdf_doc = fitz.open(stream=pdfdata)
    for page_idx in range(pdf_doc.page_count):
        page = pdf_doc.load_page(page_idx)
        pix = page.get_pixmap()
        img_bytes = pix.tobytes("ppm")
        imglist.append(cv2.imdecode(np.frombuffer(img_bytes, np.uint8), cv2.IMREAD_COLOR))
    return imglist

def preprocess(image):
    image=cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)  #grayscale
    image=cv2.medianBlur(image,1)  # noise removal
    if(image.shape[0]>1200 and image.shape[1]>1500):
        image = cv2.adaptiveThreshold(image, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 85, 11)   #adaptive thresholding
        #image=cv2.threshold(image, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]  #thresholding
        #image=cv2.dilate(image, np.ones((5,5),np.uint8), iterations = 1)  #dilation
        #image=cv2.erode(image, np.ones((5,5),np.uint8), iterations = 1)  #erosion
        #image=cv2.morphologyEx(image, cv2.MORPH_OPEN, np.ones((5,5),np.uint8))  #opening - erosion followed by dilation
        #image=cv2.Canny(image, 100, 200)  #canny edge detection
        #image=cv2.matchTemplate(image, template, cv2.TM_CCOEFF_NORMED)  #template matching
    #display(image, 'Preprocessed')
    return image

def updateStatus(conn, jid, status, details):
    conn.execute("UPDATE jobs SET status=?, details=? WHERE jobid=?", (status, details, jid,))
    conn.commit()

#Main Driver Function
def generateJson(source, sourcetype, uid, jid,json_path=outjson, dbjson=dbjson, status=status, threshold=threshold):
    start_time=time.time()
    try:
        conn = sqlite3.connect(status)
        updateStatus(conn, jid, processing_status, "")
    except sqlite3.OperationalError:
        errormsg="No valid Status database found"
        return
    doclist=[]
    try:
        db=open(dbjson, 'r')
        dbdata=json.load(db)
    except (FileNotFoundError):
        errormsg="Valid database json file not found"
        updateStatus(conn, jid, system_error, errormsg)
        return
    source=source if type(source)==list else [source]
    for dat in source:
        try:
            if sourcetype=='.pdf':
                imglist=pdftoimg(dat)
            else:
                imglist=[cv2.imdecode(np.frombuffer(i, np.uint8), cv2.IMREAD_COLOR) for i in source]
            for page in imglist:
                page=preprocess(page)
                wordlist, shape=ocr(np.array(page)), page.shape
                doclist.append((wordlist, shape))
        except (fitz.fitz.FileNotFoundError, fitz.fitz.FileDataError,cv2.error):
            errormsg="Invalid File(s) or Filepath(s)"
            updateStatus(conn, jid, user_error, errormsg)
            return
    extracted = extract(doclist)
    dic, date = extracted['Readings'], extracted['Date']
    if not dic:
        print("Warning: No Relevant Cells found in the file.")
    data, discard = cleandictwithref(dic, dbdata, threshold)
    if not data and discard:
        print("Warning: No database matches found.")
    try:
        if not os.path.exists(json_path):
            os.mkdir(json_path)
        with open(os.path.join(json_path, f"{jid}.json"), 'w') as j:
            dictionary = {'JobID':jid,'UserID':uid, 'Date':str(date), 'Time Elapsed':time.time()-start_time,'Valid':data, 'Discard':discard}
            json.dump(dictionary, j)
    except Exception as e:
        errormsg="Unable to generate output file > " + str(e)
        updateStatus(conn, jid, system_error, errormsg)
        return
    updateStatus(conn, jid, complete_status, "Json Generated Successfully")
    conn.close()
    return
