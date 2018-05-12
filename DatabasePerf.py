import memcache
import mysql.connector
from flask import Flask, redirect, url_for, request,render_template
import timeit
import hashlib
import random




memc = memcache.Client(['memcache.plxcks.cfg.usw2.cache.amazonaws.com:11211'],debug=1)

connection = mysql.connector.connect(user='Krishna', password='krishna123',
                              host='cloudinstance1.caghrbzki2pp.us-west-2.rds.amazonaws.com',port='3306',
                              database='clouddb')

cursor = connection.cursor(buffered=True,dictionary=True)

#load all data to memcache


app = Flask(__name__)

if connection.is_connected():
    print('Connected to MySQL database')
else:exit(0)



def mc_querries(sql):
    count = 0;
    hash = hashlib.sha224(sql).hexdigest()
    sql_mc = memc.get(hash)
    if not sql_mc:
        cursor.execute(sql)
        rows = cursor.fetchall()
        memc.set(hash,rows)
        for row in rows:
            count = count + 1
    else:
        rows = memc.get(hash)
        for row in rows:
            count = count + 1
    return rows

def querries(sql):
    count = 0;
    i=0;
    alldetails = {}
    # memc.flush_all()
    hash = hashlib.sha224(sql).hexdigest()
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        if i <= 10:
            alldetails[i] = row['givenname'];
            i += 1;
            alldetails[i] = row['city'];
            i += 1;
            alldetails[i] = row['state'];
            i += 1;
            alldetails[i] = row['age'];
            i += 1;
            alldetails[i] = row['latitude'];
            i += 1;
            alldetails[i] = row['longitude'];
            i += 1;
        else:
            i = i+1;
    # memc.set(hash, rows)
    return alldetails



@app.route('/')
def upload():
    #cursor = connection.cursor()
    cursor.execute(""" SELECT count(*) FROM userdetails """)
    count = cursor.fetchall()
    return render_template("Display.html",Totalcount=count[0]['count(*)'])


@app.route('/cityname',methods=['POST'])
def cityname():
    alldetails = {}
    i=0;
    if request.method == 'POST':
        user_city = request.form['city']
        query = " SELECT GivenName,City,State FROM userdetails where City = '"+ user_city +"'"
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            alldetails[i] = row['City'];i+=1;
            alldetails[i] = row['GivenName'];i+=1;
            alldetails[i] = row['State'];i += 1;
    return render_template("Download.html", result=alldetails)

@app.route('/latitude',methods=['POST'])
def latitude():
    alldetails = {}
    i=0;
    if request.method == 'POST':
        latfrom = request.form['latfrom']
        latto = request.form['latto']
        agefrom = request.form['agefrom']
        ageto = request.form['ageto']
        query = " SELECT givenname,city,state,age,latitude,longitude FROM userdetails where latitude > "+ latfrom +" and latitude < "+ latto +" and age > "+ agefrom + " and age < "+ ageto +""
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            alldetails[i] = row['givenname'];i+=1;
            alldetails[i] = row['city'];i+=1;
            alldetails[i] = row['state'];i += 1;
            alldetails[i] = row['age'];
            i += 1;
            alldetails[i] = row['latitude'];
            i += 1;
            alldetails[i] = row['longitude'];
            i += 1;
    return render_template("Download1.html", result=alldetails, count=i)


@app.route('/latitudetime',methods=['POST'])
def latitudetime():
    alldetails = {}
    i=0;
    if request.method == 'POST':
        latfrom = request.form['latfrom']
        latto = request.form['latto']
        agefrom = request.form['agefrom']
        ageto = request.form['ageto']
        start_time = timeit.default_timer()
        for i in range(1, 1000):
            rand_id = random.randint(1, 20012)
            query = " SELECT givenname,city,state,age,latitude,longitude FROM userdetails where latitude > "+ latfrom +" and latitude < "+ latto +" and age > "+ agefrom + " and age < "+ ageto +""
            alldetails = querries(query)
        finish_time = timeit.default_timer() - start_time

    return render_template("Download2.html", result=alldetails, timetaken=finish_time)



@app.route('/memcache',methods=['POST'])
def memcache():
    alldetails = {}
    i=0;
    if request.method == 'POST':
        latfrom = request.form['latfrom']
        latto = request.form['latto']
        agefrom = request.form['agefrom']
        ageto = request.form['ageto']
        start_time = timeit.default_timer()
        for i in range(1, 1000):
            rand_id = random.randint(1, 20012)
            query = " SELECT givenname,city,state,age,latitude,longitude FROM userdetails where latitude > "+ latfrom +" and latitude < "+ latto +" and age > "+ agefrom + " and age < "+ ageto +""
            alldetails = mc_querries(query)
        finish_time = timeit.default_timer() - start_time

    return render_template("Download3.html", result=alldetails, timetaken=finish_time)


if __name__ == '__main__':
    app.run(host='0.0.0.0',port=5000,debug=True)