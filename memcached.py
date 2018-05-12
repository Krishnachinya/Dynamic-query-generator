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



@app.route('/')
def upload():
    #cursor = connection.cursor()
    cursor.execute(""" SELECT count(*) FROM students """)
    count = cursor.fetchall()
    return render_template("Display.html",Totalcount=count[0]['count(*)'])


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
    # memc.flush_all()
    hash = hashlib.sha224(sql).hexdigest()
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        count = count +1
    # memc.set(hash, rows)
    return rows

@app.route('/Ipedsid',methods=['POST'])
def Ipedsid():
    if request.method == 'POST':
        user_query = request.form['qry']
        m_cache = int(request.form['one'])
        print user_query
        print m_cache
        if m_cache == 1:
            start_time = timeit.default_timer()
            #with memcache. running it for 1000 times
            for i in range(1, 1000):
                # rand_id = random.randint(100636, 483212)
                query = "SELECT * From students where " + user_query +" limit 1000"
                querymemc = mc_querries(query)
            finish_time = timeit.default_timer() - start_time
            return "Time taken using memcache is"+finish_time
        else:
            start_time = timeit.default_timer()
            # without memcache. running it for 1000 times
            for i in range(1, 1000):
                # rand_id = random.randint(100636, 483212)
                query = "SELECT * From students where " + user_query +" limit 1000"
                querymemc = querries(query)
            finish_time = timeit.default_timer() - start_time
            return "Time taken using database is" + str(finish_time)

if __name__ == '__main__':
    app.run()