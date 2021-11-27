from flask import Flask, request, send_file
from flask_restx import Resource, Api, reqparse, fields, marshal
import sqlite3, requests, json, re, time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm
from datetime import datetime, timedelta
import random

app = Flask(__name__)
api = Api(app)

def checkTableExists(dbcon, tablename):
    dbcur = dbcon.cursor()
    try:
        dbcur.execute("SELECT * FROM {}".format(tablename))
        return True
    except:
        return False
    finally:
        dbcur.close()

main_table = ' \
CREATE TABLE Shows( \
    id INTEGER UNIQUE NOT NULL PRIMARY KEY,\
    tvmaze_id INTEGER NOT NULL,\
    last_update TEXT,\
    name TEXT,\
    type TEXT,\
    language TEXT,\
    genres json,\
    status TEXT,\
    runtime INTEGER,\
    premiered TEXT,\
    officialSite TEXT,\
    schedule json,\
    rating json,\
    weight INTEGER,\
    network json,\
    summary TEXT\
)'

def myDB(query):
    conn = sqlite3.connect('z5261841.db', timeout=10)
    cur = conn.cursor()

    cur.execute(query)
    conn.commit()
    res = cur.fetchall()

    cur.close()
    conn.close()
    return res

parser1 = reqparse.RequestParser()
parser1.add_argument('name', type=str, help='TV show name')
@api.param('name', 'TV show name')
@api.response(200, 'Success')
@api.response(400, 'Malformed Request')
@api.response(404, 'Name Not Found')
@api.route('/tv-shows/import')
class importShow(Resource):
    def post(self):
        args = parser1.parse_args()
        query = str(args.get('name'))
        query = query.replace("-", " ")
        req = requests.get('http://api.tvmaze.com/search/shows?q=' + query)

        if req.status_code != 200:
            return "Name Not Found", 404

        try:
            (req.json())[0].get('show')
        except:
            return "Name Not Found", 404

        info = (req.json())[0].get('show')

        if (info.get('name')).lower() != query.lower():
            return "Name Not Found", 404

        tvmaze_id = info.get('id')
        lastUpdate =  time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime())
        showName = info.get('name') 
        showType = info.get('type')
        showLanguage = info.get('language')
        showGenres = json.dumps(info.get('genres'))
        showStat = info.get('status')
        showRt = info.get('runtime')
        premiered = info.get('premiered')
        officialSite = info.get('officialSite')
        showSchedule = json.dumps(info.get('schedule'))
        rating = json.dumps(info.get('rating'))
        weight = info.get('weight')
        showNetwork = json.dumps(info.get('network'))
        showSummary = info.get('summary')
        

        id = 1
        new_id = re.findall('\d+', str(myDB('SELECT max(id) from Shows')))
        if new_id:
            id = int(new_id[0]) + 1
        
        showSummary = showSummary.replace("\\", "")
        showSummary = showSummary.replace("\"", "\"\"")
        insertQuery = 'INSERT INTO Shows values ("{}", "{}", "{}", "{}", "{}", "{}", \'{}\', "{}", "{}", "{}", "{}", \'{}\', \'{}\', "{}", \'{}\', "{}");'.format(id, tvmaze_id, lastUpdate, showName, showType,showLanguage, showGenres, showStat, showRt, premiered, officialSite, showSchedule, rating, weight, showNetwork, showSummary)
        #"<p><b>Good Girls</b> follows three \"good girl\" suburban wives and mothers who suddenly find themselves in desperate circumstances and decide to stop playing it safe, and risk everything to take their power back.</p>"
        #insertQuery = 'INSERT INTO test values ("{}", \'{}\');'.format(id, showNetwork)
        myDB(insertQuery)

        hrefUrl = request.base_url.split('/')[2]
        ids = re.findall('\d+', str(myDB('SELECT id from Shows')))
        index = ids.index(str(id))
        total = len(ids)

        hrefDict = {
            "self": {
                "href": "http://" + hrefUrl + "/tv-shows/" + str(id)
            }
        }

        if total != 1:
            res = re.findall('\d+', 'SELECT id FROM Shows WHERE id={}'.format(ids[index - 1]))
            previousUrl = "http://" + hrefUrl + "/tv-shows/" + res[0]
            hrefDict['previous'] = {
                "href": previousUrl
            }
    
        return {
            "id" : id,  
            "last-update": lastUpdate,
            "tvmaze-id" : tvmaze_id,
            "_links": hrefDict,
        }, 201

schedule_payload = api.model('showSchedule_payload', {
    'time': fields.String(required=False),
    'days': fields.List(fields.String, required=False),
})

rating_payload = api.model('rating_payload', {
    'average': fields.Float(required=False)
})

country_payload = api.model('country_payload', {
    'name': fields.String(required=False),
    'code': fields.String(required=False),
    'timezone': fields.String(required=False),
})

network_payload = api.model('network_payload', {
    'id': fields.Integer(required=False),
    'name': fields.String(required=False),
    'country': fields.Nested(country_payload, required=False)
})

payload = api.model('Payload', {
    'name': fields.String(required=False),
    'type': fields.String(required=False),
    'language': fields.String(required=False),
    'genres': fields.List(fields.String, required=False),
    'status': fields.String(required=False),
    'runtime': fields.Integer(required=False),
    'premiered': fields.String(required=False),
    'officialSite': fields.String(required=False),
    'schedule': fields.Nested(schedule_payload, required=False),
    'rating': fields.Nested(rating_payload, required=False),
    'weight': fields.Integer(required=False),
    'network': fields.Nested(network_payload, required=False),
    'summary': fields.String(required=False),
})

@api.response(200, 'Success')
@api.response(400, 'Malformed Request')
@api.response(404, 'Id Not Found')
@api.route('/tv-shows/<int:id>')
class showId(Resource):
    @api.expect(payload, validate=True)
    def patch(self, id):
        sqlquery = 'SELECT * FROM Shows WHERE id="{}"'.format(id)
        res = myDB(sqlquery)
        if not res:
            return "Id Not Found", 404
        
        json_dict = request.json
        timeNow = time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime())
        
        conn = sqlite3.connect('myDB.db')
        cur = conn.cursor()
        cur.execute('SELECT * from Shows')
        headers = [x[0] for x in cur.description]
        cur.close()
        conn.close()

        headerDict = {}
        for header in json_dict.keys():
            if header == 'id' or header == 'last_update' or header == 'tvmaze_id':
                return "Invalid Parameters", 400

            if header in headers:
                headerDict[header] = json_dict[header]
            else:
                return "Malformed request", 400

        for header in headerDict.keys():
            if header == 'schedule' or header == 'rating' or header == 'genres' or header == 'network':
                myDB('UPDATE Shows SET "{}" = \'{}\' WHERE id="{}"'.format(header, json.dumps(headerDict[header]), id))
            else:
                myDB('UPDATE Shows SET "{}" = "{}" WHERE id="{}"'.format(header, headerDict[header], id))

        #myDB('UPDATE Shows SET "{}" = "{}" WHERE id="{}"'.format(header, json_dict[header], id))
        hrefUrl = request.base_url.split('/')[2]
        ids = re.findall('\d+', str(myDB('SELECT id from Shows')))
        index = ids.index(str(id)) 
        total = len(ids)
        
        hrefDict = {
            "self": {
                "href": "http://" + hrefUrl + "/tv-shows/" + str(id)
            }
        }

        if total != 1 and index != 0:
            res = re.findall('\d+', 'SELECT id FROM Shows WHERE id={}'.format(ids[index - 1]))
            previousUrl = "http://" + hrefUrl + "/tv-shows/" + res[0]
            hrefDict['previous'] = {
                "href": previousUrl
            }

        if total - 1 > index:
            res = re.findall('\d+', 'SELECT id FROM Shows WHERE id={}'.format(ids[index + 1]))
            nextUrl = "http://" + hrefUrl + "/tv-shows/" + res[0]
            hrefDict['next'] = {
                "href": nextUrl
            }

        return {  
            "id" : id,  
            "last-update": timeNow,
            "_links": hrefDict
        }, 200
        
        

    def get(self, id):
        sqlquery = 'SELECT * FROM Shows WHERE id="{}"'.format(id)
        res = myDB(sqlquery)
        if not res:
            return "Id Not Found", 404

        (db_id, tvmaze_id, lastUpdate, showName, showType, showLanguage, showGenres, showStat, showRt, premiered, officialSite, showSchedule, rating, weight, showNetwork, showSummary) = res[0]

        hrefUrl = request.base_url.split('/')[2]
        ids = re.findall('\d+', str(myDB('SELECT id from Shows')))
        index = ids.index(str(id))
        total = len(ids)
        
        hrefDict = {
            "self": {
                "href": "http://" + hrefUrl + "/tv-shows/" + str(id)
            }
        }

        if total != 1 and index != 0:
            res = re.findall('\d+', 'SELECT id FROM Shows WHERE id={}'.format(ids[index - 1]))
            previousUrl = "http://" + hrefUrl + "/tv-shows/" + res[0]
            hrefDict['previous'] = {
                "href": previousUrl
            }

        if total - 1 > index:
            res = re.findall('\d+', 'SELECT id FROM Shows WHERE id={}'.format(ids[index + 1]))
            nextUrl = "http://" + hrefUrl + "/tv-shows/" + res[0]
            hrefDict['next'] = {
                "href": nextUrl
            }

        return {
            "tvmaze-id" :tvmaze_id,
            "id": db_id,
            "last-update": lastUpdate,
            "name": showName,
            "type": showType,
            "language": showLanguage,
            "genres": json.loads(showGenres),
            "status": showStat,
            "runtime": showRt,
            "premiered": premiered,
            "officialSite": officialSite,
            "schedule": json.loads(showSchedule),
            "rating": json.loads(rating),
            "weight": weight,
            "network": json.loads(showNetwork),
            "summary": showSummary,
            "_links": hrefDict
        }, 200

    def delete(self, id):
        sqlquery = 'SELECT * FROM Shows WHERE id={}'.format(id)
        res = myDB(sqlquery)
        if not res:
            return "Id Not Found", 404

        deleteQuery = 'DELETE FROM Shows WHERE id={}'.format(id)
        
        myDB(deleteQuery)

        return { 
            "message" :"The tv show with id " + str(id) + " was removed from the database!",
            "id": id
        }, 200
    
parser2 = reqparse.RequestParser()
parser2.add_argument('orderby', type=str, default='+id', help='Order By')
parser2.add_argument('page', type=int, help='Page', default=1)
parser2.add_argument('page_size', type=int, help='Page size', default=100)
parser2.add_argument('filter', type=str, help='Filter', default='id,name')
@api.response(200, 'Success')
@api.response(400, 'Malformed Request')
@api.param('orderby', 'Order By')
@api.param('page', 'Page')
@api.param('page_size', 'Page Size')
@api.param('filter', 'Filter')
@api.route('/tv-shows')
class showOrderBy(Resource):
    def get(self):
        args = parser2.parse_args()
        orderbyParam = str(args.get('orderby'))
        page = args.get('page')
        pageSize = args.get('page_size')
        filterParam = str(args.get('filter'))
        orderByValid = ["id", "name", "runtime", "premiered", "rating"]
        filtersValid = ["tvmaze_id", "id", "last-update", "name", "type", "language", "genres", "status", "runtime", "premiered", "officialSite", "schedule", "rating", "weight", "network", "summary"]

        sqlquery = 'SELECT COUNT(*) FROM Shows'
        total = (myDB(sqlquery))[0][0]
        start = (page - 1) * pageSize


        if total < pageSize:
            return "Database is smaller than page size", 400

        if total <= start:
            return "No more pages", 404
        
        orderbyParamList = orderbyParam.split(',')
        orderbyList = []
        
        for orderby in orderbyParamList:
            if orderby[1:] == 'rating-average':
                orderby = orderby[:7]
            if orderby[0] == '+':
                if orderby[1:] in orderByValid:
                    orderbyList.append(orderby[1:] + " ASC")
            elif orderby[0] == '-':
                if orderby[1:] in orderByValid:
                    orderbyList.append(orderby[1:] + " DESC")
            else:
                return "Invalid Parameters", 400

        filtersParamList = filterParam.split(',')
        filtersList = []
        for filters in filtersParamList:
            if filters in filtersValid:
                if filters == "last-update":
                    filtersList.append("last_update")
                    continue
                filtersList.append(filters)
            else:
                return "Invalid Parameters", 400

        orderbyquery = ', '.join(orderbyList) 
        filtersquery = ', '.join(filtersList)
        
        
        sqlquery = 'SELECT {} FROM Shows ORDER BY {} LIMIT {}, {}'.format(filtersquery, orderbyquery, start, pageSize)
        df = pd.DataFrame(myDB(sqlquery))

        conn = sqlite3.connect('z5261841.db')
        cur = conn.cursor()
        cur.execute('SELECT {} from Shows'.format(filtersquery))
        df.columns = [x[0] for x in cur.description]
        cur.close()
        conn.close()

        if 'rating' in df.columns:
            df['rating'] = df['rating'].apply(json.loads)
        if 'network' in df.columns:
            df['network'] = df['network'].apply(json.loads)
        if 'genres' in df.columns:
            df['genres'] = df['genres'].apply(json.loads)
        if 'schedule' in df.columns:
            df['schedule'] = df['schedule'].apply(json.loads)
        
        hrefUrl = request.base_url.split('/')[2]
        
        hrefDict = {
            "self": {
                "href": "http://" + hrefUrl + "/tv-shows?" + "order_by=" + orderbyParam + "&page=" + str(page) + "&page_size=" + str(pageSize) + "&filter=" + filterParam
            }
        }

        if page != 1:
            previousUrl = "http://" + hrefUrl + "/tv-shows?" + "order_by=" + orderbyParam + "&page=" + str(page - 1) + "&page_size=" + str(pageSize) + "&filter=" + filterParam
            hrefDict['previous'] = {
                "href": previousUrl
            }

        if (start + pageSize) < total:
            nextUrl = "http://" + hrefUrl + "/tv-shows?" + "order_by=" + orderbyParam + "&page=" + str(page + 1) + "&page_size=" + str(pageSize) + "&filter=" + filterParam
            hrefDict['next'] = {
                "href": nextUrl
            }

        return {
            "page": page,
            "page-size": pageSize,
            "tv-shows": json.loads(df.to_json(orient='records')),
            "_links": hrefDict
        }, 200

def checkDate(x):
    dt = datetime.strptime(x, "%Y-%m-%d-%H:%M:%S")
    dt_to_string = dt.strftime("%Y-%m-%d %H:%M:%S")
    set_date = datetime.strptime(dt_to_string,"%Y-%m-%d %H:%M:%S")

    if datetime.now()-timedelta(hours=24) <= set_date <= datetime.now():
        return set_date 
    else:
        return "x"

def addToDict(param, count, d, total):
    d[param] = '%.2f' % ((count / total) * 100)

def addToList(param, count, lst, total):
    if (param is None):
        param = "None"
    lst.append(param + " - " + str('%.2f' % ((count / total) * 100)) + "%")

def addToListLanguage(param, lst, total=100):
    if (param is None):
        newParam = "None"
        lst.append(newParam)
        return
    if type(param) is int:
        print("AA")
        param = param/total * 100

    lst.append(param)

def labeling(rects, ax):
        for i in rects:
            height = i.get_height()
            ax.text(i.get_x() + i.get_width()/2, 1.01*height, '%.2f' % float(height), ha='center', va='bottom', fontsize=10, rotation=90)

parser3 = reqparse.RequestParser()
parser3.add_argument('format', type=str, help='Format')
parser3.add_argument('by', type=str, help='Page')
@api.response(200, 'Success')
@api.response(400, 'Malformed Request')
@api.param('format', 'Format')
@api.param('by', 'By')
@api.route('/tv-shows/statistics')
class getVisualization(Resource):
    def get(self):
        args = parser3.parse_args()
        formatParam = str(args.get('format'))
        byParam = args.get('by')
        byParamValid = ["language", "genres", "status", "type"]

        if byParam not in byParamValid:
            return "Invalid Parameters", 400

        sqlquery = 'SELECT last_update FROM Shows'
        df_date = pd.DataFrame(myDB(sqlquery))
        df_date.columns = ["last_update"]
        listofDate = list(df_date['last_update'].apply(lambda x: checkDate(x)))

        totalUpdated = len([s for s in listofDate if s != "x"])
        total = df_date.shape[0]

        sqlquery = 'SELECT {}, COUNT(*) FROM Shows GROUP BY {}'.format(byParam, byParam)
        df = pd.DataFrame(myDB(sqlquery))
        df.columns = [byParam, "value"]
        
        outDict = {}

        if formatParam == "json":
            if byParam == "genres":
                df[byParam] = df[byParam].apply(lambda x: x.replace("\"", ""))
            df.apply(lambda x: addToDict(x[byParam], x['value'], outDict, total), axis=1)
            return {
                "total": total,
                "total-updated": totalUpdated,
                "values": outDict
            }, 200
        elif formatParam == 'image':
            plt.clf()
            data = df[byParam]
            value = df['value']
            if byParam == "language" or byParam == "genres":
                labels = []
                values = []
                df.apply(lambda k: addToListLanguage(k[byParam], labels), axis=1)
                df.apply(lambda k: addToListLanguage(k['value'], values, total), axis=1)

                #plt.figure(figsize=(20,10))
                if len(labels) > 10:
                    fig, ax = plt.subplots(figsize=(20,10))
                else:
                    fig, ax = plt.subplots()
                rects = ax.bar(labels, values, color ='lightskyblue', width = 0.75)

                labeling(rects, ax)

                ax.set_ylabel('Percentage (%)', fontweight ='bold', fontsize = 12)
                ax.set_xlabel(byParam, fontweight ='bold', fontsize = 15)
                ax.xaxis.set_tick_params(rotation=90)
                #plt.rcParams["figure.figsize"] = (20,3)
                plt.savefig('z5261841.png', bbox_inches='tight')
                return send_file('z5261841.png', cache_timeout=0)
            else:
                plt.clf()
                n = len(data)

                cs = cm.Set1(np.arange(n)/n)
                patches, texts = plt.pie(value, colors=cs, startangle=90, radius=1.2)
                
                labels = []
                df.apply(lambda x: addToList(x[byParam], x['value'], labels, total), axis=1)
                plt.legend(patches, labels, loc="best", bbox_to_anchor=(-0.1, 1.), fontsize=8)
                plt.savefig('z5261841.png', bbox_inches='tight')
                return send_file('z5261841.png', cache_timeout=0)


if __name__ == "__main__":
    conn = sqlite3.connect('z5261841.db')
    cur = conn.cursor()

    if checkTableExists(conn, 'Shows'):
        cur.execute('DROP TABLE Shows')
        cur.execute(main_table)
    else:
        cur.execute(main_table)

    cur.close()
    conn.close()

    app.run(debug=True)


   #[{"id":1,"url":"https://www.tvmaze.com/shows/1/under-the-dome","name":"Under the Dome","type":"Scripted","language":"English","genres":["Drama","Science-Fiction","Thriller"],"status":"Ended","runtime":60,"premiered":"2013-06-24","officialSite":"http://www.cbs.com/shows/under-the-dome/","schedule":{"time":"22:00","days":["Thursday"]},"rating":{"average":6.6},"weight":95,"network":{"id":2,"name":"CBS","country":{"name":"United States","code":"US","timezone":"America/New_York"}},"webChannel":null,"externals":{"tvrage":25988,"thetvdb":264492,"imdb":"tt1553656"},"image":{"medium":"https://static.tvmaze.com/uploads/images/medium_portrait/81/202627.jpg","original":"https://static.tvmaze.com/uploads/images/original_untouched/81/202627.jpg"},"summary":"<p><b>Under the Dome</b> is the story of a small town that is suddenly and inexplicably sealed off from the rest of the world by an enormous transparent dome. The town's inhabitants must deal with surviving the post-apocalyptic conditions while searching for answers about the dome, where it came from and if and when it will go away.</p>","updated":1573667713,"_links":{"self":{"href":"https://api.tvmaze.com/shows/1"},"previousepisode":{"href":"https://api.tvmaze.com/episodes/185054"}}}