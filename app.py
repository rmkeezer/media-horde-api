from __future__ import print_function

from flask import Flask
from flask_restful import Resource, Api
from flask_restful import reqparse
from flaskext.mysql import MySQL
from flask_cors import CORS
import re, string

import sys

mysql = MySQL()
app = Flask(__name__)
CORS(app)

# MySQL configurations
app.config['MYSQL_DATABASE_USER'] = 'root'
app.config['MYSQL_DATABASE_PASSWORD'] = 'smoothie42' # THIS IS TEMPORARY
app.config['MYSQL_DATABASE_DB'] = 'mediahorde'
app.config['MYSQL_DATABASE_HOST'] = 'localhost'

mysql.init_app(app)

api = Api(app)

def authenticate(args):
    pattern = re.compile('[^\w_]+')
    for arg in args:
        if arg != 'email' and arg != 'password':
            args[arg] = pattern.sub('', args[arg])

    _username = args['email']
    _password = args['password']

    conn = mysql.connect()
    cursor = conn.cursor()
    cursor.callproc('spAuthenticateUser',(_username,))
    data = cursor.fetchall()

    if(len(data)>0):
        if(str(data[0][2])==_password):
            return {'status':200,'UserId':str(data[0][0])}
    return {'status':100,'message':'Authentication failure'}
    
class AuthenticateUser(Resource):
    def get(self):
        try:
            parser = reqparse.RequestParser()
            parser.add_argument('email', type=str, help='Email address for Authentication')
            parser.add_argument('password', type=str, help='Password for Authentication')
            args = parser.parse_args()

            _userEmail = args['email']
            _userPassword = args['password']

            return authenticate(_userEmail, _userPassword)

        except Exception as e:
            return {'error': str(e)}

def getRows(_tableName, _numrows):
    conn = mysql.connect()
    cursor = conn.cursor()
    if _numrows == '*':
        cursor.callproc('spGetAllRows',(_tableName,))
    else:
        cursor.callproc('spGetRows',(_tableName, _numrows))
    data = cursor.fetchall()
    
    out = []
    for item in data:
        out.append([i.decode('utf-8') if type(i) == bytes else i for i in item])

    return {'StatusCode':'200','Items':out}

def getRowsOrdered(_tableName, _numrows, _order, _dir):
    conn = mysql.connect()
    cursor = conn.cursor()
    cursor.callproc('spGetRowsOrdered',(_tableName, _numrows, _order, _dir))
    data = cursor.fetchall()
    
    out = []
    for item in data:
        out.append([i.decode('utf-8') if type(i) == bytes else i for i in item])

    return {'StatusCode':'200','Items':out}

def getJoinedRowsOrdered(_table1, _table2, _numrows, _order, _dir):
    conn = mysql.connect()
    cursor = conn.cursor()
    cursor.callproc('spGetJoinedRowsOrdered',(_table1, _table2, _numrows, _order, _dir))
    data = cursor.fetchall()
    
    out = []
    for item in data:
        out.append([i.decode('utf-8') if type(i) == bytes else i for i in item])

    return {'StatusCode':'200','Items':out}

def addAuthArgs(parser):
    parser.add_argument('email', type=str, help='Email address for Authentication')
    parser.add_argument('password', type=str, help='Password for Authentication')

class GetRows(Resource):
    def get(self):
        try: 
            parser = reqparse.RequestParser()
            addAuthArgs(parser)
            parser.add_argument('tableName', type=str)
            parser.add_argument('numRows', type=str)
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}

            _tableName = args['tableName']
            _numRows = args['numRows']

            return getRows(_tableName, _numRows)

        except Exception as e:
            return {'error': str(e)}

class GetAllRows(Resource):
    def get(self):
        try: 
            parser = reqparse.RequestParser()
            addAuthArgs(parser)
            parser.add_argument('tableName', type=str)
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}

            _tableName = args['tableName']

            return getRows(_tableName, '*')

        except Exception as e:
            return {'error': str(e)}

class GetRowsOrdered(Resource):
    def get(self):
        try: 
            parser = reqparse.RequestParser()
            addAuthArgs(parser)
            parser.add_argument('tableName', type=str)
            parser.add_argument('numRows', type=str)
            parser.add_argument('order', type=str)
            parser.add_argument('dir', type=str)
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}

            _tableName = args['tableName']
            _numRows = args['numRows']
            _order = args['order']
            _dir = args['dir']

            return getRowsOrdered(_tableName, _numRows, _order, _dir)

        except Exception as e:
            return {'error': str(e)}

class GetJoinedRowsOrdered(Resource):
    def get(self):
        try: 
            parser = reqparse.RequestParser()
            addAuthArgs(parser)
            parser.add_argument('tableName', type=str)
            parser.add_argument('numRows', type=str)
            parser.add_argument('order', type=str)
            parser.add_argument('dir', type=str)
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}

            _tableName = args['tableName']
            _numRows = args['numRows']
            _order = args['order']
            _dir = args['dir']

            return getRowsOrdered(_tableName, _numRows, _order, _dir)

        except Exception as e:
            return {'error': str(e)}

def getXRandRows(_tableName, _numRows):
    conn = mysql.connect()
    cursor = conn.cursor()
    cursor.callproc('spGetXRandRows',(_tableName,_numRows,))
    data = cursor.fetchall()

    out = []
    for item in data:
        out.append([i.decode('utf-8') if type(i) == bytes else i for i in item])

    return {'StatusCode':'200','Items':out}

class GetXRandRows(Resource):
    def get(self):
        try: 
            parser = reqparse.RequestParser()
            addAuthArgs(parser)
            parser.add_argument('tableName', type=str)
            parser.add_argument('numRows', type=str)
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}

            _tableName = args['tableName']
            _numRows = args['numRows']

            return getXRandRows(_tableName, _numRows)

        except Exception as e:
            return {'error': str(e)}

def addRow(_tableName, _argNames, _argVals):
    _argVals = ','.join([x if x.isdigit() else "'" + x + "'" for x in _argVals.split(',')])

    conn = mysql.connect()
    cursor = conn.cursor()
    cursor.callproc('spAddRow',(_tableName,_argNames,_argVals))
    data = cursor.fetchall()

    if len(data) is 0:
        conn.commit()
        return {'StatusCode':'200','Message': 'Row addition success'}
    else:
        return {'StatusCode':'1000','Message': str(data[0])}

class AddRow(Resource):
    def post(self):
        try: 
            parser = reqparse.RequestParser()
            addAuthArgs(parser)
            parser.add_argument('name', type=str)
            parser.add_argument('argnames', type=str)
            parser.add_argument('argvals', type=str)
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}

            _tableName = args['name']
            _argNames = args['argnames']
            _argVals = args['argvals']

            return addRow(_tableName, _argNames, _argVals)

        except Exception as e:
            return {'error': str(e)}

class AddMMRRow(Resource):
    def post(self):
        try: 
            parser = reqparse.RequestParser()
            addAuthArgs(parser)
            parser.add_argument('name', type=str)
            parser.add_argument('argnames', type=str)
            parser.add_argument('argvals', type=str)
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}

            _tableName = args['name']
            _argNames = args['argnames'] + ',mmr,record'
            _argVals = args['argvals'] + ',1000,0-0-0'

            return addRow(_tableName, _argNames, _argVals)

        except Exception as e:
            return {'error': str(e)}

def updateRows(_tableName, _argNames, _argVals, _argIds):
    _argVals = [x if x.isdigit() else "'" + x + "'" for x in _argVals.split(',')]
    _argChanges = ','.join([a + "=" + b for a,b in zip(_argNames.split(','), _argVals)])

    conn = mysql.connect()
    cursor = conn.cursor()
    print(_argChanges)
    cursor.callproc('spUpdateRows',(_tableName,_argChanges,_argIds))
    data = cursor.fetchall()

    if len(data) is 0:
        conn.commit()
        return {'StatusCode':'200','Message': 'Row update success'}
    else:
        return {'StatusCode':'1000','Message': str(data[0])}

class UpdateRows(Resource):
    def post(self):
        try: 
            parser = reqparse.RequestParser()
            addAuthArgs(parser)
            parser.add_argument('name', type=str)
            parser.add_argument('argnames', type=str)
            parser.add_argument('argvals', type=str)
            parser.add_argument('argids', type=str)
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}

            _tableName = args['name']
            _argNames = args['argnames']
            _argVals = args['argvals']
            _argIds = args['argids']

            return updateRows(_tableName, _argNames, _argVals, _argIds)

        except Exception as e:
            return {'error': str(e)}

class UpdateMMR(Resource):
    def post(self):
        try: 
            parser = reqparse.RequestParser()
            addAuthArgs(parser)
            parser.add_argument('name', type=str)
            parser.add_argument('argnames', type=str)
            parser.add_argument('argvals', type=str)
            parser.add_argument('argids', type=str)
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}

            _tableName = args['name']
            _argNames = args['argnames']
            _argVals = args['argvals']
            _argIds = args['argids']

            return updateRows(_tableName, _argNames, _argVals, _argIds)

        except Exception as e:
            return {'error': str(e)}

class CreateUser(Resource):
    def post(self):
        try:
            parser = reqparse.RequestParser()
            addAuthArgs(parser)
            parser.add_argument('email', type=str, help='Email address to create user')
            parser.add_argument('password', type=str, help='Password to create user')
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}

            _userEmail = args['email']
            _userPassword = args['password']

            conn = mysql.connect()
            cursor = conn.cursor()
            cursor.callproc('spCreateUser',(_userEmail,_userPassword))
            data = cursor.fetchall()

            if len(data) is 0:
                conn.commit()
                return {'StatusCode':'200','Message': 'User creation success'}
            else:
                return {'StatusCode':'1000','Message': str(data[0])}

        except Exception as e:
            return {'error': str(e)}

def createTable(_tableName, _argNames, _argTypes):
    if len(_argNames) != len(_argTypes):
        return {'error': 'Name type mismatch'}
    
    _args = ['`' + a + '` ' + b + ',' for a, b in zip(_argNames, _argTypes)]

    conn = mysql.connect()
    cursor = conn.cursor()
    cursor.callproc('spCreateTable',(_tableName,''.join(_args)))
    data = cursor.fetchall()

    if len(data) is 0:
        conn.commit()
        return {'StatusCode':'200','Message': 'Table creation success'}
    else:
        return {'StatusCode':'1000','Message': str(data[0])}

class CreateTable(Resource):
    def post(self):
        try:
            parser = reqparse.RequestParser()
            addAuthArgs(parser)
            parser.add_argument('name', type=str, help='Name of table')
            parser.add_argument('argnames', type=str, help='Names of Arguments')
            parser.add_argument('argtypes', type=str, help='Types of Arguments')
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}

            _tableName = args['name']
            _argNames = args['argnames'].split(',')
            _argTypes = args['argtypes'].split(',')

            return createTable(_tableName, _argNames, _argTypes)

        except Exception as e:
            return {'error': str(e)}
        
class CreateMMRTable(Resource):
    def post(self):
        try:
            parser = reqparse.RequestParser()
            addAuthArgs(parser)
            parser.add_argument('name', type=str, help='Name of table')
            parser.add_argument('argnames', type=str, help='Names of Arguments')
            parser.add_argument('argtypes', type=str, help='Types of Arguments')
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}

            _tableName = args['name']
            _argNames = (args['argnames'] + ',mmr,record').split(',')
            _argTypes = (args['argtypes'] + ',int,varchar(20)').split(',')
           
            return createTable(_tableName, _argNames, _argTypes)

        except Exception as e:
            return {'error': str(e)}



api.add_resource(CreateUser, '/CreateUser')
api.add_resource(AuthenticateUser, '/AuthenticateUser')
api.add_resource(GetRows, '/GetRows')
api.add_resource(GetAllRows, '/GetAllRows')
api.add_resource(GetRowsOrdered, '/GetRowsOrdered')
api.add_resource(GetJoinedRowsOrdered, '/GetJoinedRowsOrdered')
api.add_resource(GetXRandRows, '/GetXRandRows')
api.add_resource(CreateTable, '/CreateTable')
api.add_resource(CreateMMRTable, '/CreateMMRTable')
api.add_resource(AddRow, '/AddRow')
api.add_resource(AddMMRRow, '/AddMMRRow')
api.add_resource(UpdateRows, '/UpdateRows')

if __name__ == '__main__':
    app.run(debug=True, port=5000, host='0.0.0.0')