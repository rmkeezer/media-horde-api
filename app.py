from __future__ import print_function

from flask import Flask
from flask_restful import Resource, Api
from flask_restful import reqparse
from flask_cors import CORS
from dblayer import init, authenticate, getRows, getRowsOrdered,\
    getJoinedRowsOrdered, getXRandRows, addRow, updateRows, createTable,\
    removeRow
from dblayer import addGame, removeGame, getOtherGames, getMyGames,\
    getAllGames, getRandGames
import re, string

import sys

app = Flask(__name__)
CORS(app)

# MySQL configurations
app.config['MYSQL_DATABASE_USER'] = 'root'
app.config['MYSQL_DATABASE_PASSWORD'] = 'smoothie42' # THIS IS TEMPORARY
app.config['MYSQL_DATABASE_DB'] = 'mediahorde'
app.config['MYSQL_DATABASE_HOST'] = 'localhost'

init(app)

api = Api(app)

def addAuthArgs(parser):
    parser.add_argument('email', type=str, help='Email address for Authentication')
    parser.add_argument('password', type=str, help='Password for Authentication')

class GetRows(Resource):
    def get(self):
        try: 
            parser = reqparse.RequestParser()
            addAuthArgs(parser)
            parser.add_argument('tableName', type=str)
            parser.add_argument('offset', type=str)
            parser.add_argument('numRows', type=str)
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}

            _tableName = args['tableName']
            _offset = args['offset']
            _numRows = args['numRows']

            return getRows(_tableName, _offset, _numRows)

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
            parser.add_argument('offset', type=str)
            parser.add_argument('numRows', type=str)
            parser.add_argument('order', type=str)
            parser.add_argument('dir', type=str)
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}

            _tableName = args['tableName']
            _offset = args['offset']
            _numRows = args['numRows']
            _order = args['order']
            _dir = args['dir']

            return getRowsOrdered(_tableName, _offset, _numRows, _order, _dir)

        except Exception as e:
            return {'error': str(e)}

class GetJoinedRowsOrdered(Resource):
    def get(self):
        try: 
            parser = reqparse.RequestParser()
            addAuthArgs(parser)
            parser.add_argument('table1', type=str)
            parser.add_argument('table2', type=str)
            parser.add_argument('join1', type=str)
            parser.add_argument('join2', type=str)
            parser.add_argument('joinType', type=str)
            parser.add_argument('null', type=str)
            parser.add_argument('neg', type=str)
            parser.add_argument('whereNames', type=str)
            parser.add_argument('whereVals', type=str)
            parser.add_argument('offset', type=str)
            parser.add_argument('numRows', type=str)
            parser.add_argument('order', type=str)
            parser.add_argument('dir', type=str)
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}

            _table1 = args['table1']
            _table2 = args['table2']
            _join1 = args['join1']
            _join2 = args['join2']
            _joinType = args['joinType']
            _null = args['null']
            _neg = args['neg']
            _whereNames = args['whereNames']
            _whereVals = args['whereVals']
            _offset = args['offset']
            _numRows = args['numRows']
            _order = args['order']
            _dir = args['dir']

            return getJoinedRowsOrdered(_table1, _table2, _join1, _join2, _joinType, _null, _neg, _whereNames, _whereVals, _offset, _numRows, _order, _dir)

        except Exception as e:
            return {'error': str(e)}



class GetXRandRows(Resource):
    def get(self):
        try: 
            parser = reqparse.RequestParser()
            addAuthArgs(parser)
            parser.add_argument('tableName', type=str)
            parser.add_argument('offset', type=str)
            parser.add_argument('numRows', type=str)
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}

            _tableName = args['tableName']
            _offset = args['offset']
            _numRows = args['numRows']

            return getXRandRows(_tableName, _offset, _numRows)

        except Exception as e:
            return {'error': str(e)}



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



class UpdateRows(Resource):
    def post(self):
        try: 
            parser = reqparse.RequestParser()
            addAuthArgs(parser)
            parser.add_argument('name', type=str)
            parser.add_argument('argnames', type=str)
            parser.add_argument('argvals', type=str)
            parser.add_argument('idname', type=str)
            parser.add_argument('idval', type=str)
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}

            _tableName = args['name']
            _argNames = args['argnames']
            _argVals = args['argvals']
            _idName = args['idname']
            _idVal = args['idval']

            return updateRows(_tableName, _argNames, _argVals, _idName, _idVal)

        except Exception as e:
            return {'error': str(e)}

class RemoveRow(Resource):
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

            return removeRow(_tableName, _argNames, _argVals)

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
            parser.add_argument('idname', type=str)
            parser.add_argument('idval', type=str)
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}

            _tableName = args['name']
            _argNames = args['argnames']
            _argVals = args['argvals']
            _idName = args['idname']
            _idVal = args['idval']

            return updateRows(_tableName, _argNames, _argVals, _idName, _idVal)

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
api.add_resource(RemoveRow, '/RemoveRow')

class AddGame(Resource):
    def post(self):
        try: 
            parser = reqparse.RequestParser()
            addAuthArgs(parser)
            parser.add_argument('userId', type=str)
            parser.add_argument('gameId', type=str)
            parser.add_argument('bumpId', type=str)
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}
            return addGame(args['userId'], args['gameId'], args['bumpId'])
        except Exception as e:
            return {'error': str(e)}

class RemoveGame(Resource):
    def post(self):
        try: 
            parser = reqparse.RequestParser()
            addAuthArgs(parser)
            parser.add_argument('userId', type=str)
            parser.add_argument('gameId', type=str)
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}
            return removeGame(args['userId'], args['gameId'])
        except Exception as e:
            return {'error': str(e)}

class GetOtherGames(Resource):
    def get(self):
        try: 
            parser = reqparse.RequestParser()
            addAuthArgs(parser)
            parser.add_argument('whereNames', type=str)
            parser.add_argument('whereVals', type=str)
            parser.add_argument('offset', type=str)
            parser.add_argument('numRows', type=str)
            parser.add_argument('order', type=str)
            parser.add_argument('dir', type=str)
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}
            del args['email']
            del args['password']
            return getOtherGames(args)
        except Exception as e:
            return {'error': str(e)}

class GetMyGames(Resource):
    def get(self):
        try: 
            parser = reqparse.RequestParser()
            addAuthArgs(parser)
            parser.add_argument('whereNames', type=str)
            parser.add_argument('whereVals', type=str)
            parser.add_argument('offset', type=str)
            parser.add_argument('numRows', type=str)
            parser.add_argument('order', type=str)
            parser.add_argument('dir', type=str)
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}
            del args['email']
            del args['password']
            return getMyGames(args)
        except Exception as e:
            return {'error': str(e)}

class GetAllGames(Resource):
    def get(self):
        try: 
            parser = reqparse.RequestParser()
            addAuthArgs(parser)
            parser.add_argument('whereNames', type=str)
            parser.add_argument('whereVals', type=str)
            parser.add_argument('offset', type=str)
            parser.add_argument('numRows', type=str)
            parser.add_argument('order', type=str)
            parser.add_argument('dir', type=str)
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}
            del args['email']
            del args['password']
            out = getAllGames(args)
            return out
        except Exception as e:
            return {'error': str(e)}

class GetRandGames(Resource):
    def get(self):
        try: 
            parser = reqparse.RequestParser()
            addAuthArgs(parser)
            parser.add_argument('whereNames', type=str)
            parser.add_argument('whereVals', type=str)
            parser.add_argument('numRows', type=str)
            args = parser.parse_args()
            if authenticate(args)['status'] == 100:
                return {'error': 'Authentication Failed'}
            del args['email']
            del args['password']
            out = getRandGames(args)
            return out
        except Exception as e:
            return {'error': str(e)}

api.add_resource(AddGame, '/AddGame')
api.add_resource(RemoveGame, '/RemoveGame')
api.add_resource(GetOtherGames, '/GetOtherGames')
api.add_resource(GetMyGames, '/GetMyGames')
api.add_resource(GetAllGames, '/GetAllGames')
api.add_resource(GetRandGames, '/GetRandGames')

if __name__ == '__main__':
    app.run(debug=True, port=5000, host='0.0.0.0')