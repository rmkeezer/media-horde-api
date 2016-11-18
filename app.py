from flask import Flask
from flask_restful import Resource, Api
from flask_restful import reqparse
from flaskext.mysql import MySQL

mysql = MySQL()
app = Flask(__name__)

# MySQL configurations
app.config['MYSQL_DATABASE_USER'] = 'root'
app.config['MYSQL_DATABASE_PASSWORD'] = 'smoothie42' # THIS IS TEMPORARY
app.config['MYSQL_DATABASE_DB'] = 'mediahorde'
app.config['MYSQL_DATABASE_HOST'] = 'localhost'

mysql.init_app(app)

api = Api(app)

class AuthenticateUser(Resource):
    def post(self):
        try:
            parser = reqparse.RequestParser()
            parser.add_argument('email', type=str, help='Email address for Authentication')
            parser.add_argument('password', type=str, help='Password for Authentication')
            args = parser.parse_args()

            _userEmail = args['email']
            _userPassword = args['password']

            conn = mysql.connect()
            cursor = conn.cursor()
            cursor.callproc('sp_AuthenticateUser',(_userEmail,))
            data = cursor.fetchall()

            if(len(data)>0):
                if(str(data[0][2])==_userPassword):
                    return {'status':200,'UserId':str(data[0][0])}
                else:
                    return {'status':100,'message':'Authentication failure'}

        except Exception as e:
            return {'error': str(e)}


class GetAllRows(Resource):
    def post(self):
        try: 
            parser = reqparse.RequestParser()
            parser.add_argument('tableName', type=str)
            args = parser.parse_args()

            _tableName = args['tableName']

            conn = mysql.connect()
            cursor = conn.cursor()
            cursor.callproc('spGetAllRows',(_tableName,))
            data = cursor.fetchall()

            row_list=[];
            for row in data:
                i = {
                    'Id':row[0],
                    'Item':row[1]
                }
                row_list.append(i)

            return {'StatusCode':'200','Items':row_list}

        except Exception as e:
            return {'error': str(e)}

class AddRow(Resource):
    def post(self):
        try: 
            parser = reqparse.RequestParser()
            parser.add_argument('tableName', type=str)
            parser.add_argument('argNames', type=str)
            parser.add_argument('argVals', type=str)
            args = parser.parse_args()

            _tableName = args['tableName']
            _argNames = args['argNames']
            _argVals = args['argVals']

            _argVals = ','.join(["'" + x + "'" for x in _argVals.split(',')])

            conn = mysql.connect()
            cursor = conn.cursor()
            cursor.callproc('spAddRow',(_tableName,_argNames,_argVals))
            data = cursor.fetchall()

            if len(data) is 0:
                conn.commit()
                return {'StatusCode':'200','Message': 'Element addition success'}
            else:
                return {'StatusCode':'1000','Message': str(data[0])}

        except Exception as e:
            return {'error': str(e)}

class CreateUser(Resource):
    def post(self):
        try:
            parser = reqparse.RequestParser()
            parser.add_argument('email', type=str, help='Email address to create user')
            parser.add_argument('password', type=str, help='Password to create user')
            args = parser.parse_args()

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
            parser.add_argument('name', type=str, help='Name of table')
            parser.add_argument('argnames', type=str, help='Names of Arguments')
            parser.add_argument('argtypes', type=str, help='Types of Arguments')
            args = parser.parse_args()

            _tableName = args['name']
            _argNames = args['argnames'].split(',')
            _argTypes = args['argtypes'].split(',')
            
            print(_argNames)
            print(_argTypes)

            if len(_argNames) != len(_argTypes):
                return {'error': 'Name type mismatch'}
            
            _args = ['`' + a + '` ' + b + ',' for a, b in zip(_argNames, _argTypes)]

            print(_args)

            conn = mysql.connect()
            cursor = conn.cursor()
            cursor.callproc('spCreateTable',(_tableName,''.join(_args)))
            data = cursor.fetchall()

            if len(data) is 0:
                conn.commit()
                return {'StatusCode':'200','Message': 'Table creation success'}
            else:
                return {'StatusCode':'1000','Message': str(data[0])}

        except Exception as e:
            return {'error': str(e)}



api.add_resource(CreateUser, '/CreateUser')
api.add_resource(AuthenticateUser, '/AuthenticateUser')
api.add_resource(AddRow, '/AddRow')
api.add_resource(GetAllRows, '/GetAllRows')
api.add_resource(CreateTable, '/CreateTable')

if __name__ == '__main__':
    app.run(debug=True)