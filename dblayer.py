
from flaskext.mysql import MySQL
import re, string

import sys

mysql = MySQL()

def init(app):
    mysql.init_app(app)

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
    
def getRows(_tableName, _offset, _numrows):
    conn = mysql.connect()
    cursor = conn.cursor()
    if _numrows == '*':
        cursor.callproc('spGetAllRows',(_tableName,))
    else:
        cursor.callproc('spGetRows',(_tableName, _offset, _numrows))
    data = cursor.fetchall()
    
    out = []
    for item in data:
        out.append([i.decode('utf-8') if type(i) == bytes else i for i in item])

    return {'StatusCode':'200','Items':out}

def getRowsOrdered(_tableName, _offset, _numrows, _order, _dir):
    conn = mysql.connect()
    cursor = conn.cursor()
    cursor.callproc('spGetRowsOrdered',(_tableName, _offset, _numrows, _order, _dir))
    data = cursor.fetchall()
    
    out = []
    for item in data:
        out.append([i.decode('utf-8') if type(i) == bytes else i for i in item])

    return {'StatusCode':'200','Items':out}

def getJoinedRowsOrdered(_table1, _table2, _join1, _join2, _joinType, _null, _neg, _whereNames, _whereVals, _offset, _numrows, _order, _dir):
    _whereVals = [x if x.isdigit() else "'" + x + "'" for x in _whereVals.split('z')]
    _where = ' AND '.join([a + "=" + b for a,b in zip(_whereNames.split('z'), _whereVals)])
    conn = mysql.connect()
    cursor = conn.cursor()
    _hasWhere = ''
    if _where != '=\'\'':
        _hasWhere = ' AND '
    else:
        _where = ''
    cursor.callproc('spGetJoinedRowsOrdered',(_table1, _table2, _join1, _join2, _joinType, _null, _neg, _hasWhere, _where, _offset, _numrows, _order, _dir))
    data = cursor.fetchall()
    
    out = []
    for item in data:
        out.append([i.decode('utf-8') if type(i) == bytes else i for i in item])

    return {'StatusCode':'200','Items':out}

def getXRandRows(_tableName, _offset, _numRows):
    conn = mysql.connect()
    cursor = conn.cursor()
    cursor.callproc('spGetXRandRows',(_tableName, _offset, _numRows,))
    data = cursor.fetchall()

    out = []
    for item in data:
        out.append([i.decode('utf-8') if type(i) == bytes else i for i in item])

    return {'StatusCode':'200','Items':out}

def addRow(_tableName, _argNames, _argVals):
    _argNames = ','.join([x for x in _argNames.split('z')])
    _argVals = ','.join([x if x.isdigit() else "'" + x + "'" for x in _argVals.split('z')])

    conn = mysql.connect()
    cursor = conn.cursor()
    cursor.callproc('spAddRow',(_tableName,_argNames,_argVals))
    data = cursor.fetchall()

    if len(data) is 0:
        conn.commit()
        return {'StatusCode':'200','Message': 'Row addition success'}
    else:
        return {'StatusCode':'1000','Message': str(data[0])}

def updateRows(_tableName, _argNames, _argVals, _idName, _idVal):
    _argVals = [x if x.isdigit() else "'" + x + "'" for x in _argVals.split('z')]
    _argChanges = ' AND '.join([a + "=" + b for a,b in zip(_argNames.split('z'), _argVals)])

    conn = mysql.connect()
    cursor = conn.cursor()
    print(_argChanges, file=sys.stdout)
    cursor.callproc('spUpdateRows',(_tableName,_argChanges,_idName,_idVal))
    data = cursor.fetchall()

    if len(data) is 0:
        conn.commit()
        return {'StatusCode':'200','Message': 'Row update success'}
    else:
        return {'StatusCode':'1000','Message': str(data[0])}

def removeRow(_tableName, _argNames, _argVals):
    _argVals = [x if x.isdigit() else "'" + x + "'" for x in _argVals.split('z')]
    _argChanges = ' AND '.join([a + "=" + b for a,b in zip(_argNames.split('z'), _argVals)])

    conn = mysql.connect()
    cursor = conn.cursor()
    print(_argChanges, file=sys.stdout)
    cursor.callproc('spRemoveRow',(_tableName,_argChanges))
    data = cursor.fetchall()

    if len(data) is 0:
        conn.commit()
        return {'StatusCode':'200','Message': 'Row removal success'}
    else:
        return {'StatusCode':'1000','Message': str(data[0])}

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

def addGame(userId, gameId):
    conn = mysql.connect()
    c = conn.cursor()
    c.execute('REPLACE INTO usergames(user_id, game_id, position) VALUES(%s,%s,%s)' % (userId, gameId, '1'))
    conn.commit()
    return {'StatusCode':'200','Message': 'Row addition success'}

def removeGame(userId, gameId):
    conn = mysql.connect()
    c = conn.cursor()
    c.execute('DELETE FROM usergames WHERE user_id=%s AND game_id=%s' % (userId, gameId))
    conn.commit()
    return {'StatusCode':'200','Message': 'Row deletion success'}

def getGamesHelper(args, joinType, notNull, getType='join'):
    if args['whereNames'] != '':
        _whereVals = [x if x.isdigit() else "'" + x + "'" for x in args['whereVals'].split('z')]
        _where = ' AND '.join([a + "=" + b for a,b in zip(args['whereNames'].split('z'), _whereVals)])
        _where = 'AND ' + _where
    else:
        _where = ''
    conn = mysql.connect()
    c = conn.cursor()
    if getType == 'rand':
        c.execute('SELECT * FROM games ' +\
            _where + ''' AS r1 JOIN
            (SELECT CEIL(RAND() * (SELECT MAX(id) FROM games)) AS id) AS r2 WHERE r1.id >= r2.id ORDER BY r1.id ASC 
            LIMIT ''' + args['numRows'])
    elif getType == 'all':
        c.execute('SELECT * FROM games ' +\
            _where + ' ORDER BY ' + args['order'] + ' ' + args['dir'] + ' LIMIT ' + args['numRows'] + ' OFFSET ' + args['offset'])
    else:
        c.execute('SELECT * FROM usergames ' + joinType + ' JOIN games ON usergames.game_id = games.Id WHERE user_id IS ' + notNull + ' NULL ' +\
            _where + ' ORDER BY ' + args['order'] + ' ' + args['dir'] + ' LIMIT ' + args['numRows'] + ' OFFSET ' + args['offset'])
    data = c.fetchall()
    
    out = []
    for item in data:
        out.append([i.decode('utf-8') if type(i) == bytes else i for i in item])
    return {'StatusCode':'200','Items':out}

def getOtherGames(args):
    return getGamesHelper(args, 'RIGHT', '')

def getMyGames(args):
    return getGamesHelper(args, 'INNER', 'NOT')

def getAllGames(args):
    return getGamesHelper(args, '', '', getType='all')

def getRandGames(args):
    return getGamesHelper(args, '', '', getType='rand')