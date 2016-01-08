import mysql.connector
from mysql.connector import errorcode


cur = None
cursor = None
def connect_database(database, user='root',password='root',host='127.0.0.1'):
	global cur, cursor
	try:
		cur = mysql.connector.connect(user=user, password=password,host=host,database=database)
		cursor = cur.cursor()
		return  True
	except mysql.connector.Error as err:
		if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
			print("Something is wrong with your user name or password")
		elif err.errno == errorcode.ER_BAD_DB_ERROR:
			print("Database does not exist")
		else:
			print(err)
		return False

def close_database():
	cur.close()

def createtable(tablename,columnnames=None, columntypes = None,tablequery='', replace=False):
	if tablequery != '':
		cursor.execute(tablequery)
		return
	if not replace:
		tablequery = 'CREATE TABLE IF NOT EXISTS '+tablename+'('
	else:
		try:
			cursor.execute('DROP TABLE '+tablename)
		except:
			pass
		tablequery = 'CREATE TABLE '+tablename+'('
	for cindex in range(len(columnnames)):
		tablequery += 'c_'+columnnames[cindex]
		if columntypes != None:
			tablequery += ' '+str(columntypes[cindex])+', '
		else:
			tablequery += ' VARCHAR(50), '
	tablequery = tablequery[:-2]+')'
	#print tablequery
	cursor.execute(tablequery)

def inferdatatypes(data):
	datatypes = []
	for d in data:
		#print d, type(d)
		if type(d) == str:
			datatypes.append('VARCHAR(50)')
		elif type(d) == int:

			if len(str(int)) < 9:
				datatypes.append('INT')
			else:
				datatypes.append('BIGINT')
		elif type(d) == float:
			datatypes.append('FLOAT')
		elif type(d) == dict or type(d) == list:
			datatypes.append('BLOB')
		else:
			datatypes.append('VARCHAR(50)')
	return  datatypes

def importtable(tablename,jsonlist=None,keys =None,columntypes=None, datalist = None,replace=False):
	if not replace:
		checkquery = "SHOW TABLES LIKE '"+tablename+"';"
		cursor.execute(checkquery)
		if cursor.rowcount != 0:
			cursor.fetchall()
			return
	cursor.fetchall()
	if jsonlist != None:
		keys = jsonlist[0].keys()
		datalist = [x.values() for x in jsonlist]
		if columntypes == None:
			columntypes = inferdatatypes(datalist[0])

	createtable(tablename,columnnames=keys, columntypes=columntypes,replace=replace)
	for value in datalist:
		insertquery = 'INSERT INTO '+tablename+ '('
		for k in keys:
			insertquery += 'c_'+str(k) + ', '
		insertquery = insertquery[:-2]+') VALUES ('
		for v in value:
			if type(v) in [str, dict, list]:
				v = str(v).replace('"','\'')
			if type(v) ==str or columntypes == None:
				insertquery += '"'+str(v)+'", '
			elif columntypes[value.index(v)] == 'VARCHAR(50)':
				insertquery += '"'+str(v)+'", '
			elif columntypes[value.index(v)] == 'BLOB':
				insertquery += '"'+str(v)+'", '
			else:
				insertquery += str(v)+', '
		insertquery = insertquery[:-2] + ')'
		#print insertquery
		cursor.execute(insertquery)
	return

def insert(tablename,jsonlist=None,keys =None,columntypes=None, datalist = None,replace=False):
	if jsonlist != None:
		keys = jsonlist[0].keys()
		datalist = [x.values() for x in jsonlist]
		if columntypes == None:
			columntypes = inferdatatypes(datalist[0])
	if not replace:
		cursor.execute("SHOW TABLES LIKE '"+tablename+"';")
		if cursor.rowcount != 1:
			createtable(tablename,columnnames=keys, columntypes=columntypes,replace=replace)
	for value in datalist:
		try:
			insertquery = 'INSERT INTO '+tablename+ '('
			for k in keys:
				insertquery += 'c_'+str(k) + ', '
			insertquery = insertquery[:-2]+') VALUES ('
			for v in value:
				if type(v) in [str, dict, list]:
					v = str(v).replace('"','\'')
				if type(v) ==str or columntypes == None:
					insertquery += '"'+str(v)+'", '
				elif columntypes[value.index(v)] == 'VARCHAR(50)':
					insertquery += '"'+str(v)+'", '
				elif columntypes[value.index(v)] == 'BLOB':
					insertquery += '"'+str(v)+'", '
				else:
					insertquery += str(v)+', '
			insertquery = insertquery[:-2] + ')'
			#print insertquery
			cursor.execute(insertquery)
		except:
			print 'error'
	cursor.fetchall()
	return


def select(selectquery):
	cursor.execute(selectquery)
	return cursor.fetchall()
