import psycopg2
import configparser

HOME = '/media/sf_Lu_Wang/'
DATA = '/media/sf_Shared_Folders/delivery_monitor/Classes/data/'
creds = configparser.ConfigParser()
creds.read(HOME + 'databases.conf')

class Connection: 
	def __init__(self, database): 
		self.database = database

		dbname = creds.get(database, 'dbname')
		user = creds.get(database, 'user')
		password = creds.get(database, 'password')
		host = creds.get(database, 'host')
		port = creds.get(database, 'port') 
		
		self.con = psycopg2.connect("dbname='%s' user='%s' password='%s' host='%s' port='%s'" % (dbname, user, password, host, port) )

		self.cur = self.con.cursor() 

	def fetchall(self, query): 
		self.cur.execute(query)
		return self.cur.fetchall() 

	def iterable(self, query): 
		self.cur.execute(query)
		return self.cur
	
	def results_to_file(self, query, filename): 
		self.cur.execute(query)
		with open(DATA + filename, 'w') as fp: 
			for record in self.cur:
				line = ','.join([str(r) for r in record]) + '\n' 
				fp.write(line)
	def speak(self): 
		print("I'm a Connection object connected to ", self.database)

if __name__=='__main__': 
	c = Connection('piston2')
	query = 'select emd5 from users limit 10'
	print c.fetchall(query)
	for d in c.iterable(query):
		print ">>>", d 
	
	c.results_to_file(query, 'test.tsv')
