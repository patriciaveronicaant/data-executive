import pymysql

class Connection():
    def __init__(self):
        self.db = False

    def db_connect(self):
        try:
            self.db = pymysql.connect(
                host			= 'localhost',
                user			= 'root', 
                password		= '',
                autocommit		= True,  
                database		= 'data_executive',
                local_infile    = True)
            
            print ("Connected to database.")

        except Exception as e:
            print('Error: {}'.format(str(e)))
            self.db = False

    def db_connected(self):
        return bool(self.db)
    
    def db(self):
        return self.db