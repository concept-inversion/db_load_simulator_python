import psycopg2
from psycopg2 import Error
import json
class Postgre_db:
    '''
    Implementation of PostgreSQL module
    Functions:
    1. closeDB(self)
        [Connection].closeDB() : Commit the database and closes the connection with the database. 

    2. executeDB(self,statement,key)
        [Conncetion].executeDB(self, statement, key) : Executes a SQL Query in the Database
            statement -> Query
            key -> Optional parameters
    
    3. createTable()
        Creates a Table with a fixed schema 
    '''
    def __init__(self, *args, **kwargs):        
        try:
            self.Link = psycopg2.connect("dbname=postgre user=concept password=concept host=localhost port=5432 ")
            self.cursor = self.Link.cursor()
            self.createTable()
            
        except Error as err:
            print(err)
    
    def closeDB(self):
        try:
            self.Link.commit()
            self.Link.close()
            
        except Error as err:
            print(err)

    def readDB(self):
        statement = ("Select * FROM people")
        try:
            self.cursor.execute(statement)
            self.Link.commit()
        except Error as err:
            print(err)

    def Create(self,each):
        #columns = ', '.join(each.keys())
        #placeholders = ':'+', :'.join(each.keys())
        statement = ("INSERT INTO People ( Bio, Name, Dob, Gender, Email, Longitude, Latitude, Phone, Link, Image,Address )"
                  " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        data=self.executeDB(statement,(each['Bio'],each['Name'],each['Dob'],each['Gender'],each['Email'],each['Longitude'],each['Latitude'],each['Phone'],each['Link'],each['Image'],each['Address']))   
        return data
    
    def Update(self,each,Id):
        statement = "UPDATE  People SET Bio= %s, Name= %s, Dob= %s, Gender= %s, Email= %s, Longitude= %s, Latitude= %s, Phone= %s, Link= %s, Image= %s,Address= %s WHERE Id = %s"
        data=self.executeDB(statement,(each['Bio'],each['Name'],each['Dob'],each['Gender'],each['Email'],each['Longitude'],each['Latitude'],each['Phone'],each['Link'],each['Image'],each['Address'],Id))   
        return data

    def executeDB(self,statement,key):
        try:
            self.cursor.execute(statement,key)
            self.Link.commit()
            return (self.cursor)
        except Error as err:
            print(err)
            self.Link.rollback()
    
    def JsonLoader(self,data):
        raw=json.load(open(data))
        for each in raw:
            #columns = ', '.join(each.keys())
            #placeholders = ':'+', :'.join(each.keys())
            statement = ("INSERT INTO People ( Bio, Name, Dob, Gender, Email, Longitude, Latitude, Phone, Link, Image,Address )"
                  " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
            data=self.executeDB(statement,(each['Bio'],each['Name'],each['Dob'],each['Gender'],each['Email'],each['Longitude'],each['Latitude'],each['Phone'],each['Link'],each['Image'],each['Address']))
        return data

    def createTable(self):
        statement = (
         '''
        CREATE TABLE IF NOT EXISTS PEOPLE
        (
        Id SERIAL PRIMARY KEY,
        Name TEXT NOT NULL,
        Email TEXT NOT NULL,
        Bio TEXT,
        Gender VARCHAR(12),
        Link TEXT,
        Address TEXT,
        Longitude REAL,
        Latitude REAL,
        Image TEXT,
        Phone INT,
        Dob DATE
        );
        '''    )    
        try:
            self.cursor.execute(statement)
            self.Link.commit()
        except Error as err:
            print(err)