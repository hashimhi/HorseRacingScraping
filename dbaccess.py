import pyodbc
import time
import csv
import pandas as pd

server = 'handsonappdb.database.windows.net'
database = 'ScrapingDB'
username = 'Dbamin'
password = 'Pa$$w0rd1234'
driver = '{ODBC Driver 17 for SQL Server}'

def initialize_database():

        print('DB接続処理開始')
        cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = cnxn.cursor()
        print('DB接続処理終了')

        print('レース結果テーブル作成処理開始')
        query = 'select count(*) from sys.objects where type = \'U\' and name = \'RaceResultTable\''
        cursor.execute(query)
        row = cursor.fetchone()

        if str(row[0]) == '0':
                pass
        else:
                query = 'Drop Table RaceResultTable'
                cursor.execute(query)

        query = 'CREATE TABLE RaceResultTable ('
        query += ' RaceID NVARCHAR(12) NOT NULL,'
        query += ' OrderArrival INT,'
        query += ' BoxNumber INT,'
        query += ' HorseNumber INT NOT NULL,'
        query += ' HorseName NVARCHAR(100),'
        query += ' Sex NVARCHAR(2),'
        query += ' Years INT,'
        query += ' Amount INT,'
        query += ' JockeyName NVARCHAR(100),'
        query += ' RaceTime DECIMAL(15,5),'
        query += ' ArrivalDistance NVARCHAR(100),'
        query += ' TimeIndicator NVARCHAR(100),'
        query += ' PassThrough NVARCHAR(100),'
        query += ' Up DECIMAL(18,5),'
        query += ' SingleVictoryRate DECIMAL(18,5),'
        query += ' Popularity INT,'
        query += ' HorseWeight INT,'
        query += ' HorseWeightDiff DECIMAL(3,1),'
        query += ' TrainingTime NVARCHAR(100),'
        query += ' Comment NVARCHAR(100),'
        query += ' Remarks NVARCHAR(100),'
        query += ' Trainer NVARCHAR(100),'
        query += ' OWNER NVARCHAR(100),'
        query += ' PrizeAmount DECIMAL(18,5),'
        query += ' PRIMARY KEY(RaceID,HorseNumber)'
        query += ')'
        print(query)
        cursor.execute(query)
        
        print('レース結果テーブル作成処理終了') 


        print('レース情報テーブル作成処理開始')
        query = 'select count(*) from sys.objects where type = \'U\' and name = \'RaceInfoTable\''
        cursor.execute(query)
        row = cursor.fetchone()

        if str(row[0]) == '0':
                pass
        else:
                query = 'Drop Table RaceInfoTable'
                cursor.execute(query)

        query = 'CREATE TABLE RaceInfoTable ('
        query += ' RaceID NVARCHAR(12) NOT NULL PRIMARY KEY,'
        query += ' RaceOrder NVARCHAR(4) NOT NULL,'
        query += ' RaceTitle NVARCHAR(20) NOT NULL,'
        query += ' GrassDart NVARCHAR(1) NOT NULL,'
        query += ' Direction NVARCHAR(1) NOT NULL,'
        query += ' Distance INT NOT NULL,'
        query += ' Weather NVARCHAR(2) NOT NULL,'
        query += ' GroundCondition NVARCHAR(2) NOT NULL,'
        query += ' StartTime TIME NOT NULL,'
        query += ' Remarks NVARCHAR(100),'
        query += ' RaceDate DATE NOT NULL,'
        query += ' Course NVARCHAR(100)'
        query += ')'
        print(query)
        cursor.execute(query)
        cnxn.commit()

        print('競走馬情報テーブル作成処理開始')
        query = 'select count(*) from sys.objects where type = \'U\' and name = \'HorseInfoTable\''
        cursor.execute(query)
        row = cursor.fetchone()

        if str(row[0]) == '0':
                pass
        else:
                query = 'Drop Table HorseInfoTable'
                cursor.execute(query)

        query = 'CREATE TABLE HorseInfoTable ('
        query += ' HorseID NVARCHAR(20) NOT NULL PRIMARY KEY,'
        query += ' HorseName NVARCHAR(100),'
        query += ' Status NVARCHAR(20),'
        query += ' Sex NVARCHAR(5),'
        query += ' HairColor NVARCHAR(20),'
        query += ' BirthDay NVARCHAR(20),'
        query += ' Trainer NVARCHAR(100),'
        query += ' Owner NVARCHAR(100),'
        query += ' Creater NVARCHAR(100),'
        query += ' BirthPlace NVARCHAR(100),'
        query += ' MarketPrice Decimal(15,0),'
        query += ' CareerEarnings Decimal(15,0),'
        query += ' NumberOfRaces int,'
        query += ' NumberOfWins int,'
        query += ' MainWins NVARCHAR(100),'
        query += ' Relations NVARCHAR(100),'
        query += ' Father NVARCHAR(100),'
        query += ' PaternalGrandFather NVARCHAR(100),'
        query += ' PaternalGrandMother NVARCHAR(100),'
        query += ' Mother NVARCHAR(100),'
        query += ' MaternalGrandFather NVARCHAR(100),'
        query += ' MaternalGrandMother NVARCHAR(100),'
        query += ' PictureURL NVARCHAR(100)'
        query += ')'
        print(query)
        cursor.execute(query)
        cnxn.commit()

        cursor.close()
        cnxn.close()

def upload_race_info(filepath):

        print('DB接続処理開始')
        cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = cnxn.cursor()
        print('DB接続処理終了')

        print('レース情報テーブルへのロード処理開始')
        df = pd.read_csv(filepath)

        # データ追加クエリを発行
        SQL_TEMPLATE = "INSERT INTO [dbo].[RaceInfoTable]([RaceID],[RaceOrder],[RaceTitle],[GrassDart],[Direction],[Distance],[Weather],[GroundCondition],[StartTime],[Remarks],[RaceDate],[Course]) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}')"

        for line in df.values:
                editSql = SQL_TEMPLATE                      # SQL原本
                for i,col in enumerate(line):               # SQL原本に置換をかける
                        editSql = editSql.replace('{' + str(i) + '}', str(col))
        
                #INSERT文の発行
                
                print(editSql)
                cursor.execute(editSql)

        cnxn.commit()
        cursor.close()
        cnxn.close()

def upload_race_result(filepath):

        print('DB接続処理開始')
        cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = cnxn.cursor()
        print('DB接続処理終了')

        print('レース結果テーブルへのロード処理開始')
        df = pd.read_csv(filepath,dtype='object')

        # データ追加クエリを発行
        SQL_TEMPLATE = "INSERT INTO [dbo].[RaceResultTable]([RaceID],[OrderArrival],[BoxNumber],[HorseNumber],[HorseName],[Sex],[Years],[Amount],[JockeyName],[RaceTime],[ArrivalDistance],[TimeIndicator],[PassThrough],[Up],[SingleVictoryRate],[Popularity],[HorseWeight],[HorseWeightDiff],[TrainingTime],[Comment],[Remarks],[Trainer],[OWNER],[PrizeAmount]) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}','{16}','{17}','{18}','{19}','{20}','{21}','{22}','{23}')"

        for line in df.values:
                editSql = SQL_TEMPLATE                      # SQL原本
                for i,col in enumerate(line):               # SQL原本に置換をかける
                        if str(col) == "nan":
                                editSql = editSql.replace('\'{' + str(i) + '}\'', 'NULL')
                        else:
                                editSql = editSql.replace('{' + str(i) + '}', str(col).replace(',',''))
                
                #INSERT文の発行
                print(editSql)
                cursor.execute(editSql)

        cnxn.commit()
        cursor.close()
        cnxn.close()

def upload_hosrse_info(filepath):

        print('DB接続処理開始')
        cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = cnxn.cursor()
        print('DB接続処理終了')

        print('競走馬情報テーブルへのロード処理開始')
        df = pd.read_csv(filepath,dtype='object')

        # データ追加クエリを発行
        SQL_TEMPLATE = "INSERT INTO [dbo].[HorseInfoTable]([HorseID],[HorseName],[Status],[Sex],[HairColor],[BirthDay],[Trainer],[Owner],[Creater],[BirthPlace],[MarketPrice],[CareerEarnings],[NumberOfRaces],[NumberOfWins],[MainWins],[Relations],[Father],[PaternalGrandFather],[PaternalGrandMother],[Mother],[MaternalGrandFather],[MaternalGrandMother],[PictureURL]) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}','{16}','{17}','{18}','{19}','{20}','{21}','{22}')"

        for line in df.values:
                editSql = SQL_TEMPLATE                      # SQL原本
                for i,col in enumerate(line):               # SQL原本に置換をかける
                        if str(col) == "nan":
                                editSql = editSql.replace('\'{' + str(i) + '}\'', 'NULL')
                        else:
                                editSql = editSql.replace('{' + str(i) + '}', str(col).replace(',',''))
                
                #INSERT文の発行
                print(editSql)
                cursor.execute(editSql)

        cnxn.commit()
        cursor.close()
        cnxn.close()
