#必要なライブラリ
#Python 3.7
#Beatifulsoup4
#requests
#lxml
#chardet
#pyodbc
#pandas

import os
import datetime

#user files
import dbaccess
import webscraping

#parameter

def initiailize():

    #Scraping処理の初期化
    webscraping.initialize_scraping()

    #DBの初期化
    dbaccess.initialize_database()

def scraping_info():

    #レース情報のスクレイピング処理を実施
    webscraping.scraping_race_info()

def upload_data():

    #レース情報のアップロード
    dbaccess.upload_race_info(webscraping.file_output_path + webscraping.filename_race_info)
    #レース結果情報のアップロード
    dbaccess.upload_race_result(webscraping.file_output_path + webscraping.filename_race_result)

if __name__ == '__main__':

    #初期処理
    print('初期処理を開始')
    initiailize()
    print('初期処理を終了')


    #ネット競馬サイトからデータをスクレイピングし、CSV化する。
    print('スクレイピングを開始')
    scraping_info() 
    print('スクレイピングを終了')
 
    #CSVデータをDBに投入する。
    print('アップロードを開始')
    upload_data()
    print('アップロードを終了')