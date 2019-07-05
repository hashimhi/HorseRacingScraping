import requests
import datetime
from bs4 import BeautifulSoup
import time
import csv
import os

import codecs
import sys
import types
import re
import pandas as pd

#Parameter
scraping_period = 6                    #データの取得対象期間
start_date = datetime.datetime.now()    #データの取得開始日
target_base_url = 'https://db.netkeiba.com/race/list/'  #スクレイピング対象のURL
base_url = 'https://db.netkeiba.com'                    #スクレイピング対象のURL
file_output_path = 'C:/Users/hhashimoto/Documents/data/' #CSVの出力先
filename_race_result = 'race_result.csv'
filename_race_info = 'race_info.csv'
filename_horse_url_list = 'horse_url_list.csv'
filename_jockey_url_list = 'jockey_url_list.csv'
filename_trainer_url_list = 'trainer_url_list.csv'
filename_owner_url_list = 'owner_url_list.csv'
filename_horse_info = 'horse_info.csv'
filename_jockey_info = 'jockey_info.csv'
filename_trainer_info = 'trainer_info.csv'
filename_owner_info = 'owner_info.csv'



def initialize_scraping():
    
    #前回作成されたCSVファイルの削除
    if os.path.isfile(file_output_path + filename_race_result):
        os.remove(file_output_path + filename_race_result)

    if os.path.isfile(file_output_path + filename_race_info):
        os.remove(file_output_path + filename_race_info)

    if os.path.isfile(file_output_path + filename_horse_url_list):
        os.remove(file_output_path + filename_horse_url_list)
    
    if os.path.isfile(file_output_path + filename_jockey_url_list):
        os.remove(file_output_path + filename_jockey_url_list)

    if os.path.isfile(file_output_path + filename_trainer_url_list):
        os.remove(file_output_path + filename_trainer_url_list)
    
    if os.path.isfile(file_output_path + filename_owner_url_list):
        os.remove(file_output_path + filename_owner_url_list)

    if os.path.isfile(file_output_path + filename_horse_info):
        os.remove(file_output_path + filename_horse_info)

def create_csv_raceResult(current_soup,raceId,headerflag):

    #取得したHTMLからテーブルデータを抽出する
    table = current_soup.findAll("table",{"class":"race_table_01 nk_tb_common"})[0]
    rows = table.findAll("tr")
    
    with open(file_output_path + filename_race_result, "a", encoding='utf8') as file:
        writer = csv.writer(file)
    
        #CSVの出力行数
        rownum = 0
        
        for row in rows:
            rownum+=1
            counter = 0
            csvRow = []

            #CSVの先頭カラムにRace IDを付与
            if rownum==1 and headerflag == True:
                csvRow.append("Race ID")
                csvRow.append("着順")
                csvRow.append("枠番")
                csvRow.append("馬番")
                csvRow.append("馬名")
                csvRow.append("性別")
                csvRow.append("年齢")
                csvRow.append("斤量")
                csvRow.append("騎手")
                csvRow.append("タイム")
                csvRow.append("着差")
                csvRow.append("ﾀｲﾑ指数")
                csvRow.append("通過")
                csvRow.append("上り")
                csvRow.append("単勝")
                csvRow.append("人気")
                csvRow.append("馬体重")
                csvRow.append("体重増減")
                csvRow.append("調教タイム")
                csvRow.append("厩舎ｺﾒﾝﾄ")
                csvRow.append("備考")
                csvRow.append("調教師")
                csvRow.append("馬主")
                csvRow.append("賞金(万円)") 
            else:
                pass

            #CSVの内容を作成
            for cell in row.findAll(['td', 'th']):
                counter+=1

                if rownum == 1:
                    pass
                else:
                    #着順を中断や除外の場合には、ブランクにする。
                    if counter == 1:
                        csvRow.append(raceId)

                        if cell.get_text().replace("\n", "").isdecimal():
                            csvRow.append(cell.get_text().replace("\n", ""))
                        else:
                            csvRow.append('')

                    #性年を性別と年齢に分離
                    elif counter == 5:
                        csvRow.append(cell.get_text().replace("\n", "")[0])
                        csvRow.append(cell.get_text().replace("\n", "")[1:]) 

                    #タイムをTime型に変換
                    elif counter == 8:
                        tmp = re.split('[:]',cell.get_text().replace("\n", ""))
                        
                        if tmp[0] == '':
                            csvRow.append("")
                        else:
                            second = float(tmp[0])*60 + float(tmp[1])
                            csvRow.append(second)
                    #単勝のデータをクリーニング
                    elif counter == 13:
                        if cell.get_text().replace("\n", "") == '---':
                            csvRow.append('')
                        else:
                            csvRow.append(cell.get_text().replace("\n", "")) 

                    #体重と体重の増減に分離、計量不可の場合にはブランク
                    elif counter == 15:
                        tmp = re.split('[()]',cell.get_text().replace("\n", ""))


                        if cell.get_text().replace("\n", "")[0:3].isdecimal():
                            csvRow.append(cell.get_text().replace("\n", "")[0:3])
                        else:
                            csvRow.append("")
                            csvRow.append("") 
                            
                            csvRow.append(tmp[1])
                    else:
                        csvRow.append(cell.get_text().replace("\n", ""))
            if csvRow != "":    
                writer.writerow(csvRow)
            print(csvRow)

def create_csv_raceInfo(current_soup,raceId,headerflag,race_date):

    #取得したHTMLからテーブルデータを抽出する
    div = current_soup.find("div",class_="data_intro")
    csvRow = []

    with open(file_output_path + filename_race_info, "a", encoding='utf8') as file:
        writer = csv.writer(file)
    
        #CSVの初回レコード作成時のみヘッダー行を作成
        if headerflag == True:
            csvRow.append("Race ID")
            csvRow.append("Race Order")
            csvRow.append("Title")
            csvRow.append("GrassDart")
            csvRow.append("Direction")
            csvRow.append("Distance")
            csvRow.append("Weather")
            csvRow.append("GroundCondition")
            csvRow.append("Start Time")
            csvRow.append("Remarks")
            csvRow.append("RaceDate")
            csvRow.append("Course")
            
            writer.writerow(csvRow)
            print(csvRow)
            csvRow = []

        csvRow.append(raceId)

        #CSVの内容を作成
        for cell in div.findAll(['dt', 'h1','p']):
            
            #レースの詳細情報を文字列から抽出
            if "/" in cell.get_text():

                #スペースおよび改行を取り除いて、スラッシュでデータ項目ごとに分割
                #具体例 芝右 外1800m / 天候 : 晴 / 芝 : 良 / 発走 : 12:45
                tmp_string_list = cell.get_text().replace("\n", "").replace("\xa0", "").split("/")

                #「芝右 外1800m」から各項目を抽出
                tmp = tmp_string_list[0]
                csvRow.append(tmp[0])
                csvRow.append(tmp[1])
                csvRow.append(tmp[-5:-1])

                #「天候 : 晴」をスペースで分割し、晴を抽出
                tmp = tmp_string_list[1]
                tmp_list = tmp.split(" ")
                csvRow.append(tmp_list[-1])

                # [芝 : 良]から良を抽出
                tmp = tmp_string_list[2]
                tmp_list = tmp.split(" ")
                csvRow.append(tmp_list[-1])

                tmp = tmp_string_list[3]
                csvRow.append(tmp[5:10])
            else:
                csvRow.append(cell.get_text().replace("\n", "").replace("\xa0", ""))

        #レース日を設定
        csvRow.append(race_date.strftime('%Y/%m/%d'))

        #レース場所を設定
        if "札幌" in str(csvRow):
            csvRow.append("札幌")
        elif "函館" in str(csvRow):
            csvRow.append("函館")
        elif "福島" in str(csvRow):
            csvRow.append("福島")
        elif "新潟" in str(csvRow):
            csvRow.append("新潟")
        elif "東京" in str(csvRow):
            csvRow.append("東京")
        elif "中山" in str(csvRow):
            csvRow.append("中山")
        elif "中京" in str(csvRow):
            csvRow.append("中京")
        elif "京都" in str(csvRow):
            csvRow.append("京都")
        elif "阪神" in str(csvRow):
            csvRow.append("阪神")
        elif "小倉" in str(csvRow):
            csvRow.append("小倉")
        else:
            csvRow.append("NA")

        #CSVに出力
        writer.writerow(csvRow)
        print(csvRow)


def create_url_list(current_soup):

    #レース結果のテーブルからURLを抽出
    for a in current_soup.find_all('a'):
        #リストの初期化
        csv_horse_Row = []
        csv_jockey_Row = []
        csv_trainer_Row = []
        csv_owner_Row = []

        #競走馬のURLを取得する
        if '/horse/' in a.get('href'):
            csv_horse_Row.append(a.get('href'))

        #競走馬のURLを取得する
        elif '/jockey/' in a.get('href'):
            csv_jockey_Row.append(a.get('href'))

        #競走馬のURLを取得する
        elif '/trainer/' in a.get('href'):
            csv_trainer_Row.append(a.get('href'))

        #競走馬のURLを取得する
        elif '/owner/' in a.get('href'):
            csv_owner_Row.append(a.get('href'))

        else:
            pass

        if csv_horse_Row != []:
            with open(file_output_path + filename_horse_url_list, "a", encoding='utf8') as file:
                horse_url_writer = csv.writer(file)
                horse_url_writer.writerow(csv_horse_Row)
                print(csv_horse_Row) 

        if csv_jockey_Row != []:
            with open(file_output_path + filename_jockey_url_list, "a", encoding='utf8') as file:

                jockey_url_writer = csv.writer(file)
                jockey_url_writer.writerow(csv_jockey_Row)
                print(csv_jockey_Row)

        if csv_trainer_Row != []:
            with open(file_output_path + filename_trainer_url_list, "a", encoding='utf8') as file:

                trainer_url_writer = csv.writer(file)
                trainer_url_writer.writerow(csv_trainer_Row)
                print(csv_trainer_Row)

        if csv_owner_Row != []:
            with open(file_output_path + filename_owner_url_list, "a", encoding='utf8') as file:

                owner_url_writer = csv.writer(file)
                owner_url_writer.writerow(csv_owner_Row)
                print(csv_owner_Row)

def create_csv_horseInfo():

    df = pd.read_csv(file_output_path + filename_horse_url_list,dtype='object')
    df = df.drop_duplicates()

    with open(file_output_path + filename_horse_info, "a", encoding='utf8') as file:
        writer = csv.writer(file)

        #CSVの出力行数
        rownum = 0

        for path in df.values:

            #サイトからHTMLを取得
            url = base_url + str(path[0])
    
            r = requests.get(url)

            #HTMLを解析し、要素を抽出
            soup = BeautifulSoup(r.content, 'lxml')    

            #取得したHTMLからテーブルデータを抽出する
            title = soup.find("div",{"class":"db_head_name fc"})
            table = soup.find("table",{"class":re.compile('db_prof_table.*')})
            parents_table = soup.find("table",{"class":"blood_table"})
            picture = soup.find("img",{"class":"db_photo_main"})
            rows = table.findAll("tr")

            counter = 0

            csvRow = []

            #データの抽出処理を開始
            #IDを設定
            csvRow.append(str(path[0]).replace('/horse/','').replace('/',''))

            #馬名を取得
            csvRow.append(title.find("h1").get_text().replace('○外','').replace('\u3000',''))

            #ステータスを取得
            tmp = title.find("p",{"class":"txt_01"})
            tmp_string_list = tmp.get_text().replace("\n", "").replace("\xa0", "").split("\u3000")
            csvRow.append(tmp_string_list[0])

            #性別を取得
            csvRow.append(str(tmp_string_list[1])[0])

            #毛色を取得
            csvRow.append(tmp_string_list[2])
            
            for row in rows:

                if '生年月日' in row.find("th").get_text():
                    csvRow.append(row.find("td").get_text().replace("\n", "").replace("\xa0", "").replace("\u3000",""))

                if '調教師' in row.find("th").get_text():
                    csvRow.append(row.find("td").get_text().replace("\n", "").replace("\xa0", "").replace("\u3000",""))

                if '馬主' in row.find("th").get_text():
                    csvRow.append(row.find("td").get_text().replace("\n", "").replace("\xa0", "").replace("\u3000",""))

                if '生産者' in row.find("th").get_text():
                    csvRow.append(row.find("td").get_text().replace("\n", "").replace("\xa0", "").replace("\u3000",""))

                if '産地' in row.find("th").get_text():
                    csvRow.append(row.find("td").get_text().replace("\n", "").replace("\xa0", "").replace("\u3000",""))

                if 'セリ取引価格' in row.find("th").get_text():
                    tmp = row.find("td").get_text().replace("\n", "").replace("\xa0", "").replace("\u3000","")
                    if tmp == "-":
                        csvRow.append('')
                    elif "億" in tmp:
                        tmp_list = re.split('[億万]',tmp)
                        award = int(tmp_list[0].replace(',',''))*100000000 + int(tmp_list[1].replace(',',''))*10000
                        csvRow.append(str(award))
                    else:
                        tmp_list = re.split('[万]',tmp)
                        award = int(tmp_list[0].replace(',',''))*10000
                        csvRow.append(str(award))

                if '獲得賞金' in row.find("th").get_text():
                    tmp = row.find("td").get_text().replace("\n", "").replace("\xa0", "").replace("\u3000","")
                    tmp_lit = []
                    
                    if "億" in tmp:
                        tmp_list = re.split('[億万]',tmp)
                        award = int(tmp_list[0].replace(',',''))*100000000 + int(tmp_list[1].replace(',',''))*10000
                    else:
                        tmp_list = re.split('[万]',tmp)
                        award = int(tmp_list[0].replace(',',''))*10000
                    csvRow.append(str(award))

                if '通算成績' in row.find("th").get_text():
                    tmp = row.find("td").get_text().replace("\n", "").replace("\xa0", "").replace("\u3000","")
                    tmp_lit = []
                    tmplist = re.split('[戦勝]',tmp)
                    csvRow.append(tmplist[0])
                    csvRow.append(tmplist[1])

                if '主な勝鞍' in row.find("th").get_text():
                    csvRow.append(row.find("td").get_text().replace("\n", "").replace("\xa0", "").replace("\u3000",""))

                if '近親馬' in row.find("th").get_text():
                    csvRow.append(row.find("td").get_text().replace("\n", "").replace("\xa0", "").replace("\u3000",""))

            #血統情報
            rows = parents_table.findAll("a")
            for row in rows:
                csvRow.append(row.get_text().replace("\n", "").replace("\xa0", "").replace("\u3000",""))

            #写真URL
            csvRow.append(picture['src'])

            if rownum == 1:
                csvRow.append("ID")
                csvRow.append("馬名")
                csvRow.append("ステータス")
                csvRow.append("性別")
                csvRow.append("毛色")
                csvRow.append("生年月日")
                csvRow.append("調教師")
                csvRow.append("馬主")
                csvRow.append("生産者")
                csvRow.append("産地")
                csvRow.append("セリ取引価格")
                csvRow.append("獲得賞金")
                csvRow.append("通算レース数")
                csvRow.append("通算勝利数")
                csvRow.append("主な勝鞍")
                csvRow.append("近親馬")
                csvRow.append("父")
                csvRow.append("父方祖父")
                csvRow.append("父方祖母")
                csvRow.append("母")
                csvRow.append("母方祖父")
                csvRow.append("母方祖母")
                csvRow.append("写真URL")

            else:                
                if csvRow != "":
                    writer.writerow(csvRow)

            print(csvRow)

def scraping_race_info():

    #Headerの有無フラグ
    headerflag = True

    #対象期間分のレース結果を解析
    for num in range(scraping_period):

        #レース日ごとの結果を表示しているページのURLを作成
        target_date = start_date - datetime.timedelta(days=num)
        target_date_str = target_date.strftime("%Y%m%d")
        target_url = target_base_url + target_date_str + '/'
    
        print('target_url =' + target_url)

        #サイトからHTMLを取得
        r = requests.get(target_url)
        #HTMLを解析し、要素を抽出
        soup = BeautifulSoup(r.content, 'lxml') 

        #処理対象のレース日に実施された12レースの結果を取得する
        for a in soup.find_all('a'):

            #各レース結果のURLを取得する
            if '/race/201' in a.get('href'):
                current_url = base_url + a.get('href')              

                #レース結果のHTMLを取得する
                current_resource = requests.get(current_url)
                current_soup = BeautifulSoup(current_resource.content,'lxml')

                #raceIdを作成
                raceId = current_url[-13:-1] 

                #レース結果を作成
                create_csv_raceResult(current_soup,raceId,headerflag)

                #レース情報を作成
                create_csv_raceInfo(current_soup,raceId,headerflag,target_date)

                #URLリストを作成
                create_url_list(current_soup)

                headerflag = False

    #競走馬情報を作成
    create_csv_horseInfo()

