import streamlit as st
import datetime as dt
import seaborn as sns
import pandas as pd
import numpy as np
#import sklearn as sk

from bs4 import BeautifulSoup
import requests
import plotly.express as px
import sklearn as sk

import sqlalchemy
from sqlalchemy import create_engine
#======#=======#=======#=======#=======#=======#========
def pulldata():

    url="https://occovid19.ochealthinfo.com/coronavirus-in-oc"

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36'}
    # url = "https://en.wikipedia.org/wiki/List_of_national_capitals"
    r = requests.get(url, headers=headers)

    soup = BeautifulSoup(r.content, "html.parser")
    table = soup.find_all('table')[0]
    rows = table.find_all('tr')
    row_list = list()

    for tr in rows:
        td = tr.find_all('td')
        row = [i.text for i in td]
        row_list.append(row)


    df_bs = pd.DataFrame(row_list[1:],columns=row_list[0]) # remove sum total and unknow
    # df_bs.tail()

    dfo=df_bs.copy()

    df=dfo.loc[:len(dfo)-4,]
    df.reset_index(inplace=True, drop=True)
    # df.head()
    # df.tail()
    #---------------------------------------------
    # ifound there is error at value empty
    # df["POPULATION1"].apply(change_dtype)

    # def change_dtype(value):
    #     try:
    #         return int(value)
    #     except ValueError:
    #         try:
    #             return float(value)
    #         except ValueError:
    #             return value

    # for column in df.columns:
    #     df.loc[:, column] = df[column].apply(change_dtype)

    #---------------------------------------------
    # prepare for tracking change
    df_fortrackingchange=dfo.tail(1).copy()# last one
    df_fortrackingchange.drop(columns="CITY",inplace=True)
    # how many country
    # print("Qty city in Orange County", df["CITY"].nunique())

    def processdf(newdf):
        import datetime
        newdf["LoadDate"]=datetime.datetime.now()
        newdf["POPULATION1"]=newdf.apply(lambda x: float(str(x["POPULATION1"]).replace(',',"").replace('Not Available',"-1")), axis=1)
        newdf["TOTAL CASES"]=newdf.apply(lambda x: float(str(x["TOTAL CASES"]).replace(',',"").replace('Not Available',"-1")), axis=1)
        return newdf.reset_index(inplace=True, drop=True)

    processdf(df)
    processdf(df_fortrackingchange)

    # df.tail
    #handle df["POPULATION1"]

    # def handle_something(x):
    #     try:
    #         rs=str(x).replace(',',"").replace('Not Available',"-1")
    #         if rs=="":
    #             rs=-1
    #         # rs=float(x)
    #     except Exception:
    #         rs=-1
    #     return rs

    # df["POPULATION1"]=df["POPULATION1"].apply(lambda x: handle_something(x))
    # # df["POPULATION1"]=df["POPULATION1"].astype(float
    # for i in df["POPULATION1"]:
    #     # print("now browse at ",i)
    #     # print(float(i))
    # df.describe()

    # SAVE DATA TO HEROKU



    db_string = "postgres://utnralbhfjaklw:2b05fb1f95006a9910378a6a570a1b22305d32efb4445aa9484b9d7ad508af2d@ec2-34-197-212-240.compute-1.amazonaws.com:5432/d5n9cjefop6fu9"
    global newtablename_tracking
    global newtablename

    newtablename_tracking="orangecounty_tracking"
    newtablename="orangecounty"

    db = create_engine(db_string)
    global engine
    engine = sqlalchemy.create_engine(db_string)

    def save_server(newdf=df):
        # import datetime
        # engine = sqlalchemy.create_engine(db_string)
        newdf.to_sql(name=newtablename, con=engine, if_exists='append', index=False,
                dtype={
                        # 'input_data': sqlalchemy.String(),
                        # 'en': sqlalchemy.Text(), 
                        # 'vi': sqlalchemy.types.NVARCHAR(length=100), 
                        'CITY': sqlalchemy.String() ,
                        'LoadDate': sqlalchemy.DateTime(), 
                        'POPULATION1': sqlalchemy.types.Numeric(), 
                    #    'Confirmed': sqlalchemy.types.Float(precision=3, asdecimal=True), 
                    'TOTAL CASES': sqlalchemy.types.Numeric(),
                    #    'Recovered': sqlalchemy.types.Numeric(),
                    #    'Deaths': sqlalchemy.types.Numeric(),
                        })
        # engine = create_engine('mssql+pymssql://reports:reports@vnmacsdb:1433/ACS EE')
        # df = pd.read_sql(f'select * from {newtablename} limit ', engine)
        # # print(f' requested data{df.shape}')
        # print("done save table ")
        return df
    # save_server()


    def save_server_tracking(newdf=df_fortrackingchange):
        
        newdf.to_sql(name=newtablename_tracking, con=engine, if_exists='append', index=False,
                dtype={
                        'LoadDate': sqlalchemy.DateTime(), 
                        'POPULATION1': sqlalchemy.types.Numeric(), 
                    'TOTAL CASES': sqlalchemy.types.Numeric(),
                        })
    def query(selectquery):
        return pd.read_sql(selectquery, engine)
    def getValuefromtracking():
        try:
            df = pd.read_sql(f'select * from {newtablename_tracking} order by "LoadDate" desc limit 1', engine)
        except Exception:
            save_server_tracking() #first run must create table 
        finally:
            df = pd.read_sql(f'select * from {newtablename_tracking} order by "LoadDate" desc limit 1', engine)
        if len(df)>0:
            # # print(f' requested data{df.shape}')
            servervalue=df.loc[0,"TOTAL CASES"]
            # print("cases in OC website: ",servervalue)
        else:
            # print("do nothing")
            
            servervalue =0
    
        return servervalue
    # getValuefromtracking()
    def getValuefromNEWtracking(df=df_fortrackingchange):
        # df = pd.read_sql(f'select * from {newtablename_tracking} order by "LoadDate" desc limit 1', engine)
        if len(df)>0:
            # # print(f' requested data{df.shape}')
            servervalue=df.loc[0,"TOTAL CASES"]
            # print("cases in tracking server: ",servervalue)
        else:
            # print("do nothing")
            servervalue =0
        return servervalue
    # getValuefromNEWtracking()
    def check_tracking_new_or_not():
        newData=getValuefromNEWtracking()
        serverData=getValuefromtracking()
        if newData>serverData:
            #insert
            save_server_tracking()
            save_server()
        elif newData==serverData:
            #do nothing
            # print("nothing at elsif")
            pass
        else:
            #do nothing
            # print("nothing at else")
            pass
        # # engine = create_engine('mssql+pymssql://reports:reports@vnmacsdb:1433/ACS EE')

    # save_server(df)
    # df_fortrackingchange
    check_tracking_new_or_not()

    # # print(df.to_markdown())

    
    import plotly.express as px
    # # print(df.columns)
    df_px=df.sort_values(by="TOTAL CASES",ascending =False)
    df_px.reset_index(inplace=True, drop=True)
    # px.bar(df_px ,x="CITY", y="TOTAL CASES")

    lastvalue=query("""
        select * from orangecounty 
        where "CITY" ='Garden Grove'
        order by "LoadDate" desc
        limit 2
        """)[["TOTAL CASES"]].loc[1].values[0]
    # print(f'Garden Grove last update cases {lastvalue}')
    # Garden Grove
    myCity="Garden Grove"
    dataCity=df_px[df_px["CITY"]==myCity]
    city_rank=dataCity.index.values[0]
    cityqty=dataCity.reset_index(drop=True).loc[0,"TOTAL CASES"]
    # print(f'Garden Grove rank: {city_rank} and cases {cityqty}')
    # print(f' change value {cityqty- lastvalue}')
    changevalue=cityqty- lastvalue


    
    return [
        lastvalue,cityqty,changevalue
    ]

pulldata()

#======#=======#=======#=======#=======#=======#========
#      Code streamlit run below
#======#=======#=======#=======#=======#=======#========
timenow=dt.datetime.now()
st.__version__
"khang"
# st.sidebar.write("sidebar")
st.sidebar.text("my control button")
if st.sidebar.button(" report status today"):
    
    old,new, change= pulldata()
    f' sys {old}'
    f'update today {new}'
    f"change"
    st.write(f"test worked at {timenow}")
