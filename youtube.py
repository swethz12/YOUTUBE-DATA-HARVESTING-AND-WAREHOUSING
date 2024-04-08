from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#api key connection

def api_connect():
    api_id="AIzaSyBOlWgSZgqRi9vTQ6tjl8YVXql28VFZI0g"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=api_id)#using buildfun we are joining all three
    return youtube
#to access the function create a variable youtube
youtube=api_connect()

#get channel information
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()


    for i in response["items"]:
        data=dict(channelname=i["snippet"]["title"],#to convert it to a json to store it as dict so can store in mongodb
                channelid=i["id"],
                subscribers=i['statistics']['subscriberCount'],
                Views=i['statistics']['viewCount'],
                totalvideos=i['statistics']['videoCount'],
                channeldescription=i['snippet']['description'],
                playlist_id=i['contentDetails']['relatedPlaylists']['uploads'])# taken out the nessory details is taken so it would be easy to put in mongodb
    return data

#get video ids
def get_videoids(channel_id):
    videoids=[]
    response=youtube.channels().list(id=channel_id,
                                    part="ContentDetails").execute()#only content details will execute of the given perticular id
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    nextpagetoken=None#refers to the next page first page doesnt need so
    while True:#loop
        response1=youtube.playlistItems().list(
                                            part="snippet",
                                            playlistId=playlist_id,#playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                                            maxResults=50, #parameter(maxresult)50 limit
                                            pageToken=nextpagetoken).execute()#parameter (pagetoken)to go to next page
        for i in range(len(response1['items'])):#50 times video id loop
            videoids.append(response1['items'][i]['snippet']['resourceId']['videoId'])#all the videoid will be alocated to the empty list on top
        nextpagetoken=response1.get('nextPageToken')#get fun if value is present it would give value if not it would return none
        
        if nextpagetoken is None:#to break the while loop
            break
    return videoids #all values inside the list,to return to function


#get video information
def get_video_info(videoids):
    videodata=[]
    for video_id in videoids:
        request=youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id
        )
        response=request.execute()#when ever response is called it would execute
        
        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_id=item['snippet']['channelId'],
                    Video_id=item['id'],
                    Title=item['snippet']['title'],
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet']['description'],
                    Publisheddate=item['snippet']['publishedAt'],
                    Duration=item['contentDetails'] ['duration'],
                    Views=item['statistics']['viewCount'],
                    Comments=item ['statistics']. get('commentCount'),#use get when the user has not provided permision eg for tags
                    Likes=item['statistics']. get('likeCount'),
                    Favoritecount=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption=item['contentDetails']['caption']
                    )
            videodata.append(data)
    return videodata

        
#get commnt info
def get_comment_info(videoids):
        
        Commentdata=[]
        try:#if comment is disabled error shouldnt occur
                for video_id in videoids:
                        request=youtube.commentThreads().list(
                                part="snippet",
                                videoId=video_id,
                                maxResults=50
                        )
                        response=request.execute()

                        for item in response["items"]:
                                data=dict(Commentid=item["snippet"]['topLevelComment']['id'],
                                        videoid=item["snippet"]['topLevelComment']['snippet']['videoId'],
                                        commenttext=item["snippet"]['topLevelComment']['snippet']['textDisplay'],
                                        commentauthor=item["snippet"]['topLevelComment']['snippet']['authorDisplayName'],
                                        commentpublished=item["snippet"]['topLevelComment']['snippet']['publishedAt'])# dtor in dict to stor in mongodb
                                Commentdata.append(data)
        except:
            pass#ignore error
        return Commentdata

#get playlist details
def get_playlist_details(channelids):
    next_page_token=None
    alldata=[]

    while True:
        request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channelids,
            maxResults=50,
            pageToken=next_page_token #50 above playlist
        )
        response=request.execute()
        for item in response['items']:
            data=dict(playlist_id=item['id'],
                    Title=item['snippet']['title'],
                    Channel_id=item['snippet']['channelId'],
                    Channel_name=item['snippet']['channelTitle'],
                    Publishedat=item['snippet']['publishedAt'],
                    videocount=item['contentDetails']['itemCount'])
            alldata.append(data)

        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return alldata 

#upload to mongo db

client=pymongo.MongoClient("mongodb+srv://swethasajeev:swethasajeev@cluster0.i7nyp09.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db=client["youtubedata"]


def channel_details(channel_id):#at a time all fun to b called and data to be inserted
    ch_details= get_channel_info(channel_id)
    pl_detalis=get_playlist_details(channel_id)
    vi_ids=get_videoids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db["channel_details"]#creating a collection cz we are calling all fun at a time
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_detalis,
                      "video_information":vi_details,"comment_information":com_details})
    return "upload completed successfully"
        

#table creation and insertion of data channel,playlist,vdo,cmt(c*3i)
#connection
def channels_table(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="root123",
                        database="youtubedata",
                        port="5432")
    cursor=mydb.cursor()
    #create table
    try:
       
        create_query_1='''create table if not exists channels(channelname varchar(100),
                                                            channelid varchar(80)primary key,
                                                            subscribers bigint,
                                                            Views bigint,
                                                            totalvideos bigint,
                                                            channeldescription text,
                                                            playlist_id varchar(80)
                                                            )'''
        cursor.execute(create_query_1)
        mydb.commit()

    except:
        print("channel table already created")

    #extracting data from mongo db and converting it as df
    single_channel_detail=[]
    db=client["youtubedata"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.channelname": channel_name_s},{"_id":0}):
        single_channel_detail.append(ch_data["channel_information"])
    df_single_channel_detail=pd.DataFrame(single_channel_detail)
    #df to to sqltable 
    for index,row in df_single_channel_detail.iterrows():
                insert_query='''insert into channels(channelid,
                                                    channelname, 
                                                    subscribers,
                                                    Views,
                                                    totalvideos,
                                                    channeldescription,
                                                    playlist_id)
                                                    
                                                    values(%s,%s,%s,%s,%s,%s,%s)'''
                values=(row['channelid'],
                        row['channelname'],
                        row['subscribers'],
                        row['Views'],
                        row['totalvideos'],
                        row['channeldescription'],
                        row['playlist_id'])
                try:
                    cursor.execute(insert_query,values)
                    mydb.commit()
                    
                except:
                   news= f"Your Provided Channel Name{channel_name_s} Already Exists "

                   return news


#PLAYLIST connection table
#connection playlist table
def playlist_table(channel_name_s):
        mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="root123",
                        database="youtubedata",
                        port="5432")
        cursor=mydb.cursor()
#creation
        create_query='''create table if not exists playlists(playlist_id varchar(100)primary key,
                                                                        Title varchar(80),
                                                                        Channel_id varchar(100),
                                                                        Channel_name varchar(80),
                                                                        Publishedat timestamp,
                                                                        videocount int
                                                                        )'''
        
        cursor.execute(create_query)
        mydb.commit()

#extracting data from mongo db and converting it as df
        single_playlist_detail=[]
        db=client["youtubedata"]
        coll1=db["channel_details"]
        for pl_data in coll1.find({"channel_information.channelname":channel_name_s},{"_id":0}):
                single_playlist_detail.append(pl_data["playlist_information"])

        df_single_playlist_detail=pd.DataFrame(single_playlist_detail[0])


# inserting into sql table from df

        for index,row in df_single_playlist_detail.iterrows():
                        insert_query='''insert into playlists(playlist_id,
                                                        Title, 
                                                        Channel_id,
                                                        Channel_name,
                                                        Publishedat,
                                                        videocount)
                                                        
                                                        values(%s,%s,%s,%s,%s,%s)'''
                        values=(row['playlist_id'],
                                row['Title'],
                                row['Channel_id'],
                                row['Channel_name'],
                                row['Publishedat'],
                                row['videocount'])
                
                        cursor.execute(insert_query,values)
                        mydb.commit()



#VIDEO connection
#connection
def video_table(channel_name_s):
        mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="root123",
                        database="youtubedata",
                        port="5432")
        cursor=mydb.cursor()

#creating table for videoinformation
    
        create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                                Channel_id varchar(80),
                                                                Video_id varchar(20)primary key,
                                                                Title varchar(100),
                                                                Thumbnail varchar(200) ,
                                                                Description text,
                                                                Publisheddate timestamp,
                                                                Duration interval,
                                                                Views bigint,
                                                                Comments int,
                                                                Likes int,
                                                                Favoritecount int,
                                                                Definition varchar(200),
                                                                Caption varchar(20)
                                                                )'''


       
        cursor.execute(create_query)
        mydb.commit()

# inserting into sql table from df
        single_video_detail=[]
        db=client["youtubedata"]
        coll1=db["channel_details"]
        for vdo_data in coll1.find({"channel_information.channelname":channel_name_s},{"_id":0}):
                single_video_detail.append(vdo_data["video_information"])

        df_single_video_detail=pd.DataFrame(single_video_detail[0])
        
        #inserting into sql table from df
        for index,row in df_single_video_detail.iterrows():
                        insert_query='''insert into videos(Channel_Name,
                                                        Channel_id, 
                                                        Video_id,
                                                        Title,
                                                        Thumbnail,
                                                        Description,
                                                        Publisheddate,
                                                        Duration,
                                                        Views,
                                                        Comments,
                                                        Likes,
                                                        Favoritecount,
                                                        Definition,
                                                        Caption)
                                                        
                                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        
                        values=(row['Channel_Name'],
                                row['Channel_id'],
                                row['Video_id'],
                                row['Title'],
                                row['Thumbnail'],
                                row['Description'],
                                row['Publisheddate'],
                                row['Duration'],
                                row['Views'],
                                row['Comments'],
                                row['Likes'],
                                row['Favoritecount'],
                                row['Definition'],
                                row['Caption'])
                
                        cursor.execute(insert_query,values)
                        mydb.commit()




#COMMENT table
#connection
def comment_table(channel_name_s):
        mydb=psycopg2.connect(host="localhost",
                                user="postgres",
                                password="root123",
                                database="youtubedata",
                                port="5432")
        cursor=mydb.cursor()
#creation

        create_query_1='''create table if not exists comments(Commentid varchar(100)primary key,
                                                                videoid varchar(80),
                                                                commenttext text,
                                                                commentauthor varchar(100),
                                                                commentpublished timestamp 
                                                                )'''
        cursor.execute(create_query_1)
        mydb.commit()

# converting into df
        single_comment_detail=[]
        db=client["youtubedata"]
        coll1=db["channel_details"]
        for cmt_data in coll1.find({"channel_information.channelname":channel_name_s},{"_id":0}):
                single_comment_detail.append(cmt_data["comment_information"])

        df_single_comment_detail=pd.DataFrame(single_comment_detail[0])

#inserting into table
        for index,row in df_single_comment_detail.iterrows():
                                insert_query='''insert into comments(Commentid,
                                                                videoid, 
                                                                commenttext,
                                                                commentauthor,
                                                                commentpublished
                                                                )
                                                                
                                                                values(%s,%s,%s,%s,%s)'''
                                values=(row['Commentid'],
                                        row['videoid'],
                                        row['commenttext'],
                                        row['commentauthor'],
                                        row['commentpublished']
                                )
                        
                                cursor.execute(insert_query,values)
                                mydb.commit()

#CONVERTING ALL FUNCTIONS TO ONE SINGLE FUNCTION
def tables(single_channel):
    news=channels_table(single_channel)
    if news:
          return news
    else:
        playlist_table(single_channel)
        video_table(single_channel)
        comment_table(single_channel)

    return"Tables created successfully"

#to see df in streamlit
def show_channel_table():
    ch_list=[]
    db=client["youtubedata"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):#empty{}denotes all the channel details #only channel info will b extracte
        ch_list.append(ch_data['channel_information'])
    df=st.dataframe(ch_list)
    return df

def show_playlist_table():
    pl_list=[]
    db=client["youtubedata"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):#runs four times
            for i in range(len(pl_data['playlist_information'])):
                    pl_list.append(pl_data['playlist_information'][i])#slicing 
            df1=st.dataframe(pl_list)
            return df1
    
def show_video_table():
    vdo_list=[]
    db=client["youtubedata"]
    coll1=db["channel_details"]
    for vdo_data in coll1.find({},{"_id":0,"video_information":1}):#runs four times
            for i in range(len(vdo_data['video_information'])):
                    vdo_list.append(vdo_data['video_information'][i])#slicing 
            df2=st.dataframe(vdo_list)
            return df2

def show_comment_table():
    cmt_list=[]
    db=client["youtubedata"]
    coll1=db["channel_details"]
    for cmt_data in coll1.find({},{"_id":0,"comment_information":1}):#runs four times
            for i in range(len(cmt_data['comment_information'])):
                    cmt_list.append(cmt_data['comment_information'][i])#slicing 
            df3=st.dataframe(cmt_list)

    return df3


#STREAMLIT PART 

with st.sidebar:
    st.title(":black[YouTube Data Harvesting and Warehousing using SQL and Streamlit]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption('DataCollection')
    st.caption("Mongo DB")
    st.caption('API integration')
    st.caption('Data Management using Mongo DB & SQL')

channel_id=st.text_input("Enter the Channel ID")#channel id

if st.button("Collect and store data"):#creating a button# by clicking the button all the data should be transfered from yt to mdb
    ch_ids=[]
    db=client["youtubedata"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["channelid"])

    if channel_id in ch_ids:#if channalmid already exists
        st.success('details already exists')
    else:
        insert=channel_details(channel_id)# if not insert
        st.success(insert)

all_channels=[]
db=client["youtubedata"]
coll1=db["channel_details"]
for ch_data in coll1.find({},{"_id":0,"channel_information":1}):#empty{}denotes all the channel details #only channel info will b extracte
    all_channels.append(ch_data["channel_information"]["channelname"])

unique_channel=st.selectbox('select channel',all_channels)

if st. button('Migrate to SQL'):
    table=tables(unique_channel)
    st.success(table)

show_table=st.radio("SELECT TABLE TO VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channel_table()
elif show_table=="PLAYLISTS":
    show_playlist_table()
elif show_table=="VIDEOS":
    show_video_table()
elif show_table=="COMMENTS":
    show_comment_table()



#SQL connection
mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="root123",
                        database="youtubedata",
                        port="5432")
cursor=mydb.cursor()

question=st.selectbox("select yout question",("1. All the videos and channel name",
                                              "2. Channels with most number of viewes",
                                              "3. 10 most viewed video",
                                              "4. comments in each videos",
                                              "5. videos with highest likes",
                                              "6. Likes of all videos",
                                              "7. Views of each channel",
                                              "8. Videos published int the year of 2023",
                                              "9. average duration of all video's in each channel",
                                              "10. video's with highest number of comments"))



#sql connection
mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="root123",
                        database="youtubedata",
                        port="5432")
cursor=mydb.cursor()
"4. comments in each videos",
"5. videos with highest likes",
"6. Likes of all videos",
"7. Views of each channel",

if question=="1. All the videos and channel name":
    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif question=="2. Channels with most number of viewes":
    query2='''select channelname as channelname,totalvideos as no_videos from channels
                order by totalvideos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","no_videos"])
    st.write(df2)

elif question=="3. 10 most viewed video":
    query3='''select views as views,channel_name as channelname,title as videotitle from videos
                where videos is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views ","channel name","videotitle" ])
    st.write(df3)
    
elif question=="4. comments in each videos":
    query4='''select comments as no_comments,title as videotitle from videos
                    where comments is not null '''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no_comment","videotitle" ])
    st.write(df4)

elif question=="5. videos with highest likes":
                                            
    query5='''select channel_name as channel,title as videotitle,likes as likes from videos
                    where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["channel","videotitle","likes"])
    st.write(df5)

elif question=="6. Likes of all videos":
    query6='''select channel_name as channel,title as videotitle,likes as likes from videos
                        where likes is not null order by likes desc'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["channel","videotitle","likes"])
    st.write(df6)

elif question=="7. Views of each channel":
    query7='''select channelname as channel,views  as views  from channels
                        where views is not null '''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel","view"])
    st.write(df7)

elif question=="8. Videos published int the year of 2023":
    query8='''select channel_name as channel,title  as videotitle,publisheddate as date  from videos
                    where videos is not null '''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["channel","videotitle","date"])
    st.write(df8)

elif question=="9. average duration of all video's in each channel":
    query9='''select channel_name as channelname,AVG(duration) as averageduration from videos 
    group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])  

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row['channelname']
        average_duration=row ["averageduration"]
        average_duration_str=(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str)) 

    df1=pd.DataFrame(T9)
    st.write(df1)

elif question=="10. video's with highest number of comments":
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="root123",
                        database="youtubedata",
                        port="5432")
    cursor=mydb.cursor()
    query10='''select videos as videotitle,channel_name as channelname,comments as comments  from videos
                where comments is not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["videotitle","channelname","comments"])
    st.write(df10)
    