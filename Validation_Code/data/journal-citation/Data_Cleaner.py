import numpy as np
import pandas as pd
import csv
import sys
from itertools import islice
import datetime as dt
import codecs
import time


#NOTE: THIS FILE CAN ONLY BE RUN IF THE PAPERS, REFERENCES, AND JOURNAL TABLES ARE WITHIN THE SAME FOLDER AS THIS FILE.


def clean_journals():
    #Create the csv file that contains extra info about the journals but also the separate ID-Name csv file 
    data = list()
    with open('MAG_Untouched_Journals.txt', newline='', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            try:
                row[0] = int(row[0])
                data.append(row)
            except:
                pass

    journals = np.array([[row[0] for row in data],[row[3] for row in data],[row[7] for row in data],[row[9] for row in data]])
    journals = np.transpose(journals)
    df = pd.DataFrame(journals, columns = ["Journal_ID", "Journal_Name", "Num_Papers","Num_Citations"])
    df.dropna(inplace=True)
    df.to_csv("More_Journal_Info.csv",index = False)

    df = pd.DataFrame(journals[:,[0,1]],columns = ["Journal_ID", "Journal_Name"])
    df.dropna(inplace=True)
    df.to_csv("Journal_ID-Journal_Names.csv",index = False)

def clean_papers():
    #Create the csv file that contains Paper-Journal connections
    maxInt = sys.maxsize
    while True:
        try:
            csv.field_size_limit(maxInt)
            break
        except OverflowError:
            maxInt = int(maxInt/10)
    df201819, df2020 = [], []
    cnt201819,cnt2020,totalcnt = 0, 0, 0
    bot2018 = dt.datetime.strptime('2018-01-01','%Y-%m-%d')
    mid2020 = dt.datetime.strptime('2020-01-01','%Y-%m-%d')
    top = dt.datetime.strptime('2021-01-01','%Y-%m-%d')
    with open('MAG_Untouched_Papers.txt', 'rb') as f:
        reader = csv.reader(codecs.iterdecode(f, 'utf-8'), delimiter='\t')
        cnt201819,cnt2020,totalcnt = 0, 0, 0
        try:
            for row in reader:
                if row != '':
                    try:
                        paper_date = dt.datetime.strptime(row[24],'%Y-%m-%d')
                        if paper_date >= mid2020 and paper_date < top and row[0] != '' and row[11] != '':
                            row[0] = int(row[0])
                            row[11] = int(row[11])
                            df2020.append([row[0],row[11]])
                            cnt2020 += 1
                        elif paper_date > bot2018 and paper_date < mid2020 and row[0] != '' and row[11] != '':
                            row[0] = int(row[0])
                            row[11] = int(row[11])
                            df201819.append([row[0],row[11]])
                            cnt201819 += 1
                    except IndexError:
                        pass
                    except ValueError:
                        pass
                    totalcnt += 1
                    if totalcnt % 100000 == 0 and totalcnt != 0:
                        #Report progress
                        print("2020 items found:" + str(cnt2020))
                        print("201819 items found:" + str(cnt201819))
                        print("Items searched:" + str(totalcnt))
                else:
                    pass
        except:
            print(row)
    print("Final 2020 items found:" + str(cnt2020))
    print("Final 201819 items found:" + str(cnt201819))
    print("Items found: "+str(totalcnt))
    #store 201819
    papers = np.array([[row[0] for row in df201819],[row[1] for row in df201819]])
    papers = np.transpose(papers)
    df201819 = pd.DataFrame(papers,columns = ["Paper_ID", "Journal_ID"])
    df201819.dropna(inplace=True)
    df201819.to_csv("Paper_ID-Journal_ID201819.csv",index = False)
    df201819 = []
    #store 2020
    papers = np.array([[row[0] for row in df2020],[row[1] for row in df2020]])
    papers = np.transpose(papers)
    df2020 = pd.DataFrame(papers,columns = ["Paper_ID", "Journal_ID"])
    df2020.dropna(inplace=True)
    df2020.to_csv("Paper_ID-Journal_ID2020.csv",index = False)
    df2020 = []
    

def clean_Refs():
    #Create the csv file that contains source-target connections
    maxInt = sys.maxsize
    while True:
        try:
            csv.field_size_limit(maxInt)
            break
        except OverflowError:
            maxInt = int(maxInt/10)

    #Create csv file of just references citer -> citee
    df = []
    cnt,totalcnt = 0, 0
    papers2020 = pd.read_csv("Paper_ID-Journal_ID2020.csv")
    papers201819 = pd.read_csv("Paper_ID-Journal_ID201819.csv")

    #Remove duplicates as this is an ID file
    papers2020 = papers2020.drop_duplicates()
    papers201819 = papers201819.drop_duplicates()

    #Converet to numpy array
    papers2020 = papers2020.values
    papers201819 = papers201819.values

    #Exctract ID lists
    paper2020IDs = papers2020[:,0]
    journal2020IDs = papers2020[:,1]
    paper201819IDs = papers201819[:,0]
    journal201819IDs = papers201819[:,1]

    #Delete on data to save space
    del papers2020, papers201819

    #Make a set so that they can be searched faster
    paper2020IDs = set(paper2020IDs.flatten())
    journal2020IDs = set(journal2020IDs.flatten())
    paper201819IDs = set(paper201819IDs.flatten())
    journal201819IDs = set(journal201819IDs.flatten())

    totalcnt = 0
    cnt = 0
    backup = 21
    check = True
    pastbackup20 = False

    with open("test1.csv", 'rt') as f:
        reader = csv.reader(f)
        df = pd.DataFrame(columns=['src','trg'])

        for row in reader:
            try:
                current_citing_paper = int(row[0])
                current_cited_paper = int(row[1])
                if current_citing_paper in paper2020IDs and current_cited_paper in paper201819IDs:
                    a_series = pd.Series([current_citing_paper,current_cited_paper], index = df.columns)
                    df = df.append(a_series, ignore_index=True)
                    cnt += 1
            except IndexError:
                pass
            
            except ValueError:
                pass

            if pastbackup20 == True:
                totalcnt += 1
                if totalcnt % 100000 == 0 and totalcnt != 0:
                    print(totalcnt)
            
                if totalcnt % 5000000 == 0 and totalcnt != 0:
                #Report progress
                    print("Items found:" + str(cnt))
                    print("Items searched:" + str(totalcnt)+" start this far after 3092559809 in cutrefs12 if messed up (replace 3092559809 with the next unique number after" +  str(totalcnt)+")")
                    df.to_csv('backup{}.csv'.format(backup), index=False)
                    backup += 1
                
        print("Final Items found: "+ str(cnt))
        print("Final Items searched: "+str(totalcnt))
        print(df.head())
        df.to_csv('testedUNTRANSLATED.csv', index=False)

    journal201819IDs = list(journal201819IDs)
    journal2020IDs = list(journal2020IDs)
    paper201819IDs = list(paper201819IDs)
    paper2020IDs = list(paper2020IDs)
    print(df)

    print("Replacing Ids...")
    for i in range(len(journal201819IDs)):
        df.replace(paper201819IDs[i], journal201819IDs[i],inplace=True)

    print(paper2020IDs)
    for i in range(len(journal2020IDs)):
        df.replace(paper2020IDs[i], journal2020IDs[i],inplace=True)
        
    df = df.groupby(df.columns.values.tolist()).size().reset_index()
    df = df.rename(columns={0:'weight'})
    print(df)
    df.to_csv('testedTRANSLATED.csv', index=False)
    print("Final Items removed: "+ str(cnt))
    print("Final Items searched: "+str(totalcnt))
    del df



def replace():
    #Create the csv file that contains source-target connections
    maxInt = sys.maxsize
    while True:
        try:
            csv.field_size_limit(maxInt)
            break
        except OverflowError:
            maxInt = int(maxInt/10)

    #Create csv file of just references citer -> citee
    cnt,totalcnt = 0, 0
    papers2020 = pd.read_csv("Paper_ID-Journal_ID2020.csv")
    papers201819 = pd.read_csv("Paper_ID-Journal_ID201819.csv")

    #Remove duplicates as this is an ID file
    papers2020 = papers2020.drop_duplicates()
    papers201819 = papers201819.drop_duplicates()

    #Converet to numpy array
    papers2020 = papers2020.values
    papers201819 = papers201819.values

    #Exctract ID lists
    paper2020IDs = papers2020[:,0]
    journal2020IDs = papers2020[:,1]
    paper201819IDs = papers201819[:,0]
    journal201819IDs = papers201819[:,1]

    #Delete on data to save space
    del papers2020, papers201819
    
    df = pd.read_csv("EDGETABLEUNTRANSLATED.csv")

    totalcnt = 0

    df = np.transpose(df.to_numpy())
    untrans2020 = df[0]
    untrans201819 = df[1]

    untrans2020dict = dict.fromkeys(np.unique(untrans2020))
    untrans201819dict = dict.fromkeys(np.unique(untrans201819))

    for i,ref2020 in enumerate(untrans2020):
        if untrans2020dict[ref2020] == None:
            untrans2020dict[ref2020] = [i]
        else:
            untrans2020dict[ref2020].append(i)
    
    totalcnt = 0
    for i,ref201819 in enumerate(untrans201819):
        if untrans201819dict[ref201819] == None:
            untrans201819dict[ref201819] = [i]
        else:
            untrans201819dict[ref201819].append(i)
    
    search2020dict = dict(zip(paper2020IDs, journal2020IDs))
    search201819dict = dict(zip(paper201819IDs, journal201819IDs))

    #search2020dict {paperID: journalID}
    #untrans2020dict {paperID: [indices]}

    totalcnt = 0
    print("starting 2020")
    for paperID2020 in search2020dict.keys():
        if paperID2020 not in untrans2020dict.keys():
            totalcnt += 1
            continue
        for index in untrans2020dict[paperID2020]:
            untrans2020[index] = search2020dict[paperID2020]

        totalcnt += 1
    
    totalcnt = 0
    print("starting 201819")
    for paperID201819 in search201819dict.keys():
        if paperID201819 not in untrans201819dict.keys():
            continue
        for index in untrans201819dict[paperID201819]:
            untrans201819[index] = search201819dict[paperID201819]
        totalcnt += 1
        
    journal_df = np.vstack((untrans201819,untrans2020))
    journal_df = np.transpose(journal_df)

    journal_df = pd.DataFrame(journal_df,columns=['src','trg'])

    del df

    journal_df = journal_df.groupby(journal_df.columns.values.tolist()).size().reset_index()
    journal_df = journal_df.rename(columns={0:'weight'})
    print(journal_df)
    journal_df.to_csv('edge-table-2020.csv', index=False)
    print("Final Items removed: "+ str(cnt))
    print("Final Items searched: "+str(totalcnt))
    del journal_df

def check():
    #Create the csv file that contains source-target connections
    maxInt = sys.maxsize
    while True:
        try:
            csv.field_size_limit(maxInt)
            break
        except OverflowError:
            maxInt = int(maxInt/10)

    #Create csv file of just references citer -> citee
    df = []
    cnt,totalcnt = 0, 0
    papers2020 = pd.read_csv("Paper_ID-Journal_ID2020.csv")
    papers201819 = pd.read_csv("Paper_ID-Journal_ID201819.csv")
    papers2020 = papers2020.values
    papers201819 = papers201819.values
    paper2020IDs = papers2020[:,0]
    journal2020IDs = papers2020[:,1]
    paper201819IDs = papers201819[:,0]
    journal201819IDs = papers201819[:,1]
    del papers2020, papers201819
    min2020, min201819 = np.min(paper2020IDs), np.min(paper201819IDs)
    cnt = 0
    filenum = 9
    with open('MAG_Untouched_References.txt', newline='', encoding='utf-8') as f:
        reader = csv.reader(f,delimiter='\t')
        cnt = 0
        totalcnt = 0
        for row in reader:
            if totalcnt >= 1650000000:
                try:
                    current_citing_paper = int(row[0])
                    current_cited_paper = int(row[1])
                    if current_citing_paper >= min2020 and current_cited_paper >= min201819:
                        df.append([current_citing_paper,current_cited_paper])
                        cnt += 1
                except IndexError:
                    pass
                except ValueError:
                    pass
            if totalcnt % 100000000 == 0 and totalcnt != 0:
                #Report progress
                print("Items collected:" + str(cnt))
                print("Items searched:" + str(totalcnt))
            totalcnt += 1
            

        print("Final Items collected: "+str(cnt))
        print("Final Items searched: "+str(totalcnt))
        print("making df...")
        df = pd.DataFrame(df,columns = ["2020Paper_ID(Source)", "2018/2019Paper_ID(Target)"])
        print("checking na...")
        df.dropna(inplace=True)
        print("saving to file...")
        print(df)
        df.to_csv("Cut_Refs12.csv",index = False)
        print("Done!")
        filenum += 1
        del df
    print(cnt)



if __name__ == "__main__":
    #clean_journals()
    #clean_papers()
    #check()
    #clean_Refs()
    replace()