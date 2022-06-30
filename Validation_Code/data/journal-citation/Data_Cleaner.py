#This Python file can organise any MAG dataset for use with CIDRE using the Papers, Journals, and References MAG datasets.
import numpy as np
import pandas as pd
import csv
import sys
from itertools import islice
import datetime as dt
import codecs
import time


#NOTE: THIS FILE CAN ONLY BE RUN IF THE PAPERS, REFERENCES, AND JOURNAL MAG TABLES ARE WITHIN THE SAME FOLDER AS THIS FILE.

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
    df = pd.DataFrame(journals, columns = ["journal_id", "namename", "Num_Papers","Num_Citations"])
    df.dropna(inplace=True)
    df.to_csv("More_Journal_Info.csv",index = False)

    df = pd.DataFrame(journals[:,[0,1]],columns = ["journal_id", "name"])
    df.dropna(inplace=True)
    df.to_csv("ALLjournal_id-name.csv",index = False)

##########################################################################################

def clean_papers():
    #Create the csv file that contains Paper-Journal translations
    maxInt = sys.maxsize
    while True:
        try:
            csv.field_size_limit(maxInt)
            break
        except OverflowError:
            maxInt = int(maxInt/10)

    #Store 201819 and 2020 separately in dicts
    df201819, df2020 = dict.fromkeys(["PaperID","JournalID"]), dict.fromkeys(["PaperID","JournalID"])
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
                            if df2020["PaperID"] == None or df2020["JournalID"] == None:
                                df2020["PaperID"] = [int(row[0])]
                                df2020["JournalID"] = [int(row[11])]
                            else:
                                df2020["PaperID"].append(int(row[0]))
                                df2020["JournalID"].append(int(row[11]))
                            cnt2020 += 1
                        elif paper_date > bot2018 and paper_date < mid2020 and row[0] != '' and row[11] != '':
                            if df201819["PaperID"] == None or df201819["JournalID"] == None:
                                df201819["PaperID"] = [int(row[0])]
                                df201819["JournalID"] = [int(row[11])]
                            else:
                                df201819["PaperID"].append(int(row[0]))
                                df201819["JournalID"].append(int(row[11]))
                            cnt201819 += 1
                    except IndexError:
                        pass
                    except ValueError:
                        pass
                totalcnt += 1
                if totalcnt % 1000000 == 0 and totalcnt != 0:
                    #Report progress
                    print("2020 items found:" + str(cnt2020))
                    print("201819 items found:" + str(cnt201819))
                    print("Items searched:" + str(totalcnt))
        except Exception as e:
            pass
                
            

    print("Final 2020 items found:" + str(cnt2020))
    print("Final 201819 items found:" + str(cnt201819))
    print("Items searched: "+str(totalcnt))

    #store 2020
    df2020 = pd.DataFrame.from_dict(df2020)
    df2020.to_csv('2020paper-journals.csv', index=False)

    #store 201819
    df201819 = pd.DataFrame.from_dict(df201819)
    df201819.to_csv('201819paper-journals.csv', index=False)
    
################################################################################

def translate_refs():
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
    papers2020 = pd.read_csv("2020paper-journals.csv")
    papers201819 = pd.read_csv("201819paper-journals.csv")

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
    
    df = pd.read_csv("Untranslated_Refs.csv")

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

    # Make a journal names file for journals only of interests
    all_journal_names = pd.read_csv("ALLjournal_id-name.csv")
    all_journal_names = dict(zip(all_journal_names.values[:,0],all_journal_names.values[:,1]))

    journal_names_of_interest = dict.fromkeys(["journal_id","name"])

    lenjournals = len(all_journal_names.keys())

    unique_journals_of_interest = np.unique(np.concatenate([journal_df.values[:,0],journal_df.values[:,1]]))

    journalcount = 0

    print("Finding journals of interest")
    for journalID in all_journal_names:
        if journalID in unique_journals_of_interest:
            if journal_names_of_interest["journal_id"] == None or journal_names_of_interest["name"] == None:
                print("Done initial")
                journal_names_of_interest["journal_id"] = [journalID]
                journal_names_of_interest["name"] = [all_journal_names[journalID]]

            else:
                journal_names_of_interest["journal_id"].append(journalID)
                journal_names_of_interest["name"].append(all_journal_names[journalID])
        journalcount += 1
        if journalcount % 1000 == 0 and journalcount != 0:
            print(str(lenjournals-journalcount) + " journals left!")

    journal_names_of_interest = pd.DataFrame.from_dict(journal_names_of_interest)
    
    # Find wieghts of edges by counting duplicates, save names and edge table to file
    journal_df = journal_df.groupby(journal_df.columns.values.tolist()).size().reset_index()
    journal_df = journal_df.rename(columns={0:'weight'})
    print("Journals of interest:")
    print(journal_names_of_interest)
    print("Edge table:")
    print(journal_df)
    journal_names_of_interest.to_csv('journals_of_interest.csv', index=False)
    journal_df.to_csv('edge-table-2020.csv', index=False)
    print("Final Items removed: "+ str(cnt))
    print("Final Items searched: "+str(totalcnt))
    del journal_df

#####################################################################

def clean_refs():
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
    papers2020 = pd.read_csv("2020paper-journals.csv")
    papers201819 = pd.read_csv("201819paper-journals.csv")

    paperdict2020 = dict(zip(papers2020.values[:,0],papers2020.values[:,1]))
    paperdict201819 = dict(zip(papers201819.values[:,0],papers201819.values[:,1]))
    del papers2020, papers201819

    refdict = dict.fromkeys(["src","trg"])

    with open('MAG_Untouched_References.txt', newline='', encoding='utf-8') as f:
        reader = csv.reader(f,delimiter='\t')
        for row in reader:
            if int(row[0]) in paperdict2020 and int(row[1]) in paperdict201819:
                if refdict["src"] == None or refdict["trg"] == None:
                    refdict["src"] = [int(row[0])]
                    refdict["trg"] = [int(row[1])]
                else:
                    refdict["src"].append(int(row[0]))
                    refdict["trg"].append(int(row[1]))
                
                cnt += 1

            totalcnt += 1
            if totalcnt % 1000000 == 0 and totalcnt != 0:
                #Report progress
                print("Items collected:" + str(cnt))
                print("Items searched:" + str(totalcnt))
        
    print("Final references seached through: " +str(totalcnt))
    print("Final refs from 2020 to 2018 or 2019 papers: " +str(cnt))

    # Store refs
    refdict = pd.DataFrame.from_dict(refdict)
    refdict.to_csv('Untranslated_Refs.csv', index=False)

if __name__ == "__main__":
    #clean_journals()
    #clean_papers()
    #clean_refs()
    translate_refs()