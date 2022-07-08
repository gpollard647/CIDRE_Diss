#This Python file can organise any MAG dataset for use with CIDRE using the Papers, Journals, and References MAG datasets.
#This Python file takes the bottom, middle and top limit years for extraction as arguments (see clean_papers for the definitions)
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

def clean_papers(bot = None,mid = None,top = None):
    # bot = The bottom year to start taking citations from in the window of interest
    # mid = The year to start taking citations from
    # top = The first year in which citations should stop being considered
    # EG: Analysiing JIF for year 2020, bot = 2018, mid = 2020, top = 2021 
    # Create the csv file that contains Paper-Journal translations
    maxInt = sys.maxsize
    while True:
        try:
            csv.field_size_limit(maxInt)
            break
        except OverflowError:
            maxInt = int(maxInt/10)

    # Store receiving and giving separately in dicts
    df_rec_period, df_give_period = dict.fromkeys(["PaperID","JournalID"]), dict.fromkeys(["PaperID","JournalID"])
    cnt_give_period,cnt_rec_period,totalcnt = 0, 0, 0
    bot_lab = dt.datetime.strptime('{}-01-01'.format(bot),'%Y-%m-%d')
    mid_lab = dt.datetime.strptime('{}-01-01'.format(mid),'%Y-%m-%d')
    top_lab = dt.datetime.strptime('{}-01-01'.format(top),'%Y-%m-%d')
    with open('MAG_Untouched_Papers.txt', 'rb') as f:
        reader = csv.reader(codecs.iterdecode(f, 'utf-8'), delimiter='\t')
        try:
            for row in reader:
                if row != '':
                    try:
                        paper_date = dt.datetime.strptime(row[24],'%Y-%m-%d')
                        if paper_date >= mid_lab and paper_date < top_lab and row[0] != '' and row[11] != '':
                            if df_give_period["PaperID"] == None or df_give_period["JournalID"] == None:
                                df_give_period["PaperID"] = [int(row[0])]
                                df_give_period["JournalID"] = [int(row[11])]
                            else:
                                df_give_period["PaperID"].append(int(row[0]))
                                df_give_period["JournalID"].append(int(row[11]))
                            cnt_give_period += 1
                        elif paper_date >= bot_lab and paper_date < mid_lab and row[0] != '' and row[11] != '':
                            if df_rec_period["PaperID"] == None or df_rec_period["JournalID"] == None:
                                df_rec_period["PaperID"] = [int(row[0])]
                                df_rec_period["JournalID"] = [int(row[11])]
                            else:
                                df_rec_period["PaperID"].append(int(row[0]))
                                df_rec_period["JournalID"].append(int(row[11]))
                            cnt_rec_period += 1
                    except IndexError:
                        pass
                    except ValueError:
                        pass
                totalcnt += 1
                if totalcnt % 1000000 == 0 and totalcnt != 0:
                    #Report progress
                    print("Giving citations period items found:" + str(cnt_give_period))
                    print("Receiving citations period items found:" + str(cnt_rec_period))
                    print("Items searched:" + str(totalcnt))
        except Exception as e:
            pass
                
            

    print("Final giving period items found:" + str(cnt_give_period))
    print("Final receiving period items found:" + str(cnt_rec_period))
    print("Items searched: "+str(totalcnt))

    #store giving
    df_give_period = pd.DataFrame.from_dict(df_give_period)
    df_give_period.to_csv('{}_paper-journals.csv'.format(mid), index=False)

    #store receiving
    df_rec_period = pd.DataFrame.from_dict(df_rec_period)
    df_rec_period.to_csv('{}_paper-journals.csv'.format(bot), index=False)

    return bot,mid
    
################################################################################

def translate_refs(bot,mid):
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
    df_give_period = pd.read_csv("{}_paper-journals.csv".format(mid))
    df_recv_period = pd.read_csv("{}_paper-journals.csv".format(bot))

    #Remove duplicates as this is an ID file
    df_give_period = df_give_period.drop_duplicates()
    df_recv_period = df_recv_period.drop_duplicates()

    #Converet to numpy array
    df_give_period = df_give_period.values
    df_recv_period = df_recv_period.values

    #Exctract ID lists
    give_paper_IDs = df_give_period[:,0]
    give_journal_IDs = df_give_period[:,1]
    recv_paper_IDs = df_recv_period[:,0]
    recv_journal_IDs = df_recv_period[:,1]

    #Delete on data to save space
    del df_recv_period, df_give_period
    
    df = pd.read_csv("Untranslated_Refs.csv")

    totalcnt = 0

    df = np.transpose(df.to_numpy())
    untrans_give = df[0]
    untrans_recv = df[1]

    untrans_give_dict = dict.fromkeys(np.unique(untrans_give))
    untrans_recv_dict = dict.fromkeys(np.unique(untrans_recv))

    for i,ref_give in enumerate(untrans_give):
        if untrans_give_dict[ref_give] == None:
            untrans_give_dict[ref_give] = [i]
        else:
            untrans_give_dict[ref_give].append(i)
    
    totalcnt = 0
    for i,ref_recv in enumerate(untrans_recv):
        if untrans_recv_dict[ref_recv] == None:
            untrans_recv_dict[ref_recv] = [i]
        else:
            untrans_recv_dict[ref_recv].append(i)
    
    search_give_dict = dict(zip(give_paper_IDs, give_journal_IDs))
    search_recv_dict = dict(zip(recv_paper_IDs, recv_journal_IDs))

    #search_give_dict {paperID: journalID}
    #untrans_give_dict {paperID: [indices]}

    totalcnt = 0


    print("starting givers")
    for untrans_give_ref in list(untrans_give_dict.keys()).copy():
        if untrans_give_ref not in search_give_dict.keys():
            untrans_give_dict.pop(untrans_give_ref)
            print("pop: "+str(untrans_give_ref)+ " from givers as it is not in givers papers.")
        else:
            for index in untrans_give_dict[untrans_give_ref]:
                untrans_give[index] = search_give_dict[untrans_give_ref]
        totalcnt += 1

    print("starting receivers")
    for untrans_recv_ref in list(untrans_recv_dict.keys()).copy():
        if untrans_recv_ref not in search_recv_dict.keys():
            untrans_recv_dict.pop(untrans_recv_ref)
            print("pop: "+str(untrans_recv_ref)+ " from receieving refs as it is not in receving papers.")
        else:
            for index in untrans_recv_dict[untrans_recv_ref]:
                untrans_recv[index] = search_recv_dict[untrans_recv_ref]
        totalcnt += 1

    temp = np.vstack((untrans_recv,untrans_give))
    temp = np.transpose(temp)
    temp = pd.DataFrame(temp,columns=['src','trg'])

    # Make a journal names file for journals only of interests
    all_journal_names = pd.read_csv("ALLjournal_id-name.csv")
    all_journal_names = dict(zip(all_journal_names.values[:,0],all_journal_names.values[:,1]))

    journal_names_of_interest = dict.fromkeys(["journal_id","name"])

    unique_journals_of_interest = np.unique(np.concatenate((temp.values[:,0],temp.values[:,1]), axis = None))

    del temp, df

    journalcount,notint,interesting = 0,0,0
    remove= list()

    print("Finding journals of interest")
    for journalID in unique_journals_of_interest:
        if journalID in all_journal_names.keys():
            if journal_names_of_interest["journal_id"] == None or journal_names_of_interest["name"] == None:
                print("Done initial")
                journal_names_of_interest["journal_id"] = [journalID]
                journal_names_of_interest["name"] = [all_journal_names[journalID]]
                interesting += 1

            else:
                journal_names_of_interest["journal_id"].append(journalID)
                journal_names_of_interest["name"].append(all_journal_names[journalID])
                interesting += 1

        else:
            remove.append(journalID)
            notint += 1
            
        journalcount += 1
        if journalcount % 1000 == 0 and journalcount != 0:
            print(str(len(unique_journals_of_interest)-journalcount) + " journals left!")
    
    print(str(interesting) + " journals of interest.")
    print(str(notint) + " journals not in the names file.")
    before = len(untrans_give)

    for removalID in remove:
        tempremove = np.where(removalID == untrans_recv)
        untrans_recv = np.delete(untrans_recv,tempremove)
        untrans_give = np.delete(untrans_give,tempremove)

        tempremove = np.where(removalID == untrans_give)
        untrans_recv = np.delete(untrans_recv,tempremove)
        untrans_give = np.delete(untrans_give,tempremove)
    
    print(str(before-len(untrans_give)) + " references removed as their journal ID was not in the journal names file.")

    # Transform refs to table form
    journal_names_of_interest = pd.DataFrame.from_dict(journal_names_of_interest)
    journal_df = np.vstack((untrans_recv,untrans_give))
    journal_df = np.transpose(journal_df)
    journal_df = pd.DataFrame(journal_df,columns=['src','trg'])

    # Find wieghts of edges by counting duplicates, save names and edge table to file
    journal_df = journal_df.groupby(journal_df.columns.values.tolist()).size().reset_index()
    journal_df = journal_df.rename(columns={0:'weight'})
    print("Journals of interest:")
    print(journal_names_of_interest)
    print("Edge table:")
    print(journal_df)
    journal_names_of_interest.to_csv('{}-{}_journals_of_interest.csv'.format(bot,mid), index=False)
    journal_df.to_csv('{}-{}-edge-table.csv'.format(bot,mid), index=False)
    print("Final Items removed: "+ str(cnt))
    print("Final Items searched: "+str(totalcnt))
    print("Number of journals of interest: "+str(len(journal_names_of_interest.index)))
    print("Length of final records aftere weighting: "+str(len(journal_df.index)))
    del journal_df

#####################################################################

def clean_refs(bot,mid):
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
    give_paper_journals = pd.read_csv('{}_paper-journals.csv'.format(mid))
    recv_paper_journals = pd.read_csv('{}_paper-journals.csv'.format(bot))

    give_paper_dict = dict(zip(give_paper_journals.values[:,0],give_paper_journals.values[:,1]))
    recv_paper_dict = dict(zip(recv_paper_journals.values[:,0],recv_paper_journals.values[:,1]))
    del give_paper_journals, recv_paper_journals

    refdict = dict.fromkeys(["src","trg"])

    with open('MAG_Untouched_References.txt', newline='', encoding='utf-8') as f:
        reader = csv.reader(f,delimiter='\t')
        for row in reader:
            if int(row[0]) in give_paper_dict and int(row[1]) in recv_paper_dict:
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
    print("Final refs from giving papers to receiving papers: " +str(cnt))

    # Store refs
    refdict = pd.DataFrame.from_dict(refdict)
    refdict.to_csv('Untranslated_Refs.csv', index=False)

if __name__ == "__main__":
    clean_journals()
    b, m = clean_papers(bot = sys.argv[1], mid = sys.argv[2], top = sys.argv[3])
    clean_refs(b,m)
    translate_refs(b,m)