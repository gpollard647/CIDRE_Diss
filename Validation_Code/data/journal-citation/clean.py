import numpy as np
import pandas as pd
import csv

def clean_journals():
    #Create the csv file that contains extra info about the journals but also the separate ID-Name csv file 
    data = list()
    with open('MAG_Untouched_Journals.txt', newline='', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            data.append(row)

    journals = np.array([[row[0] for row in data],[row[3] for row in data],[row[7] for row in data],[row[9] for row in data]])
    journals = np.transpose(journals)
    df = pd.DataFrame(journals, columns = ["Journal_ID", "Journal_Name", "Num_Papers","Num_Citations"])
    df.dropna(inplace=True)
    df.to_csv("More_Journal_Info.csv",index = False)

    df = pd.DataFrame(journals[:,[0,1]],columns = ["Journal_ID", "Journal_Name"])
    df.dropna(inplace=True)
    df.to_csv("Journal_Names.csv",index = False)

if __name__ == "__main__":
    clean_journals()