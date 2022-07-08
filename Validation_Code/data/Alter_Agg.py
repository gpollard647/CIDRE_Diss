import pandas

if __name__ == "__main__":
    
    agg_df = pandas.read_csv("aggregated-community.csv", sep='\t')
    agg_df = agg_df.filter(['community_id','journal_id',''], axis=1)
    print(agg_df)
    agg_df.to_csv('aggregated-community.csv', index=False)