import pandas as pd
from pathlib import Path

class RosterConsol:
    
    def __init__(self):
        self.df1 = df1
        self.df2 = df2
        self.df3 = df3
        self.df4 = df4
        self.df5 = df5
        self.dtmsVerify = dtmsVerify
        self.vantageVerify = vantageVerify
        self.ippsaVerify = ippsaVerify
        self.disVerify = disVerify
        self.perstatVerify = perstatVerify

    @classmethod
    def csv_DF(self):
        if Path("Rosters/DTMS.csv").exists():
            self.df1 = pd.read_csv('Rosters/DTMS.csv')
            self.dtmsVerify = True
            print("DTMS Dataframe: Success")
            self.proc_D()
        else:
            self.dtmsVerify = False
            print("DTMS Dataframe: Fail")
            
        if Path("Rosters/Vantage.csv").exists():
            self.df2 = pd.read_csv('Rosters/Vantage.csv')
            self.vantageVerify = True
            print("Vantage Dataframe: Success")
            self.proc_V()
        else:
            self.vantageVerify = False
            print("Vantage Dataframe: Fail")
                
        if Path("Rosters/IPPSA.csv").exists():
            self.df3 = pd.read_csv('Rosters/IPPSA.csv')
            self.ippsaVerify = True
            print("IPPSA Dataframe: Success")
            self.proc_I()
        else:
            self.ippsaVerify = False
            print("IPPSA Dataframe: Fail")

        if Path("Rosters/DISS.csv").exists():
            self.df4 = pd.read_csv('Rosters/DISS.csv')
            self.dissVerify = True
            print("DISS Dataframe: Success")
            self.proc_DISS()
        else:
            self.perstatVerify = False
            print("DISS Dataframe: Fail")

        if Path("Rosters/PERSTAT.csv").exists():
            self.df5 = pd.read_csv('Rosters/PERSTAT.csv')
            self.perstatVerify = True
            print("PERSTAT Dataframe: Success")
            self.proc_P()
        else:
            self.perstatVerify = False
            print("PERSTAT Dataframe: Fail")
        
    @classmethod
    def proc_D(self):
        if self.dtmsVerify == True:
            self.df1.rename(columns={'Name': 'DTMS_Name'}, inplace=True)
            print(f"DTMS Column NAME: Success\n", self.df1.head())
            self.df1.insert(0, "DTMS_ID", self.df1.index+1)
            print("DTMS Enumeration: Success")
        else:
            print("DTMS Processing: Fail")
            
    @classmethod
    def proc_V(self):
        if self.vantageVerify == True:
            self.df2.rename(columns={'Name': 'Vantage_Name', 'DoDID':'EDIPI'}, inplace=True)
            print(f"Vantage Column Name and EDIPI: Success\n", self.df2.head())
            self.df2.insert(0, "Vantage_ID", self.df2.index+1)
            print("Vantage Enumeration: Success")

        else:
            print(f"Vantage Processing: Fail")

    @classmethod
    def proc_I(self):
        if self.ippsaVerify == True:
            self.df3.rename(columns={'Name': 'IPPSA_Name', 'DOD ID':'EDIPI'}, inplace=True)
            print(f"IPPS-A Column EDIPI: Success\n", self.df3.head())
            self.df3.insert(0, "IPPSA_ID", self.df3.index+1)
            print("IPPS-A Enumeration: Success")

        else:
            print(f"IPPS-A Processing: Fail")

    @classmethod
    def proc_DISS(self):
        if self.dissVerify == True:
            self.df4.rename(columns={'SUBJECT NAME': 'DISS_Name'}, inplace=True)
            print(f"DISS Column EDIPI: Success\n", self.df4.head())
            self.df4.insert(0, "DISS_ID", self.df4.index+1)
            print("DISS Enumeration: Success")

        else:
            print(f"DISS Processing: Fail")

    @classmethod
    def proc_P(self):
        if self.perstatVerify == True:
            self.df5.rename(columns={'Name': 'PERSTAT_Name'}, inplace=True)
            print(f"PERSTAT Column EDIPI: Success\n", self.df5.head())
            self.df5.insert(0, "PERSTAT_ID", self.df5.index+1)
            print("PERSTAT Enumeration: Success")

        else:
            print(f"PERSTAT Processing: Fail")

    @classmethod
    def merge_DV(self):
        if self.dtmsVerify == True and self.vantageVerify == True:
            self.dfm1 = pd.merge(self.df1, self.df2, on='EDIPI', how="outer")
            self.dfm1.to_csv("Rosters/Merge_EDIPI.csv", index=None)
            print("DTMS-Vantage Merge: Success")
        else:
            print("DTMS-Vantage Merge: Fail")

    @classmethod
    def merge_DVI(self):
        if self.dtmsVerify == True and self.vantageVerify == True and self.ippsaVerify ==True:
            self.dfm2 = pd.merge(self.dfm1, self.df3, on='EDIPI', how="outer")
            self.dfm2.to_csv("Rosters/Merge_EDIPI2.csv", index=None)
            print("D-V-IPPSA Merge: Success")
        else:
            print("D-V-IPPSA Merge: Fail")
            
    @classmethod
    def merge_DVID(self):
        if self.dtmsVerify == True and self.vantageVerify == True and self.ippsaVerify ==True and self.perstatVerify ==True:
            self.dfm3 = pd.merge(self.dfm2, self.df4, on='EDIPI', how="outer")
            self.dfm3.to_csv("Rosters/Merge_EDIPI3.csv", index=None)
            print("D-V-I-DISS Merge: Success")
        else:
            print("D-V-I-DISS Merge: Fail")
            
    @classmethod
    def merge_DVIDP(self):
        if self.dtmsVerify == True and self.vantageVerify == True and self.ippsaVerify ==True and self.perstatVerify ==True:
            self.dfm4 = pd.merge(self.dfm3, self.df5, on='EDIPI', how="outer")
            print("D-V-I-D-PERSTAT Merge: Success")
            self.dfm4.insert(0, "Merge_ID", self.dfm4.index+1)
            print("Merge Enumeration: Success")
            self.dfm4.to_csv("Rosters/Merge_EDIPI3.csv", index=None)
            print("Roster Consolidation: Success")
        else:
            print("D-V-I-D-PERSTAT Merge: Fail")

rc = RosterConsol

rc.csv_DF()
rc.merge_DV()
rc.merge_DVI()
rc.merge_DVID()
rc.merge_DVIDP()
