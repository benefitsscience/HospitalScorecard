import pandas as pd

# Import Data
pd.set_option('display.max_columns', None)


Elig = pd.read_csv('Data Sources/Elig.txt', sep='\t')
Baptist = pd.read_csv('Hospital Systems/Baptist St Anthony.csv')
BSW = pd.read_csv('Hospital Systems/BSW.csv')
CHRISTUS = pd.read_csv('Hospital Systems/CHRISTUS.csv')
Community = pd.read_csv('Hospital Systems/Community.csv')
Cook = pd.read_csv('Hospital Systems/Cook.csv')
Covenant = pd.read_csv('Hospital Systems/Covenant.csv')
EastTX = pd.read_csv('Hospital Systems/East Texas.csv')
HoustonHCA = pd.read_csv('Hospital Systems/Houston HCA.csv')
MemorialHermann = pd.read_csv('Hospital Systems/Memorial Hermann.csv')
MethodistDallas = pd.read_csv('Hospital Systems/Methodist Dallas.csv')
MethodistHospital = pd.read_csv('Hospital Systems/Methodist Hospital.csv')
NorthTXHCA = pd.read_csv('Hospital Systems/North Texas HCA.csv')
SanAntonioHCA = pd.read_csv('Hospital Systems/San Antonio HCA.csv')
Seton = pd.read_csv('Hospital Systems/Seton.csv')
StDavids = pd.read_csv('Hospital Systems/St Davids.csv')
StLukes = pd.read_csv('Hospital Systems/St Lukes.csv')
Tenet = pd.read_csv('Hospital Systems/Tenet.csv')
TexasChildrens = pd.read_csv('Hospital Systems/Texas Childrens.csv')
THR = pd.read_csv('Hospital Systems/THR.csv')
UTS = pd.read_csv('Hospital Systems/UTS.csv')

Concat = pd.concat([Baptist, BSW, CHRISTUS, Community, Cook, Covenant, EastTX, HoustonHCA, MemorialHermann, MethodistDallas, MethodistHospital, NorthTXHCA, SanAntonioHCA, Seton, StDavids, StLukes, Tenet, TexasChildrens, THR, UTS])
IDN = Concat.copy()
IDN = IDN.groupby('IDN').agg({'amtbilled': 'sum', 'amtcovered': 'sum', 'amtallowed': 'sum'}).reset_index()
IDN.sort_values(by='amtcovered', ascending=False, inplace=True)
IDN['AvgDiscount'] = 1 - IDN['amtallowed'] / IDN['amtcovered']

Accounts = Concat.copy()
Accounts = Accounts.groupby('IDN').agg({'tenantid': 'nunique'}).reset_index()
Accounts.rename(columns={'tenantid': 'UniqueAccounts'}, inplace=True)

Claimants = Concat.copy()
Claimants['personid'] = Claimants['personid'].astype(str)
Claimants = Claimants.groupby('IDN').agg({'personid': 'nunique'}).reset_index()
Claimants.rename(columns={'personid': 'UniqueClaimants'}, inplace=True)

MultiClaimants = Concat.copy()
MultiClaimants = MultiClaimants[MultiClaimants['servicecategory_details'] != 'Outpatient - Emergency']
MultiClaimants = MultiClaimants.groupby(['IDN', 'personid']).agg({'ClaimCount': 'sum'}).reset_index()
MultiClaimants = MultiClaimants[MultiClaimants['ClaimCount'] > 1]
MultiClaimants = MultiClaimants.groupby('IDN').agg({'personid': 'nunique'}).reset_index()
MultiClaimants.rename(columns={'personid': 'MultipleClaimants'}, inplace=True)

Hospitals = pd.merge(IDN, Accounts, on='IDN', how='left')
Hospitals = pd.merge(Hospitals, Claimants, on='IDN', how='left')
Hospitals = pd.merge(Hospitals, MultiClaimants, on='IDN', how='left')

Mbrs = Elig['MbrCount'].sum()

Hospitals['PctofPop'] = Hospitals['UniqueClaimants'] / Mbrs
Hospitals['PctofAllowed'] = Hospitals['amtallowed'] / 28524910806.37

print(Hospitals)
Hospitals.to_csv('Outputs/Hospitals.csv', index=False)