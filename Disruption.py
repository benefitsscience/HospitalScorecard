import pandas as pd

# Import Data
pd.set_option('display.max_columns', None)

# Hospitals = pd.read_csv('Hospital Systems/Hospital Systems.txt', sep='\t')
# Claimants = pd.read_csv('Claimants/Hospital System Claimants.txt', sep='\t')
Lookup = pd.read_csv('Data Sources/HospitalLookup.csv', dtype={'Zip': 'str'})
ZipMSA = pd.read_csv('Data Sources/ZipMSA.txt', sep='\t', dtype={'zipcode': 'str'})
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
Elig = pd.read_csv('Data Sources/Elig.txt', sep='\t')
AccountInfo = pd.read_csv('Data Sources/AccountInfo.csv')

Concat = pd.concat(
    [Baptist, BSW, CHRISTUS, Community, Cook, Covenant, EastTX, HoustonHCA, MemorialHermann, MethodistDallas,
     MethodistHospital, NorthTXHCA, SanAntonioHCA, Seton, StDavids, StLukes, Tenet, TexasChildrens, THR, UTS])
Concat['billingproviderzip'] = Concat['billingproviderzip'].astype(str).replace('\.0', '', regex=True)
Concat['personid'] = Concat['personid'].astype(str)

Lookup = Lookup[['Tax ID', 'Zip Code']]
Lookup['Zip Code'] = Lookup['Zip Code'].apply(lambda x: x[:5])
Lookup.rename(columns={'Tax ID': 'billingprovidertaxid'}, inplace=True)

ZipMSA = ZipMSA[['zipcode', 'cbsa_name']]
ZipMSA.rename(columns={'zipcode': 'billingproviderzip'}, inplace=True)
ZipMSA['billingproviderzip'] = ZipMSA['billingproviderzip'].apply(lambda x: x[:5])

Elig['Zip'] = Elig['Zip'].astype(str).replace('\.0', '', regex=True)
Elig.rename(columns={'Zip': 'billingproviderzip'}, inplace=True)
Elig = pd.merge(Elig, ZipMSA, on='billingproviderzip', how='left')
Elig = Elig.groupby(['tenantid', 'company', 'cbsa_name']).agg({'MbrCount': 'sum'}).reset_index()
TenantMbrs = Elig.copy()
TenantMbrs = TenantMbrs.groupby('tenantid').agg({'MbrCount': 'sum'}).reset_index()
TenantMbrs.rename(columns={'MbrCount': 'TenantMbrCount'}, inplace=True)

Claimants = Concat.copy()
Claimants = pd.merge(Claimants, ZipMSA, on='billingproviderzip', how='left')
Claimants = Claimants.groupby(['IDN', 'cbsa_name', 'tenantid']).agg({'personid': 'nunique'}).reset_index()
Claimants.rename(columns={'personid': 'UniqueClaimants'}, inplace=True)
Claimants = pd.merge(Claimants, Elig, on=['tenantid', 'cbsa_name'], how='right')
Claimants.fillna(0, inplace=True)
Claimants.sort_values(by=['IDN', 'tenantid'], ascending=[True, True], inplace=True)
Claimants.rename(columns={'MbrCount': 'MSAMbrCount'}, inplace=True)
Claimants = pd.merge(Claimants, TenantMbrs, on='tenantid', how='left')
Claimants['MSAEnrolledPct'] = Claimants['MSAMbrCount'] / Claimants['TenantMbrCount']
Claimants['Util'] = Claimants['UniqueClaimants'] / Claimants['MSAMbrCount']


def utilrisk(row):
    enrolledpct = row['MSAEnrolledPct']
    util = row['Util']
    if enrolledpct >= 0.2 and util >= 0.2:
        return "High"
    elif enrolledpct >= 0.1 and util >= 0.1:
        return "Medium"
    else:
        return "Low"


def attrititionrisk(utilrisk):
    if utilrisk == 'High':
        return 0.3
    elif utilrisk == 'Medium':
        return 0.1
    else:
        return 0


def splitrisk(row):
    enrolledmbrs = row['TenantMbrCount']
    enrolledpct = row['MSAEnrolledPct']
    msambrs = row['MSAMbrCount']
    if enrolledmbrs >= 5000 and msambrs >= 500 and enrolledpct < 0.5:
        return "Split"
    else:
        return "No Split"

def AccountUtil(row):
    util = row['MbrsImpacted']
    if util >= 0.2:
        return "High"
    elif util >= 0.1:
        return "Medium"
    else:
        return "Low"


Claimants['UtilRisk'] = Claimants.apply(utilrisk, axis=1)
Claimants['AttritionRisk'] = Claimants['UtilRisk'].apply(attrititionrisk)
Claimants['SplitRisk'] = Claimants.apply(splitrisk, axis=1)

KeyAccounts = Claimants.copy()
# KeyAccounts = KeyAccounts[KeyAccounts['tenantid'] == 'nonev_385000']
# KeyAccounts = KeyAccounts[KeyAccounts['IDN'] == 'Baylor Scott and White Health']
KeyAccounts = KeyAccounts.groupby(['IDN', 'tenantid', 'company', 'TenantMbrCount']).agg({'UniqueClaimants': 'sum', 'MSAMbrCount': 'sum'}).reset_index()
KeyAccounts['MbrsImpacted'] = KeyAccounts['UniqueClaimants'] / KeyAccounts['TenantMbrCount']
KeyAccounts['UtilRisk'] = KeyAccounts.apply(AccountUtil, axis=1)
AccountInfo = AccountInfo[['acct_nbr', 'Grp_MS', 'product_name', 'fincl_arngmt_cd']]
AccountInfo.rename(columns={'acct_nbr': 'tenantid'}, inplace=True)
AccountInfo['tenantid'] = AccountInfo['tenantid'].astype(str)
KeyAccounts['tenantid'] = KeyAccounts['tenantid'].replace('nonev_', '', regex=True)
KeyAccounts = pd.merge(KeyAccounts, AccountInfo, on='tenantid', how='left')

KeyAccounts.to_csv('Outputs/KeyAccounts.csv', index=False)
print(KeyAccounts)

GroupMetrics = Claimants.groupby(['IDN', 'UtilRisk', 'AttritionRisk']).agg({'tenantid': 'nunique', 'TenantMbrCount': 'sum'}).reset_index()

NoSplit = Claimants[Claimants['SplitRisk'] == 'No Split']
NoSplit = NoSplit[NoSplit['UtilRisk'] != 'Low']
NoSplit['ExpLostMbrs'] = NoSplit['TenantMbrCount'] * NoSplit['AttritionRisk']
NoSplit = NoSplit.groupby(['IDN', 'cbsa_name']).agg({'ExpLostMbrs': 'sum'}).reset_index()

Split = Claimants[Claimants['SplitRisk'] == 'Split']
Split = Split[Split['UtilRisk'] != 'Low']
Split['ExpLostMbrs'] = Split['TenantMbrCount'] * Split['AttritionRisk']
Split = Split.groupby(['IDN', 'cbsa_name']).agg({'ExpLostMbrs': 'sum'}).reset_index()

Disruption = pd.concat([NoSplit, Split])

Disruption = Disruption.groupby(['IDN', 'cbsa_name']).agg({'ExpLostMbrs': 'sum'}).reset_index()
# Disruption['AvgExpLostMbrs'] = Disruption['ExpLostMbrs'].mean()
# Disruption['StDevExpLostMbrs'] = Disruption['ExpLostMbrs'].std()
# Disruption['DisruptionScore'] = ((Disruption['ExpLostMbrs'] - Disruption['AvgExpLostMbrs'])).clip(0) / Disruption['StDevExpLostMbrs']
Disruption['PctOfTotal'] = Disruption['ExpLostMbrs'] / Disruption['ExpLostMbrs'].sum()

Disruption.to_csv('Outputs/Disruption.csv', index=False)
# print(Disruption)

Footprint = Concat.copy()
Footprint = pd.merge(Footprint, ZipMSA, on='billingproviderzip', how='left')
Footprint = Footprint.groupby(['IDN', 'billingprovidername', 'billingproviderid', 'billingprovidertaxid', 'servicecategory_details', 'cbsa_name']).agg(
    {'amtbilled': 'sum', 'amtcovered': 'sum', 'amtallowed': 'sum'}).reset_index()
Footprint.to_csv('Outputs/Footprint.csv', index=False)


# # print(Hospitals)
#
# Claimants.sort_values(by=['IDN', 'tenantid'], ascending=[True, True], inplace=True)
# Elig = pd.read_csv('Data Sources/Elig.txt', sep='\t')
#
# TXElig = Elig[Elig['state'] == 'TX']
# TXElig = TXElig.groupby('tenantid').agg({'MbrCount': 'sum'}).reset_index()
#
# Claimants = pd.merge(Claimants, TXElig, on='tenantid', how='left')
# Claimants['Util'] = Claimants['UniqueClaimants'] / Claimants['MbrCount']
# Claimants['HighUtil'] = Claimants['MultipleClaimants'] / Claimants['MbrCount']
# Claimants.fillna(0, inplace=True)
#
# print(Claimants)
#
# def utilrisk(x):
#     if x >= 0.2:
#         return "High"
#     elif x >= 0.1:
#         return "Medium"
#     else:
#         return "Low"
#
#
# def attrititionrisk(x):
#     if x >= 0.2:
#         return 0.3
#     elif x >= 0.1:
#         return 0.1
#     else:
#         return 0
#
#
# Disruption = Claimants.copy()
# Disruption['UtilRisk'] = Disruption['Util'].apply(utilrisk)
# Disruption['AttritionRisk'] = Disruption['Util'].apply(attrititionrisk)
# Disruption = Disruption.groupby(['IDN', 'UtilRisk', 'AttritionRisk']).agg({'UniqueClaimants': 'sum', 'MbrCount': 'sum'}).reset_index()
# Disruption['MbrsAtRisk'] = Disruption['MbrCount'] * Disruption['AttritionRisk']
# Disruption = Disruption.groupby(['IDN']).agg({'UniqueClaimants': 'sum', 'MbrCount': 'sum', 'MbrsAtRisk': 'sum'}).reset_index()
# Disruption = Disruption[Disruption['IDN'] != 'Kindred Healthcare Operating']
# Disruption['AvgMbrsAtRisk'] = Disruption['MbrsAtRisk'].mean()
# Disruption['StDevMbrsAtRisk'] = Disruption['MbrsAtRisk'].std()
# Disruption['DisruptionScore'] = (Disruption['MbrsAtRisk'] - Disruption['AvgMbrsAtRisk']) / Disruption['StDevMbrsAtRisk']
#
# print(Disruption.head(20))

# Footprint.to_csv('Outputs/Footprint.csv', index=False)
# Elig.to_csv('Outputs/Disruption.csv', index=False)
