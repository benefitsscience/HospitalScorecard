import pandas as pd

pd.set_option('display.max_columns', None)

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
RiskScores = pd.read_csv('Data Sources/RiskScore.csv')

Concat = pd.concat([Baptist, BSW, CHRISTUS, Community, Cook, Covenant, EastTX, HoustonHCA, MemorialHermann, MethodistDallas, MethodistHospital, NorthTXHCA, SanAntonioHCA, Seton, StDavids, StLukes, Tenet, TexasChildrens, THR, UTS])
Concat = Concat[Concat['servicecategory_details'] == 'Inpatient - Hospital']
RiskScores = RiskScores[['personid', 'riskscore', 'min', 'max', 'cost_prediction']]

Concat['personid'] = Concat['personid'].astype(str)
RiskScores['personid'] = RiskScores['personid'].astype(str)
Concat = pd.merge(Concat, RiskScores, on='personid', how='left')
Concat['riskscore'].fillna(1, inplace=True)
Concat = Concat.groupby(['IDN', 'billingprovidername', 'billingprovidertaxid', 'billingproviderid', 'riskscore']).agg({'personid': 'nunique', 'ClaimCount': 'sum', 'amtbilled': 'sum', 'amtcovered': 'sum'}).reset_index()
Concat['ScoreXClaims'] = Concat['riskscore'] * Concat['ClaimCount']

IDN = Concat.copy()
IDN = IDN.groupby(['IDN']).agg({'personid': 'sum', 'ClaimCount': 'sum', 'ScoreXClaims': 'sum'}).reset_index()
IDN['AvgRiskScore'] = IDN['ScoreXClaims'] / IDN['ClaimCount']
IDN['TotalAvg'] = IDN['AvgRiskScore'].mean()
IDN['StdDev'] = IDN['AvgRiskScore'].std()
IDN['ZScore'] = (IDN['AvgRiskScore'] - IDN['TotalAvg']) / IDN['StdDev']

IDN.to_csv('Outputs/IDNRiskScores.csv', index=False)

HospitalLvl = Concat.copy()
HospitalLvl = HospitalLvl.groupby(['IDN', 'billingprovidername', 'billingprovidertaxid']).agg({'personid': 'nunique', 'ClaimCount': 'sum', 'ScoreXClaims': 'sum', 'amtbilled': 'sum', 'amtcovered': 'sum'}).reset_index()
HospitalLvl['AvgRiskScore'] = HospitalLvl['ScoreXClaims'] / HospitalLvl['ClaimCount']
TotalAvg = IDN['AvgRiskScore'].mean()
StdDev = IDN['AvgRiskScore'].std()
HospitalLvl['TotalAvg'] = TotalAvg
HospitalLvl['StdDev'] = StdDev
HospitalLvl['ZScore'] = (HospitalLvl['AvgRiskScore'] - TotalAvg) / StdDev
HospitalLvl = HospitalLvl[HospitalLvl['amtcovered'] > 1000000]
HospitalLvl.to_csv('Outputs/HospitalLvlRiskScores.csv', index=False)

print(HospitalLvl)
