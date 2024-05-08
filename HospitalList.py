import pandas as pd

# Import Data
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

Concat = pd.concat([Baptist, BSW, CHRISTUS, Community, Cook, Covenant, EastTX, HoustonHCA, MemorialHermann, MethodistDallas, MethodistHospital, NorthTXHCA, SanAntonioHCA, Seton, StDavids, StLukes, Tenet, TexasChildrens, THR, UTS])

List = Concat.groupby(['IDN', 'billingprovidertaxid']).agg({'amtbilled': 'sum'}).reset_index()
List = pd.merge(List, Concat, on=['IDN', 'billingprovidertaxid'], how='left')
List.sort_values(by=['IDN', 'amtbilled_x'], ascending=[True, False], inplace=True)
List.drop_duplicates(subset=['IDN', 'billingprovidertaxid'], inplace=True)
List.to_csv('Outputs/ProviderList.csv', index=False)
print(List)