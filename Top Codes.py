import pandas as pd
import trino

pd.set_option('display.max_columns', None)

# Create a Trino client.
client = trino.dbapi.connect(
    host='presto.bstis.com',
    port=8080,
    user='hadoop',
    catalog='hive',
    schema='bcbstx_nonev_prod',
)

# Get a cursor object.
cursor = client.cursor()

# Execute multiple queries.
BSW = """
SELECT billingprovidername,
       'Baylor Scott and White Health' AS IDN,
       billingprovidertaxid,
       icd9_1_chapter,
       procedurecode,
       procedurecategory,
       proceduretype,
       procedurename,
       servicecategory_details,
       facility_indicator,
       COUNT(DISTINCT tpaclaimid) AS ClaimCount,
       SUM(amtbilled) AS amtbilled,
       sum(amtcovered) as amtcovered,
       SUM(amtallowed) AS amtallowed
FROM hive.bcbstx_nonev_prod.claims
WHERE billingprovidertaxid IN ('030380493','201508140','201937212','202850920','203749695','205506447','208077072','208303422','260194016','260308454','261578178','262978009','263603862','263896477','271835675','273026151','273578014','273635726','274434451','274586141','352199232','364755936','412101361','462873916','462908086','464007700','465530768','470985876','474798129','481260190','510570864','542086863','562297308','562357079','562399993','562487696','611762781','731697736','741161944','741166904','741595711','742519752','742958277','751008430','751037591','751777119','751837454','751844139','752536818','752586857','752592508','752658178','752708579','752764855','752813815','752829613','752834135','752854711','752865177','752900902','752951355','753242749','812480586','812931756','813040663','813127185','820551704','824052186','831498677','861402910','920758698','931494996')
AND   paiddate >= DATE ('2023-01-01')
AND   paiddate <= DATE ('2023-12-31')

GROUP BY billingprovidername,
         billingprovidertaxid,
         icd9_1_chapter,
         procedurecode,
         procedurecategory,
         proceduretype,
         procedurename,
         servicecategory_details,
         facility_indicator
ORDER BY amtbilled DESC 

    """

BSW = cursor.execute(BSW).fetchall()
BSW = pd.DataFrame(BSW)
BSW.columns = ['billingprovidername', 'IDN', 'billingprovidertaxid', 'icd9_1_chapter', 'procedurecode', 'procedurecategory', 'proceduretype', 'procedurename', 'servicecategory_details', 'facility_indicator', 'ClaimCount', 'amtbilled', 'amtcovered', 'amtallowed']
BSW.to_csv('TopCodes/BSW_codes.csv', index=False)
