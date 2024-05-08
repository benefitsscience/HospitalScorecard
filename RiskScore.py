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
RiskScore = """
  SELECT *
  FROM hive.bcbstx_nonev_prod.bda_riskscore
  WHERE year = 2023
  and month = 3
  order by tenantid, personid

    """

RiskScore = cursor.execute(RiskScore).fetchall()
RiskScore = pd.DataFrame(RiskScore)
RiskScore.columns = ['employer', 'employeeid', 'personid', 'year', 'month', 'riskscore', 'min', 'max', 'cost_prediction', 'recent_riskscore', 'tenantid', 'date']
RiskScore.to_csv('Data Sources/RiskScore.csv', index=False)
