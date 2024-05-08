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
MemorialHermann = """
  SELECT billingprovidername,
         'Memorial Hermann Hospital System' AS IDN,
         billingprovidertaxid,
         billingproviderid,
         billingproviderzip,
         servicecategory_details,
         tenantid,
         personid,
         personstate,
         personzip,
         COUNT(DISTINCT tpaclaimid) AS ClaimCount,
         SUM(amtbilled) AS amtbilled,
         sum(amtcovered) as amtcovered,
         SUM(amtallowed) AS amtallowed
  FROM hive.bcbstx_nonev_prod.claims
  WHERE billingprovidertaxid IN ('200689650','201765863','202184459','202866884','203233617','203233666','203360737','208385914','262809136','263464776','263896170','264276930','270289493','270552583','272800702','273520409','352167462','364773048','364801162','371789833','371796296','384114079','462464605','741152597','741334678','752832459','760241678','760689675','760697645','830759140')
  AND   paiddate >= DATE ('2023-01-01')
  AND   paiddate <= DATE ('2023-12-31')
  GROUP BY billingprovidername,
           billingprovidertaxid,
           billingproviderid,
           billingproviderzip,
           servicecategory_details,
           tenantid,
           personid,
           personstate,
           personzip

    """

UTS = """
  SELECT billingprovidername,
         'University of Texas System' AS IDN,
         billingprovidertaxid,
         billingproviderid,
         billingproviderzip,
         servicecategory_details,
         tenantid,
         personid,
         personstate,
         personzip,
         COUNT(DISTINCT tpaclaimid) AS ClaimCount,
         SUM(amtbilled) AS amtbilled,
         sum(amtcovered) as amtcovered,
         SUM(amtallowed) AS amtallowed
  FROM hive.bcbstx_nonev_prod.claims
  WHERE billingprovidertaxid IN ('746000949','746001118')
  AND   paiddate >= DATE ('2023-01-01')
  AND   paiddate <= DATE ('2023-12-31')
  GROUP BY billingprovidername,
           billingprovidertaxid,
           billingproviderid,
           billingproviderzip,
           servicecategory_details,
           tenantid,
           personid,
           personstate,
           personzip
    """

BSW = """
  SELECT billingprovidername,
         'Baylor Scott and White Health' AS IDN,
         billingprovidertaxid,
         billingproviderid,
         billingproviderzip,
         servicecategory_details,
         tenantid,
         personid,
         personstate,
         personzip,
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
           billingproviderid,
           billingproviderzip,
           servicecategory_details,
           tenantid,
           personid,
           personstate,
           personzip
    """

MethodistHospital = """
  SELECT billingprovidername,
         'Methodist Hospital' AS IDN,
         billingprovidertaxid,
         billingproviderid,
         billingproviderzip,
         servicecategory_details,
         tenantid,
         personid,
         personstate,
         personzip,
         COUNT(DISTINCT tpaclaimid) AS ClaimCount,
         SUM(amtbilled) AS amtbilled,
         sum(amtcovered) as amtcovered,
         SUM(amtallowed) AS amtallowed
  FROM hive.bcbstx_nonev_prod.claims
  WHERE billingprovidertaxid IN ('464389870','464402004','741180155','741287015','760545192')
  AND   paiddate >= DATE ('2023-01-01')
  AND   paiddate <= DATE ('2023-12-31')
  GROUP BY billingprovidername,
           billingprovidertaxid,
           billingproviderid,
           billingproviderzip,
           servicecategory_details,
           tenantid,
           personid,
           personstate,
           personzip
    """

THR = """
  SELECT billingprovidername,
         'Texas Health Resources' AS IDN,
         billingprovidertaxid,
         billingproviderid,
         billingproviderzip,
         servicecategory_details,
         tenantid,
         personid,
         personstate,
         personzip,
         COUNT(DISTINCT tpaclaimid) AS ClaimCount,
         SUM(amtbilled) AS amtbilled,
         sum(amtcovered) as amtcovered,
         SUM(amtallowed) AS amtallowed
  FROM hive.bcbstx_nonev_prod.claims
  WHERE billingprovidertaxid IN ('020555370','113699951','201728912','202848116','203003947','203742012','203991622','260684968','261914835','262310072','262429878','271385885','272248103','274816583','300957868','320571301','364499777','383897811','432008974','451484375','451502252','452694620','470926556','474425996','481281376','731662763','742411643','750972805','751047527','751438726','751648589','751748586','751752253','751925497','751977850','752008026','752055800','752678857','752723958','752770738','752771437','752862780','752890358','753175627','753175630','756001743','756002868','770368346','770628004','800866449','812813227','812833150','813020487','814317635','814977249','821289045','821307876','822296081','831085415','831869297','831954982','841953918','843814490','851225852','861354607','881643733')
  AND   paiddate >= DATE ('2023-01-01')
  AND   paiddate <= DATE ('2023-12-31')
  GROUP BY billingprovidername,
           billingprovidertaxid,
           billingproviderid,
           billingproviderzip,
           servicecategory_details,
           tenantid,
           personid,
           personstate,
           personzip
    """

Seton = """
  SELECT billingprovidername,
         'Seton Family of Hospitals' AS IDN,
         billingprovidertaxid,
         billingproviderid,
         billingproviderzip,
         servicecategory_details,
         tenantid,
         personid,
         personstate,
         personzip,
         COUNT(DISTINCT tpaclaimid) AS ClaimCount,
         SUM(amtbilled) AS amtbilled,
         sum(amtcovered) as amtcovered,
         SUM(amtallowed) AS amtallowed
  FROM hive.bcbstx_nonev_prod.claims
  WHERE billingprovidertaxid IN ('201211544','203904667','205537270','270543475','270929515','272533497','272814378','274579547','412235372','582028767','741109643','742800601','844818321')
  AND   paiddate >= DATE ('2023-01-01')
  AND   paiddate <= DATE ('2023-12-31')
  GROUP BY billingprovidername,
           billingprovidertaxid,
           billingproviderid,
           billingproviderzip,
           servicecategory_details,
           tenantid,
           personid,
           personstate,
           personzip
    """

CHRISTUS = """
  SELECT billingprovidername,
         'CHRISTUS Health' AS IDN,
         billingprovidertaxid,
         billingproviderid,
         billingproviderzip,
         servicecategory_details,
         tenantid,
         personid,
         personstate,
         personzip,
         COUNT(DISTINCT tpaclaimid) AS ClaimCount,
         SUM(amtbilled) AS amtbilled,
         sum(amtcovered) as amtcovered,
         SUM(amtallowed) AS amtallowed
  FROM hive.bcbstx_nonev_prod.claims
  WHERE billingprovidertaxid IN ('200424958','203805709','205657181','384092858','412092141','472897722','741109665','741109836','741191729','742898615','750818167','750974351','751041154','751976930','752727769','752771569','752796815','760591590','760591592','810571409','811708177')
  AND   paiddate >= DATE ('2023-01-01')
  AND   paiddate <= DATE ('2023-12-31')
  GROUP BY billingprovidername,
           billingprovidertaxid,
           billingproviderid,
           billingproviderzip,
           servicecategory_details,
           tenantid,
           personid,
           personstate,
           personzip
    """

TexasChildrens = """
  SELECT billingprovidername,
         'Texas Childrens Hospital' AS IDN,
         billingprovidertaxid,
         billingproviderid,
         billingproviderzip,
         servicecategory_details,
         tenantid,
         personid,
         personstate,
         personzip,
         COUNT(DISTINCT tpaclaimid) AS ClaimCount,
         SUM(amtbilled) AS amtbilled,
         sum(amtcovered) as amtcovered,
         SUM(amtallowed) AS amtallowed
  FROM hive.bcbstx_nonev_prod.claims
  WHERE billingprovidertaxid IN ('472029489','741100555')
  AND   paiddate >= DATE ('2023-01-01')
  AND   paiddate <= DATE ('2023-12-31')
  GROUP BY billingprovidername,
           billingprovidertaxid,
           billingproviderid,
           billingproviderzip,
           servicecategory_details,
           tenantid,
           personid,
           personstate,
           personzip
    """

StDavids = """
  SELECT billingprovidername,
         'St Davids Healthcare - HCA' AS IDN,
         billingprovidertaxid,
         billingproviderid,
         billingproviderzip,
         servicecategory_details,
         tenantid,
         personid,
         personstate,
         personzip,
         COUNT(DISTINCT tpaclaimid) AS ClaimCount,
         SUM(amtbilled) AS amtbilled,
         sum(amtcovered) as amtcovered,
         SUM(amtallowed) AS amtallowed
  FROM hive.bcbstx_nonev_prod.claims
  WHERE billingprovidertaxid IN ('200648730','300924492','301073754','384007312','611760247','621641024','621775267','742781812','752467365','800776412','800899088','844484446')
  AND   paiddate >= DATE ('2023-01-01')
  AND   paiddate <= DATE ('2023-12-31')
  GROUP BY billingprovidername,
           billingprovidertaxid,
           billingproviderid,
           billingproviderzip,
           servicecategory_details,
           tenantid,
           personid,
           personstate,
           personzip
    """

StLukes = """
  SELECT billingprovidername,
         'CHI St Lukes' AS IDN,
         billingprovidertaxid,
         billingproviderid,
         billingproviderzip,
         servicecategory_details,
         tenantid,
         personid,
         personstate,
         personzip,
         COUNT(DISTINCT tpaclaimid) AS ClaimCount,
         SUM(amtbilled) AS amtbilled,
         sum(amtcovered) as amtcovered,
         SUM(amtallowed) AS amtallowed
  FROM hive.bcbstx_nonev_prod.claims
  WHERE billingprovidertaxid IN ('261947374','263734606','273280598','300427437','462795726','710959365','741161938','760536234','810743412','812482854')
  AND   paiddate >= DATE ('2023-01-01')
  AND   paiddate <= DATE ('2023-12-31')
  GROUP BY billingprovidername,
           billingprovidertaxid,
           billingproviderid,
           billingproviderzip,
           servicecategory_details,
           tenantid,
           personid,
           personstate,
           personzip
    """

Tenet = """
  SELECT billingprovidername,
         'Tenet Healthcare' AS IDN,
         billingprovidertaxid,
         billingproviderid,
         billingproviderzip,
         servicecategory_details,
         tenantid,
         personid,
         personstate,
         personzip,
         COUNT(DISTINCT tpaclaimid) AS ClaimCount,
         SUM(amtbilled) AS amtbilled,
         sum(amtcovered) as amtcovered,
         SUM(amtallowed) AS amtallowed
  FROM hive.bcbstx_nonev_prod.claims
  WHERE billingprovidertaxid IN ('020594510','200285362','201482218','202511329','202829728','203793927','204911604','331058382','331082073','352240144','352599588','372021101','421731685','452497248','452662980','452663071','453047741','462942963','550912886','611699459','742797719','742901471','743013713','752521339','752793512','752893120','753049514','760246499','760354630','760452319','760714523','813935393','821680961','954537720')
  AND   paiddate >= DATE ('2023-01-01')
  AND   paiddate <= DATE ('2023-12-31')
  GROUP BY billingprovidername,
           billingprovidertaxid,
           billingproviderid,
           billingproviderzip,
           servicecategory_details,
           tenantid,
           personid,
           personstate,
           personzip
    """

Cook = """
  SELECT billingprovidername,
         'Cook Childrens Medical Center & Home Health' AS IDN,
         billingprovidertaxid,
         billingproviderid,
         billingproviderzip,
         servicecategory_details,
         tenantid,
         personid,
         personstate,
         personzip,
         COUNT(DISTINCT tpaclaimid) AS ClaimCount,
         SUM(amtbilled) AS amtbilled,
         sum(amtcovered) as amtcovered,
         SUM(amtallowed) AS amtallowed
  FROM hive.bcbstx_nonev_prod.claims
  WHERE billingprovidertaxid IN ('205227064','470871715','752051646','752896983','852354189')
  AND   paiddate >= DATE ('2023-01-01')
  AND   paiddate <= DATE ('2023-12-31')
  GROUP BY billingprovidername,
           billingprovidertaxid,
           billingproviderid,
           billingproviderzip,
           servicecategory_details,
           tenantid,
           personid,
           personstate,
           personzip
    """

Covenant = """
  SELECT billingprovidername,
         'Covenant Health System' AS IDN,
         billingprovidertaxid,
         billingproviderid,
         billingproviderzip,
         servicecategory_details,
         tenantid,
         personid,
         personstate,
         personzip,
         COUNT(DISTINCT tpaclaimid) AS ClaimCount,
         SUM(amtbilled) AS amtbilled,
         sum(amtcovered) as amtcovered,
         SUM(amtallowed) AS amtallowed
  FROM hive.bcbstx_nonev_prod.claims
  WHERE billingprovidertaxid IN ('750818174','752177401','752246348','752426010','752428911','752765566','822913146')
  AND   paiddate >= DATE ('2023-01-01')
  AND   paiddate <= DATE ('2023-12-31')
  GROUP BY billingprovidername,
           billingprovidertaxid,
           billingproviderid,
           billingproviderzip,
           servicecategory_details,
           tenantid,
           personid,
           personstate,
           personzip
    """

BaptistStAnthony = """
  SELECT billingprovidername,
         'Baptist St. Anthonys Health System' AS IDN,
         billingprovidertaxid,
         billingproviderid,
         billingproviderzip,
         servicecategory_details,
         tenantid,
         personid,
         personstate,
         personzip,
         COUNT(DISTINCT tpaclaimid) AS ClaimCount,
         SUM(amtbilled) AS amtbilled,
         sum(amtcovered) as amtcovered,
         SUM(amtallowed) AS amtallowed
  FROM hive.bcbstx_nonev_prod.claims
  WHERE billingprovidertaxid IN ('200929321','201425596','202395634','203590535','204861955','205532412','237297190','260004207','260031017','261095447','300754305','320144950','320288792','451655734','452230827','464512337','474219674','731466355','731620274','750703337','751188675','751211903','751228349','751302152','751424965','751566271','751576230','751621519','751851652','752020021','752292467','752399247','752552480','752670892','752685656','752715646','752729136','752794967','752814617','752864951','752894260','752900294','756001297','756002888','756003939','770604139','900124414')
  AND   paiddate >= DATE ('2023-01-01')
  AND   paiddate <= DATE ('2023-12-31')
  GROUP BY billingprovidername,
           billingprovidertaxid,
           billingproviderid,
           billingproviderzip,
           servicecategory_details,
           tenantid,
           personid,
           personstate,
           personzip
    """

EastTexas = """
  SELECT billingprovidername,
         'East Texas Health System' AS IDN,
         billingprovidertaxid,
         billingproviderid,
         billingproviderzip,
         servicecategory_details,
         tenantid,
         personid,
         personstate,
         personzip,
         COUNT(DISTINCT tpaclaimid) AS ClaimCount,
         SUM(amtbilled) AS amtbilled,
         sum(amtcovered) as amtcovered,
         SUM(amtallowed) AS amtallowed
  FROM hive.bcbstx_nonev_prod.claims
  WHERE billingprovidertaxid IN ('203084947','750823252','756001354','811424190','823817196','823878395','823885642','823896865','823913174','823934511','823953636','823970937','824005981','824019349','824037220')
  AND   paiddate >= DATE ('2023-01-01')
  AND   paiddate <= DATE ('2023-12-31')
  GROUP BY billingprovidername,
           billingprovidertaxid,
           billingproviderid,
           billingproviderzip,
           servicecategory_details,
           tenantid,
           personid,
           personstate,
           personzip
    """

MethodistDallas = """
  SELECT billingprovidername,
         'Methodist Hospitals of Dallas' AS IDN,
         billingprovidertaxid,
         billingproviderid,
         billingproviderzip,
         servicecategory_details,
         tenantid,
         personid,
         personstate,
         personzip,
         COUNT(DISTINCT tpaclaimid) AS ClaimCount,
         SUM(amtbilled) AS amtbilled,
         sum(amtcovered) as amtcovered,
         SUM(amtallowed) AS amtallowed
  FROM hive.bcbstx_nonev_prod.claims
  WHERE billingprovidertaxid IN ('205000978','208847736','260869371','263195791','352583971','465265469','750800661','752896138','843935720')
  AND   paiddate >= DATE ('2023-01-01')
  AND   paiddate <= DATE ('2023-12-31')
  GROUP BY billingprovidername,
           billingprovidertaxid,
           billingproviderid,
           billingproviderzip,
           servicecategory_details,
           tenantid,
           personid,
           personstate,
           personzip
    """

NorthTexasHCA = """
  SELECT billingprovidername,
         'North Texas Division - HCA' AS IDN,
         billingprovidertaxid,
         billingproviderid,
         billingproviderzip,
         servicecategory_details,
         tenantid,
         personid,
         personstate,
         personzip,
         COUNT(DISTINCT tpaclaimid) AS ClaimCount,
         SUM(amtbilled) AS amtbilled,
         sum(amtcovered) as amtcovered,
         SUM(amtallowed) AS amtallowed
  FROM hive.bcbstx_nonev_prod.claims
  WHERE billingprovidertaxid IN ('141948102','200270605','201043104','201477775','464027347','621489404','621560420','621650582','621682198','621682201','621682202','621682203','621682205','621682207','621682210','621682213','621771716','621797829','710985084','752290624','752490627','800002697','822073410','822432064','822480225','842824507','881391141','923326615','932081386','932106228')
  AND   paiddate >= DATE ('2023-01-01')
  AND   paiddate <= DATE ('2023-12-31')
  GROUP BY billingprovidername,
           billingprovidertaxid,
           billingproviderid,
           billingproviderzip,
           servicecategory_details,
           tenantid,
           personid,
           personstate,
           personzip
    """

SanAntonioHCA = """
  SELECT billingprovidername,
         'Methodist Healthcare System of San Antonio - HCA' AS IDN,
         billingprovidertaxid,
         billingproviderid,
         billingproviderzip,
         servicecategory_details,
         tenantid,
         personid,
         personstate,
         personzip,
         COUNT(DISTINCT tpaclaimid) AS ClaimCount,
         SUM(amtbilled) AS amtbilled,
         sum(amtcovered) as amtcovered,
         SUM(amtallowed) AS amtallowed
  FROM hive.bcbstx_nonev_prod.claims
  WHERE billingprovidertaxid IN ('300796793','731713543','742730328','742998447','823532569','842492174','873925488')
  AND   paiddate >= DATE ('2023-01-01')
  AND   paiddate <= DATE ('2023-12-31')
  GROUP BY billingprovidername,
           billingprovidertaxid,
           billingproviderid,
           billingproviderzip,
           servicecategory_details,
           tenantid,
           personid,
           personstate,
           personzip
    """

HoustonHCA = """
  SELECT billingprovidername,
         'HCA Houston Healthcare' AS IDN,
         billingprovidertaxid,
         billingproviderid,
         billingproviderzip,
         servicecategory_details,
         tenantid,
         personid,
         personstate,
         personzip,
         COUNT(DISTINCT tpaclaimid) AS ClaimCount,
         SUM(amtbilled) AS amtbilled,
         sum(amtcovered) as amtcovered,
         SUM(amtallowed) AS amtallowed
  FROM hive.bcbstx_nonev_prod.claims
  WHERE billingprovidertaxid IN ('050631189','261512163','320383244','432016059','621600411','621619857','621742196','621771718','621801359','621801360','621801361','621801363','621810381','752387418','752399524','760418502','800444943','814537533','821349955','821412000','821635538','842342776','851383722','862290088','900768532')
  AND   paiddate >= DATE ('2023-01-01')
  AND   paiddate <= DATE ('2023-12-31')
  GROUP BY billingprovidername,
           billingprovidertaxid,
           billingproviderid,
           billingproviderzip,
           servicecategory_details,
           tenantid,
           personid,
           personstate,
           personzip
    """

Community = """
  SELECT billingprovidername,
         'Community Health Systems' AS IDN,
         billingprovidertaxid,
         billingproviderid,
         billingproviderzip,
         servicecategory_details,
         tenantid,
         personid,
         personstate,
         personzip,
         COUNT(DISTINCT tpaclaimid) AS ClaimCount,
         SUM(amtbilled) AS amtbilled,
         sum(amtcovered) as amtcovered,
         SUM(amtallowed) AS amtallowed
  FROM hive.bcbstx_nonev_prod.claims
  WHERE billingprovidertaxid IN ('200175530','621754940','621762420','621762428','621762559','752682017')
  AND   paiddate >= DATE ('2023-01-01')
  AND   paiddate <= DATE ('2023-12-31')
  GROUP BY billingprovidername,
           billingprovidertaxid,
           billingproviderid,
           billingproviderzip,
           servicecategory_details,
           tenantid,
           personid,
           personstate,
           personzip
    """

# Fetch the results.
MemorialHermann = cursor.execute(MemorialHermann).fetchall()
MemorialHermann = pd.DataFrame(MemorialHermann)
MemorialHermann.columns = ['billingprovidername', 'IDN', 'billingprovidertaxid', 'billingproviderid', 'billingproviderzip', 'servicecategory_details', 'tenantid', 'personid', 'personstate', 'personzip', 'ClaimCount', 'amtbilled', 'amtcovered', 'amtallowed']
MemorialHermann.to_csv('Hospital Systems/Memorial Hermann.csv', index=False)

UTS = cursor.execute(UTS).fetchall()
UTS = pd.DataFrame(UTS)
UTS.columns = ['billingprovidername', 'IDN', 'billingprovidertaxid', 'billingproviderid', 'billingproviderzip', 'servicecategory_details', 'tenantid', 'personid', 'personstate', 'personzip', 'ClaimCount', 'amtbilled', 'amtcovered', 'amtallowed']
UTS.to_csv('Hospital Systems/UTS.csv', index=False)

BSW = cursor.execute(BSW).fetchall()
BSW = pd.DataFrame(BSW)
BSW.columns = ['billingprovidername', 'IDN', 'billingprovidertaxid', 'billingproviderid', 'billingproviderzip', 'servicecategory_details', 'tenantid', 'personid', 'personstate', 'personzip', 'ClaimCount', 'amtbilled', 'amtcovered', 'amtallowed']
BSW.to_csv('Hospital Systems/BSW.csv', index=False)

MethodistHospital = cursor.execute(MethodistHospital).fetchall()
MethodistHospital = pd.DataFrame(MethodistHospital)
MethodistHospital.columns = ['billingprovidername', 'IDN', 'billingprovidertaxid', 'billingproviderid', 'billingproviderzip', 'servicecategory_details', 'tenantid', 'personid', 'personstate', 'personzip', 'ClaimCount', 'amtbilled','amtcovered', 'amtallowed']
MethodistHospital.to_csv('Hospital Systems/Methodist Hospital.csv', index=False)

THR = cursor.execute(THR).fetchall()
THR = pd.DataFrame(THR)
THR.columns = ['billingprovidername', 'IDN', 'billingprovidertaxid', 'billingproviderid', 'billingproviderzip', 'servicecategory_details', 'tenantid', 'personid', 'personstate', 'personzip', 'ClaimCount', 'amtbilled', 'amtcovered', 'amtallowed']
THR.to_csv('Hospital Systems/THR.csv', index=False)

Seton = cursor.execute(Seton).fetchall()
Seton = pd.DataFrame(Seton)
Seton.columns = ['billingprovidername', 'IDN', 'billingprovidertaxid', 'billingproviderid', 'billingproviderzip', 'servicecategory_details', 'tenantid', 'personid', 'personstate', 'personzip', 'ClaimCount', 'amtbilled', 'amtcovered', 'amtallowed']
Seton.to_csv('Hospital Systems/Seton.csv', index=False)

CHRISTUS = cursor.execute(CHRISTUS).fetchall()
CHRISTUS = pd.DataFrame(CHRISTUS)
CHRISTUS.columns = ['billingprovidername', 'IDN', 'billingprovidertaxid', 'billingproviderid', 'billingproviderzip', 'servicecategory_details', 'tenantid', 'personid', 'personstate', 'personzip', 'ClaimCount', 'amtbilled', 'amtcovered', 'amtallowed']
CHRISTUS.to_csv('Hospital Systems/CHRISTUS.csv', index=False)

TexasChildrens = cursor.execute(TexasChildrens).fetchall()
TexasChildrens = pd.DataFrame(TexasChildrens)
TexasChildrens.columns = ['billingprovidername', 'IDN', 'billingprovidertaxid', 'billingproviderid', 'billingproviderzip', 'servicecategory_details', 'tenantid', 'personid', 'personstate', 'personzip', 'ClaimCount', 'amtbilled', 'amtcovered', 'amtallowed']
TexasChildrens.to_csv('Hospital Systems/Texas Childrens.csv', index=False)

StDavids = cursor.execute(StDavids).fetchall()
StDavids = pd.DataFrame(StDavids)
StDavids.columns = ['billingprovidername', 'IDN', 'billingprovidertaxid', 'billingproviderid', 'billingproviderzip', 'servicecategory_details', 'tenantid', 'personid', 'personstate', 'personzip', 'ClaimCount', 'amtbilled', 'amtcovered', 'amtallowed']
StDavids.to_csv('Hospital Systems/St Davids.csv', index=False)

StLukes = cursor.execute(StLukes).fetchall()
StLukes = pd.DataFrame(StLukes)
StLukes.columns = ['billingprovidername', 'IDN', 'billingprovidertaxid', 'billingproviderid', 'billingproviderzip', 'servicecategory_details', 'tenantid', 'personid', 'personstate', 'personzip', 'ClaimCount', 'amtbilled', 'amtcovered', 'amtallowed']
StLukes.to_csv('Hospital Systems/St Lukes.csv', index=False)

Tenet = cursor.execute(Tenet).fetchall()
Tenet = pd.DataFrame(Tenet)
Tenet.columns = ['billingprovidername', 'IDN', 'billingprovidertaxid', 'billingproviderid', 'billingproviderzip', 'servicecategory_details', 'tenantid', 'personid', 'personstate', 'personzip', 'ClaimCount', 'amtbilled', 'amtcovered', 'amtallowed']
Tenet.to_csv('Hospital Systems/Tenet.csv', index=False)

Cook = cursor.execute(Cook).fetchall()
Cook = pd.DataFrame(Cook)
Cook.columns = ['billingprovidername', 'IDN', 'billingprovidertaxid', 'billingproviderid', 'billingproviderzip', 'servicecategory_details', 'tenantid', 'personid', 'personstate', 'personzip', 'ClaimCount', 'amtbilled', 'amtcovered', 'amtallowed']
Cook.to_csv('Hospital Systems/Cook.csv', index=False)

Covenant = cursor.execute(Covenant).fetchall()
Covenant = pd.DataFrame(Covenant)
Covenant.columns = ['billingprovidername', 'IDN', 'billingprovidertaxid', 'billingproviderid', 'billingproviderzip', 'servicecategory_details', 'tenantid', 'personid', 'personstate', 'personzip', 'ClaimCount', 'amtbilled', 'amtcovered', 'amtallowed']
Covenant.to_csv('Hospital Systems/Covenant.csv', index=False)

BaptistStAnthony = cursor.execute(BaptistStAnthony).fetchall()
BaptistStAnthony = pd.DataFrame(BaptistStAnthony)
BaptistStAnthony.columns = ['billingprovidername', 'IDN', 'billingprovidertaxid', 'billingproviderid', 'billingproviderzip', 'servicecategory_details', 'tenantid', 'personid', 'personstate', 'personzip', 'ClaimCount', 'amtbilled', 'amtcovered', 'amtallowed']
BaptistStAnthony.to_csv('Hospital Systems/Baptist St Anthony.csv', index=False)

EastTexas = cursor.execute(EastTexas).fetchall()
EastTexas = pd.DataFrame(EastTexas)
EastTexas.columns = ['billingprovidername', 'IDN', 'billingprovidertaxid', 'billingproviderid', 'billingproviderzip', 'servicecategory_details', 'tenantid', 'personid', 'personstate', 'personzip', 'ClaimCount', 'amtbilled', 'amtcovered', 'amtallowed']
EastTexas.to_csv('Hospital Systems/East Texas.csv', index=False)

MethodistDallas = cursor.execute(MethodistDallas).fetchall()
MethodistDallas = pd.DataFrame(MethodistDallas)
MethodistDallas.columns = ['billingprovidername', 'IDN', 'billingprovidertaxid', 'billingproviderid', 'billingproviderzip', 'servicecategory_details', 'tenantid', 'personid', 'personstate', 'personzip', 'ClaimCount', 'amtbilled', 'amtcovered', 'amtallowed']
MethodistDallas.to_csv('Hospital Systems/Methodist Dallas.csv', index=False)

NorthTexasHCA = cursor.execute(NorthTexasHCA).fetchall()
NorthTexasHCA = pd.DataFrame(NorthTexasHCA)
NorthTexasHCA.columns = ['billingprovidername', 'IDN', 'billingprovidertaxid', 'billingproviderid', 'billingproviderzip', 'servicecategory_details', 'tenantid', 'personid', 'personstate', 'personzip', 'ClaimCount', 'amtbilled', 'amtcovered', 'amtallowed']
NorthTexasHCA.to_csv('Hospital Systems/North Texas HCA.csv', index=False)

SanAntonioHCA = cursor.execute(SanAntonioHCA).fetchall()
SanAntonioHCA = pd.DataFrame(SanAntonioHCA)
SanAntonioHCA.columns = ['billingprovidername', 'IDN', 'billingprovidertaxid', 'billingproviderid', 'billingproviderzip', 'servicecategory_details', 'tenantid', 'personid', 'personstate', 'personzip', 'ClaimCount', 'amtbilled', 'amtcovered', 'amtallowed']
SanAntonioHCA.to_csv('Hospital Systems/San Antonio HCA.csv', index=False)

HoustonHCA = cursor.execute(HoustonHCA).fetchall()
HoustonHCA = pd.DataFrame(HoustonHCA)
HoustonHCA.columns = ['billingprovidername', 'IDN', 'billingprovidertaxid', 'billingproviderid', 'billingproviderzip', 'servicecategory_details', 'tenantid', 'personid', 'personstate', 'personzip', 'ClaimCount', 'amtbilled', 'amtcovered', 'amtallowed']
HoustonHCA.to_csv('Hospital Systems/Houston HCA.csv', index=False)

Community = cursor.execute(Community).fetchall()
Community = pd.DataFrame(Community)
Community.columns = ['billingprovidername', 'IDN', 'billingprovidertaxid', 'billingproviderid', 'billingproviderzip', 'servicecategory_details', 'tenantid', 'personid', 'personstate', 'personzip', 'ClaimCount', 'amtbilled', 'amtcovered', 'amtallowed']
Community.to_csv('Hospital Systems/Community.csv', index=False)



