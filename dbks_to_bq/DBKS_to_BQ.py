# Especifica la ubicación del archivo JSON de credenciales en DBFS
dbfs_path = "path"

# Configura las credenciales del servicio en el entorno
spark.conf.set("google.cloud.auth.service.account.json.keyfile", dbfs_path)

!pip uninstall google-cloud 
!pip uninstall google-cloud-bigquery
!pip install --upgrade google-cloud-bigquery
!pip install --upgrade google-cloud
!pip show google-cloud
!pip show google-cloud-bigquery

#Importación de liberías
from google.cloud import bigquery
import os
import pandas as pd

credentials_path = "credential path"

# Establece la variable de entorno GOOGLE_APPLICATION_CREDENTIALS con la ruta del archivo
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

print(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

# Crea un cliente de BigQuery

client = bigquery.Client()

# Especifica la consulta para seleccionar datos de una tabla en Databricks
query = """
Select 
CONCAT(e.event_code,LEFT(CAST(s.sale_date AS STRING),10)) AS ID_EVENT
, CAST(TRUNC(s.sale_date,'MM') AS timestamp) AS FECHA
, CAST(s.sale_date AS DATE) AS SDATE
, CASE WHEN pm.Pmt_Method_Name = 'MURPHY' THEN 'MC'
WHEN table.pm = 'CASHN' THEN 'VISA'
---...
ELSE table:pm END AS METODO   -- Need to find out why MOP names are different.
, e.event_code AS CLAVE
, cast(e.event_dt as timestamp) as EPDATE -- Need to convert to DBx syntax
, CONCAT(e.event_name,"/",(CASE WHEN e.zone_cd=1 THEN 'MEX' WHEN e.zone_cd=2 THEN 'MTY' WHEN e.zone_cd=3 THEN 'GDL' ELSE 'OTR' END),"/",v.venue_name,"/",RIGHT(CAST(DATEADD(hour, 6, e.event_date_time) AS VARCHAR(114)),8)) AS EVENTO_CONCEPTO
, case when ot.opr_type_Cd = 'I' then 'PHONE' 
whentable_op = 'B' then 'PRIMARY'
else upper(table_op) end as CANAL
, case when tabl_opd = 'I' then 'INTERNET'
when ot.opr_type_Cd in ('P', 'B', 'O') then 'NONE' else ot.opr_type_nm end as SUBCANAL
, CAST(e.zone_cd AS STRING) as ZONA
, s.opr_cd as CLAVE_ORIGE
, o.Opr_name AS ORIGEN
, e.zone_cd AS OPZONE
, 1 AS LOCNUM
, sum(Seat_Cnt) as NSEATS
, 0 AS SSEATS
, 0 AS RSEATS
, sum(s.tkt_cnt) as NTQTY
, 0 AS STQTY
, 0 AS RTQTY
, sum(FaceVal_Amt) as NDOLLARS
, CAST(0 AS FLOAT) AS SDOLLARS
, CAST(0 AS FLOAT) AS RDOLLARS
, (sum(ServCharge_Amt)/1.16) as SCHGDOL
, sum(SvcRetain_Amt) as SCHGRET
, sum(OptDollars_Amt) as OPTDOL
, sum( FaceVal_Amt + FacCharge_Amt+ ServCharge_Amt + OptDollars_Amt + ZoneCharge_Amt) AS GRANDTOT
, case when (FaceVal_Amt/s.tkt_cnt) >=-2 and (FaceVal_Amt/s.tkt_cnt) <=2 THEN 'Cortesía' ELSE 'Venta' END AS CORTESIA --Este probablemente nos marque error
, NULL AS FESTIVALES
, CASE WHEN s.### =  'MXC' THEN 1 ELSE 2 END AS IDHOST
, tabla_h as HEX

from table_s s			-- The sales_Summary table shows sales by opcode, by day, by event. 
join table_e e on tabl2_e = e.event_id and s.event_id_src_sys_cd = e.event_id_src_sys_cd
join tabla_d v on e.ven_id = v.venue_id  
join table_o o on s.host_sys_cd = o.host_sys_cd and s.opr_cd = o.opr_cd   -- The Opr table shows information about Operator Codes. 
join table_op ot on s.OVRRD_OPR_TYPE_CD = ot.opr_type_cd
join table_ty pm on s.host_sys_cd = pm.host_sys_cd and s.Pmt_Method_Cd = pm.pmt_method_ID and pm.cur_ind = 'Y'   -- pmt_method_type is used to get the MOP name. 
where s.hod in ( 'MXC','MX2')
and s.sale_date = CAST(DATEADD(day,-1,DATEADD(hour,-5,getdate())) AS DATE)
group by ID_EVENT, FECHA, SDATE, METODO, CLAVE, EPDATE, EVENTO_CONCEPTO, CANAL, SUBCANAL, ZONA, CLAVE_ORIGEN, ORIGEN, OPZONE, LOCNUM, CORTESIA, IDHOST, HEX
order by ID_EVENT, FECHA
"""

# Realiza una consulta y guarda los resultados en un DataFrame
df = spark.sql(query)

display(df)

project_id = "project"

table_id = "table"

client = bigquery.Client()

esquema = [{'name': 'ID_EVENT', 'type': 'STRING','mode': 'NULLABLE'},
{'name': 'FECHA', 'type': 'TIMESTAMP','mode': 'NULLABLE'},
{'name': 'SDATE', 'type': 'DATE','mode': 'NULLABLE'},
{'name': 'METODO', 'type': 'STRING','mode': 'NULLABLE'},
{'name': 'CLAVE', 'type': 'STRING','mode': 'NULLABLE'},
{'name': 'EPDATE', 'type': 'TIMESTAMP','mode': 'NULLABLE'},
{'name': 'EVENTO_CONCEPTO', 'type': 'STRING','mode': 'NULLABLE'},
{'name': 'CANAL', 'type': 'STRING','mode': 'NULLABLE'},
{'name': 'SUBCANAL', 'type': 'STRING','mode': 'NULLABLE'},
{'name': 'ZONA', 'type': 'STRING','mode': 'NULLABLE'},
{'name': 'CLAVE_ORIGEN', 'type': 'STRING','mode': 'NULLABLE'},
{'name': 'ORIGEN', 'type': 'STRING','mode': 'NULLABLE'},
{'name': 'OPZONE', 'type': 'INTEGER','mode': 'NULLABLE'},
{'name': 'LOCNUM', 'type': 'INTEGER','mode': 'NULLABLE'},
{'name': 'NSEATS', 'type': 'INTEGER','mode': 'NULLABLE'},
{'name': 'SSEATS', 'type': 'INTEGER','mode': 'NULLABLE'},
{'name': 'RSEATS', 'type': 'INTEGER','mode': 'NULLABLE'},
{'name': 'NTQTY', 'type': 'INTEGER','mode': 'NULLABLE'},
{'name': 'STQTY', 'type': 'INTEGER','mode': 'NULLABLE'},
{'name': 'RTQTY', 'type': 'INTEGER','mode': 'NULLABLE'},
{'name': 'NDOLLARS', 'type': 'FLOAT','mode': 'NULLABLE'},
{'name': 'SDOLLARS', 'type': 'FLOAT','mode': 'NULLABLE'},
{'name': 'RDOLLARS', 'type': 'FLOAT','mode': 'NULLABLE'},
{'name': 'SCHGDOL', 'type': 'FLOAT','mode': 'NULLABLE'},
{'name': 'SCHGRET', 'type': 'FLOAT','mode': 'NULLABLE'},
{'name': 'OPTDOL', 'type': 'FLOAT','mode': 'NULLABLE'},
{'name': 'GRANDTOT', 'type': 'FLOAT','mode': 'NULLABLE'},
{'name': 'CORTESIA', 'type': 'STRING','mode': 'NULLABLE'},
{'name': 'FESTIVALES', 'type': 'STRING','mode': 'NULLABLE'},
{'name': 'IDHOST', 'type': 'INTEGER','mode': 'NULLABLE'},
{'name': 'HEX', 'type': 'STRING','mode': 'NULLABLE'}
]

job_config = bigquery.LoadJobConfig(schema=esquema,write_disposition="WRITE_APPEND")

#Convierte el Dataframe de Spark a Pandas
df2 = df.toPandas()

df2['ID_EVENT'] = df2['ID_EVENT'].astype(str)
df2['METODO'] = df2['METODO'].astype(str)
df2['FECHA'] = pd.to_datetime(df2['FECHA'])
df2['SDATE'] = pd.to_datetime(df2['SDATE'])
df2['EPDATE'] = pd.to_datetime(df2['EPDATE'])
df2['CLAVE'] = df2['CLAVE'].astype(str)
df2['EVENTO_CONCEPTO'] = df2['EVENTO_CONCEPTO'].astype(str)
df2['CANAL'] = df2['CANAL'].astype(str)
df2['SUBCANAL'] = df2['SUBCANAL'].astype(str)
df2['ZONA'] = df2['ZONA'].astype(str)
df2['CLAVE_ORIGEN'] = df2['CLAVE_ORIGEN'].astype(str)
df2['ORIGEN'] = df2['ORIGEN'].astype(str)
df2['SDATE'] = pd.to_datetime(df2['SDATE'], format = '%y-%m-%d',errors="coerce")
df2['OPZONE'] = pd.to_numeric(df2['OPZONE'], errors="coerce")
df2['OPZONE'] = df2['OPZONE'].fillna(0).astype(int)
df2['LOCNUM'] = pd.to_numeric(df2['LOCNUM'], errors="coerce")
df2['LOCNUM'] = df2['LOCNUM'].fillna(0).astype(int)
df2['NSEATS'] = pd.to_numeric(df2['NSEATS'], errors="coerce")
df2['NSEATS'] = df2['NSEATS'].fillna(0).astype(int)
df2['SSEATS'] = pd.to_numeric(df2['SSEATS'], errors="coerce")
df2['SSEATS'] = df2['SSEATS'].fillna(0).astype(int)
df2['RSEATS'] = pd.to_numeric(df2['RSEATS'], errors="coerce")
df2['RSEATS'] = df2['RSEATS'].fillna(0).astype(int)
df2['NTQTY'] = pd.to_numeric(df2['NTQTY'], errors="coerce")
df2['NTQTY'] = df2['NTQTY'].fillna(0).astype(int)
df2['STQTY'] = pd.to_numeric(df2['STQTY'], errors="coerce")
df2['STQTY'] = df2['STQTY'].fillna(0).astype(int)
df2['RTQTY'] = pd.to_numeric(df2['RTQTY'], errors="coerce")
df2['RTQTY'] = df2['RTQTY'].fillna(0).astype(int)
df2['NDOLLARS'] = pd.to_numeric(df2['NDOLLARS'], errors="coerce")
df2['NDOLLARS'] = df2['NDOLLARS'].fillna(0).astype(float)
df2['SDOLLARS'] = pd.to_numeric(df2['SDOLLARS'], errors="coerce")
df2['SDOLLARS'] = df2['SDOLLARS'].fillna(0).astype(float)
df2['RDOLLARS'] = pd.to_numeric(df2['RDOLLARS'], errors="coerce")
df2['RDOLLARS'] = df2['RDOLLARS'].fillna(0).astype(float)
df2['SCHGDOL'] = pd.to_numeric(df2['SCHGDOL'], errors="coerce")
df2['SCHGDOL'] = df2['SCHGDOL'].fillna(0).astype(float)
df2['SCHGRET'] = pd.to_numeric(df2['SCHGRET'], errors="coerce")
df2['SCHGRET'] = df2['SCHGRET'].fillna(0).astype(float)
df2['OPTDOL'] = pd.to_numeric(df2['OPTDOL'], errors="coerce")
df2['OPTDOL'] = df2['OPTDOL'].fillna(0).astype(float)
df2['GRANDTOT'] = pd.to_numeric(df2['GRANDTOT'], errors="coerce")
df2['GRANDTOT'] = df2['GRANDTOT'].fillna(0).astype(float)
df2['CORTESIA'] = df2['CORTESIA'].astype(str)
df2['FESTIVALES'] = df2['FESTIVALES'].astype(str)
df2['IDHOST'] = pd.to_numeric(df2['IDHOST'], errors="coerce")
df2['HEX'] = df2['HEX'].astype(str)
df2.info(verbose=True)

display(df2)

# Carga en BigQuery
job = client.load_table_from_dataframe(
df2, table_id, job_config = job_config)

job.result()  # Espera a que la carga se complete

tabla_destino = table_id

print(f"Los resultados se han insertado en la tabla {tabla_destino} en BigQuery.")

----UDI

# Especifica la ubicación del archivo JSON de credenciales en DBFS
dbfs_path = "path"

# Configura las credenciales del servicio en el entorno
spark.conf.set("google.cloud.auth.service.account.json.keyfile", dbfs_path)

!pip uninstall google-cloud 
!pip uninstall google-cloud-bigquery
!pip install --upgrade google-cloud-bigquery
!pip install --upgrade google-cloud
!pip show google-cloud
!pip show google-cloud-bigquery

import requests
import pandas as pd
import json
from google.cloud import bigquery
import os
import pandas as pd
from datetime import datetime, timedelta
import numpy  as np

credentials_path = "path"
# Establece la variable de entorno GOOGLE_APPLICATION_CREDENTIALS con la ruta del archivo
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

print(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

# Crea un cliente de BigQuery
client = bigquery.Client()

# Obtener la fecha actual y la fecha siguiente
fecha_actual = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
fecha_siguiente = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

# Construir la URL con las fechas dinámicas
url = f"https://www.banxico.org.mx/SieAPIRest/service/v1/series/SP68257/datos/{fecha_actual}/{fecha_siguiente}"
headers = {
    "Accept": "accept",
    "Bmx-Token": "token",
    "Accept-Encoding": "gzip"
}

# Realizar la solicitud HTTP
response = requests.get(url, headers=headers)

if response.status_code == 200:
    # Convertir los datos de la respuesta en un DataFrame de pandas
    data = response.json()
    df = pd.DataFrame(data['bmx']['series'][0]['datos'])
    
    # Procesar el DataFrame según tus necesidades
    print(df)
    df.info(verbose=True)

else:
    print(f"Error al realizar la solicitud. Código de estado: {response.status_code}")
    print(response.text)

df = df.set_axis(["Fecha","UDI"], axis=1, inplace=False)

project_id = "sacred-epigram-307901"

table_id = "TKM_RV_SALES.100_UDI_DBKS"

client = bigquery.Client()

esquema = [{"name": "Fecha","type": "DATE","mode": "NULLABLE"},
{"name": "UDI","type": "FLOAT","mode": "NULLABLE"}]

df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d/%m/%Y',errors="coerce")
df['UDI'] = pd.to_numeric(df['UDI'], errors="coerce")
df['UDI'] = df['UDI'].fillna(0).astype(float)

job_config = bigquery.LoadJobConfig(schema=esquema,write_disposition="WRITE_APPEND")

job = client.load_table_from_dataframe(
df, table_id, job_config = job_config)

job.result()  # Espera a que la carga se complete

tabla_destino = table_id

print(f"Los resultados se han insertado en la tabla {tabla_destino} en BigQuery.")

