!pip install google-cloud-bigquery pandas-gbq --quiet
!pip install office365-rest-python-client pyarrow xlrd openpyxl --quiet

import os
import io
import pandas as pd
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.files.file import File
from google.cloud import bigquery
from google.oauth2 import service_account

credentials_path = "Path"
project_id = "Project"
table_id = "Table"
full_table_id = f"{project_id}.{table_id}"

credentials = service_account.Credentials.from_service_account_file(credentials_path)
client = bigquery.Client(credentials=credentials, project=project_id)
site_url = "url"
username = "user name"
password = "psw"

ctx = ClientContext(site_url).with_credentials(UserCredential(username, password))
folder = ctx.web.get_folder_by_server_relative_url("/url")
files = folder.files
ctx.load(files)
ctx.execute_query()

# Esquema de BigQuery
esquema = [
    {'name': 'Date','type': 'TIMESTAMP','mode': 'NULLABLE'},
    {'name': 'Transaction_ID','type': 'INTEGER','mode': 'NULLABLE'},
    {'name': 'Transaction_Result','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Transaction_Type','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Client_Unique_ID','type': 'INTEGER','mode': 'NULLABLE'},
    {'name': 'Authorization_Type','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Related_Transaction_ID','type': 'INTEGER','mode': 'NULLABLE'},
    {'name': 'Currency','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Amount','type': 'FLOAT','mode': 'NULLABLE'},
    {'name': 'Is_Partial_Approval','type': 'BOOLEAN','mode': 'NULLABLE'},
    {'name': 'Reason_Code','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Filter_Reason','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Processing_Channel','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Acquiring_Bank','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Multi_Client','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Client_Name','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Site_Name','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'APM_Reference','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Affiliate','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'PPP_Order_ID','type': 'INTEGER','mode': 'NULLABLE'},
    {'name': 'Bank_Transaction_ID','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Auth_Code','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Custom_Data','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Is_Credited','type': 'BOOLEAN','mode': 'NULLABLE'},
    {'name': 'Credit_Type','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Is_Cascaded','type': 'BOOLEAN','mode': 'NULLABLE'},
    {'name': 'Is_Modified','type': 'BOOLEAN','mode': 'NULLABLE'},
    {'name': 'Modification_Reason','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Product_ID','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'URL','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Entry_Mode','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Terminal_ID','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Terminal_Country','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Offline_Transaction','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Merchant_Attendance','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Is_Currency_Converted','type': 'BOOLEAN','mode': 'NULLABLE'},
    {'name': 'ARN','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Lifecycle_ID','type': 'INTEGER','mode': 'NULLABLE'},
    {'name': 'Is_Fast_Funds','type': 'BOOLEAN','mode': 'NULLABLE'},
    {'name': 'Provider_Decline_Reason','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Descriptor','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Sub_Merchant_Name','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Sub_Merchant_Country','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Sub_Merchant_City','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Subscription_ID','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Was_Transmitted','type': 'BOOLEAN','mode': 'NULLABLE'},
    {'name': 'Encrypted_ID','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Batch_ID','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'BSP_Country','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Source_Application','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'APM_Request_ID','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Converted_Currency','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Converted_Amount','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'External_Scheme_Identifier','type': 'INTEGER','mode': 'NULLABLE'},
    {'name': 'Is_AFT','type': 'BOOLEAN','mode': 'NULLABLE'},
    {'name': 'Scheme_Identifier','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Provider_Additional_Information','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Original_Client','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Industry','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Personal_ID','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Lot_Number','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Trace_Number','type': 'INTEGER','mode': 'NULLABLE'},
    {'name': 'RRN','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Refund_Authorization_Status','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Anticipation_Type','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Installment_Funding_Type','type': 'INTEGER','mode': 'NULLABLE'},
    {'name': 'Payment_Method','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Payment_Option','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Payment_Sub_method','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'PAN','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Issuing_Country','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'BIN','type': 'INTEGER','mode': 'NULLABLE'},
    {'name': 'Card_Type','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Expiration_Date','type': 'INTEGER','mode': 'NULLABLE'},
    {'name': 'Issuer_Bank','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Name_on_Card','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Card_Brand','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Card_Product_Type','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Network_Token_Used','type': 'BOOLEAN','mode': 'NULLABLE'},
    {'name': 'UPO','type': 'INTEGER','mode': 'NULLABLE'},
    {'name': 'Invoice','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Tip','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Clerk_ID','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'External_Token_Provider','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Billing_Country','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'IP_Country','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Email_Address','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'User_ID','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Billing_First_Name','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Billing_Last_Name','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Billing_Address','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Billing_City','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Billing_State','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'ZIP_Code','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Billing_Phone','type': 'INTEGER','mode': 'NULLABLE'},
    {'name': 'IP_Address','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Device_Type','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Device_Name','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Device_OS','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'User_Token_ID','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Transaction_Highlights','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'AVS_Result','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'CVV2_Result','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Is_3D','type': 'BOOLEAN','mode': 'NULLABLE'},
    {'name': 'ECI','type': 'INTEGER','mode': 'NULLABLE'},
    {'name': 'Authentication_Status','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Enrollment_Status','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'PSD2_Scope','type': 'BOOLEAN','mode': 'NULLABLE'},
    {'name': 'Is_SCA_Mandated','type': 'BOOLEAN','mode': 'NULLABLE'},
    {'name': '_3D_Version','type': 'STRING','mode': 'NULLABLE'},
    {'name': '_3D_Message_Version','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Authentication_Flow','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Is_Liability_Shift','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Is_External_MPI','type': 'BOOLEAN','mode': 'NULLABLE'},
    {'name': 'Acquirer_Decision','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Acquirer_Decision_Reason','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Authentication_Status_Reason','type': 'STRING','mode': 'NULLABLE'},
    {'name': 'Number_of_Installments','type': 'INTEGER','mode': 'NULLABLE'}]

expected_columns = [field['name'] for field in esquema]

column_mapping = {
    "3D_Version": "_3D_Version",
    "3D_Message_Version": "_3D_Message_Version"
}

for file in files:
    filename = file.properties["Name"]
    
    if not filename.lower().endswith('.xlsx'):
        continue
        
    print(f"\nProcesando archivo: {filename}")
    
    file_url = file.properties["ServerRelativeUrl"]
    
    excel_buffer = io.BytesIO()
    ctx.web.get_file_by_server_relative_url(file_url).download(excel_buffer)
    ctx.execute_query()
    excel_buffer.seek(0)
    
    
    try:
        df = pd.read_excel(excel_buffer, skiprows=11)
        df = df.iloc[:-1]  # Eliminar última fila si es totales
        
       
        df.columns = df.columns.str.strip().str.replace(" ", "_")
        df.columns = df.columns.str.strip().str.replace("-", "_")
        df.columns = df.columns.str.strip()
        
       
        df.rename(columns=column_mapping, inplace=True)
        
        
        available_columns = [col for col in expected_columns if col in df.columns]
        df = df[available_columns]
        

        missing_columns = [col for col in expected_columns if col not in df.columns]
        for col in missing_columns:
            df[col] = None
        
        print("Columnas procesadas:", df.columns.tolist())
        print(df.head())
        
       
        bool_columns = [
            col for col in [
                "Is_Partial_Approval", "Is_Credited", "Is_Cascaded", "Is_Modified",
                "Is_Currency_Converted", "Is_Fast_Funds", "Was_Transmitted", "Is_AFT",
                "Network_Token_Used", "Is_3D", "PSD2_Scope", "Is_SCA_Mandated", "Is_External_MPI"
            ] if col in df.columns
        ]
        
        for col in bool_columns:
            df[col] = df[col].map({"Yes": True, "No": False})
        
      
        for field in esquema:
            col_name = field['name']
            if col_name in df.columns:
                if field['type'] == 'INTEGER':
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce').astype('Int64')
                elif field['type'] == 'FLOAT':
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                elif field['type'] == 'BOOLEAN':
                    df[col_name] = df[col_name].astype('boolean')
                elif field['type'] == 'TIMESTAMP':
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
        
      
        job_config = bigquery.LoadJobConfig(schema=esquema, write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        
        print(f"Datos de {filename} insertados en la tabla {table_id}")
        
    
        sharepoint_file = ctx.web.get_file_by_server_relative_url(file_url)
        sharepoint_file.recycle()
        ctx.execute_query()
        print(f"Archivo {filename} movido a la papelera de reciclaje")
        
    except Exception as e:
        print(f"Error procesando archivo {filename}: {str(e)}")

print("\nProceso completado. Todos los archivos XLSX han sido procesados

