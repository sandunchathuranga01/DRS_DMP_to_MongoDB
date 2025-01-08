import pandas as pd
from pymongo import MongoClient
import numpy as np

# Load Excel file using pandas
def load_excel_data(file_path):
    return pd.read_excel(file_path, sheet_name='Insident test Data')  # Replace 'Combined_Data' if needed

# Connect to MongoDB database
def get_mongo_collection():
    client = MongoClient("mongodb://localhost:27017/")
    db = client['DRS']
    return db['accountDetails']

# Helper function to convert data to native Python types
def convert_to_native_types(data):
    if isinstance(data, (np.integer, np.int64)):
        return int(data)
    elif isinstance(data, (np.floating, np.float64)):
        return float(data)
    elif isinstance(data, np.bool_):
        return bool(data)
    elif pd.isnull(data):
        return None
    return data

# Function to build the MongoDB document structure
def build_document(row):
    document = {
        "account_no": convert_to_native_types(row.get("Account_Num")),
        "Arrears": convert_to_native_types(row.get("Arrears")),
        "Created_By": convert_to_native_types(row.get("Created_By")),
        "Created_Dtm": convert_to_native_types(row.get("Created_Dtm")),
        "Incident_Status": convert_to_native_types(row.get("Incident_Status")),
        "Incident_Status_Dtm": convert_to_native_types(row.get("Incident_Status_Dtm")),
        "Status_Description": convert_to_native_types(row.get("Status_Description")),
        "File_Name_Dump": convert_to_native_types(row.get("File_Name_Dump")),
        "Batch_Id": convert_to_native_types(row.get("Batch_Id")),
        "Batch_Id_Tag_Dtm": convert_to_native_types(row.get("Batch_Id_Tag_Dtm")),
        "External_Data_Update_On": convert_to_native_types(row.get("External_Data_Update_On")),
        "Filtered_Reason": convert_to_native_types(row.get("Filtered_Reason")),
        "Export_On": convert_to_native_types(row.get("Export_On")),
        "File_Name_Rejected": convert_to_native_types(row.get("File_Name_Rejected")),
        "Rejected_Reason": convert_to_native_types(row.get("Rejected_Reason")),
        "Incident_Forwarded_By": convert_to_native_types(row.get("Incident_Forwarded_By")),
        "Incident_Forwarded_On": convert_to_native_types(row.get("Incident_Forwarded_On")),
        "Contact_Details": [
            {
                "Contact_Type": convert_to_native_types(row.get("Contact_Type")),
                "Contact": convert_to_native_types(row.get("Contact")),
                "Create_Dtm": convert_to_native_types(row.get("Create_Dtm")),
                "Create_By": convert_to_native_types(row.get("Create_By"))
            }
        ],
        "Product_Details": [
            {
                "Product_Label": convert_to_native_types(row.get("Product_Label")),
                "Customer_Ref": convert_to_native_types(row.get("Customer_Ref")),
                "Product_Seq": convert_to_native_types(row.get("Product_Seq")),
                "Equipment_Ownership": convert_to_native_types(row.get("Equipment_Ownership")),
                "Product_Id": convert_to_native_types(row.get("Product_Id")),
                "Product_Name": convert_to_native_types(row.get("Product_Name")),
                "Product_Status": convert_to_native_types(row.get("Product_Status")),
                "Effective_Dtm": convert_to_native_types(row.get("Effective_Dtm")),
                "Service_Address": convert_to_native_types(row.get("Service_Address")),
                "Cat": convert_to_native_types(row.get("Cat")),
                "Db_Cpe_Status": convert_to_native_types(row.get("Db_Cpe_Status")),
                "Received_List_Cpe_Status": convert_to_native_types(row.get("Received_List_Cpe_Status")),
                "Service_Type": convert_to_native_types(row.get("Service_Type")),
                "Region": convert_to_native_types(row.get("Region")),
                "Province": convert_to_native_types(row.get("Province"))
            }
        ],
        "Customer_Details": {
            "Customer_Name": convert_to_native_types(row.get("Customer_Name")),
            "Company_Name": convert_to_native_types(row.get("Company_Name")),
            "Company_Registry_Number": convert_to_native_types(row.get("Company_Registry_Number")),
            "Full_Address": convert_to_native_types(row.get("Full_Address")),
            "Zip_Code": convert_to_native_types(row.get("Zip_Code")),
            "Customer_Type_Name": convert_to_native_types(row.get("Customer_Type_Name")),
            "Nic": convert_to_native_types(row.get("Nic")),
            "Customer_Type_Id": convert_to_native_types(row.get("Customer_Type_Id"))
        },
        "Account_Details": {
            "Account_Status": convert_to_native_types(row.get("Account_Status")),
            "Acc_Effective_Dtm": convert_to_native_types(row.get("Acc_Effective_Dtm")),
            "Acc_Activate_Date": convert_to_native_types(row.get("Acc_Activate_Date")),
            "Credit_Class_Id": convert_to_native_types(row.get("Credit_Class_Id")),
            "Credit_Class_Name": convert_to_native_types(row.get("Credit_Class_Name")),
            "Billing_Centre": convert_to_native_types(row.get("Billing_Centre")),
            "Customer_Segment": convert_to_native_types(row.get("Customer_Segment")),
            "Mobile_Contact_Tel": convert_to_native_types(row.get("Mobile_Contact_Tel")),
            "Daytime_Contact_Tel": convert_to_native_types(row.get("Daytime_Contact_Tel")),
            "Email_Address": convert_to_native_types(row.get("Email_Address")),
            "Last_Rated_Dtm": convert_to_native_types(row.get("Last_Rated_Dtm"))
        },
        "Last_Actions": {
            "Billed_Seq": convert_to_native_types(row.get("Billed_Seq")),
            "Billed_Created": convert_to_native_types(row.get("Billed_Created")),
            "Payment_Seq": convert_to_native_types(row.get("Payment_Seq")),
            "Payment_Created": convert_to_native_types(row.get("Payment_Created"))
        },
        "Marketing_Details": {
            "ACCOUNT_MANAGER": convert_to_native_types(row.get("ACCOUNT_MANAGER")),
            "CONSUMER_MARKET": convert_to_native_types(row.get("CONSUMER_MARKET")),
            "Informed_To": convert_to_native_types(row.get("Informed_To")),
            "Informed_On": convert_to_native_types(row.get("Informed_On"))
        }
    }
    return document

# Main function
def main():
    file_path = 'ExcelFiles/test_account.xlsx'  # Replace with the actual file path
    combined_data = load_excel_data(file_path)
    collection = get_mongo_collection()

    documents = []
    for _, row in combined_data.iterrows():
        document = build_document(row)
        documents.append(document)

    if documents:
        collection.insert_many(documents)
        print("Data migration completed successfully.")

if __name__ == "__main__":
    main()
