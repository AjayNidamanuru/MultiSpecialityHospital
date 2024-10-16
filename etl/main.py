import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# MongoDB connection string
MONGO_URI = 'mongodb+srv://<username>:<password>@atlascluster.bouds8i.mongodb.net/'
DB_NAME = 'Hospital'

# Function: Extract Data from Text File and Load into Customers Collection
def extract_data(file_path):
    try:
        # Read the data from the text file
        with open(file_path, 'r') as f:
            lines = f.readlines()
        
        # Read the header, skip the leading 'H'
        header = lines[0].strip().split('|')[1:]  # Skip the first character 'H'
        
        # Prepare a list to hold valid data rows
        valid_data = []

        # Read the data rows
        for line in lines[1:]:
            # Split the line by '|' and strip whitespace, filtering out empty strings
            row_data = [col.strip() for col in line.strip().split('|')[1:]]  # Skip first 'D'
            
            # Add the row to valid_data without checking for column matching
            valid_data.append(row_data)

        # Create DataFrame from valid data
        data = pd.DataFrame(valid_data, columns=header)

        # Connect to MongoDB and Insert Data into Customers Collection
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db['Customers']
        
        data_dict = data.to_dict(orient='records')  # Convert DataFrame to list of dictionaries
        
        # Insert the data
        if data_dict:
            collection.insert_many(data_dict)
            print("Data inserted into Customers collection successfully!")
        else:
            print("No valid data to insert.")

        client.close()

    except Exception as e:
        print(f"Error processing the file: {e}")

# Load Data into Staging Collection with Validations
def load_staging():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    source_collection = db['Customers']  # Source collection
    staging_collection = db['Staging_data']  # Staging collection

    # Extract Data from Source Collection
    source_data = list(source_collection.find())

    # Transform Data with Validations
    transformed_data = []
    for record in source_data:
        # Check for mandatory fields
        if record.get("Customer_Name") and record.get("Customer_Id") and record.get("Open_Date"):
            transformed_record = {
                "Name": record.get("Customer_Name"),
                "Cust_I": record.get("Customer_Id"),
                "Open_Dt": record.get("Open_Date"),
                "Consul_Dt": record.get("Last_Consulted_Date"),
                "VAC_ID": record.get("Vaccination_Id"),
                "DR_Name": record.get("Dr_Name"),
                "State": record.get("State"),
                "County": record.get("Country"),
                "DOB": record.get("DOB"),
                "FLAG": record.get("Is_Active")
            }
            transformed_data.append(transformed_record)

    # Load Data into Staging Collection
    if transformed_data:
        staging_collection.insert_many(transformed_data)
        print("Data loaded into Staging_data Collection successfully!")
    else:
        print("No valid data to load into staging.")

    client.close()

# Transform and Load into Country-Specific Collections
def transform_and_load():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    staging_collection = db['Staging_data']  # Staging collection

    # Fetch all records from Staging Collection
    staging_data = list(staging_collection.find())

    # Process each record to create country-specific collections
    for record in staging_data:
        country = record.get("County")  # Get the customer's country

        # Check if country data is present
        if not country:
            print(f"Skipping record for {record.get('Name')}: No country data available.")
            continue  # Skip this record if there's no country data
        
        collection_name = f"Table_{country}"  # Create collection name based on country

        # Prepare the record for insertion
        try:
            # Update the date parsing according to the format in the data
            dob = datetime.strptime(record.get("DOB"), '%d%m%Y')  # Parsing as DDMMYYYY
            consul_dt = datetime.strptime(record.get("Consul_Dt"), '%Y%m%d')  # Adjust if needed

            # Calculate derived columns: age and days since last consultation
            today = datetime.today()
            age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
            days_since_last_consult = (today - consul_dt).days

            # Add derived columns to the record
            customer_record = {
                "Name": record.get("Name"),
                "Cust_I": record.get("Cust_I"),
                "Open_Dt": record.get("Open_Dt"),
                "Consul_Dt": record.get("Consul_Dt"),
                "VAC_ID": record.get("VAC_ID"),
                "DR_Name": record.get("DR_Name"),
                "State": record.get("State"),
                "County": record.get("County"),
                "DOB": record.get("DOB"),
                "FLAG": record.get("FLAG"),
                "age": age,  # Derived column: age
                "days_since_last_consult": days_since_last_consult  # Derived column: days since last consultation
            }

            # Insert/update only if the customer was consulted more than 30 days ago
            if days_since_last_consult > 30:
                # Check if the collection exists, create if not
                if collection_name not in db.list_collection_names():
                    db.create_collection(collection_name)

                # Insert/Update Logic
                existing_record = db[collection_name].find_one({"Cust_I": customer_record["Cust_I"]})

                if existing_record:
                    if customer_record["Consul_Dt"] > existing_record["Consul_Dt"]:
                        db[collection_name].update_one(
                            {"Cust_I": customer_record["Cust_I"]},
                            {"$set": customer_record}
                        )
                        print(f"Updated customer {customer_record['Name']} in {collection_name}.")
                    else:
                        print(f"No update needed for customer {customer_record['Name']} in {collection_name}.")
                else:
                    db[collection_name].insert_one(customer_record)
                    print(f"Inserted new customer {customer_record['Name']} into {collection_name}.")

        except Exception as e:
            print(f"Error processing record {record['Name']} with error: {e}")

    client.close()

# Main execution
if __name__ == "__main__":
    # Extract data from file and load into Customers collection
    file_path = 'C:/Users/ajayn/Desktop/Hospital/data/customers.txt'  # Replace with your file path
    extract_data(file_path)

    # Load data from Customers to Staging with validation
    load_staging()

    # Transform and load data into country-specific collections
    transform_and_load()
