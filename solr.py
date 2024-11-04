import pysolr
import requests
import pandas as pd
import kagglehub

# Base URL for Apache Solr
SOLR_BASE_URL = "http://localhost:8989/solr"

# Download the dataset from Kaggle
def downloadDataset():
    path = kagglehub.dataset_download("williamlucas0/employee-sample-data")
    print("Path to dataset files:", path)
    return path

# Function to create a collection
def createCollection(p_collection_name):
    try:
        params = {
            'action': 'CREATE',
            'name': p_collection_name,
            'numShards': 1,
            'replicationFactor': 1,
        }
        response = requests.get(f"{SOLR_BASE_URL}/admin/collections", params=params)
        if response.status_code == 200:
            print(f"Collection '{p_collection_name}' created successfully.")
        else:
            print(f"Error creating collection: {response.text}")
    except Exception as e:
        print(f"Exception occurred: {e}")

def indexData(p_collection_name, p_exclude_column, csv_file_path):
    def convert_date(date_str):
        if pd.isna(date_str):  # Check for NaN
            return None  # Return None for Solr to interpret as null

    try:
        # Read data from the CSV file
        data = pd.read_csv(csv_file_path)

        # Convert date fields while keeping NaN values
        if 'Exit Date' in data.columns:
            data['Exit Date'] = data['Exit Date'].apply(convert_date)
        if 'Hire Date' in data.columns:
            data['Hire Date'] = data['Hire Date'].apply(convert_date)

        # Exclude the specified column from the data
        indexed_data = data.drop(columns=[p_exclude_column]).to_dict(orient='records')

        # Index data into Solr
        solr = pysolr.Solr(f"{SOLR_BASE_URL}/{p_collection_name}", always_commit=True)
        solr.add(indexed_data)
        print("Data indexed successfully.")
    except Exception as e:
        print(f"Exception occurred: {e}")



# Function to search by column
def searchByColumn(p_collection_name, p_column_name, p_column_value):
    try:
        solr = pysolr.Solr(f"{SOLR_BASE_URL}/{p_collection_name}")
        query = f"{p_column_name}:{p_column_value}"
        results = solr.search(query)
        for result in results:
            print(result)
    except Exception as e:
        print(f"Exception occurred: {e}")

# Function to get the count of employees (total documents)
def getEmpCount(p_collection_name):
    try:
        solr = pysolr.Solr(f"{SOLR_BASE_URL}/{p_collection_name}")
        response = solr.search('*:*', rows=0)
        print(f"Employee count: {response.hits}")
        return response.hits
    except Exception as e:
        print(f"Exception occurred: {e}")

def delEmpById(p_collection_name, p_employee_id):
    try:
        solr = pysolr.Solr(f"{SOLR_BASE_URL}/{p_collection_name}")
        query = f'Employee_ID:{p_employee_id}'
        results = solr.search(query)
        if results.hits > 0:
            solr.delete(id=p_employee_id)
            solr.commit()  # Ensure the delete operation is committed
            post_delete_results = solr.search(query)
            print(f"Results after deletion: {post_delete_results.hits}")
            print(f"Employee with ID '{p_employee_id}' deleted successfully.")
        else:
            print(f"No employee found with ID '{p_employee_id}' to delete.")
    except Exception as e:
        print(f"Exception occurred: {e}")


# Function to get department facet counts
def getDepFacet(p_collection_name):
    try:
        solr = pysolr.Solr(f"{SOLR_BASE_URL}/{p_collection_name}")
        params = {
            'q': '*:*',
            'facet': 'true',
            'facet.field': 'department',
            'rows': 0
        }
        response = solr.search('*:*', **params)
        print("Department facet counts:")
        if 'facet_counts' in response.raw_response:
            facet_fields = response.raw_response['facet_counts']['facet_fields']
            for department, count in zip(facet_fields['department'][::2], facet_fields['department'][1::2]):
                print(f"{department}: {count}")
        else:
            print("No facet counts available.")
    except Exception as e:
        print(f"Exception occurred: {e}")

# Execute the functions as specified
if __name__ == "__main__":
    # Download the dataset
    dataset_path = downloadDataset()
    csv_file_path = f"{dataset_path}/Employee Sample Data 1.csv"  # Adjust file name if needed
    
    # Variables for collection names
    v_nameCollection = "Hash_Subish"
    v_phoneCollection = "Hash_3464"  # Replace '1234' with the last four digits of your phone number

    # createCollection(v_nameCollection)
    # createCollection(v_phoneCollection)

    # getEmpCount(v_nameCollection)

    # Index data into collections
    # indexData(v_nameCollection, 'Department', csv_file_path)
    # indexData(v_phoneCollection, 'Gender', csv_file_path)

    # getEmpCount(v_nameCollection)

    # delEmpById(v_nameCollection, 'E02003')
    # getEmpCount(v_nameCollection)

    # # # Search by column
    # print(f"{searchByColumn = }")
    # searchByColumn(v_nameCollection, 'Department', 'IT')
    # searchByColumn(v_nameCollection, 'Gender', 'Male')
    # searchByColumn(v_phoneCollection, 'Department', 'IT')

    # # # Get department facet counts
    # print(f"{getDepFacet = }")
    # getDepFacet(v_nameCollection)
    # getDepFacet(v_phoneCollection)