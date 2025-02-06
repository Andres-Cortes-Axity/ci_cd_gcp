import pandas as pd
import os 
from pathlib import Path
from contextlib import contextmanager
import json
import ndjson
import google.auth
from google.auth import compute_engine

import gcsfs

from google.cloud import storage

class Utils:
    """
    A utility class that provides helper methods for handling data validation results,
    file operations, and expectation configuration parsing.
    """
    def get_name(self, path: str)-> str:
        """
        This methods proces a given path and returns just the full name of file contained in path

        Parameters
        ----------
        path: full path of a file (local path or url)

        Returns:
        -------
        Name of the file cointained inthe path
        """
        last_backslash_posititon = len(path)-path[::-1].find("/")
        return(path[last_backslash_posititon:])
    

    def parse_validation_results(self, validation_results,elapsed_time,file_name):
        """
        Process validation results into a structured DataFrame.

        Parameters
        ----------
        validation_results : dict
            Dictionary containing validation results
        elapsed_time : float
            Time taken for validation execution
        file_name : str
            Name of the file being validated

        Returns
        -------
        pandas.DataFrame
            DataFrame containing validation metrics including Project, Sub_project,
            Stage, Source, Status, Type, and Critical flags
        """
        data = []

        # Fecha y Hora
        load_time =  validation_results['meta']['batch_markers']['ge_load_time']
        formatted_time = f"{load_time[:4]}-{load_time[4:6]}-{load_time[6:8]} {load_time[9:11]}:{load_time[11:13]}:{load_time[13:15]}"

        # Iterar sobre los resultados
        for aux in  validation_results['results']:
            
            # Extraer las columnas o lista de columnas
            kwargs = aux['expectation_config']['kwargs']
            if 'column' in kwargs:
                columns = kwargs['column']
            elif 'column_list' in kwargs:
                columns = ", ".join(kwargs['column_list'])
            elif 'column_A' in kwargs:
                columns =  kwargs['column_A'] + ', ' +  kwargs['column_B']
            else:
                columns = "N/A"
            

            # Agregar la fila al conjunto de datos
            data.append({
                        "Project" : aux["expectation_config"]["meta"].get('project').replace('_', ' ' ).title(),
                        "Sub_project": aux["expectation_config"]["meta"].get('sub_project').replace('_', ' ' ).title(),
                        "Stage" : aux["expectation_config"]["meta"].get('stage'),
                        "Source" : aux["expectation_config"]["meta"].get('source'),
                        "SourceName" :file_name,
                        "Initialized": formatted_time,
                        "Submodulo": aux["expectation_config"]["meta"].get('expectation_type').replace('-',' ').replace('_', ' ' ).title(),
                        "quality_dimension": aux["expectation_config"]["meta"].get('dama_dimension'),
                        "Status": 1 if aux['success'] is True else 0 ,
                        "Type": aux['expectation_config']['type'].replace('_', ' ' ),
                        "Column(s)": columns,
                        "Elapsed" :str(elapsed_time),
                        "Critical" : aux["expectation_config"]["meta"].get('is_critical') if aux['success'] is False else 0,
                       
                    })
                
        # Convertir a DataFrame
        df = pd.DataFrame(data)
        return df
    
    
    def save_processed_log_local(self, path_logs,project,stage,table,file_name):
        lots_folder = Path(path_logs) / 'csv'
        lots_folder.mkdir(parents=True, exist_ok=True)



        file_path = lots_folder / f"{project}_{stage}"#_{file_name}"
        table.to_csv(file_path, mode='a', index=False, header=not os.path.exists(file_path))

    def save_processed_log(self, path_logs, project, stage, table, file_name):
        """
        Save logs to Google Cloud Storage
        """
        try:
            # Initialize GCS client
            credentials, project_id = google.auth.default()
            print(f"Using project ID: {project_id}")
            storage_client = storage.Client(credentials=credentials, project=project_id)
            
            # Get bucket name and sanitize path
            bucket_name = "gcp-arquitecture-space-datalake"
            
            # Sanitize the path by removing any leading ./ or / and trailing /
            path_logs = path_logs.lstrip('./').lstrip('/').rstrip('/')
            
            # Create the GCS path - ensure clean path construction
            gcs_path = f"{path_logs}/csv/{project}_{stage}.csv".replace('//', '/')
            
            print(f"Bucket: {bucket_name}")
            print(f"Full GCS path: {gcs_path}")
            
            # Get bucket
            try:
                bucket = storage_client.get_bucket(bucket_name)
                print(f"Successfully connected to bucket: {bucket_name}")
            except Exception as e:
                print(f"Error accessing bucket {bucket_name}: {str(e)}")
                raise
                
            # Create blob object
            blob = bucket.blob(gcs_path)
            
            try:
                # Convert DataFrame to CSV string (with error handling)
                try:
                    csv_data = table.to_csv(index=False)
                except Exception as e:
                    print(f"Error converting DataFrame to CSV: {str(e)}")
                    raise
                    
                # Check if file exists
                try:
                    file_exists = blob.exists()
                    print(f"File exists: {file_exists}")
                except Exception as e:
                    print(f"Error checking file existence: {str(e)}")
                    raise
                    
                if file_exists:
                    print(f"Appending to existing file: {gcs_path}")
                    try:
                        # Download existing content
                        existing_data = blob.download_as_string().decode('utf-8')
                        # Append new data without header
                        new_data = table.to_csv(index=False, header=False)
                        # Combine existing and new data
                        combined_data = existing_data + new_data
                        # Upload combined data
                        blob.upload_from_string(combined_data)
                        print(f"Successfully appended data to: {gcs_path}")
                    except Exception as e:
                        print(f"Error during append operation: {str(e)}")
                        raise
                else:
                    print(f"Creating new file: {gcs_path}")
                    try:
                        # Upload new file
                        blob.upload_from_string(csv_data)
                        print(f"Successfully created new file: {gcs_path}")
                    except Exception as e:
                        print(f"Error creating new file: {str(e)}")
                        raise
                        
            except Exception as e:
                print(f"Error handling blob operations: {str(e)}")
                raise
                
        except Exception as e:
            print(f"Failed to save logs to GCS: {str(e)}")
            raise
        
        return gcs_path


    def save_validation_results_as_json(self, validation_results,path_logs,project,stage,file_name):
        lots_folder = Path(path_logs) / 'json'
        lots_folder.mkdir(parents=True, exist_ok=True)
        file_path = lots_folder / f"{project}_{stage}_{file_name}"

        # Save the validation results to a JSON file
        with open(file_path, 'w') as json_file:
            json.dump(validation_results, json_file, default=str, indent=4)  # Using default=str to handle non-serializable data

        """# Load JSON from file
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)

        # Convert JSON to ndjson and save it
        with open(os.join.path("ndjson",file_path), 'w') as ndjson_file:
            writer = ndjson.writer(ndjson_file)
            
            # If the JSON is an array, convert each item to a line
            if isinstance(data, list):
                for item in data:
                    writer.writerow(item)
            # If it's a dictionary, we need to convert its components
            elif isinstance(data, dict):
                for key, value in data.items():
                    writer.writerow({key: value})"""
    def define_expectations_dicts(self,config,dataset):
        """
        Extract different types of expectations from configuration.

        Parameters
        ----------
        config : dict
            Configuration dictionary containing expectation suites
        dataset : dict
            Dataset configuration containing expectations suite reference

        Returns
        -------
        tuple
            Contains dictionaries for different types of expectations:
            (missingness, data_integrity, cardinality, sets, distribution,
            numerical_data, schema, volume, pattern_matching)
        """
                #Creating  missingness_expectations_dict
        if "missingness_expectations" in config["expectation_suites"][dataset["expectations_suite"]]["expectations"]:
            missingness_expectations_dict = config["expectation_suites"][dataset["expectations_suite"]]["expectations"]["missingness_expectations"]
        else:
            missingness_expectations_dict ={}

        if "data_integrity" in config["expectation_suites"][dataset["expectations_suite"]]["expectations"]:
            data_integrity_dict = config["expectation_suites"][dataset["expectations_suite"]]["expectations"]["data_integrity"]
        else:
            data_integrity_dict = {}

        if "cardinality_expectations" in config["expectation_suites"][dataset["expectations_suite"]]["expectations"]:
            cardinality_expectations_dict = config["expectation_suites"][dataset["expectations_suite"]]["expectations"]["cardinality_expectations"]
        else:
            cardinality_expectations_dict = {}

        if "sets_expectation" in config["expectation_suites"][dataset["expectations_suite"]]["expectations"]:
            sets_expectation_dict = config["expectation_suites"][dataset["expectations_suite"]]["expectations"]["sets_expectation"]
        else:
            sets_expectation_dict = {}

        if "distribution_expectations" in config["expectation_suites"][dataset["expectations_suite"]]["expectations"]:
            distribution_expectations_dict = config["expectation_suites"][dataset["expectations_suite"]]["expectations"]["distribution_expectations"]
        else:
            distribution_expectations_dict = {}
            
        if "numerical_data" in config["expectation_suites"][dataset["expectations_suite"]]["expectations"]:
            numerical_data_dict = config["expectation_suites"][dataset["expectations_suite"]]["expectations"]["numerical_data"]
        else:
            numerical_data_dict = {}
            
        if "schema" in config["expectation_suites"][dataset["expectations_suite"]]["expectations"]:
            schema_dict = config["expectation_suites"][dataset["expectations_suite"]]["expectations"]["schema"]
        else:
            schema_dict = {}

        if "volume" in config["expectation_suites"][dataset["expectations_suite"]]["expectations"]:
            volume_dict = config["expectation_suites"][dataset["expectations_suite"]]["expectations"]["volume"]
        else:
            volume_dict = {}

        if "pattern_matching" in config["expectation_suites"][dataset["expectations_suite"]]["expectations"]:
            pattern_matching_dict = config["expectation_suites"][dataset["expectations_suite"]]["expectations"]["pattern_matching"]
        else:
            pattern_matching_dict = {}
        return (missingness_expectations_dict, data_integrity_dict,cardinality_expectations_dict,sets_expectation_dict,
                    distribution_expectations_dict,numerical_data_dict,schema_dict,volume_dict,pattern_matching_dict)
    
    def define_global_parameters(self,config):
        """
        Extract global configuration parameters.

        Parameters
        ----------
        config : dict
            Configuration dictionary containing global parameters

        Returns
        -------
        tuple
            Contains (project, sub_project, stage, path_logs, data_source_info,
            expectations, sources_dict)
            where sources_dict includes configurations for database, file,
            BigQuery, and GCP cloud storage sources
        """
        project = config["global"]["project"]
        sub_project= config["global"]["sub_project"]
        stage= config["global"]["stage"]
        path_logs= config["global"]["path_logs"]
        data_source_info = config["data_sources"]
        expectations = config["expectation_suites"]
        parallelize = config["global"]["parallelize"]
        if "method" in config["global"]:
            method = config["global"]["method"]
        if 'database_source' in config["data_sources"]:
            database_source= config["data_sources"]['database_source']
        else:
            database_source = {}
        if 'file_source' in config["data_sources"]:
            file_source= config["data_sources"]['file_source']
        else: 
            file_source ={}
        if 'bigquery_source' in config["data_sources"]:
            bigquery_source = config["data_sources"]['bigquery_source'] 
        else:
            bigquery_source = {}
        if 'gcp_cloud_storage_source' in config["data_sources"]:
            gcp_cloud_storage_source= config["data_sources"]['gcp_cloud_storage_source'] 
        else:
            gcp_cloud_storage_source = {}
        sources = {"database_source":database_source, "file_source":file_source,"bigquery_source":bigquery_source,'gcp_cloud_storage_source':gcp_cloud_storage_source }
        return (project ,sub_project,stage, path_logs,data_source_info,expectations,sources,parallelize,method)