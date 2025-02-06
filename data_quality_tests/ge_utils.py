import great_expectations as ge
import gcsfs
import pandas as pd
from typing import Dict, Any
import time
class geutils:
    """
    A utility class for managing Great Expectations operations, including connection handling,
    batch creation, suite management, and validation execution.

    This class provides functionality to work with different data sources (databases, files, 
    BigQuery, GCP cloud storage) and manages various types of expectations for data validation.

    Parameters
    ----------
    missingness_expectations_dict : dict
        Dictionary containing missingness validation rules
    data_integrity_dict : dict
        Dictionary containing data integrity validation rules
    cardinality_expectations_dic : dict
        Dictionary containing cardinality validation rules
    sets_expectation_dict : dict
        Dictionary containing set-based validation rules
    distribution_expectations_dict : dict
        Dictionary containing distribution validation rules
    numerical_data_dict : dict
        Dictionary containing numerical data validation rules
    schema_dict : dict
        Dictionary containing schema validation rules
    volume_dict : dict
        Dictionary containing volume validation rules
    pattern_matching_dict : dict
        Dictionary containing pattern matching validation rules
    create_expectation_instance : object
        Instance of expectation creation class
    """
    def __init__(self, missingness_expectations_dict, data_integrity_dict,cardinality_expectations_dic,sets_expectation_dict,
                 distribution_expectations_dict,numerical_data_dict,schema_dict,volume_dict,pattern_matching_dict,create_expectation_instance):
        
        self.missingness_expectations_dict = missingness_expectations_dict
        self.data_integrity_dict= data_integrity_dict
        self.cardinality_expectations_dict=cardinality_expectations_dic
        self.sets_expectation_dict = sets_expectation_dict
        self.distribution_expectations_dict = distribution_expectations_dict
        self.numerical_data_dict = numerical_data_dict
        self.schema_dict = schema_dict
        self.volume_dict = volume_dict
        self.pattern_matching_dict = pattern_matching_dict
        self.create_expectation_instance = create_expectation_instance


    def connect_to_source(self,config: Dict[str, Any],table_name:str):
        """
        Creates a connection string for database or BigQuery sources based on configuration.

        Parameters
        ----------
        config : dict
            Configuration dictionary containing credentials and connection details
            Must include 'type' key and corresponding credentials
        table_name : str
            Name of the table to connect to

        Returns
        -------
        str
            Connection string for the specified data source

        Raises
        ------
        ValueError
            If the source type is not supported
        """
 
        if config["type"] == "db":
                if config["credentials"]["database_type"]== "mssql":
                        try:
                                connection_string = f'mssql+pyodbc://@{config["credentials"]["server"]}/{config["credentials"]["database"]}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes' 
                        except:
                                connection_string= f'mssql+pyodbc://{config["credentials"]["username"]}:{config["credentials"]["password"]}@{config["credentials"]["server"]}/{config["credentials"]["database"]}?driver=ODBC+Driver+17+for+SQL+Server'
                elif config["credentials"]["database_type"]== "mysql":
                        connection_string = f'mysql://{config["credentials"]["username"]}:{config["credentials"]["password"]}@localhost:3306/{config["credentials"]["database"]}'#?charset=utf8mb4
        elif config["type"]=="bigquery":       
                connection_string = f'bigquery://{config["credentials"]["bigquery_gcp_project_id"]}/{table_name}?credentials_path={config["credentials"]["bigquery_credentials_path"]}'
        

        if config["type"] == "file" or config["type"] == "gcp_cloud_storage":
                pass
        elif config["type"] == "db" or config["type"] == "bigquery":
                return connection_string
        else:
                raise ValueError("No supported source")




    def create_batch(self,config: Dict[str, Any], data_source_name:str, dataset_name:str,context:object):
        """
        Creates a batch definition or request for data validation.

        Parameters
        ----------
        config : dict
            Configuration dictionary containing source type and credentials
        data_source_name : str
            Name of the data source
        dataset_name : str
            Name of the dataset or table
        context : great_expectations.DataContext
            Great Expectations context object

        Returns
        -------
        Union[great_expectations.core.batch.BatchDefinition, great_expectations.core.batch.BatchRequest]
            Batch definition for file sources or batch request for database sources
        """
        if config["type"] == "file"  or config["type"] == "gcp_cloud_storage":
            batch_definition_name = "batch definition"
            data_asset_name = f"{data_source_name}_asset"
            try:
                data_source = context.data_sources.add_pandas(data_source_name)
                data_asset = data_source.add_dataframe_asset(name=data_asset_name)
                
            except: 
                data_asset = context.data_sources.get(data_source_name).get_asset(data_asset_name)
            try:
                batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)
            except:
                batch_definition = (
                context.data_sources.get(data_source_name)
                .get_asset(data_asset_name)
                .get_batch_definition(batch_definition_name)
        )
            return batch_definition

        elif config["type"] == "db":
            connection_string = self.connect_to_source(config,table_name =dataset_name)
            table_name = dataset_name
            table_asset_name =f'{config["credentials"]["database_type"]}_{table_name}'
            schema_name = config["credentials"]["schema_database"]
            if config["credentials"]["schema_database"] == "":
                db_datasource = context.data_sources.add_or_update_sql(name = config["credentials"]["database_type"], connection_string=connection_string)
                try:
                    db_datasource.add_table_asset(
                        name=table_asset_name, table_name=table_name)
                except:
                    db_datasource.get_asset(table_asset_name)
                batch_request = db_datasource.get_asset(table_asset_name).build_batch_request()
                return batch_request

            else:
                db_datasource = context.data_sources.add_or_update_sql(name = config["credentials"]["database_type"], connection_string=connection_string)
                try:
                    db_datasource.add_table_asset(
                        name=table_asset_name, table_name=table_name
                    ,schema_name=schema_name )
                except:
                    db_datasource.get_asset(table_asset_name)
                batch_request = db_datasource.get_asset(table_asset_name).build_batch_request()
                return batch_request
        elif config["type"] == "bigquery":
                connection_string = self.connect_to_source(config,dataset_name)
                table_asset_name =config["type"] 
                table_name =dataset_name
                schema_name = config["credentials"]["schema"]
                db_datasource = context.data_sources.add_or_update_sql(name = config["type"] , connection_string=connection_string)
                try:
                    db_datasource.add_table_asset(
                        name=table_asset_name, table_name=table_name
                    ,schema_name=schema_name )
                except:
                    db_datasource.get_asset(table_asset_name)
                batch_request = db_datasource.get_asset(table_asset_name).build_batch_request()
                return batch_request
    def create_expectation_suite(self, expectation_suite_name:str ,context:object):
        """
        Creates or retrieves an expectation suite.

        Parameters
        ----------
        expectation_suite_name : str
            Name of the expectation suite
        context : great_expectations.DataContext
            Great Expectations context object

        Returns
        -------
        great_expectations.core.expectation_suite.ExpectationSuite
            Created or retrieved expectation suite
        """
        #Create suite of expectations if does not exists
        suite_name = expectation_suite_name
        try:
            suite = ge.ExpectationSuite(name=suite_name)
            suite = context.suites.add(suite)
        except:
            suite = context.suites.get(name=suite_name)
        return suite

    def add_expectations_to_suite(self,suite: object, ge: object, project: str, source: str, stage: str,subproject:str, create_expectation_instance):
        """
        Adds all types of expectations to a suite using the configured dictionaries.

        Parameters
        ----------
        suite : great_expectations.core.expectation_suite.ExpectationSuite
            Expectation suite to add expectations to
        ge : great_expectations
            Great Expectations module instance
        project : str
            Project name
        source : str
            Source identifier
        stage : str
            Processing stage
        subproject : str
            Subproject name
        create_expectation_instance : object
            Instance of expectation creation class
        """
        #Adds missingness expectations to a Great Expectations suite based on provided dictionary.
        create_expectation_instance.add_missingness_expectations(self.missingness_expectations_dict,ge, suite, project, source, stage,subproject)

        #Adds data integrity expectations to a Great Expectations suite based on provided dictionary.
        create_expectation_instance.add_data_integrity_expectations(self.data_integrity_dict,ge, suite, project, source, stage,subproject)
        
        #Adds Pattern Matching expectations to a Great Expectations suite based on providen dictionary.
        create_expectation_instance.add_pattern_matching(self.pattern_matching_dict, ge, suite, project, source, stage,subproject)
        
        #Adds cardinality expectations to a Great Expectations suite based on provided dictionary
        create_expectation_instance.add_cardinality_expectations(self.cardinality_expectations_dict,ge, suite, project, source, stage,subproject)
        
        #Adds sets expectations to a Great Expectations suite based on provided dictionary
        create_expectation_instance.add_sets_expectation(self.sets_expectation_dict,ge, suite, project, source, stage,subproject)
        
        #Adds Numerical Expectations to a Great Expectations suite based on provided dictionary.
        create_expectation_instance.add_numerical_data_expectations(self.numerical_data_dict, ge, suite, project, source, stage,subproject)

        #Adds Distribution Expectations to a Great Expectations suite based on provided dictionary.
        create_expectation_instance.add_distribution_data_expectations(self.distribution_expectations_dict,ge,suite, project, source, stage,subproject)
        
        #Adds Volume_expectations to a Great Expectations suite based on provided dictionary.
        create_expectation_instance.add_volume_expectations(self.volume_dict,ge,suite, project, source, stage,subproject)
        
        #Adds Schema_expectations to a Great Expectations suite based on provided dictionary.
        create_expectation_instance.add_schema_expectations(self.schema_dict,ge,suite, project, source, stage,subproject)

    def run_validation(self,  context,config,suite,dataset_name, validation_definition_name,expectation_suite_name,path=""):
        """
        Executes validation against the defined expectations.

        Parameters
        ----------
        context : great_expectations.DataContext
            Great Expectations context object
        config : dict
            Configuration dictionary containing source type and credentials
        suite : great_expectations.core.expectation_suite.ExpectationSuite
            Expectation suite to validate against
        dataset_name : str
            Name of the dataset or table
        validation_definition_name : str
            Name for the validation definition
        expectation_suite_name : str
            Name of the expectation suite
        path : str, optional
            File path for file-based sources

        Returns
        -------
        tuple
            (validation_results, elapsed_time)
            - validation_results: Results of the validation
            - elapsed_time: Time taken for validation in seconds

        Notes
        -----
        Supports multiple file formats (.csv, .parquet) for file-based sources
        and handles different data sources (database, BigQuery, GCP cloud storage)
        """
        


        start_time = time.time()   
        if config["type"] == "file" or  config["type"] == "gcp_cloud_storage": 
            if config["type"] == "file":
                    if path.endswith(".csv"):
                            df = pd.read_csv(
                                    path
                            )
                    elif path.endswith(".xlsm"):
                            df = pd.read_excel(
                            path
                            )
                    elif path.endswith(".parquet"):
                            df = pd.read_parquet(
                            path
                            ) 
            elif config["type"] == "gcp_cloud_storage":
                    fs = gcsfs.GCSFileSystem(token=config["credentials"]["storage_credentials_path"])
                    with fs.open(path, 'rb') as f:
                            df = pd.read_csv(f)
            expectation_suite = suite
            batch_parameters = {"dataframe": df} 

            batch_definition = self.create_batch(config = config,data_source_name = config["type"], dataset_name=dataset_name,context = context)
        
            try: 
                definition_name = validation_definition_name
                validation_definition = ge.ValidationDefinition(name=definition_name,
                data=batch_definition, suite=expectation_suite
                )
                validation_definition = context.validation_definitions.add(validation_definition)
            except: 
                validation_definition = context.validation_definitions.get(validation_definition_name)
            validation_results = validation_definition.run( batch_parameters = batch_parameters)
        ####################    
        elif config["type"] == "db" or config["type"]== "bigquery":
            batch_request = self.create_batch(config = config,data_source_name = config["type"],dataset_name=dataset_name, context = context)
            expectation_suite_name = expectation_suite_name
            validator = context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=expectation_suite_name,
            )

            validation_results=validator.validate()
        end_time = time.time()
        elapsed_time = round(end_time - start_time, 4)
        return (validation_results, elapsed_time)
    


    