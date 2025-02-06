
import concurrent.futures
import time
from typing import Dict, Any
import great_expectations as ge
from pathlib import Path
import threading
import queue
#great expectations library
import great_expectations as ge
# Connect with gcp storage
import gcsfs
#to handle pandas dataframes 
import pandas as pd
#for multiprocessing
import multiprocessing
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
## several purposes
from typing import Dict, Any
import time
import logging
logger = logging.getLogger(__name__)
import sys
##load configuration file
import yaml
import queue
import threading
from pathlib import Path
##parallel process
import multiprocessing as mp
from pathlib import Path
import logging
import contextlib
import signal


#Auxiliar classes
sys.path.append("src")
from CreateExpectations import CreateExpectations
from utils import Utils
from ge_utils import geutils

"""
Great Expectations Data Validation Framework

This script implements a flexible data validation framework using the Great Expectations 
library with support for multiple execution modes (sequential, based-thread parallel validation, and 
based-process parallel validation). It validates datasets from various sources including databases, 
BigQuery, files, and GCP cloud storage.

Required Custom Classes:
    - CreateExpectations: Creates validation expectations
    - Utils: Provides utility functions
    - geutils: Great Expectations utility functions

 Notes
    -----
    - The function supports different validation scenarios based on the source type
    - When using parallelization, results handling differs between ThreadPoolExecutor
      and ProcessPoolExecutor
    - For database sources, the source identifier is created by combining the type
      with the database/schema name


"""



context = ge.get_context()

def run_validation_dataset(config, source_config: Dict[str, Any], 
                           dataset: Dict[str, Any], utils: object,
                           create_expectations_instance: object,
                           sources: object,
                            project :str,
                            sub_project: str,
                            stage : str, 
                            path_logs :str,
                            data_source_info: dict,
                            expectations: dict,
                            parallelize: bool,
                            method: str,
                           results_queue : queue.Queue = None):
    

    """
    Runs data validation on a specified dataset using Great Expectations framework.

    This function performs data validation by creating and running expectation suites
    based on the provided configuration. It supports different data sources including
    files, cloud storage, databases, and BigQuery.

    Parameters
    ----------
    config : dict
        Global configuration settings for the validation process.
    source_config : Dict[str, Any]
        Configuration for the data source, including type and credentials.
        Supported types: "file", "gcp_cloud_storage", "db", "bigquery".
    dataset : Dict[str, Any]
        Dataset configuration containing:
        - table_name or name: Name of the dataset
        - expectations_suite: Name of the expectations suite
        - path: File path (required for file/cloud storage sources)
    utils : object
        Utility class instance containing helper methods for validation.
    create_expectations_instance : object
        Instance responsible for creating expectations.
    sources : object
        Source configuration handler.
    project : str
        Project identifier.
    sub_project : str
        Sub-project identifier.
    stage : str
        Processing stage identifier.
    path_logs : str
        Path where validation logs will be stored.
    data_source_info : dict
        Additional information about the data source.
    expectations : dict
        Expectations configuration.
    parallelize : bool
        Whether to run validations in parallel.
    method : str
        Parallelization method: "ThreadPoolExecutor" or "ProcessPoolExecutor".
    results_queue : queue.Queue, optional
        Queue for storing validation results when using ThreadPoolExecutor.

    Returns
    -------
    Union[bool, Tuple[str, pd.DataFrame]]
        - If parallelize=False: Returns True on success, False on failure
        - If parallelize=True with ProcessPoolExecutor: Returns tuple of (file_name, results_table)
        - If parallelize=True with ThreadPoolExecutor: Returns None (results put in queue)

    Raises
    ------
    ValueError
        If an unsupported parallelization method is provided.
    Exception
        If any error occurs during validation process.
        
        
    Best Practices:
    1. Use process-parallel execution for CPU-intensive validations
    2. Use thread-parallel execution for I/O-bound validations
    3. Configure appropriate timeouts based on dataset sizes
    4. Monitor the logs directory for validation results
    5. Ensure proper error handling in configuration file

    Notes:
    - Validation results are saved in CSV format
    - The script supports multiple data source types
    - Process-parallel execution includes proper cleanup
    - Thread-parallel execution is best for I/O-bound operations"""
    try:

        
        try:
            asset_name = dataset["table_name"]
        except:
            asset_name = dataset["name"]

        (missingness_expectations_dict, data_integrity_dict, 
         cardinality_expectations_dict, sets_expectation_dict,
         distribution_expectations_dict, numerical_data_dict, 
         schema_dict, volume_dict, pattern_matching_dict) = utils.define_expectations_dicts(config, dataset)


        ge_utils_instance = geutils(missingness_expectations_dict, data_integrity_dict,
                                  cardinality_expectations_dict, sets_expectation_dict,
                                  distribution_expectations_dict, numerical_data_dict,
                                  schema_dict, volume_dict, pattern_matching_dict,
                                  create_expectations_instance)

        suite = ge_utils_instance.create_expectation_suite(
            expectation_suite_name=dataset["expectations_suite"], 
            context=context
        )

        # Determine source type and add expectations
        source_type = source_config["type"]
        source_identifier = source_type
        if source_type == "db":
            source_identifier += f"_{source_config['credentials']['database']}"
        elif source_type == "bigquery":
            source_identifier += f"_{source_config['credentials']['schema']}"

        ge_utils_instance.add_expectations_to_suite(
            suite=suite, ge=ge, project=project,
            source=source_identifier, stage=stage,
            subproject=sub_project,
            create_expectation_instance=create_expectations_instance
        )

        logger.info(f"Starting validation for {asset_name}")

        # Run validation based on source type
        if source_type in ["file", "gcp_cloud_storage"]:
            validation_results, elapsed_time = ge_utils_instance.run_validation(
                context=context, config=source_config,
                suite=suite, dataset_name=asset_name,
                validation_definition_name=f"{asset_name}_validation_{dataset['expectations_suite']}",
                expectation_suite_name=dataset["expectations_suite"],
                path=dataset["path"]
            )
            file_name = utils.get_name(dataset["path"])
        else:  # db or bigquery
            validation_results, elapsed_time = ge_utils_instance.run_validation(
                context=context, config=source_config,
                suite=suite, dataset_name=asset_name,
                validation_definition_name=f"{asset_name}_validation_{dataset['expectations_suite']}",
                expectation_suite_name=dataset["expectations_suite"]
            )
            file_name = asset_name

        logger.info(f"{asset_name} validation completed")
        
        # Parse results and put them in queue instead of saving directly
        table = utils.parse_validation_results(validation_results, elapsed_time, file_name)
        if parallelize == True and method == "ThreadPoolExecutor":
            results_queue.put((file_name, table))
        elif parallelize == True and method == "ProcessPoolExecutor":  
            return (file_name,table)
        elif parallelize == False:
            utils.save_processed_log(path_logs,project,stage,table,file_name)
            return True
        else:
            raise ValueError("Not supported process")
    except Exception as e:
        logger.error(f"Error validating {dataset.get('name', 'unknown dataset')}: {str(e)}")
        return False
    

def run_sequential_validation(sources: dict,config: dict,utils: object,create_expectations_instance:object,
                                    project: str ,sub_project: str,stage: str, path_logs: str,data_source_info: dict,expectations: dict,
                                  parallelize: bool,method: str):
    """
    Executes validation tasks sequentially for each dataset in the sources.

    This function processes each source and its datasets one at a time, performing
    validation tasks in a sequential manner without parallelization.

    Parameters:
        sources :dict 
            Dictionary containing source configurations and their datasets
        config :dict
            Global configuration dictionary
        utils : object 
            Utility class instance for helper functions
        create_expectations_instance : object
            Instance for creating expectations
        project :str
            Project identifier
        sub_project :str
            Sub-project identifier
        stage :str
            Processing stage (e.g., 'ingesta')
        path_logs :str
            Path to store validation logs
        data_source_info :dict
            Information about data sources
        expectations :dict
            Validation expectations configuration
        parallelize :bool
            Flag for parallelization (should be False for this method)
        method :str
            Parallelization method (not used in sequential processing)

    Returns:
        None

    Notes:
        - Logs execution time for the entire validation process
        - Processes each dataset sequentially
        - Suitable for small datasets or when resource usage needs to be controlled
    """
    logger.info("Running sequential validation") 
    start_time = time.time()  
    for source in sources:
        if sources[source] != {}:    
            source_config = sources[source]
        
            if source_config != None:
                datasets = source_config["datasets"]
            else:
                pass


            for dataset in datasets:
                run_validation_dataset(config=config,source_config=source_config,dataset=dataset,
                                       utils=utils,
                                       create_expectations_instance=create_expectations_instance,
                                       sources=sources,
                                        project=project ,
                                        sub_project=sub_project,
                                        stage=stage, 
                                        path_logs=path_logs,
                                        data_source_info=data_source_info,
                                        expectations=expectations,
                                        parallelize=parallelize,
                                        method=method)
    end_time = time.time()
    logger.info(f"Total execution time: {end_time - start_time}")



def thread_parallel_validation(sources,config,utils,create_expectations_instance,
                                    project ,sub_project,stage, path_logs,data_source_info,expectations,
                                   parallelize,method):
    
    """
    Executes validation tasks in parallel using thread-based parallelization.

    This function processes multiple datasets concurrently using ThreadPoolExecutor,
    making it suitable for I/O-bound validation tasks.

    Parameters:
        sources :dict
            Dictionary containing source configurations and their datasets
        config :dict
            Global configuration dictionary
        utils : object 
            Utility class instance for helper functions
        create_expectations_instance : object
            Instance for creating expectations
        project :str
            Project identifier
        sub_project :str
            Sub-project identifier
        stage :str
            Processing stage (e.g., 'ingesta')
        path_logs :str
            Path to store validation logs
        data_source_info :dict
            Information about data sources
        expectations :dict
            Validation expectations configuration
        parallelize :bool
            Flag for parallelization (should be False for this method)
        method :str
            Parallelization method (not used in sequential processing)
        
        
    Returns:
        None

    Notes:
        - Uses Python's ThreadPoolExecutor for parallel processing
        - Suitable for I/O-bound operations
        - Thread-safe queue implementation for collecting results
        - Creates necessary directories for storing results
        - Logs total execution time

    """
    logger.info("Running validation thread parallel")      
    start_time = time.time()
    results_queue = queue.Queue()
    
    # Create results directory
    lots_folder = Path(path_logs) / 'csv'
    lots_folder.mkdir(parents=True, exist_ok=True)

    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        
        for source, source_config in sources.items():
            if not source_config:
                continue
                
            datasets = source_config.get("datasets", [])
            if not datasets:
                continue

            for dataset in datasets:
                future = executor.submit(
                    run_validation_dataset,
                    config=config,
                    source_config=source_config,
                    dataset=dataset,
                    utils=utils,
                    create_expectations_instance=create_expectations_instance,
                    sources=sources,
                    project=project ,
                    sub_project=sub_project,
                    stage=stage, 
                    path_logs=path_logs,
                    data_source_info=data_source_info,
                    expectations=expectations,
                    parallelize=parallelize,
                    method=method,
                    results_queue=results_queue
                )
                futures.append(future)

        # Wait for all validations to complete
        completed_futures = concurrent.futures.wait(futures)
        
        # Process results from the queue and save them
        results = []
        while not results_queue.empty():
            file_name, table = results_queue.get()
            results.append((file_name, table))
        
        # Save all results in a thread-safe manner
        for file_name, table in results:
            utils.save_processed_log(path_logs, project, stage, table, file_name)

    end_time = time.time()
    logger.info(f"Total execution time: {end_time - start_time}")

def init_worker():
    """Initialize worker process to ignore SIGINT"""
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def worker_function(config, source_config, dataset,utils,create_expectations_instance,
                   sources,project ,sub_project,stage, path_logs,data_source_info,expectations,
                                   parallelize,method):
    """Worker function to be run in separate processes"""
    try:
        logger.info(f"Starting validation for dataset: {dataset}")
        result = run_validation_dataset(
            config=config,
            source_config=source_config,
            dataset=dataset,
            utils=utils,
            create_expectations_instance=create_expectations_instance,
            sources=sources,
            project=project ,
            sub_project=sub_project,
            stage=stage, 
            path_logs=path_logs,
            data_source_info=data_source_info,
            expectations=expectations,
            parallelize=parallelize,
            method=method
        )
        logger.info(f"Completed validation for dataset: {dataset}")
        return result
    except Exception as e:
        logger.error(f"Error processing dataset {dataset}: {str(e)}")
        return None, str(e)

def processes_parallel_validation(sources,config,utils,create_expectations_instance,
                                    project ,sub_project,stage, path_logs,data_source_info,expectations,
                                   parallelize,method,timeout_per_dataset=3600):
    """
    Executes validation tasks in parallel using process-based parallelization.

    This function processes multiple datasets concurrently using ProcessPoolExecutor,
    making it suitable for CPU-bound validation tasks. Includes timeout handling and
    proper process management.

    Parameters:
        sources :dict: 
            Dictionary containing source configurations and their datasets
        config :dict: 
            Global configuration dictionary
        utils : object 
            Utility class instance for helper functions
        create_expectations_instance : object
            Instance for creating expectations
        project :str: 
            Project identifier
        sub_project :str: 
            Sub-project identifier
        stage :str: 
            Processing stage (e.g., 'ingesta')
        path_logs :str) 
            Path to store validation logs
        data_source_info :dict: 
            Information about data sources
        expectations :dict: 
            Validation expectations configuration
        parallelize :bool: 
            Flag for parallelization (should be False for this method)
        method :str: 
            Parallelization method (not used in sequential processing)
        timeout_per_dataset :int (optional)
            Maximum execution time per dataset in seconds. 
                                           Defaults to 3600 (1 hour)
        
                                           
    Returns:
        list: List of tuples containing (file_name, table) for successfully processed datasets

    Notes:
        - Uses Python's multiprocessing Pool for parallel processing
        - Implements proper process initialization and signal handling
        - Includes timeout handling per dataset
        - Handles keyboard interrupts gracefully
        - Creates necessary directories for storing results
        - Provides detailed logging of success/failure for each dataset
        - Manages process cleanup in case of errors
        - Logs execution statistics including total time and success rate



    """
    logger.info("Running validation processes parallel") 
    start_time = time.time()
    
    # Create results directory
    lots_folder = Path(path_logs) / 'csv'
    lots_folder.mkdir(parents=True, exist_ok=True)


    
    # Prepare tasks list with explicit logging
    tasks = []
    logger.info("Preparing validation tasks...")
    for source, source_config in sources.items():
        if not source_config:
            logger.debug(f"Skipping empty source config for {source}")
            continue
            
        datasets = source_config.get("datasets", [])
        if not datasets:
            logger.debug(f"No datasets found for source {source}")
            continue

        for dataset in datasets:
            tasks.append((config, source_config, dataset,utils,create_expectations_instance,sources,
                                    project ,sub_project,stage, path_logs,data_source_info,expectations,
                                   parallelize,method))
    
    logger.info(f"Found {len(tasks)} tasks to process")
    
    # Set up multiprocessing
    num_processes = min(mp.cpu_count(), len(tasks))
    logger.info(f"Using {num_processes} processes")
    
    results = []
    
    try:
        # Using Pool with context manager for proper cleanup
        with contextlib.closing(mp.Pool(processes=num_processes, initializer=init_worker)) as pool:
            # Create async result object
            async_results = []
            
            # Submit all tasks
            for task in tasks:
                async_result = pool.apply_async(worker_function, task)
                async_results.append((task[2], async_result))  # Store dataset name with async result
            # Collect results with timeout
            for dataset, async_result in async_results:
                try:
                    result = async_result.get(timeout=timeout_per_dataset)
                    if result[0] is not None:  # If we have a valid result
                        results.append(result)
                        logger.info(f"Successfully processed dataset: {dataset}")
                    else:
                        logger.warning(f"Failed to process dataset: {dataset}")
                except mp.TimeoutError:
                    logger.error(f"Timeout occurred for dataset: {dataset}")
                except Exception as e:
                    logger.error(f"Error processing dataset {dataset}: {str(e)}")
            
            # Explicitly close and join pool
            pool.close()
            pool.join()
            
    except KeyboardInterrupt:
        logger.warning("Received keyboard interrupt, terminating processes...")
        if 'pool' in locals():
            pool.terminate()
            pool.join()
        raise
    except Exception as e:
        logger.error(f"Unexpected error in parallel processing: {str(e)}")
        if 'pool' in locals():
            pool.terminate()
            pool.join()
        raise
    finally:
        # Save successful results even if we had some failures
        logger.info(f"Saving {len(results)} successful results...")
        for file_name, table in results:
            try:
                utils.save_processed_log(path_logs, project, stage, table, file_name)
            except Exception as e:
                logger.error(f"Error saving result {file_name}: {str(e)}")

    end_time = time.time()
    total_time = end_time - start_time
    logger.info(f"Total execution time: {total_time:.2f} seconds")
    logger.info(f"Successfully processed {len(results)} out of {len(tasks)} tasks")
    
    return results




if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    #Set up global variables
    global utils
    global create_expectations_instance
    global project 
    global sub_project,stage
    global path_logs
    global data_source_info
    global expectations
    global sources
    global parallelize
    global method
     # Set up Great Expectations logger to a higher level to suppress these messages
    ge_logger = logging.getLogger('great_expectations')
    ge_logger.setLevel(logging.WARNING)  # This will only show WARNING and above

    logger.info("Loading configuration file")
    #load configuration file
    path_config_file = sys.argv[1]#"../config_templates/multiple_datasets_prueba.yml"
    with open(path_config_file) as f:
                config = yaml.load(f, Loader=yaml.FullLoader)
    logger.info("Configuration loaded")
    create_expectations_instance= CreateExpectations()
    utils = Utils()

    (project ,sub_project,stage, path_logs,data_source_info,expectations,sources,parallelize,method) = utils.define_global_parameters(config)

    
    if parallelize == True and method == "ThreadPoolExecutor":
        thread_parallel_validation(sources,config,utils,create_expectations_instance,
                                    project ,sub_project,stage, path_logs,data_source_info,expectations,
                                   parallelize,method) 
    elif parallelize == True and method == "ProcessPoolExecutor":  
        processes_parallel_validation(sources,config,utils,create_expectations_instance,
                                    project ,sub_project,stage, path_logs,data_source_info,expectations,
                                   parallelize,method)
    elif parallelize == False:
        run_sequential_validation(sources,config,utils,create_expectations_instance,
                                    project ,sub_project,stage, path_logs,data_source_info,expectations,
                                   parallelize,method)
        
    else:
            raise ValueError("Not supported process")