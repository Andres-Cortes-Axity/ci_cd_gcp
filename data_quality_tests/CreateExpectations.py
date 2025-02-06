class CreateExpectations:
    def __init__(self):
        self.categorized_expectations_dict = {
    # Consistency - Checking for data that should match or follow specific relationships
    'ExpectColumnPairValuesToBeEqual': {'is_critical': 1, 'dimension': 'Consistency'},
    'ExpectMulticolumnSumToEqual': {'is_critical': 1, 'dimension': 'Consistency'},

    # Consistency - Checking for structural consistency
    'ExpectTableColumnCountToBeBetween': {'is_critical': 0, 'dimension': 'Consistency'},
    'ExpectTableColumnCountToEqual': {'is_critical': 0, 'dimension': 'Consistency'},
    'ExpectTableColumnsToMatchOrderedList': {'is_critical': 0, 'dimension': 'Consistency'},
    'ExpectTableColumnsToMatchSet': {'is_critical': 1, 'dimension': 'Consistency'},
    'ExpectTableRowCountToBeBetween': {'is_critical': 1, 'dimension': 'Consistency'},
    'ExpectTableRowCountToEqual': {'is_critical': 1, 'dimension': 'Consistency'},
    'ExpectTableRowCountToEqualOtherTable': {'is_critical': 0, 'dimension': 'Consistency'},
    
    # Completeness - Checking for presence/absence of data
    'ExpectColumnValuesToBeNull': {'is_critical': 0, 'dimension': 'Completeness'},
    'ExpectColumnValuesToNotBeNull': {'is_critical': 1, 'dimension': 'Completeness'},
    'ExpectColumnToExist': {'is_critical': 1, 'dimension': 'Completeness'},
    
    # Uniqueness - Checking for duplicate values
    'ExpectColumnProportionOfUniqueValuesToBeBetween': {'is_critical': 0, 'dimension': 'Uniqueness'},
    'ExpectColumnUniqueValueCountToBeBetween': {'is_critical': 0, 'dimension': 'Uniqueness'},
    'ExpectColumnValuesToBeUnique': {'is_critical': 1, 'dimension': 'Uniqueness'},
    'ExpectCompoundColumnsToBeUnique': {'is_critical': 1, 'dimension': 'Uniqueness'},
    'ExpectSelectColumnValuesToBeUniqueWithinRecord': {'is_critical': 1, 'dimension': 'Uniqueness'},
    
    # Validity - Checking if values conform to specified formats/rules
    'ExpectColumnDistinctValuesToBeInSet': {'is_critical': 0, 'dimension': 'Validity'},
    'ExpectColumnDistinctValuesToContainSet': {'is_critical': 0, 'dimension': 'Validity'},
    'ExpectColumnDistinctValuesToEqualSet': {'is_critical': 0, 'dimension': 'Validity'},
    'ExpectColumnMostCommonValueToBeInSet': {'is_critical': 0, 'dimension': 'Validity'},
    'ExpectColumnPairValuesToBeInSet': {'is_critical': 0, 'dimension': 'Validity'},
    'ExpectColumnValuesToBeInSet': {'is_critical': 1, 'dimension': 'Validity'},
    'ExpectColumnValuesToNotBeInSet': {'is_critical': 0, 'dimension': 'Validity'},
    'ExpectColumnValuesToBeInTypeList': {'is_critical': 0, 'dimension': 'Validity'},
    'ExpectColumnValuesToBeOfType': {'is_critical': 1, 'dimension': 'Validity'},
    'ExpectColumnValueLengthsToBeBetween': {'is_critical': 1, 'dimension': 'Validity'},
    'ExpectColumnValueLengthsToEqual': {'is_critical': 0, 'dimension': 'Validity'},
    'ExpectColumnValuesToMatchLikePattern': {'is_critical': 0, 'dimension': 'Validity'},
    'ExpectColumnValuesToMatchLikePatternList': {'is_critical': 0, 'dimension': 'Validity'},
    'ExpectColumnValuesToMatchRegex': {'is_critical': 1, 'dimension': 'Validity'},
    'ExpectColumnValuesToMatchRegexList': {'is_critical': 0, 'dimension': 'Validity'},
    'ExpectColumnValuesToNotMatchLikePattern': {'is_critical': 0, 'dimension': 'Validity'},
    'ExpectColumnValuesToNotMatchLikePatternList': {'is_critical': 0, 'dimension': 'Validity'},
    'ExpectColumnValuesToNotMatchRegex': {'is_critical': 0, 'dimension': 'Validity'},
    'ExpectColumnValuesToNotMatchRegexList': {'is_critical': 0, 'dimension': 'Validity'},
    
    # Accuracy - Checking for statistical properties and numerical accuracy
    'ExpectColumnKLDivergenceToBeLessThan': {'is_critical': 0, 'dimension': 'Accuracy'},
    'ExpectColumnPairValuesAToBeGreaterThanB': {'is_critical': 0, 'dimension': 'Accuracy'},
    'ExpectColumnStdevToBeBetween': {'is_critical': 0, 'dimension': 'Accuracy'},
    'ExpectColumnSumToBeBetween': {'is_critical': 0, 'dimension': 'Accuracy'},
    'ExpectColumnValueZScoresToBeLessThan': {'is_critical': 0, 'dimension': 'Accuracy'},
    'ExpectColumnValuesToBeBetween': {'is_critical': 1, 'dimension': 'Accuracy'},
    'ExpectColumnMaxToBeBetween': {'is_critical': 0, 'dimension': 'Accuracy'},
    'ExpectColumnMeanToBeBetween': {'is_critical': 0, 'dimension': 'Accuracy'},
    'ExpectColumnMedianToBeBetween': {'is_critical': 0, 'dimension': 'Accuracy'},
    'ExpectColumnMinToBeBetween': {'is_critical': 0, 'dimension': 'Accuracy'},
    'ExpectColumnQuantileValuesToBeBetween': {'is_critical': 0, 'dimension': 'Accuracy'}
    

}

    def add_missingness_expectations(self,missingness_expectations_dict:dict,ge:object, suite:object, project: str, source: str, stage: str,subproject:str):
        """
        Adds null and not-null expectations to a Great Expectations suite based on provided dictionary.

        :param: missingness_expectations_dict : dict
            A dictionary containing two keys:
            - "ExpectColumnValuesToBeNull": List of tuples, each containing:
                - column name (str): name of column to be tested
                - threshold value (float between 0 and 1): represents the minimum fraction of rows that should meet the expectation, 0.7 means at least 70% of values should be null
            - "ExpectColumnValuesToNotBeNull": List of tuples, each containing:  
                - column name (str): name of column to be tested
                - threshold value (float between 0 and 1): represents the minimum fraction of rows that should meet the expectation,0.7 means at least 70% of values should not be null
        :param ge: module of Great Expectations
        :param suite: global great expectations suite used to group the expectations
        :returns: None
        :example:
        --------
        expectations_dict = {
            "ExpectColumnValuesToBeNull": [
                    ("rate_code_id", 1),      # Expects rate_code_id to be 100% null
                    ("store_and_fwd_flag", 0.1)  # Expects store_and_fwd_flag to be at least 10% null
            ],
            "ExpectColumnValuesToNotBeNull": [
                ("vendor_id", 1),         # Expects vendor_id to be 100% not null
                ("total_amount", 0.5)     # Expects total_amount to be at least 50% not null
            ]
        }
        """
        # Check for null expectations and add them to the suite
        if "ExpectColumnValuesToBeNull" in missingness_expectations_dict.keys() and missingness_expectations_dict["ExpectColumnValuesToBeNull"]:
            for col in missingness_expectations_dict["ExpectColumnValuesToBeNull"]:
                suite.add_expectation(
                    ge.expectations.ExpectColumnValuesToBeNull(column=col[0],mostly=col[1],
                                                               meta={"expectation_type": "missingness", 
                                                                     "project":project, "source":source, "stage":stage, 
                                                                     "is_critical":self.categorized_expectations_dict['ExpectColumnValuesToBeNull']['is_critical'],
                                                                     "dama_dimension":self.categorized_expectations_dict['ExpectColumnValuesToBeNull']['dimension'],
                                                                     "sub_project":subproject})
                    )
        # Check for not-null expectations and add them to the suite
        if "ExpectColumnValuesToNotBeNull" in missingness_expectations_dict.keys() and missingness_expectations_dict["ExpectColumnValuesToNotBeNull"]:
            for col in missingness_expectations_dict["ExpectColumnValuesToNotBeNull"]:
                suite.add_expectation(
                    ge.expectations.ExpectColumnValuesToNotBeNull(column=col[0],mostly=col[1],
                                                                  meta={"expectation_type": "missingness", 
                                                                        "project":project, "source":source, "stage":stage,
                                                                        "is_critical":self.categorized_expectations_dict['ExpectColumnValuesToNotBeNull']['is_critical'],
                                                                        "dama_dimension":self.categorized_expectations_dict['ExpectColumnValuesToNotBeNull']['dimension'],
                                                                     "sub_project":subproject})
                                                                  )

    def add_data_integrity_expectations(self,data_integrity_dict:dict, ge : object, suite: object, project: str, source: str, stage: str,subproject:str):
        """

        Adds data integrity expectations to a Great Expectations suite based on provided dictionary.
            :param data_integrity_dict:
                A dictionary containing two keys:
                    - "ExpectColumnPairValuesToBeEqual": List of tuples, each containing:
                        - column_A (str): The first column name.
                        - column_B (str): The second column name.
                        - mostly(threshold value, a float between 0 and 1): represents the minimum fraction of rows that should meet the expectation of column_A to be the same as column_B, 0.7 means at least 70% of values should be equals.
                    - "ExpectMulticolumnSumToEqual": List of tuples, each containing:
                        - column_list (tuple or list): Set of columns to be checked.
                        - sum_total (int or float): Expected sum of columns.
                        - threshold value (float between 0 and 1): represents the minimum fraction of rows that should meet the expectation,0.7 means the sum of values for each column in column_list must be equals to sum_total at least in the 70% of the rows.
            :param ge: module of Great Expectations
            :param suite: global great expectations suite used to group the expectations
            :returns: None
            :example:
                --------   
                data_integrity_dict = {"ExpectColumnPairValuesToBeEqual":[
                    ("fare_amount","total_amount",0.7) # Expect the values in column "fare_amount" to be the same as column "total_amount" in at least the 70% of the rows.
                    ], 
                    "ExpectMulticolumnSumToEqual": [
                    (["vendor_id", "passenger_count"],2,0.33) #Expect that the sum of "vendor_id", "passenger_count" for at least in the 33% of the rows values to be equal to 2 .
                    ]
            }
        """
        # Check for column entries to be strings with length between a minimum value and a maximum value (inclusive). This expectation only works for string-type values. 
        if "ExpectColumnPairValuesToBeEqual" in data_integrity_dict.keys() and data_integrity_dict["ExpectColumnPairValuesToBeEqual"]:
            for col in data_integrity_dict["ExpectColumnPairValuesToBeEqual"]:
                suite.add_expectation(
                    ge.expectations.ExpectColumnPairValuesToBeEqual(column_A=col[0],column_B=col[1],mostly=col[2], 
                                                                    meta={"expectation_type": "data_integrity", 
                                                                          "project":project, "source":source, "stage":stage,
                                                                          "is_critical":self.categorized_expectations_dict['ExpectColumnPairValuesToBeEqual']['is_critical'],
                                                                          "dama_dimension":self.categorized_expectations_dict['ExpectColumnPairValuesToBeEqual']['dimension'],
                                                                          "sub_project":subproject})
                                                                    )
        # Check for not-null expectations and add them to the suite
        if "ExpectMulticolumnSumToEqual" in data_integrity_dict.keys() and data_integrity_dict["ExpectMulticolumnSumToEqual"]:
            for col in data_integrity_dict["ExpectMulticolumnSumToEqual"]:
                suite.add_expectation(
                    ge.expectations.ExpectMulticolumnSumToEqual(column_list=col[0],sum_total=col[1],mostly=col[2],
                                                                meta={"expectation_type": "data_integrity", 
                                                                      "project":project, "source":source, "stage":stage,
                                                                      "is_critical":self.categorized_expectations_dict['ExpectMulticolumnSumToEqual']['is_critical'],
                                                                      "dama_dimension":self.categorized_expectations_dict['ExpectMulticolumnSumToEqual']['dimension']
                                                                      ,"sub_project":subproject})
                                                                )

    def add_cardinality_expectations(self,cardinality_expectations:dict, ge : object, suite: object, project: str, source: str, stage: str,subproject:str):
        '''
        Adds data integrity expectations to a Great Expectations suite based on provided dictionary.
            :param data_integrity_dict:
                A dictionary containing two keys:
                    - "ExpectColumnProportionOfUniqueValuesToBeBetween": List of tuples, each containing:
                        - column (str): The column name.
                        - min_value (float or None): The minimum proportion of unique values (Proportions are on the range 0 to 1).
                        - max_value (float or None): The maximum proportion of unique values (Proportions are on the range 0 to 1).
                        - strict_min (boolean): If True, the minimum proportion of unique values must be strictly larger than min_value. default=False
                        - strict_max (boolean): If True, the maximum proportion of unique values must be strictly smaller than max_value. default=False
                    - "ExpectColumnUniqueValueCountToBeBetween": List of tuples, each containing:
                        - column (str): The column name.
                        - min_value (int or None): The minimum number of unique values allowed.
                        - max_value (int or None): The maximum number of unique values allowed.
                        - strict_min (bool): If True, the column must have strictly more unique value count than min_value to pass.
                        - strict_max (bool): If True, the column must have strictly fewer unique value count than max_value to pass.
                    - "ExpectColumnValuesToBeUnique"
                        - column (str): The column name.
                    - "ExpectCompoundColumnsToBeUnique"
                        - column_list (tuple or list): Set of columns to be checked.
                    - "ExpectSelectColumnValuesToBeUniqueWithinRecord"
                        - column_list (tuple or list): The column names to evaluate.
            :param ge: module of Great Expectations
            :param suite: global great expectations suite used to group the expectations
            :returns: None
            :example:
                    cardinality_expectations_dict = {
                                # Expect the values ​​in the "payment_type" column to have a proportion between 0 to 0.01 cn with respect to the total values
                                 "ExpectColumnProportionOfUniqueValuesToBeBetween":[("payment_type",0,0.01)],
                                # Expect the values ​​in the "pickup_location_id" column to be between 1 and 250 values ​​relative to the total values
                                 "ExpectColumnUniqueValueCountToBeBetween":[("pickup_location_id",1,250,False,True)],
                                # Expect values ​​in the "pickup_datatime" column to be unique
                                 "ExpectColumnValuesToBeUnique":[(["pickup_datetime"])],
                                # Expect the values ​​in the "pickup_datatime" and "dropoff_datatime" columns to be unique for each row
                                 "ExpectCompoundColumnsToBeUnique":[(["pickup_datetime", "dropoff_datetime"])],
                                # Expect the values ​​in the "pickup_datatime" and "dropoff_datatime" columns to be unique for each record
                                 "ExpectSelectColumnValuesToBeUniqueWithinRecord":[(["pickup_datetime", "dropoff_datetime"])]
                                 }
        '''
        
        #Check validates that the proportion of unique values in a column falls within a specified range.
        if "ExpectColumnProportionOfUniqueValuesToBeBetween" in cardinality_expectations.keys() and cardinality_expectations["ExpectColumnProportionOfUniqueValuesToBeBetween"]:
            for col in cardinality_expectations["ExpectColumnProportionOfUniqueValuesToBeBetween"]:
                suite.add_expectation(
                    ge.expectations.ExpectColumnProportionOfUniqueValuesToBeBetween(column=col[0],min_value=col[1],max_value=col[2],strict_min=col[3] if len(col) > 3 else False,strict_max=col[4]if len(col) > 4 else False, 
                                                                                    meta={"expectation_type": "cardinality", 
                                                                                          "project":project, "source":source, "stage":stage,
                                                                                          "is_critical":self.categorized_expectations_dict['ExpectColumnProportionOfUniqueValuesToBeBetween']['is_critical']
                                                                                          ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnProportionOfUniqueValuesToBeBetween']['dimension']
                                                                                          ,"sub_project":subproject})
                                                                                    )
        
        #Check expectation validates that the number of unique values in a column falls within a specified range.
        if "ExpectColumnUniqueValueCountToBeBetween" in cardinality_expectations.keys() and  cardinality_expectations["ExpectColumnUniqueValueCountToBeBetween"]:
            for col in cardinality_expectations["ExpectColumnUniqueValueCountToBeBetween"]:
                suite.add_expectation(
                    ge.expectations.ExpectColumnUniqueValueCountToBeBetween(column=col[0],min_value=col[1],max_value=col[2],strict_min=col[3] if len(col) > 3 else False,strict_max=col[4]if len(col) > 4 else False,
                                                                             meta={"expectation_type": "cardinality", 
                                                                                   "project":project, "source":source, "stage":stage,
                                                                                  "is_critical":self.categorized_expectations_dict['ExpectColumnUniqueValueCountToBeBetween']['is_critical']
                                                                                  ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnUniqueValueCountToBeBetween']['dimension']
                                                                                  ,"sub_project":subproject})
                                                                             )
        
        #checks for duplicate values in a column, flagging any duplicates as exceptions.
        if "ExpectColumnValuesToBeUnique" in cardinality_expectations.keys() and cardinality_expectations["ExpectColumnValuesToBeUnique"]:
            for col in cardinality_expectations["ExpectColumnValuesToBeUnique"]:
                suite.add_expectation(
                    ge.expectations.ExpectColumnValuesToBeUnique(column=col[0], 
                                                                 meta={"expectation_type": "cardinality", 
                                                                       "project":project, "source":source, "stage":stage,
                                                                       "is_critical":self.categorized_expectations_dict['ExpectColumnValuesToBeUnique']['is_critical']
                                                                       ,
                                                                       "dama_dimension":self.categorized_expectations_dict['ExpectColumnValuesToBeUnique']['dimension']
                                                                       ,"sub_project":subproject})
                    )
        
        #checks for unique row-wise combinations across multiple columns, flagging duplicates as exceptions.
        if "ExpectCompoundColumnsToBeUnique" in cardinality_expectations.keys() and cardinality_expectations["ExpectCompoundColumnsToBeUnique"]:
            for col in cardinality_expectations["ExpectCompoundColumnsToBeUnique"]:
                suite.add_expectation(ge.expectations.ExpectCompoundColumnsToBeUnique(column_list=col, 
                                                                                      meta={"expectation_type": "cardinality",
                                                                                             "project":project, "source":source, "stage":stage,
                                                                                             "is_critical":self.categorized_expectations_dict['ExpectCompoundColumnsToBeUnique']['is_critical']
                                                                                             ,"dama_dimension":self.categorized_expectations_dict['ExpectCompoundColumnsToBeUnique']['dimension']
                                                                                             ,"sub_project":subproject}))
        
        #checks that the values within specified columns are unique for each record, allowing for duplicated records.
        if "ExpectSelectColumnValuesToBeUniqueWithinRecord" in cardinality_expectations.keys() and cardinality_expectations["ExpectSelectColumnValuesToBeUniqueWithinRecord"]:    
            for col in cardinality_expectations["ExpectSelectColumnValuesToBeUniqueWithinRecord"]:
                suite.add_expectation(ge.expectations.ExpectSelectColumnValuesToBeUniqueWithinRecord(column_list=col, 
                                                                                                     meta={"expectation_type": "cardinality", 
                                                                                                           "project":project, "source":source, "stage":stage,
                                                                                                           "is_critical":self.categorized_expectations_dict['ExpectSelectColumnValuesToBeUniqueWithinRecord']['is_critical']
                                                                                                           ,"dama_dimension":self.categorized_expectations_dict['ExpectSelectColumnValuesToBeUniqueWithinRecord']['dimension']
                                                                                                           ,"sub_project":subproject}))
                
    def add_sets_expectation(self,sets_expectation:dict, ge : object, suite: object, project: str, source: str, stage: str,subproject:str):
        '''
        Adds data integrity expectations to a Great Expectations suite based on provided dictionary.
            :param data_integrity_dict:
                A dictionary containing two keys:
                    - "ExpectColumnDistinctValuesToBeInSet": List of tuples, each containing:
                        - column (str): The column name.
                        - value_set (set-like): A set of objects used for comparison
                    - "ExpectColumnDistinctValuesToContainSet": List of tuples, each containing:
                        - column (str): The column name.
                        - value_set (set-like): A set of objects used for comparison
                    - "ExpectColumnDistinctValuesToEqualSet"
                        - column (str): The column name.
                        - value_set (set-like): A set of objects used for comparison
                    - "ExpectColumnMostCommonValueToBeInSet"
                        - column (str): The column name.
                        - value_set (set-like): A set of objects used for comparison
                    - "ExpectColumnPairValuesToBeInSet"
                        - column_A (str): The first column name.
                        - column_B (str): The second column name.
                        - value_pairs_set (list of tuples): All the valid pairs to be matched.
                    - "ExpectColumnValuesToBeInSet"
                        - column (str): The column name.
                        - value_set (set-like): A set of objects used for comparison
                    - "ExpectColumnValuesToNotBeInSet"
                        - column (str): The column name.
                        - value_set (set-like): A set of objects used for comparison
            :param ge: module of Great Expectations
            :param suite: global great expectations suite used to group the expectations
            :returns: None
            :example:
                     sets_expectation_dict =  {
                         #Expect that in the "extra" column it only has the following values [-36.71,-1.0,-0.5,0.0,0.5,0.8,1.0,4.5]
                         "ExpectColumnDistinctValuesToBeInSet":[("extra",[-36.71,-1.0,-0.5,0.0,0.5,0.8,1.0,4.5])],
                         #Expects the "passenger" column to contain the following values[1,2,4,5]
                         "ExpectColumnDistinctValuesToContainSet":[("passenger_count",[1,2,4,5])],
                         #Expect that in the "extra" column it only has the following values [0,0.5,1]
                         "ExpectColumnDistinctValuesToEqualSet":[("extra",[0,0.5,1])],
                         #Expects that in the "passenger" column the most common value is found in the following values [2,3,1]
                         "ExpectColumnMostCommonValueToBeInSet":[("passenger_count",[2,3,1])],
                         #Expects the values ​​of the "passenger_count" and "passenger_type" columns to be more than 50% between the following combinations
                         "ExpectColumnPairValuesToBeInSet":[("payment_type","passenger_count",[(1,1),(1,2),(1,3),(1,4)],0.5)],
                         #Expects the values ​​of the "passenger_type" column to be in the following elements [1,2,3,4,5,6]
                         "ExpectColumnValuesToBeInSet":[("passenger_count",[1,2,3,4,5,6])],
                         #Expects that the values ​​of the column "passenger_type" will not be found in the following elements [7,8,9]
                         "ExpectColumnValuesToNotBeInSet":[("passenger_count",[7,8,9])]
                         }
        '''
        #checks that all distinct values in a column are contained within a specified set.
        if "ExpectColumnDistinctValuesToBeInSet" in sets_expectation.keys() and sets_expectation["ExpectColumnDistinctValuesToBeInSet"]:
            for col in sets_expectation["ExpectColumnDistinctValuesToBeInSet"]:
                suite.add_expectation(ge.expectations.ExpectColumnDistinctValuesToBeInSet(column=col[0], value_set=col[1], 
                                                                                          meta={"expectation_type": "sets", 
                                                                                                "project":project, "source":source, "stage":stage,
                                                                                                "is_critical":self.categorized_expectations_dict['ExpectColumnDistinctValuesToBeInSet']['is_critical']
                                                                                                ,
                                                                                            "dama_dimension":self.categorized_expectations_dict['ExpectColumnDistinctValuesToBeInSet']['dimension']
                                                                                                ,"sub_project":subproject}))
        
        #checks that a column's distinct values include a specified set.
        if "ExpectColumnDistinctValuesToContainSet" in sets_expectation.keys() and sets_expectation["ExpectColumnDistinctValuesToContainSet"]:
            for col in sets_expectation["ExpectColumnDistinctValuesToContainSet"]:
                suite.add_expectation(ge.expectations.ExpectColumnDistinctValuesToContainSet(column=col[0], value_set=col[1], 
                                                                                             meta={"expectation_type": "sets", 
                                                                                                   "project":project, "source":source, "stage":stage,
                                                                                                   "is_critical":self.categorized_expectations_dict['ExpectColumnDistinctValuesToContainSet']['is_critical']
                                                                                                   ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnDistinctValuesToContainSet']['dimension']
                                                                                                   ,"sub_project":subproject}))
        
        #checks that the distinct values in a column exactly match a specified set.
        if "ExpectColumnDistinctValuesToEqualSet" in sets_expectation.keys() and sets_expectation["ExpectColumnDistinctValuesToEqualSet"]:
            for col in sets_expectation["ExpectColumnDistinctValuesToEqualSet"]:
                suite.add_expectation(ge.expectations.ExpectColumnDistinctValuesToEqualSet(column=col[0], value_set=col[1], 
                                                                                           meta={"expectation_type": "sets", 
                                                                                                 "project":project, "source":source, "stage":stage,
                                                                                                 "is_critical":self.categorized_expectations_dict['ExpectColumnDistinctValuesToEqualSet']['is_critical']
                                                                                                 , "dama_dimension":self.categorized_expectations_dict['ExpectColumnDistinctValuesToEqualSet']['dimension']
                                                                                                 ,"sub_project":subproject}))
        
        #Checks that the most common value in a column is present in a specified set of values.
        if  "ExpectColumnMostCommonValueToBeInSet" in sets_expectation.keys() and sets_expectation["ExpectColumnMostCommonValueToBeInSet"]:
            for col in sets_expectation["ExpectColumnMostCommonValueToBeInSet"]:
                suite.add_expectation(ge.expectations.ExpectColumnMostCommonValueToBeInSet(column=col[0], value_set=col[1], 
                                                                                           meta={"expectation_type": "sets", 
                                                                                                 "project":project, "source":source, "stage":stage,
                                                                                                 "is_critical":self.categorized_expectations_dict['ExpectColumnMostCommonValueToBeInSet']['is_critical']
                                                                                                 ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnMostCommonValueToBeInSet']['dimension']
                                                                                                 ,"sub_project":subproject}))
        
        #Verifies that pairs of values in two specific columns belong to a defined set.
        if "ExpectColumnPairValuesToBeInSet" in sets_expectation.keys() and sets_expectation["ExpectColumnPairValuesToBeInSet"]:
            for col in sets_expectation["ExpectColumnPairValuesToBeInSet"]:
                suite.add_expectation(ge.expectations.ExpectColumnPairValuesToBeInSet(column_A=col[0], column_B=col[1], value_pairs_set=col[2], mostly=col[3], 
                                                                                      meta={"expectation_type": "sets", 
                                                                                            "project":project, "source":source, "stage":stage,
                                                                                           "is_critical":self.categorized_expectations_dict['ExpectColumnPairValuesToBeInSet']['is_critical']
                                                                                           ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnPairValuesToBeInSet']['dimension']
                                                                                           ,"sub_project":subproject}))
        
        #Ensures that the values in a column belong to a specified set.
        if "ExpectColumnValuesToBeInSet" in sets_expectation.keys() and sets_expectation["ExpectColumnValuesToBeInSet"]:
            for col in sets_expectation["ExpectColumnValuesToBeInSet"]:
                suite.add_expectation(ge.expectations.ExpectColumnValuesToBeInSet(column=col[0], value_set=col[1],
                                                                                   meta={"expectation_type": "sets", 
                                                                                         "project":project, "source":source, "stage":stage,
                                                                                         "is_critical":self.categorized_expectations_dict['ExpectColumnValuesToBeInSet']['is_critical']
                                                                                         ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnValuesToBeInSet']['dimension']
                                                                                         ,"sub_project":subproject}))
        
        #Validates that the values in a column do not belong to a specified set.
        if "ExpectColumnValuesToNotBeInSet" in sets_expectation.keys() and  sets_expectation["ExpectColumnValuesToNotBeInSet"]:
            for col in sets_expectation["ExpectColumnValuesToNotBeInSet"]:
                suite.add_expectation(ge.expectations.ExpectColumnValuesToNotBeInSet(column=col[0], value_set=col[1], 
                                                                                     meta={"expectation_type": "sets", 
                                                                                           "project":project, "source":source, "stage":stage,
                                                                                           "is_critical":self.categorized_expectations_dict['ExpectColumnValuesToNotBeInSet']['is_critical']
                                                                                           ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnValuesToNotBeInSet']['dimension']
                                                                                           ,"sub_project":subproject}))
                
    def add_numerical_data_expectations(self,numerical_data_dict: dict, ge: object, suite: object, project: str, source: str, stage: str,subproject:str):
        """
            Adds numerical data expectations to a Great Expectations suite based on the provided dictionary.
    :param numerical_data_dict: 
        A dictionary containing various keys, each representing a different expectation for numerical data:
        
        - "ExpectColumnMaxToBeBetween": List of tuples, each containing:
            - column (str): The column name.
            - min_value (int or float): The expected minimum value for the column.
            - max_value (int or float): The expected maximum value for the column.
            - Expectation: The maximum value of the column should fall within the range [min_value, max_value].
        
        - "ExpectColumnMeanToBeBetween": List of tuples, each containing:
            - column (str): The column name.
            - min_value (int or float): The expected minimum mean value for the column.
            - max_value (int or float): The expected maximum mean value for the column.
            - Expectation: The mean value of the column should be between min_value and max_value.

        - "ExpectColumnMedianToBeBetween": List of tuples, each containing:
            - column (str): The column name.
            - min_value (int or float): The expected minimum median value for the column.
            - max_value (int or float): The expected maximum median value for the column.
            - Expectation: The median value of the column should be between min_value and max_value.

        - "ExpectColumnMinToBeBetween": List of tuples, each containing:
            - column (str): The column name.
            - min_value (int or float): The expected minimum value for the column.
            - max_value (int or float): The expected maximum value for the column.
            - Expectation: The minimum value of the column should be between min_value and max_value.

        - "ExpectColumnQuantileValuesToBeBetween": List of dictionaries, each containing:
            - column (str): The column name.
            - quantile_ranges (dict): A dictionary mapping quantile ranges to minimum and maximum values.
            - allow_relative_error (str): Optional error margin allowed when calculating quantiles.
            - Expectation: The values for the specified quantiles should fall between the provided ranges.

    :param ge: module of Great Expectations.
    :param suite: global Great Expectations suite used to group the expectations.
    
    :returns: None
    
    :example:
        --------
        numerical_data_dict = {
            "ExpectColumnMaxToBeBetween": [("passenger_count", 1, 6)], # Expect the maximum value in the "passenger_count" column to be between 1 and 6.
            "ExpectColumnMeanToBeBetween": [("trip_distance", 1.5, 4.0)], # Expect the mean value in the "trip_distance" column to be between 1.5 and 4.0.
            "ExpectColumnMedianToBeBetween": [("trip_distance", 1.0, 4.0)], # Expect the median value in the "trip_distance" column to be between 1.0 and 4.0.
            "ExpectColumnMinToBeBetween": [("trip_distance", 0.5, 1.0)], # Expect the minimum value in the "trip_distance" column to be between 0.5 and 1.0.
            "ExpectColumnQuantileValuesToBeBetween": [
                {
                    "column": "trip_distance", 
                    "quantile_ranges": {
                        "quantiles": [0, 0.25, 0.5, 0.75], 
                        "value_ranges": [[0, 1], [1, 2], [2, 3], [3, 6]]}, 
                    "allow_relative_error": False
                }
            ] # Expect the values at the quantiles [0, 0.25, 0.5, 0.75] for the "trip_distance" column to fall within the specified ranges.
        }
        
        """
        # Expect the column maximum to be between a minimum value and a maximum value.
        if "ExpectColumnMaxToBeBetween" in numerical_data_dict.keys() and numerical_data_dict["ExpectColumnMaxToBeBetween"]:
            for col in numerical_data_dict["ExpectColumnMaxToBeBetween"]:
                suite.add_expectation(ge.expectations.ExpectColumnMaxToBeBetween(column=col[0],min_value=col[1],max_value=col[2],
                                                                                 meta={"expectation_type": "Numerical_Data", 
                                                                                       "project":project, "source":source, "stage":stage,
                                                                                       "is_critical":self.categorized_expectations_dict['ExpectColumnMaxToBeBetween']['is_critical']
                                                                                       ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnMaxToBeBetween']['dimension']
                                                                                       ,"sub_project":subproject}))
               
                
        # Expect the column mean to be between a minimum value and a maximum value (inclusive).
        if "ExpectColumnMeanToBeBetween" in numerical_data_dict.keys() and numerical_data_dict["ExpectColumnMeanToBeBetween"]:
            for col in numerical_data_dict["ExpectColumnMeanToBeBetween"]:
                suite.add_expectation(ge.expectations.ExpectColumnMeanToBeBetween(column=col[0], min_value=col[1], max_value=col[2],
                                                                                  meta={"expectation_type": "Numerical_Data", 
                                                                                        "project":project, "source":source, "stage":stage,
                                                                                       "is_critical":self.categorized_expectations_dict['ExpectColumnMeanToBeBetween']['is_critical']
                                                                                       , "dama_dimension":self.categorized_expectations_dict['ExpectColumnMeanToBeBetween']['dimension']
                                                                                       ,"sub_project":subproject}))

                
        # Expect the column median to be between a minimum value and a maximum value.
        if "ExpectColumnMedianToBeBetween" in numerical_data_dict.keys() and  numerical_data_dict["ExpectColumnMedianToBeBetween"]:
            for col in numerical_data_dict["ExpectColumnMedianToBeBetween"]:
                suite.add_expectation(ge.expectations.ExpectColumnMedianToBeBetween(column=col[0], min_value=col[1], max_value=col[2],
                                                                                    meta={"expectation_type": "Numerical_Data", 
                                                                                          "project":project, "source":source, "stage":stage,
                                                                                          "is_critical":self.categorized_expectations_dict['ExpectColumnMedianToBeBetween']['is_critical']
                                                                                          ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnMedianToBeBetween']['dimension']
                                                                                          ,"sub_project":subproject}))
                
        # Expect the column minimum to be between a minimum value and a maximum value.
        if "ExpectColumnMinToBeBetween" in numerical_data_dict.keys() and numerical_data_dict["ExpectColumnMinToBeBetween"]:
            for col in numerical_data_dict["ExpectColumnMinToBeBetween"]:
                suite.add_expectation(ge.expectations.ExpectColumnMinToBeBetween(column=col[0],min_value=col[1],max_value=col[2],
                                                                                 meta={"expectation_type": "Numerical_Data", 
                                                                                       "project":project, "source":source, "stage":stage,
                                                                                          "is_critical":self.categorized_expectations_dict['ExpectColumnMinToBeBetween']['is_critical']
                                                                                          ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnMinToBeBetween']['dimension']
                                                                                          ,"sub_project":subproject}))
                
        # Expect the specific provided column quantiles to be between a minimum value and a maximum value.
        if "ExpectColumnQuantileValuesToBeBetween" in numerical_data_dict.keys() and  numerical_data_dict["ExpectColumnQuantileValuesToBeBetween"]:
            for col in numerical_data_dict["ExpectColumnQuantileValuesToBeBetween"]:
                suite.add_expectation(ge.expectations.ExpectColumnQuantileValuesToBeBetween( column=col["column"],quantile_ranges=col["quantile_ranges"],allow_relative_error=col["allow_relative_error"],
                                                                                            meta={"expectation_type": "Numerical_Data", 
                                                                                                  "project":project, "source":source, "stage":stage,
                                                                                                  "is_critical":self.categorized_expectations_dict['ExpectColumnQuantileValuesToBeBetween']['is_critical']
                                                                                                  ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnQuantileValuesToBeBetween']['dimension']
                                                                                                  ,"sub_project":subproject}))
        
    def add_distribution_data_expectations(self, distribution_expectations_dict: dict, ge: object, suite: object, project: str, source: str, stage: str,subproject:str):
        """
        Adds distribution data expectations to a Great Expectations suite based on the provided dictionary.

        :param distribution_expectations_dict: 
            A dictionary containing various keys, each representing a different expectation for distribution data:
            
            - "ExpectColumnKLDivergenceToBeLessThan": List of tuples, each containing:
                - column (str): The column name.
                - threshold (float): The maximum allowed Kullback-Leibler divergence value.
                - Expectation: The Kullback-Leibler divergence for the specified column should be less than the threshold.

            - "ExpectColumnPairValuesAToBeGreaterThanB": List of tuples, each containing:
                - column_A (str): The first column name.
                - column_B (str): The second column name.
                - or_equal (bool): Whether column_A is expected to be greater than or equal to column_B.
                - Expectation: The values in column_A should be greater than (or equal to) the values in column_B.

            - "ExpectColumnStdevToBeBetween": List of tuples, each containing:
                - column (str): The column name.
                - min_value (float): The expected minimum standard deviation value.
                - max_value (float): The expected maximum standard deviation value.
                - strict_min (bool): Whether the minimum value is strict (True) or inclusive (False).
                - strict_max (bool): Whether the maximum value is strict (True) or inclusive (False).
                - Expectation: The standard deviation of the column should be between min_value and max_value, according to the strictness.

            - "ExpectColumnSumToBeBetween": List of tuples, each containing:
                - column (str): The column name.
                - min_value (float): The expected minimum sum value.
                - max_value (float): The expected maximum sum value.
                - strict_min (bool): Whether the minimum value is strict (True) or inclusive (False).
                - strict_max (bool): Whether the maximum value is strict (True) or inclusive (False).
                - Expectation: The sum of the column values should be between min_value and max_value, according to the strictness.

            - "ExpectColumnValueZScoresToBeLessThan": List of tuples, each containing:
                - column (str): The column name.
                - threshold (float): The maximum allowed Z-score.
                - double_sided (bool): Whether the Z-score expectation is double-sided.
                - Expectation: The Z-scores for the specified column should be less than the threshold.

            - "ExpectColumnValuesToBeBetween": List of tuples, each containing:
                - column (str): The column name.
                - min_value (float): The expected minimum value.
                - max_value (float): The expected maximum value.
                - strict_min (bool): Whether the minimum value is strict (True) or inclusive (False).
                - strict_max (bool): Whether the maximum value is strict (True) or inclusive (False).
                - Expectation: The values in the column should be between min_value and max_value, according to the strictness.

                :param ge: module of Great Expectations.
                :param suite: global Great Expectations suite used to group the expectations.

                :returns: None

        :example:
        --------
            distribution_expectations_dict = {
                        "ExpectColumnKLDivergenceToBeLessThan": [("trip_distance", 0.1)],  # Expect the KL divergence for the "trip_distance" column to be less than |0.1.
                        "ExpectColumnPairValuesAToBeGreaterThanB": [("fare_amount", "total_amount", False)],  # Expect "fare_amount" to be greater than or equal to "total_amount".
                        "ExpectColumnStdevToBeBetween": [("trip_distance", 1.0, 3.0, False, False)],  # Expect the standard deviation of "trip_distance" to be between 1.0 and 3.0.
                        "ExpectColumnSumToBeBetween": [("passenger_count", 1, 10, False, True)],  # Expect the sum of "passenger_count" to be between 1 and 10, with max being inclusive.
                        "ExpectColumnValueZScoresToBeLessThan": [("trip_distance", 2.5, True)],  # Expect the Z-scores of "trip_distance" to be less than 2.5.
                        "ExpectColumnValuesToBeBetween": [("fare_amount", 2.5, 50.0, False, True)]  # Expect the values in "fare_amount" to be between 2.5 and 50.0, with max being inclusive.
                        }
        """
        # Handle KL Divergence expectations
        if "ExpectColumnKLDivergenceToBeLessThan" in distribution_expectations_dict.keys() and distribution_expectations_dict.get("ExpectColumnKLDivergenceToBeLessThan"):
            for col in distribution_expectations_dict["ExpectColumnKLDivergenceToBeLessThan"]:
                # Ensure col is a tuple or list with at least two elements
                if isinstance(col, (tuple, list)) and len(col) >= 2:
                    try:
                        threshold_value = float(col[1])  # Convert threshold to float
                        suite.add_expectation(ge.expectations.ExpectColumnKLDivergenceToBeLessThan(
                            column=col[0], threshold=threshold_value,meta={"expectation_type": "Distribution_Data", 
                                                                           "project":project, "source":source, "stage":stage,
                                                                           "is_critical":self.categorized_expectations_dict['ExpectColumnKLDivergenceToBeLessThan']['is_critical']
                                                                           ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnKLDivergenceToBeLessThan']['dimension']
                                                                           ,"sub_project":subproject}))
                    except ValueError:
                        print(f"Error: Threshold value {col[1]} for column {col[0]} is not a valid float.")
                else:
                    print(f"Error: Invalid format for {col}. Expected a tuple or list with at least 2 elements.")
        
        # Handle Column Pair Values A Greater Than B expectations
        if "ExpectColumnPairValuesAToBeGreaterThanB" in distribution_expectations_dict.keys() and distribution_expectations_dict.get("ExpectColumnPairValuesAToBeGreaterThanB"):
            for col in distribution_expectations_dict["ExpectColumnPairValuesAToBeGreaterThanB"]:
                if isinstance(col, (tuple, list)) and len(col) == 3:
                    try:
                        or_equal_value = bool(col[2])  # Ensure third value is parsed as boolean
                        suite.add_expectation(ge.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
                            column_A=col[0], column_B=col[1], or_equal=or_equal_value,meta={"expectation_type": "Distribution_Data", 
                                                                                            "project":project, "source":source, "stage":stage,
                                                                                            "is_critical":self.categorized_expectations_dict['ExpectColumnPairValuesAToBeGreaterThanB']['is_critical']
                                                                                            ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnPairValuesAToBeGreaterThanB']['dimension']
                                                                                            ,"sub_project":subproject}))
                    except ValueError:
                        print(f"Error: Unable to convert or_equal to boolean for columns {col[0]} and {col[1]}")
                else:
                    print(f"Error: Invalid format for {col}. Expected a tuple/list with exactly 3 elements.")
        
        # Handle Column Stdev expectations
        if "ExpectColumnStdevToBeBetween" in distribution_expectations_dict.keys() and distribution_expectations_dict.get("ExpectColumnStdevToBeBetween"):
            for col in distribution_expectations_dict["ExpectColumnStdevToBeBetween"]:
                if isinstance(col, (tuple, list)) and len(col) == 5:
                    suite.add_expectation(ge.expectations.ExpectColumnStdevToBeBetween(
                        column=col[0], min_value=col[1], max_value=col[2], strict_min=col[3], strict_max=col[4],
                        meta={"expectation_type": "Distribution_Data", 
                              "project":project, "source":source, "stage":stage,
                              "is_critical":self.categorized_expectations_dict['ExpectColumnStdevToBeBetween']['is_critical']
                              ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnStdevToBeBetween']['dimension']
                              ,"sub_project":subproject}))
                    
                else:
                    print(f"Error: Invalid format for {col}. Expected a tuple/list with exactly 5 elements.")
        
        # Handle Column Sum expectations
        if "ExpectColumnSumToBeBetween" in distribution_expectations_dict.keys() and  distribution_expectations_dict.get("ExpectColumnSumToBeBetween"):
            for col in distribution_expectations_dict["ExpectColumnSumToBeBetween"]:
                if isinstance(col, (tuple, list)) and len(col) == 5:
                    suite.add_expectation(ge.expectations.ExpectColumnSumToBeBetween(
                        column=col[0], min_value=col[1], max_value=col[2], strict_min=col[3], strict_max=col[4],
                        meta={"expectation_type": "Distribution_Data", 
                              "project":project, "source":source, "stage":stage,
                              "is_critical":self.categorized_expectations_dict['ExpectColumnSumToBeBetween']['is_critical']
                              ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnSumToBeBetween']['dimension']
                              ,"sub_project":subproject}))
                    
                else:
                    print(f"Error: Invalid format for {col}. Expected a tuple/list with exactly 5 elements.")
        
        # Handle Z-score expectations
        if "ExpectColumnValueZScoresToBeLessThan" in distribution_expectations_dict.keys() and  distribution_expectations_dict.get("ExpectColumnValueZScoresToBeLessThan"):
            for col in distribution_expectations_dict["ExpectColumnValueZScoresToBeLessThan"]:
                if isinstance(col, (tuple, list)) and len(col) == 3:
                    suite.add_expectation(ge.expectations.ExpectColumnValueZScoresToBeLessThan(
                        column=col[0], threshold=col[1], double_sided=col[2],
                        meta={"expectation_type": "Distribution_Data", 
                              "project":project, "source":source, "stage":stage,
                              "is_critical":self.categorized_expectations_dict['ExpectColumnValueZScoresToBeLessThan']['is_critical']
                              ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnValueZScoresToBeLessThan']['dimension']
                              ,"sub_project":subproject}))
                    
                else:
                    print(f"Error: Invalid format for {col}. Expected a tuple/list with exactly 3 elements.")
        
        # Handle Column Values Between expectations
        if "ExpectColumnValuesToBeBetween" in distribution_expectations_dict.keys() and distribution_expectations_dict.get("ExpectColumnValuesToBeBetween"):
            for col in distribution_expectations_dict["ExpectColumnValuesToBeBetween"]:
                if isinstance(col, (tuple, list)) and len(col) == 5:
                    suite.add_expectation(ge.expectations.ExpectColumnValuesToBeBetween(
                        column=col[0], min_value=col[1], max_value=col[2], strict_min=col[3], strict_max=col[4],
                        meta={"expectation_type": "Distribution_Data", 
                              "project":project, "source":source, "stage":stage,
                              "is_critical":self.categorized_expectations_dict['ExpectColumnValuesToBeBetween']['is_critical']
                              ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnValuesToBeBetween']['dimension']
                              ,"sub_project":subproject}))
                    
                    
                else:
                    print(f"Error: Invalid format for {col}. Expected a tuple/list with exactly 5 elements.")
        
    def add_schema_expectations(self, schema_dict:dict, ge:object, suite:object, project: str, source: str, stage: str,subproject:str) -> None:

        """
        Adds schema expectations to a Great Expectations suite based on provided dictionary.
        :param schema_dict: A dictionary containing seven keys:
            - "ExpectColumnToExist": List with names of columns.
            - "ExpectColumnValuesToBeOfType": List of tuples, each containing:  
                - column_name (str): Name of the column to be checked.
                - data_type (str): A string representing a data type.
                Valid data types are defined by the current backend implementation and are
                dynamically loaded. This means, for example, when using pandas, valid data
                types include any numpy dtype values such as int64 or float64, and it also
                includes native python types like int or float. As another example, when
                using a SqlAlchemy dataset you would use INTEGER.
                The given data types need to be precise, for example, when working with
                an int64 column in pandas, the given data type needs to be int64 a native
                int from python will not suceed the test. It can also be case-sensitive
                depending on the backend implementation.
            - "ExpectColumnValuesToBeInTypeList": List of tuples, each containing:
                - column_name (str): Name of the column to be checked.
                - data_types_list (list[str]): A list of strings that represent data types.
                Valid data types are defined by the current backend implementation and are
                dynamically loaded. This means, for example, when using pandas, valid data
                types include any numpy dtype values such as int64 or float64, and it also
                includes native python types like int or float. As another example, when
                using a SqlAlchemy dataset you would use INTEGER.
                The given data types need to be precise, for example, when working with
                an int64 column in pandas, the given data type needs to be int64 a native
                int from python will not suceed the test. It can also be case-sensitive
                depending on the backend implementation.
            - "ExpectTableColumnCountToBeBetween": Dictionary with 2 keys -> min_value and max_value.
                - 'min_value':(int or None) -> Minimum number of columns (inclusive) or None
                    if None: max_value is treated as an upper bound, and the number
                    of acceptable columns has no minimum.
                - 'max_value':(int or None) -> Maximum number of columns (inclusive) or None
                    if None: min_value is treated as a lower bound, and the number
                    of acceptable columns has no maximum.
                If one of the keys is omitted, then None is assigned to it.
            - "ExpectTableColumnCountToEqual": List with a single value:
                - expected_value: The expected number of columns.
            - "ExpectTableColumnsToMatchOrderedList": List of strings representing column
            names. To suceed the test the given strings must match every column of the
            dataframe in order.
            - "ExpectTableColumnsToMatchSet": List with 2 values:
                - column_set (list[str]): An unordered list of strings representing column
                names.
                - exact_match (bool): A boolean flag. If True, every string in the
                column_set needs to match a column of the dataframe to suceed the
                test. If False, even one matching string from the column_set is enough
                to suceed the test. Defaults to True.
                Note that giving a column in the column_set that doesn't belong to the
                dataframe will always fails the test.
        :param ge: module of Great Expectations
        :param suite: global great expectations suite used to group the expectations
        :returns: None
        :example:
        schema_dict = {
            "ExpectColumnToExist" : ["total_amount", "tip_amount", "colName3", ...],
            "ExpectColumnValuesToBeOfType" : [("total_amount", "float64"),
                                              ("tip_amount", "float32"),
                                              ("colName3", "dataType"), ...],
            "ExpectColumnValuesToBeInTypeList" : [("total_amount", ["float64", "int64"]),
                                                  ("tip_amount", ["float32", "int"])],
                                                  ("colName3", ["dataType1", "dataType2"], ...)],
            "ExpectTableColumnCountToBeBetween" : { "min_value":2, "max_value":25 },
            "ExpectTableColumnCountToEqual" : [13],
            "ExpectTableColumnsToMatchOrderedList" : ["total_amount", "tip_amount",
                                                      "colName3", "colName4", "finalColumn"],
            "ExpectTableColumnsToMatchSet" : [["total_amount", "tip_amount", "colName3",
                                               "colName4", "finalColumn"], True]
        }
 
        """
        if "ExpectColumnToExist" in schema_dict.keys() and schema_dict["ExpectColumnToExist"]:
            for expected_column in schema_dict["ExpectColumnToExist"]:
                suite.add_expectation(ge.expectations.ExpectColumnToExist(column=expected_column,meta={"expectation_type": "Schema_Data",
                                                                                                        "project":project, "source":source, "stage":stage,
                                                                                                        "is_critical":self.categorized_expectations_dict['ExpectColumnToExist']['is_critical']
                                                                                                        ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnToExist']['dimension']
                                                                                                        ,"sub_project":subproject}))
       
        if "ExpectColumnValuesToBeOfType" in schema_dict.keys() and schema_dict["ExpectColumnValuesToBeOfType"]:
            for column, data_type in schema_dict["ExpectColumnValuesToBeOfType"]:
                suite.add_expectation(ge.expectations.ExpectColumnValuesToBeOfType(column=column,
                                                                                   type_=data_type,
                                                                                   meta={"expectation_type": "Schema_Data", 
                                                                                         "project":project, "source":source, "stage":stage,
                                                                                         "is_critical":self.categorized_expectations_dict['ExpectColumnValuesToBeOfType']['is_critical']
                                                                                         , "dama_dimension":self.categorized_expectations_dict['ExpectColumnValuesToBeOfType']['dimension']
                                                                                         ,"sub_project":subproject}))
               
        if "ExpectColumnValuesToBeInTypeList" in schema_dict.keys() and schema_dict["ExpectColumnValuesToBeInTypeList"]:
            for column, col_types in schema_dict["ExpectColumnValuesToBeInTypeList"]:
                suite.add_expectation(ge.expectations.ExpectColumnValuesToBeInTypeList(column=column,
                                                                                       type_list=col_types,
                                                                                       meta={"expectation_type": "Schema_Data", 
                                                                                             "project":project, "source":source, "stage":stage,
                                                                                             "is_critical":self.categorized_expectations_dict['ExpectColumnValuesToBeInTypeList']['is_critical']
                                                                                             ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnValuesToBeInTypeList']['dimension']
                                                                                             ,"sub_project":subproject}))
        if "ExpectTableColumnCountToBeBetween" in schema_dict.keys() and schema_dict["ExpectTableColumnCountToBeBetween"]:
            if "min_value" in schema_dict["ExpectTableColumnCountToBeBetween"]:
                min_value = schema_dict["ExpectTableColumnCountToBeBetween"]["min_value"]
            else:
                min_value = None
           
            if "max_value" in schema_dict["ExpectTableColumnCountToBeBetween"]:
                max_value = schema_dict["ExpectTableColumnCountToBeBetween"]["max_value"]
            else:
                max_value = None
 
            suite.add_expectation(ge.expectations.ExpectTableColumnCountToBeBetween(min_value=min_value,
                                                                                    max_value=max_value,
                                                                                    meta={"expectation_type": "Schema_Data", 
                                                                                          "project":project, "source":source, "stage":stage,
                                                                                          "is_critical":self.categorized_expectations_dict['ExpectTableColumnCountToBeBetween']['is_critical']
                                                                                          , "dama_dimension":self.categorized_expectations_dict['ExpectTableColumnCountToBeBetween']['dimension']
                                                                                          ,"sub_project":subproject}))
       
        if "ExpectTableColumnCountToEqual" in schema_dict.keys() and schema_dict["ExpectTableColumnCountToEqual"]:
            expected_value = schema_dict["ExpectTableColumnCountToEqual"][0]
            suite.add_expectation(ge.expectations.ExpectTableColumnCountToEqual(value=expected_value,meta={"expectation_type": "Schema_Data", 
                                                                                                           "project":project, "source":source, "stage":stage,
                                                                                                           "is_critical":self.categorized_expectations_dict['ExpectTableColumnCountToEqual']['is_critical']
                                                                                                           , "dama_dimension":self.categorized_expectations_dict['ExpectTableColumnCountToEqual']['dimension']
                                                                                                           ,"sub_project":subproject}))
 
         
        if "ExpectTableColumnsToMatchOrderedList" in schema_dict.keys() and schema_dict["ExpectTableColumnsToMatchOrderedList"]:
            oredered_columns = schema_dict["ExpectTableColumnsToMatchOrderedList"]
            suite.add_expectation(ge.expectations.ExpectTableColumnsToMatchOrderedList(column_list=oredered_columns,
                                                                                       meta={"expectation_type": "Schema_Data", 
                                                                                             "project":project, "source":source, "stage":stage,
                                                                                             "is_critical":self.categorized_expectations_dict['ExpectTableColumnsToMatchOrderedList']['is_critical']
                                                                                             ,"dama_dimension":self.categorized_expectations_dict['ExpectTableColumnsToMatchOrderedList']['dimension']
                                                                                             ,"sub_project":subproject}))
       
       
        if "ExpectTableColumnsToMatchSet" in schema_dict.keys() and schema_dict["ExpectTableColumnsToMatchSet"]:
            cols_set = schema_dict["ExpectTableColumnsToMatchSet"][0]
            exact_match = True
            if len(schema_dict["ExpectTableColumnsToMatchSet"]) > 1:
                exact_match = schema_dict["ExpectTableColumnsToMatchSet"][1]
            suite.add_expectation(ge.expectations.ExpectTableColumnsToMatchSet(column_set=cols_set,
                                                                               exact_match=exact_match,
                                                                               meta={"expectation_type": "Schema_Data", 
                                                                                     "project":project, "source":source, "stage":stage,
                                                                                     "is_critical":self.categorized_expectations_dict['ExpectTableColumnsToMatchSet']['is_critical']
                                                                                     ,"dama_dimension":self.categorized_expectations_dict['ExpectTableColumnsToMatchSet']['dimension']
                                                                                     ,"sub_project":subproject}))

    def add_volume_expectations(self, volume_dict:dict, ge:object, suite:object, project: str, source: str, stage: str,subproject:str) -> None:
        """
        Adds volume expectations to a Great Expectations suite based on provided dictionary.
        :param volume_dict: A dictionary containing three keys:
            - "ExpectTableRowCountToBeBetween": Dictionary with 2 keys -> min_value and max_value.
                - 'min_value':(int or None) -> Minimum number of rows (inclusive) or None
                    if None: max_value is treated as an upper bound, and the number
                    of acceptable rows has no minimum.
                - 'max_value':(int or None) -> Maximum number of rows (inclusive) or None
                    if None: min_value is treated as a lower bound, and the number
                    of acceptable rows has no maximum.
                If one of the keys is omitted, then None is assigned to it.
            - "ExpectTableRowCountToEqual": List with a single value.
                - expected_value (int): The expected number of rows
            - "ExpectTableRowCountToEqualOtherTable":
        :param ge: module of Great Expectations
        :param suite: global great expectations suite used to group the expectations
        :returns: None
        :example:
        volume_dict = {
            "ExpectTableRowCountToBeBetween" : { "min_value":2, "max_value":25 },
            "ExpectTableRowCountToEqual" : [25000],
            "ExpectTableRowCountToEqualOtherTable" : [table]
       
       
        }
        """
 
        if "ExpectTableRowCountToBeBetween" in volume_dict.keys() and volume_dict["ExpectTableRowCountToBeBetween"]:
            if "min_value" in volume_dict["ExpectTableRowCountToBeBetween"]:
                min_value = volume_dict["ExpectTableRowCountToBeBetween"]["min_value"]
            else:
                min_value = None
           
            if "max_value" in volume_dict["ExpectTableRowCountToBeBetween"]:
                max_value = volume_dict["ExpectTableRowCountToBeBetween"]["max_value"]
            else:
                max_value = None
 
            suite.add_expectation(ge.expectations.ExpectTableRowCountToBeBetween(min_value=min_value,
                                                                                 max_value=max_value,
                                                                                 meta={"expectation_type": "Volume_Data", 
                                                                                       "project":project, "source":source, "stage":stage,
                                                                                       "is_critical":self.categorized_expectations_dict['ExpectTableRowCountToBeBetween']['is_critical']
                                                                                       , "dama_dimension":self.categorized_expectations_dict['ExpectTableRowCountToBeBetween']['dimension']
                                                                                       ,"sub_project":subproject}))
       
        if "ExpectTableRowCountToEqual" in volume_dict.keys() and volume_dict["ExpectTableRowCountToEqual"]:
            expected_value = volume_dict["ExpectTableRowCountToEqual"][0]
            suite.add_expectation(ge.expectations.ExpectTableRowCountToEqual(value=expected_value,meta={"expectation_type": "Volume_Data", 
                                                                                                        "project":project, "source":source, "stage":stage,
                                                                                                        "is_critical":self.categorized_expectations_dict['ExpectTableRowCountToEqual']['is_critical']
                                                                                                        ,"dama_dimension":self.categorized_expectations_dict['ExpectTableRowCountToEqual']['dimension']
                                                                                                        ,"sub_project":subproject}))
            
        if "ExpectTableRowCountToEqualOtherTable" in volume_dict.keys() and volume_dict["ExpectTableRowCountToEqualOtherTable"]:
            test_table_two = volume_dict["ExpectTableRowCountToEqualOtherTable"][0]
            suite.add_expectation(
                ge.expectations.ExpectTableRowCountToEqualOtherTable(
                    other_table_name=test_table_two,
                    meta={"expectation_type": "Volume_Data", 
                          "project":project, "source":source, "stage":stage,
                          "is_critical":self.categorized_expectations_dict['ExpectTableRowCountToEqualOtherTable']['is_critical']
                          ,"dama_dimension":self.categorized_expectations_dict['ExpectTableRowCountToEqualOtherTable']['dimension']
                          ,"sub_project":subproject}))
                    

    def add_pattern_matching(self, pattern_matching_dict:dict,ge:object,suite:object, project: str, source: str, stage: str,subproject:str):
        
        """
        Adds pattern matching expectations to a Great Expectations suite based on provided dictionary.
 
        :param pattern_matching_dict: dict
            A dictionary containing various expectations related to pattern matching and value lengths.
            Supported keys:
            - "ExpectColumnValueLengthsToBeBetween": List of dictionaries, each containing:
                - column (str): name of column to be tested
                - min_value (int): minimum length
            - max_value (int): maximum length
            Example:
            ```python
              {
               "ExpectColumnValueLengthsToBeBetween": [
              {"column": "user_id", "min_value": 5, "max_value": 10}
              ]
               }
                ```
               Expected Result: Success if the lengths of the values in the `user_id` column are between 5 and 10 characters.
 
            - "ExpectColumnValueLengthsToEqual": List of dictionaries, each containing:
               - column (str): name of column to be tested
               - value (int): exact length expected for the values
               Example:
               ```python
               {
               "ExpectColumnValueLengthsToEqual": [
                 {"column": "phone_number", "value": 10}
                ]
                }
                ```
                Expected Result: Success if all values in the `phone_number` column are exactly 10 characters long.
 
            - "ExpectColumnValuesToMatchLikePattern": List of dictionaries, each containing:
                - column (str): name of column to be tested
                - pattern (str): SQL-like pattern to match
                Example:
                ```python
                {
                "ExpectColumnValuesToMatchLikePattern": [
                {"column": "pickup_datetime", "like_pattern": "%-%-% %:%:%"}
                ]
                }
                ```
                Expected Result: Success if the values in the `pickup_datetime` column match the specified SQL-like pattern (e.g., dates formatted as `YYYY-MM-DD HH:MM:SS`).
 
                > **Note:** This pattern validation submodule is only compatible with the following data sources:
                > - **SQLite**
                > - **PostgreSQL**
                > - **MySQL**
                > - **MSSQL**
                > - **Redshift**
                >
                > Support for other data sources may not be available or could result in unexpected behavior.
 
            - "ExpectColumnValuesToMatchLikePatternList": List of dictionaries, each containing:
                - column (str): name of column to be tested
                - pattern_list (list of str): list of SQL-like patterns to match
                Example:
                ```python
                {
                "ExpectColumnValuesToMatchLikePatternList": [
              {"column": "status", "pattern_list": ["active", "inactive", "pending"]}
                ]
                }
                ```
                Expected Result: Success if the values in the `status` column match any of the patterns in the `pattern_list` (e.g., "active", "inactive", "pending").
 
                > **Note:** This pattern validation submodule is only compatible with the following data sources:
                > - **SQLite**
                > - **PostgreSQL**
                > - **MySQL**
                > - **MSSQL**
                > - **Redshift**
                >
                > Support for other data sources may not be available or could result in unexpected behavior.
 
            - "ExpectColumnValuesToMatchRegex": List of dictionaries, each containing:
                - column (str): name of column to be tested
                - regex (str): regex pattern to match
                Example:
                ```python
                {
                "ExpectColumnValuesToMatchRegex": [
                   {"column": "email", "regex": "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$"}
                ]
                }
                ```
                Expected Result: Success if the values in the `email` column match the specified regex pattern for email addresses.
 
            - "ExpectColumnValuesToMatchRegexList": List of dictionaries, each containing:
                - column (str): name of column to be tested
                - regex_list (list of str): list of regex patterns to match
                Example:
                ```python
                {
                "ExpectColumnValuesToMatchRegexList": [
                 {"column": "phone_number", "regex_list": ["^\\d{3}-\\d{3}-\\d{4}$", "^\\d{10}$"]}
                ]
                }
                ```
                Expected Result: Success if the values in the `phone_number` column match any of the specified regex patterns (e.g., formatted as `123-456-7890` or `1234567890`).
 
            - "ExpectColumnValuesToNotMatchLikePattern": List of dictionaries, each containing:
                 - column (str): name of column to be tested
                - pattern (str): SQL-like pattern to not match
                Example:
                ```python
                {
                    "ExpectColumnValuesToNotMatchLikePattern": [
                    {"column": "store_and_fwd_flag", "like_pattern": "Y"}
                    ]
                    }
                    ```
                    Expected Result: Success if none of the values in the `store_and_fwd_flag` column match the specified pattern (e.g., "Y" should not appear).
                    > **Note:** This pattern validation submodule is only compatible with the following data sources:
                > - **SQLite**
                > - **PostgreSQL**
                > - **MySQL**
                > - **MSSQL**
                > - **Redshift**
                >
                > Support for other data sources may not be available or could result in unexpected behavior.
 
                - "ExpectColumnValuesToNotMatchLikePatternList": List of dictionaries, each containing:
                - column (str): name of column to be tested
                - pattern_list (list of str): list of SQL-like patterns to not match
                Example:
                ```python
                {
                "ExpectColumnValuesToNotMatchLikePatternList": [
                 {"column": "status", "pattern_list": ["cancelled", "failed"]}
                ]
                }
                ```
                Expected Result: Success if none of the values in the `status` column match any of the specified patterns (e.g., "cancelled" or "failed" should not appear).
                Expected Result: Success if none of the values in the `store_and_fwd_flag` column match the specified pattern (e.g., "Y" should not appear).
                    > **Note:** This pattern validation submodule is only compatible with the following data sources:
                > - **SQLite**
                > - **PostgreSQL**
                > - **MySQL**
                > - **MSSQL**
                > - **Redshift**
                >
                > Support for other data sources may not be available or could result in unexpected behavior.
 
            - "ExpectColumnValuesToNotMatchRegex": List of dictionaries, each containing:
                - column (str): name of column to be tested
                - regex (str): regex pattern to not match
                Example:
                ```python
                {
                    "ExpectColumnValuesToNotMatchRegex": [
                {"column": "ssn", "regex": "^000-\\d{2}-\\d{4}$"}
                ]
                }
                ```
                Expected Result: Success if none of the values in the `ssn` column match the specified regex pattern (e.g., SSNs should not start with `000-`).
 
            - "ExpectColumnValuesToNotMatchRegexList": List of dictionaries, each containing:
                - column (str): name of column to be tested
                - regex_list (list of str): list of regex patterns to not match
                Example:
                ```python
                {
                    "ExpectColumnValuesToNotMatchRegexList": [
                 {"column": "zip_code", "regex_list": ["^00000$", "^12345$"]}
                ]
                }
                ```
                Expected Result: Success if none of the values in the `zip_code` column match any of the specified regex patterns (e.g., zip codes `00000` or `12345` should not appear).
 
        :param ge: module of Great Expectations
        :param suite: global great expectations suite used to group the expectations
        :returns: None
        """


        # Length between expectations
        if "ExpectColumnValueLengthsToBeBetween" in pattern_matching_dict.keys() and pattern_matching_dict["ExpectColumnValueLengthsToBeBetween"]:
            for col in pattern_matching_dict["ExpectColumnValueLengthsToBeBetween"]:
                suite.add_expectation(
                    ge.expectations.ExpectColumnValueLengthsToBeBetween(
                        column=col["column"],
                        min_value=col["min_value"],
                        max_value=col["max_value"],
                        meta={"expectation_type": "pattern_matching", 
                              "project":project, "source":source, "stage":stage,
                              "is_critical":self.categorized_expectations_dict['ExpectColumnValueLengthsToBeBetween']['is_critical']
                              ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnValueLengthsToBeBetween']['dimension']
                              ,"sub_project":subproject}
                    )
                )

        # Length equal expectations
        if "ExpectColumnValueLengthsToEqual" in pattern_matching_dict.keys() and pattern_matching_dict["ExpectColumnValueLengthsToEqual"]:
            for col in pattern_matching_dict["ExpectColumnValueLengthsToEqual"]:
                suite.add_expectation(
                    ge.expectations.ExpectColumnValueLengthsToEqual(
                        column=col[0], value=col[1], meta={"expectation_type": "pattern_matching", 
                                                           "project":project, "source":source, "stage":stage,
                                                           "is_critical":self.categorized_expectations_dict['ExpectColumnValueLengthsToEqual']['is_critical']
                                                           ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnValueLengthsToEqual']['dimension']
                                                           ,"sub_project":subproject}
                    )
                )

        # SQL LIKE pattern match expectations
        if "ExpectColumnValuesToMatchLikePattern" in pattern_matching_dict.keys() and pattern_matching_dict["ExpectColumnValuesToMatchLikePattern"]:
            for col in pattern_matching_dict["ExpectColumnValuesToMatchLikePattern"]:
                suite.add_expectation(
                    ge.expectations.ExpectColumnValuesToMatchLikePattern(
                        column=col["column"], like_pattern=col["like_pattern"], meta={"expectation_type": "pattern_matching", 
                                                                                      "project":project, "source":source, "stage":stage,
                                                                                      "is_critical":self.categorized_expectations_dict['ExpectColumnValuesToMatchLikePattern']['is_critical']
                                                                                      ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnValuesToMatchLikePattern']['dimension']
                                                                                      ,"sub_project":subproject}
                    )
                )

        # SQL LIKE pattern list match expectations
        if "ExpectColumnValuesToMatchLikePatternList" in pattern_matching_dict.keys() and pattern_matching_dict["ExpectColumnValuesToMatchLikePatternList"]:
            for col in pattern_matching_dict["ExpectColumnValuesToMatchLikePatternList"]:
                suite.add_expectation(
                    ge.expectations.ExpectColumnValuesToMatchLikePatternList(
                        column=col["column"], like_pattern_list=col["pattern_like_list"], meta={"expectation_type": "pattern_matching", 
                                                                                                "project":project, "source":source, "stage":stage,
                                                                                                "is_critical":self.categorized_expectations_dict['ExpectColumnValuesToMatchLikePatternList']['is_critical']
                                                                                                ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnValuesToMatchLikePatternList']['dimension']
                                                                                                ,"sub_project":subproject}
                    )
                )

        # Regex match expectations
        if "ExpectColumnValuesToMatchRegex" in pattern_matching_dict.keys() and pattern_matching_dict["ExpectColumnValuesToMatchRegex"]:
            for col in pattern_matching_dict["ExpectColumnValuesToMatchRegex"]:
                suite.add_expectation(
                    ge.expectations.ExpectColumnValuesToMatchRegex(
                        column=col["column"], regex=eval(col["regex"]), meta={"expectation_type": "pattern_matching", 
                                                                              "project":project, "source":source, "stage":stage,
                                                                              "is_critical":self.categorized_expectations_dict['ExpectColumnValuesToMatchRegex']['is_critical']
                                                                              ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnValuesToMatchRegex']['dimension']
                                                                              ,"sub_project":subproject}
                    )
                )

        # Regex list match expectations
        if "ExpectColumnValuesToMatchRegexList" in pattern_matching_dict.keys() and pattern_matching_dict["ExpectColumnValuesToMatchRegexList"]:
            for col in pattern_matching_dict["ExpectColumnValuesToMatchRegexList"]:
                suite.add_expectation(
                    ge.expectations.ExpectColumnValuesToMatchRegexList(
                        column=col[ "column"], regex_list=[eval(reg) for reg in col["regex_list"]], meta={"expectation_type": "pattern_matching", 
                                                                                                          "project":project, "source":source, "stage":stage,
                                                                                                          "is_critical":self.categorized_expectations_dict['ExpectColumnValuesToMatchRegexList']['is_critical']
                                                                                                          ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnValuesToMatchRegexList']['dimension']
                                                                                                          ,"sub_project":subproject}
                    )
                )

        # SQL LIKE pattern not match expectations
        if "ExpectColumnValuesToNotMatchLikePattern" in pattern_matching_dict.keys() and pattern_matching_dict["ExpectColumnValuesToNotMatchLikePattern"]:
            for col in pattern_matching_dict["ExpectColumnValuesToNotMatchLikePattern"]:
                suite.add_expectation(
                    ge.expectations.ExpectColumnValuesToNotMatchLikePattern(
                        column=col["column"], like_pattern=col["like_pattern"], meta={"expectation_type": "pattern_matching", 
                                                                                      "project":project, "source":source, "stage":stage,
                                                                                      "is_critical":self.categorized_expectations_dict['ExpectColumnValuesToNotMatchLikePattern']['is_critical']
                                                                                      ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnValuesToNotMatchLikePattern']['dimension']
                                                                                      ,"sub_project":subproject}
                    )
                )

        # SQL LIKE pattern list not match expectations
        if "ExpectColumnValuesToNotMatchLikePatternList" in pattern_matching_dict.keys() and pattern_matching_dict["ExpectColumnValuesToNotMatchLikePatternList"]:
            for col in pattern_matching_dict["ExpectColumnValuesToNotMatchLikePatternList"]:
                suite.add_expectation(
                    ge.expectations.ExpectColumnValuesToNotMatchLikePatternList(
                        column=col["column"], like_pattern_list=col["pattern_list"], meta={"expectation_type": "pattern_matching", 
                                                                                           "project":project, "source":source, "stage":stage,
                                                                                           "is_critical":self.categorized_expectations_dict['ExpectColumnValuesToNotMatchLikePatternList']['is_critical']
                                                                                           ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnValuesToNotMatchLikePatternList']['dimension']
                                                                                           ,"sub_project":subproject}
                    )
                )

        # Regex not match expectations
        if "ExpectColumnValuesToNotMatchRegex" in pattern_matching_dict.keys() and pattern_matching_dict["ExpectColumnValuesToNotMatchRegex"]:
            for col in pattern_matching_dict["ExpectColumnValuesToNotMatchRegex"]:
                suite.add_expectation(
                    ge.expectations.ExpectColumnValuesToNotMatchRegex(
                        column=col["column"], regex=eval(col["regex"]), meta={"expectation_type": "pattern_matching",
                                                                               "project":project, "source":source, "stage":stage,
                                                                               "is_critical":self.categorized_expectations_dict['ExpectColumnValuesToNotMatchRegex']['is_critical']
                                                                               ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnValuesToNotMatchRegex']['dimension']
                                                                               ,"sub_project":subproject}
                    )
                )

        # Regex list not match expectations
        if "ExpectColumnValuesToNotMatchRegexList" in pattern_matching_dict.keys() and pattern_matching_dict["ExpectColumnValuesToNotMatchRegexList"]:
            for col in pattern_matching_dict["ExpectColumnValuesToNotMatchRegexList"]:
                suite.add_expectation(
                    ge.expectations.ExpectColumnValuesToNotMatchRegexList(
                        column=col["column"], regex_list=[eval(reg) for reg in col["regex_list"]], meta={"expectation_type": "pattern_matching", 
                                                                                                         "project":project, "source":source, "stage":stage,
                                                                                                         "is_critical":self.categorized_expectations_dict['ExpectColumnValuesToNotMatchRegexList']['is_critical']
                                                                                                         ,"dama_dimension":self.categorized_expectations_dict['ExpectColumnValuesToNotMatchRegexList']['dimension']
                                                                                                         ,"sub_project":subproject}
                    )
                )
                
                