Spark Tabulation and Extraction

The Spark tabulator-and-extractor is a python django server that recieves json api requests to run jobs creating tabulations or extractions from our data store. In the case of tabulation these jobs are running in real time, with a user waiting at a web browser for the job to complete and the results to be displayed. Tabulation jobs deal in "person record" datasets, where each row in the parquet file represents a person. The columns are attributes on that person such as AGE and SEX. Sometimes these datasets are sample subsets representing a larger dataset, in which case a PERWT (person weight) column is included. This column indicates the number of persons in the actual population represented by this person in the sample. A tabulation job is described by a list of columns the output should contain. The server then uses spark to group the input dataset by those columns and return a file which looks like the following:
```csv
Age,Sex,Marital Status,Sum Perwt
"0_to_10", "1", "married", "434616"
"0_to_10", "1", "single", "1226802598"
"0_to_10", "2", "married", "348731"
"0_to_10", "2", "single", "1182614836"
"10_to_20", "1", "married", "8795781"
```

In the above case, the dataset was tabulated by (or grouped by) “AGE”, “SEX”, and “MARTIAL STATUS”, then the variable “PERWT” was summed in each of the groupings. The json request that describes this job includes a list of columns for the output to provide, such as the following which describes the “Marital Status” column: 
```json
{
   "variable_type": "GroupOnVariable",
      "column_expression": { "column_name": "MARST" },
      "output_name": "Marital Status",
      "group_on_bucket": {
         "bucket_dict": {
            "married": {
               "join_operator": "OR",
               "group_expression_a": {
                  "operator": "=",
                  "value": 200
               },
               "group_expression_b": {
                  "operator": "=",
                  "value": 211
               }
            },
            "single": {
               "operator": "=",
               "value": 111
            }
         }
      }	
}
```
The “variable_type” in the case of tabulation is “GroupOnVariable” for variables that we’re grouping on. Extractions use different types of variables but tabulations do not. The “column_expression” is a description of how to select the column out of the dataset. In this case we are just selecting the “MARST” column. Expressions can be more descriptive and include operations such as adding two columns together or concatenating a fixed string to the beginning of a column. The “group_on_bucket” object is a description of what types of groupings we want the output file to have. In this case we say that if the MARST code is 200 (Married Formally) or 211 (Civil Marriage) we’re going to output the string “married” and if the code is 111 (Never Married) we’re going to output “single”. All other values we’re going to not count in the tabulation. 

The other “variable_type” is “TabulationVariable”. These variables are the ones that we want to aggregate on. In the following case we are aggregating on PERWT.
```json
{
   "variable_type": "TabulationVariable",
      "column_expression": { "column_name": "cast(PERWT as DECIMAL(10,0))"},
      "aggregation_method": "sum"
}
```


The code for running this looks like the following:
```python
# Get the column expressions for each variable to group on
group_col_expressions = [self.string_for_column_expression(var.column_expression) for var in self.group_on_variables]
select_group_col_expressions = ["{} as df{}_v{}".format(expr, self.data_frame_id, var.id) for expr, var in zip(group_col_expressions, self.group_on_variables)]

# Get the column expressions for each of the variables to aggregate on
tabulate_column_expressions = ["{}({}) as df{}_v{}".format(var.aggregation_method, self.string_for_column_expression(var.column_expression), self.data_frame_id, var.id) for var in self.tabulation_variables]

#Select out the grouping columns and the aggregation columns and GROUP BY the aggregation columns
sql_string = "SELECT {}, {} FROM ds{} GROUP BY {}".format(", ".join(select_group_col_expressions), ", ".join(tabulate_column_expressions), self.data_frame_id, ", ".join(group_col_expressions))
pre_bucket_tabulation_results = self.sqlContext.sql(sql_string)

# Bucketize the results by replacing the grouped variables with the bucket equivalents
replace_values_with_buckets = self.create_replace_values_with_buckets_func()
bucket_maps = pre_bucket_tabulation_results.flatMap(lambda row: replace_values_with_buckets(row.asDict()))

#Reduce on all the datublation values, summing up the counts
bucket_reduce = bucket_maps.reduceByKey(lambda tab_values_1, tab_values_2: [v1 + v2 for v1, v2 in zip(tab_values_1, tab_values_2)]).sortByKey()
```

