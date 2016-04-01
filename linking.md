Linking

This is the process of taking two datasets, such as the US 1930 Census and US 1940 Census, and trying to match records between them.
None of these records have unique id's so we have to go off of other attributes such as age, place of birth, sex, and first and last name.
We use an ipython notebook to do this that reads in a series of json files to configure the linking steps.
There are multiple stetps in this process:
  1. Read in the json config file and validate it.
  2. Read in the parquet files for each dataset and filter them to the records we want to link (or don't filter if we're linking all records).
  3. If we've already found some links in these datasets, read in the parquet file which represents those links and drop those records from each dataset.
  4. Select out the columns and apply transforms to clean them. These transforms include name substitution (Al for Albert) and string cleaning (O'harris -> o harris).
  ```python
    #### Create new dataframe by selecting columns and apply transforms (Lazy Spark)
      # This is where the "column_mappings" and "substitution_columns" sections of the configs get processed.
  
      prepped_df_a = prep_dataframe(df_a, config["column_mappings"], config["substitution_columns"], True, config["id_column"]).coalesce(df_a.rdd.getNumPartitions())
      print("Success -- prepped df_a with columns: {}".format(prepped_df_a.columns))
      prepped_df_b = prep_dataframe(df_b, config["column_mappings"], config["substitution_columns"], False, config["id_column"]).coalesce(df_b.rdd.getNumPartitions())
      print("Success -- prepped df_a with columns: {}".format(prepped_df_b.columns))
      print("Success -- finished prepping dataframes")
  ```
  5. Prepare the columns for blocking. Blocking is a step that groups similar records in each dataset so we can use a string comparison function on them later in the process.
  If you had a small enough dataset, then you wouldn't need this step because you could just compare every record to every other record.
  Our datsets are large enough that this step is required. We usually block on age (with a 1 year leniency), sex, and birthplace.
  Occasionally we'll map birthplaces from smaller regions to larger ones as well(Hennepin county -> Minnesota).
  In order to map one record to multiple groups (such as different ages), we map that column to an array, then explode it.
  ```python
  #### Run mappings for blockings on the selected columns (Lazy Spark)
  #Blocking is simply a grouping by the blocking keys on each dataset, and then a join on those groups.
  #
  #    Example grouping and join on BPL and AGE using psuedo-code:
  #    GROUPING_BPL_11_AGE_10 = { 
  #        DATASET_A: [(ID=0, BPL=11, AGE=10, NAME=Beatrice), (ID=1, BPL=11, AGE=10, NAME=Benedick)],
  #        DATASET_B: [(ID=22, BPL=11, AGE=10, NAME=Beatrice), (ID=23, BPL=11, AGE=10, NAME=Don Jon)]
  #    }
  #
  #In the comparison step, records 0 and 1 from dataset A, would get compared with records 22 and 23 from dataset B. The only possible link found would be between record 0 and record 22 and the rest of the pairings would get discarded.
  #
  #Sometimes records need to be in multiple groups, such as records with wildcard blockings or when using age ranges. In order to do this, the records are duplicated before the actual blocking occurs. The following is what the duplication would look like for an age range of one below and one above:
  #
  #    DATASET_A_BEFORE_DUPLICATION = [
  #        (ID=0, BPL=11, AGE=10, NAME=Beatrice),
  #        (ID=1, BPL=11, AGE=10, NAME=Benedick)
  #    ]
  #    DATASET_A_AFTER_DUPLICATION = [
  #        (ID=0, BPL=11, AGE=10, NAME=Beatrice),
  #        (ID=1, BPL=11, AGE=10, NAME=Benedick),
  #        (ID=0, BPL=11, AGE=9, NAME=Beatrice),
  #        (ID=1, BPL=11, AGE=9, NAME=Benedick),
  #        (ID=0, BPL=11, AGE=11, NAME=Beatrice),
  #        (ID=1, BPL=11, AGE=11, NAME=Benedick)
  #    ]
  #
  #The following step is what creates that duplication in both datasets. See [generate_mapping_function](/edit/fuzzy_linking/linking_lib/helpers.py) for more technical information.

  exploded_df_a = prepped_df_a
  exploded_df_b = prepped_df_b
  for mapping_conf in mapping_confs:
      column_name = mapping_conf["column_name"]
      mapping_udf = generate_mapping_function(mapping_conf, wildcard_lookup, column_types)
      explode_selects = [explode(mapping_udf(col(column))).alias(column) if column == column_name else column for column in all_columns]
      if "dataset" in mapping_conf and mapping_conf["dataset"] == "a":
          exploded_df_a = exploded_df_a.select(explode_selects)
      elif "dataset" in mapping_conf and mapping_conf["dataset"] == "b":
          exploded_df_b = exploded_df_b.select(explode_selects)
      else:
          exploded_df_a = exploded_df_a.select(explode_selects)
          exploded_df_b = exploded_df_b.select(explode_selects)
  ```
  6. Transform the dataframes into rdd tuples, where the first value is an array of values to be blocked on (age, sex, birthplace), 
  and the second value contains that values that need to be compared using a string comparison function (first and last name).
  Then cogroup the rdds together on these blocking values.
  ```python
  #### Map dataframes to blocking tuples (Lazy Spark)
  #The dataframes now are transformed into RDD's with the following form:
  #
  #    (blocking_keys, (values_for_comparison, id))

  row_to_blocking_tuple = create_row_to_blocking_tuple_function(config["blocking"], comparison_columns, config["id_column"])
  mapped_a2 = exploded_df_a.rdd.map(row_to_blocking_tuple)
  mapped_b2 = exploded_df_b.rdd.map(row_to_blocking_tuple)
  print("Success -- Created blocking tuples")

  #### The datasets are joined toegther using a cogroup (Lazy Spark)
  #The cogroup is joining on blocking keys to create groups that look like this:
  #
  #    GROUPING_BPL_11_AGE_10 = { 
  #        DATASET_A: [(ID=0, BPL=11, AGE=10, NAME=Beatrice), (ID=1, BPL=11, AGE=10, NAME=Benedick)],
  #        DATASET_B: [(ID=22, BPL=11, AGE=10, NAME=Beatrice), (ID=23, BPL=11, AGE=10, NAME=Don Jon)]
  #    }

  grouped2 = mapped_a2.cogroup(mapped_b2).persist()
  print("Success -- joined the dataframes")
  ```
  7. Flat map each group using a function that will compare the names of everyon in the first dataset with everyone in the second.
  This function will keep those scores and then only return those records that have high enough scores to be a possible match.
  ```python
  #### Create a cache for comparisons that appear often (Eager spark)

  comp_caches = [frequent_comps_dict(grouped2, idx) for idx, comparison in comparisons_with_idx]

  #### Generate logical compare function (No Spark)

  logical_comp_function = generate_top_level_comp_function(config["comparisons"], idx_by_comparison_columns)
  print("Successfully created comparison function.")

  #### Create possible links (Lazy Spark)
  #Map each block to an array of possible links by comparing each element in the first dataset to each in the second.

  group_compare = create_group_compare_function(comparisons_with_idx, logical_comp_function, comp_caches)
  all_matches_dup_links = grouped2.flatMap(lambda group: group_compare(group))
  ```
  8. Remove the same links that were found in two different groups. For example if two records both had their age mapped to the same groups. 
  ```python
  #### Remove duplicate links (Eager Spark)
  #Some potential links are exact copies of one another, because they were found within multiple blocks. This step removes any duplicates.

  grouped_match_ab = all_matches_dup_links.groupBy(lambda match: (match[0][-1], match[1][-1]))
  all_matches = grouped_match_ab.map(lambda group: list(group[1])[0])
  all_matches.persist().count()
  ```
  9. Turn the reltant links into a dataframe, attach some metadata to them (what config file generated them), 
  and then sepearate out links that have records that were found multiple times with those that weren't found multiple times.
  This gives us a unique links dataframe and a duplicates dataframe. Then append those links onto a list of links found
  for these datasets.
  ```python
   #### Convert links to new data frame (Lazy Spark)

  def create_fields_for_output_df(comparison_columns, id_column, a_postfix, b_postfix):
      output_a_columns = [c + a_postfix for c in comparison_columns]
      output_b_columns = [c + b_postfix for c in comparison_columns]
      output_comp_value_columns = [c + "_comp_value" for c in comparison_columns]
      output_id_columns = [id_column + a_postfix, id_column + b_postfix]
      output_all_columns = output_a_columns + output_b_columns + output_comp_value_columns + output_id_columns
      output_ab_types = [StringType() for c in comparison_columns] + [StringType() for c in comparison_columns]
      output_value_types = [DoubleType() for c in comparison_columns] + [IntegerType(), IntegerType()]
      output_all_types = output_ab_types + output_value_types
      return StructType([StructField(field_name, field_type, True) for field_name, field_type in zip(output_all_columns, output_all_types)])
  id_col = config["id_column"]
  a_postfix = "_a"
  b_postfix = "_b"
  match_df_fields = create_fields_for_output_df(comparison_columns, id_col, a_postfix, b_postfix)
  match_df_rows = all_matches.map(lambda m: list(m[0][0:-1]) + list(m[1][0:-1]) + m[2] + [m[0][-1], m[1][-1]])
  match_df = sqlContext.createDataFrame(match_df_rows, match_df_fields).persist()
  
  #### Prep Metadata (No Spark)
  
  git_commit_hash_array = get_ipython().getoutput('git rev-parse HEAD')
  git_commit_hash = git_commit_hash_array[0]
  json_file_name = os.path.basename(json_file.name).replace(".json", "")
  commit_hash_udf = udf(lambda throwaway: git_commit_hash, StringType())
  json_file_name_udf = udf(lambda throwaway: json_file_name, StringType())
  
  #### Attach metadata and pull out duplicates from non-duplicates (Lazy Spark)
  
  a_id = id_col + a_postfix
  b_id = id_col + b_postfix
  
  a_dup_id = a_id + "_dup"
  b_dup_id = b_id + "_dup"
  
  duplicate_a_matches = match_df.groupBy(col(a_id).alias(a_dup_id)).count().filter("count > 1")
  duplicate_b_matches = match_df.groupBy(col(b_id).alias(b_dup_id)).count().filter("count > 1")
  match_join_dup_a = match_df.join(duplicate_a_matches.select(a_dup_id), match_df[a_id] == duplicate_a_matches[a_dup_id], "leftouter")
  match_join_dup_ab = match_join_dup_a.join(duplicate_b_matches.select(b_dup_id), match_join_dup_a[b_id] == duplicate_b_matches[b_dup_id], "leftouter")
  match_join_dup_ab.persist()
  
  match_join_dup_ab_git = match_join_dup_ab.withColumn("git_hash", commit_hash_udf(col(a_id)))
  match_join_dup_ab_with_metadata = match_join_dup_ab_git.withColumn("config_name", json_file_name_udf(col(a_id)))
  
  final_links = match_join_dup_ab_with_metadata.filter("{} is NULL AND {} is NULL".format(a_dup_id, b_dup_id))
  final_dups = match_join_dup_ab_with_metadata.filter("{} is NOT NULL OR {} is NOT NULL".format(a_dup_id, b_dup_id))
  
  
  #### Write out links and duplicates (Eager Spark)
  
  final_links.coalesce(50).write.mode('append').parquet(config["progressive_output_links_file"])
  final_dups.coalesce(50).write.mode('append').parquet(config["progressive_output_duplicates_file"])
  
  ```python
  10. Redo steps 1-9 with another config file, whose blocks are more lenient. For example, we might start looking at ages that are
  perfect in one step, and then start blocking on ages that are up to 1 off in the next step. Each step doesn't have to look at any of
  the records that were found in the previous step, so the datasets get smaller and smaller as our blocks become more and more lenient.
  This allows us to eventually use very lenient blocking techniques on the data without paying the price of comparison on the whole datset.
