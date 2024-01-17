# scorecards

Pipeline to support all Performance Scorecards. Currently, the following 

This directory houses the scorecard pipeline that centralizes metric staging tables, calculates metric values, and manipulates the resulting data structure to support front-end scorecard display. 

Please find more information about Performance Scorecards porfolio [here](https://wiki.chop.edu/display/DA/Performance+Scorecards?src=contextnavpagetreemode). 

### Owners: 
- DnA Lead: Matt Dye 
- DTO: Genna Kreher
- Technical owners: Jessica Yarnall, Matt Devine, Paul Wildenhain, Rob Olsen

### Contributing

Follow the below instructions for adding a new metric to the scorecard pipeline. These instructions assume that all of your data sources are already in blocks/stacks/data lake. See the [`contributing.md`](../../contributing.md) for instructions on how to add blocks/stacks to support your metrics.

1. Does your metric pull directly from a stack or block without needing to add where logic to a SQL statement?
    1. Yes
        1. Does the table have values for the following columns? `primary_key`, `drill_down_one`, `drill_down_two`, `metric_date`, `num`, `denom`, `visual_month`, `month_target`
            1. Yes (Skip to step 3)
            2. Not quite (move to step 2)
    2. No (move to step 2)
2. Create staging tables
    1. Staging tables are stored in the `Metric_Staging` directory of the related domain area. For example, the neonatology staging tables can be found in [`Neonatology/Metric_Staging`](../Neonatology/Metric_Staging). Will the new metric(s) be added to an existing artifact?
        1. Yes (move to step 2.3) 
        2. No (move to step 2.2)
    2. Create a `Metric_Staging` and a `Metric_Targets` (if using targets) folder in the appropriate domain area, such as [`Neonatology/Metric_Staging`](../Neonatology/Metric_Staging).
    3. Create `.sql` files in the `Metric_Staging` and `Metric_Targets` folders.
        1. Metric Files
            1. The following columns are required: `primary_key`, `metric_date`, `num`, `domain`, `metric_name`, `num_calculation`, `metric_id`, `desired_direction`, `metric_type`. See the appropriate values for and more information on these columns in the [appendix](appendix.md).
            2. The following columns are optional: `drill_down_one`, `drill_down_two`, `denom`, `denom_calculation`, `subdomain`, `metric_lag`. See the appropriate values for and more information on these columns in the [appendix](appendix.md).
            3. Should be named `stg_scorecard_[artifact abv.]_[metric name abv.]`
                1. Ex. `stg_scorecard_cardiac_harm_index.sql` for Cardiac’s harm index metric
            4. Metric tables are expected to be un-aggregated raw data. The pipeline will automatically aggregate the metrics up to the month, quarter, year, etc levels.
        2. Target files 
            1. The following columns are required: `visual_month`, `metric_type`, `metric_id`, `month_target`. See the appropriate values for and more information on these columns in the [appendix](appendix.md).
            2. The following columns are optional: `drill_down_one`, `drill_down_two`. See the appropriate values for and more information on these columns in the [appendix](appendix.md).
            3. Should be named `stg_scorecard_[artifact abv.]_[metric name abv.]_targets`
                1. Ex. `stg_scorecard_cardiac_harm_index_targets.sql` for Cardiac’s harm index targets
            4. Target tables, unlike metric tables, are expected to be strictly at the month level. This will be extrapolated out to apply to all the days in that month.
        3. Add any sources outside the dbt project to the [`sources.yml`](../sources.yml)
            1. Tables should be added in alphabetical order under their appropriate database
3. Add new metrics to the [`stg_scorecard_data`](catalog/stg_scorecard_data.sql) table
    1. Add new metric to the appropriate artifact (i.e dashboard or report) set
        1. If an artifact does not exist for this metric, you may create a new set to hold the metric after the existing sets. After creating the set, ensure you add the set name to the list of sets below your newest set as follows `{% set metrics = (..........) + [new_artifact]_metrics %}`
        2. Fill in the following variables. See `2.3.1` for which columns are required and which are optional. Details on column requirements can be found in the [appendix](appendix.md)
            ```
              {
                "domain": "",
                "subdomain": "",
                "metric_name": "",
                "primary_key": "",
                "drill_down_one": "",
                "drill_down_two": "",
                "metric_date": "",
                "num": "",
                "denom": "",
                "num_calculation": "",
                "denom_calculation": "",
                "metric_type": "",
                "direction": "",
                "metric_lag": "",
                "metric_id": "",
                "lookback_period": "",
                "table": ""
              }
            ```
4. Add new metrics to the [`stg_scorecard_targets`](targets/stg_scorecard_targets.sql) table
    1. Add new target to the appropriate artifact set
        1. If an artifact does not exist for this target, you may create a new set to hold the target after the existing sets. After creating the set, ensure you add the set name to the list of sets below your newest set as follows `{% set targets = (..........) + [new_artifact]_targets %}`
        2. Fill in the following variables. See `2.3.2` for which columns are required and which are optional. Details on column requirements can be found in the [appendix](appendix.md)
            ```
            {
              "visual_month": "",
              "drill_down_one": "",
              "drill_down_two": "",
              "metric_type": "",
              "month_target": "",
              "metric_id": "",
              "table": ""
            }
            ```
5. Add the metric to [`data/scorecard_metrics/lookup_scorecard_metric_ids.csv`](../../data/scorecard_metrics/lookup_scorecard_metric_ids.csv)
    1. Ensure your `metric_id` is not already in the table.
        1. `metric_id` should begin with an abbreviation of the artifact followed but an abbreviated metric name. i.e. Neonatology’s Average Daily Census has a `metric_id = neo_adc`
        2. Fill in `artifact`,  `metric_id`, `metric_group`
            1. `metric_id` must match the `metric_id` for your metric in the `stg_scorecard_data.sql` and `stg_scorecard_targets.sql` files
            2. `artifact` and `metric_group` will be used in the QS load script. The Scorecard sheet will display metrics by `metric_group` in the same table. Ensure your `metric_group` is consistent with how you’ll be displaying the data.
6. Run `dbt run` and `dbt test` commands.
7. Commit changes and open a PR. Once merged, your metrics will be ready to display in a QS app using the template!

### Testing

Use `dbt` to run the regular test suite

```
dbt test
```

In order to run our equality tests run the `seed` and `run` commands first and use the `ci` target

```
dbt seed --target ci
dbt run --target ci
dbt test --target ci
```

Note: This will not work unless you have a `ci` target in your `profiles.yml`. Follow this template in order to add it:

```yml
# example profiles.yml file
cdw:
  target: local
  outputs:
    ci:
      type: netezza
      database: QMR_DEV
      host: uat.cdw.chop.edu
      user: ""
      pass: ""
      port: 5480
      schema: ADMIN
      threads: 4
```