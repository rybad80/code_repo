***Last Update:** 07/2023*

This document contains information related to developing within the ED QI Pipeline.

1. [Cohorts](#cohorts)
2. [Seeds (Lookups)](#seeds-lookups)
3. [Metric Components](#metric-components)
   - [ed_encounter_metric_medication](#ed_encounter_metric_medication)
   - [ed_encounter_metric_medication_details](#ed_encounter_metric_medication_details)
   - [ed_encounter_metric_medication_history](#ed_encounter_metric_medication_history)
   - [ed_encounter_metric_medical_history](#ed_encounter_metric_medical_history)
   - [ed_encounter_metric_pathway](#ed_encounter_metric_pathway)
   - [ed_encounter_metric_procedure_order](#ed_encounter_metric_procedure_order)
   - [ed_encounter_metric_procedure_details](#ed_encounter_metric_procedure_details)
   - [ed_encounter_metric_smart_data_element](#ed_encounter_metric_smart_data_element)
   - [ed_encounter_metric_flowsheet](#ed_encounter_metric_flowsheet)
   - [ed_encounter_metric_descriptive](#ed_encounter_metric_descriptive)
   - [ed_encounter_metric_flow_timestamp](#ed_encounter_metric_flow_timestamp)
4. [Additional Models and Functionality](#additional-models-and-functionality)
   - [ed_encounter_metric_additional_transform](#ed_encounter_metric_additional_transform)
   - [ed_qi_concatenated_value_compare](#ed_qi_concatenated_value_compare)


# Cohorts

## Structure
The ulimate structure of that your cohort's staging table must adhere to is as follows:

1. `visit_key`
2. `pat_key`
3. `cohort`
    * The convention for `cohort` is fully capitalized and to utilize `_`instead of spaces (e.g. `FEBRILE_INFANT`)
4. `subcohort`
    * `subcohort` can follow any naming convention

## Development

0. Create a new branch to work on your cohort based on the default branch (`uat`)
1. To begin, you should develop the initial version of this cohort the same way you would any other data pull, in your normal tool of choice (e.g. DBeaver)
    * Ideally, you can use this more agile version of your code (i.e. not subject to weekly releases) to complete your validation and UAT with your team
2. Please ensure your base cohort utilizes `stg_encounter_ed` as the base for ED visits
3. Many existing cohorts will use a rolling 5 year timeframe, but this is optional and can be adjusted to fit your project's needs
    * You can achieve this by using thise code: `where year(stg_encounter_ed.encounter_date) >= year(current_date) - 5`
4. This is a good time to make sure your local branch is up-to-date with the latest release
    * If you are using GitHub Desktop, you can simple use the "Update from uat" option under the Branch menu
5. Move over to Visual Studio (VS) Code, create a file named `stg_ed_encounter_cohort_*` (with `*` being replaced by the name of your cohort), and place it in [/etl/ED/QI/cohort_stage](/etl/ED/QI/cohort_stage)
6. You can now copy/paste your current cohort code into this file and replace the table names with dbt's `{{ ref() }}` and `{{ source() }}` functions
    * If a source table is not in the etl/sources.yml, be sure remember to add it and remember this for later (there are instructions to follow in the GitHub pull request)
7. Run your new model (see examples below, be sure to replace with the name of your file):
    * Marx
    ```
    dbt run -m stg_ed_encounter_cohort_fever_discharge --defer
    ```
    * Local:
    ```
    dbt run -m stg_ed_encounter_cohort_fever_discharge --defer --state uat-run-artifacts
    ```
8. Add your new cohort stage table as a `ref()` in the list at the top of [/etl/ED/QI/cohort/ed_encounter_cohort_long.sql](/etl/ED/QI/cohort/ed_encounter_cohort_long.sql)
9. Add your cohort and relevant information (including a [Gene](http://gene.chop.edu/) article ID) to [/data/ED/lookup_ed_cohort_metadata.csv](/data/ED/lookup_ed_cohort_metadata.csv) and run it to populate your cohort for testing:
  ```
  dbt seed --select lookup_ed_cohort_metadata --full-refresh
  ```
  * Note:
    * You will need to circle back to finalizing this Article once the table exists in order to link to it on the right-hand side as the Cohort Source Table
10. As you've already run your model and the seed file, you only need to run the rest of the cohort pipeline to test:
    * Marx:
    ```
    dbt run -m stg_ed_cohort_metadata_acuity_dist ed_encounter_cohort_long ed_encounter_cohort_wide ed_encounter_cohort_metadata --defer
    ```
    * Local:
    ```
    dbt run -m stg_ed_cohort_metadata_acuity_dist ed_encounter_cohort_long ed_encounter_cohort_wide ed_encounter_cohort_metadata --defer --state uat-run-artifacts
    ```
11. Confirm your new cohort ran successfully into `chop_analytics_dev.admin.ed_encounter_cohort_wide__*` and `chop_analytics_dev.admin.ed_encounter_cohort_metadata__*`, where `*` is replaced with your login
12. Open a pull request into `uat` from your branch and follow the instructions provided in the pull request template and any recommendations from reviewers
    * Note: In terms of [Metadata Universe](https://mu.analytics.chop.edu/) entries, as of writing you only need to add your cohort's column to `ed_encounter_cohort_wide`, as the pipeline will create a column for it there automatically and we do not need to document stage tables
13. After your cohort has been released to production, do not forget to circle back and update your Gene cohort article from earlier.
    * Please follow current recommendations/best practices to populate your Gene Article
        * ***NOTE: As of writing, groups should not be placed in Gene articles under certain roles (e.g. technical owner). Please refer to the #gene channel within Slack to confirm if this is still true.***
    * In addition, please conform to the standards present in other ED cohorts found within the overarching [Emergency Department Cohorts](https://chop.alationcloud.com/article/583/) Article
        * Once you're done with yours, please also add it to the list on this article as well!

Some additional background, tips, and legacy instructions are still available on the [ED QI Central: Development Steps](https://wiki.chop.edu/display/DA/ED+QI+Central%3A+Development+Steps) Wiki Article. This includes information on working within Qlik Sense, which is outside the scope of this document.

If you have any questions, please reach out to the ED data team via email to [pediatricsdata@chop.edu](mailto:pediatricsdata@chop.edu).

# Seeds (Lookups)
The majority of Lookup tables within the pipelines are tables that are based on dbt seeds. The seeds for the `chop-data-blocks` repo are contained within the `data` folder (`data/ED` for ED-specific lookups). Once you've updated a seed file, you can run that file with `dbt seed --select *seed name*`. For example, to add a new procedure for general procedure information, you'd update `lookup_ed_events_procedure_order_clinical.csv` and run `dbt seed --select lookup_ed_events_procedure_order_clinical`.


# Metric Components
While the naming convention of *Metric* is used, it may be helpful to think of this section of the ED Pipeline as a mechanism through which you can ultimately generate a series of facts within the CDW. These facts are intended to serve as building blocks, upon which true Metrics can be built within the various ED BI applications. This approach allows for the component parts (i.e facts) of metrics to both share a common definition and be easily reused for future projects, accelerating and easing future development.

| :information_source: Example |
|:---------------------------|
| Say we create a fact around when a medication was administered within the ED. Within the CDW this is merely a fact, however, this fact can then be used within the ED QI Central Qlik Sense application to easily create two Metrics. When used in conjunction with two other readily available facts (ED Arrival Date and ED Roomed Date), we can easily create both Minutes from Arrival to Medication *and* Minutes from Rooming to Administration. |

Functionally, the **majority** (though not all, see each table below) of the pipeline starts with the definition of an *event* in a [lookup](#seeds-lookups) file. Why add this extra layer of events? Flexibility! This affords us the ability to have multiple criteria (think *OR* logic) define the same base *event*, or *thing* we are measuring, and still maintain a simple to use `.csv` flat file-based entry point into the pipeline. From there, the pipeline processes each event through a series of steps, generating a pre-defined set of facts on each event. Just be sure to be mindful of your `event_name` and ensure it is specific enough to adequately represent the resultant data (e.g. `cbc_bands_last_ed`).

In the following sections, we'll walk through each resultant metric table and provide additional details on how you interact with them (e.g. add to them), what you get out of each one, and any special notes.

In addition to the primary fact generation portions of the ED QI Central pipeline described below, there is also the ability to perform an additional level of transformation for certain data elements that are otherwise too complex/non-performant to follow the standard approach of generating facts within the pipeline and creating metrics upon those facts within the BI tool. Additional details can be found in the [Additional Models and Functionality](#additional-models-and-functionality) section below.


## ed_encounter_metric_medication
### Description
Creates common indicators and timestamps based on the medications input into the pipeline.

### Outputs
* Ordered in Visit Ind
* Ordered in Visit Timestamp
* Admin in Visit Ind
* Admin in Visit Timestamp
* Discharge Med in Visit Ind
* Discharge Med in Visit Timestamp

### Notes
Pipeline matches based on Medication Name ***OR*** Medication Generic Name matching the supplied pattern.

Ordered and Administered require timestamps prior to ED Depart (Leaving the ED and/or EDECU).

Only facts pertaining to the first instance of the event are carried to the final table.

### DAG
<pre>lookup_ed_events_medication_order_administration
  ↳ stg_ed_events_medication_order
    ↳ ed_events
      ↳ stg_ed_encounter_metric_medication
        ↳ <b><i>ed_encounter_metric_medication</b></i></pre>


## ed_encounter_metric_medication_details
### Description
Generates additional details about medications administered in the ED ***OR*** are discharge medications

### Outputs
* Duration
* Route

### Notes
Pipeline matches based on Medication Name ***OR*** Medication Generic Name matching the supplied pattern.

Both Order and Admin Routes are ran through `lookup_ed_events_medication_route` to group them, with a preference given to Order Route (assuming it's not `Not Applicable`).

All routes are group concated together and all durations summed (not just from the first instance of the event).

### DAG
<pre>lookup_ed_events_medication_order_administration (req.) + lookup_ed_events_medication_route (left/grouper)
  ↳ stg_ed_events_medication_details
    ↳ ed_events
      ↳ stg_ed_encounter_metric_medication_details
        ↳ <b><i>ed_encounter_metric_medication_details</b></i></pre>


## ed_encounter_metric_medication_history
### Description
Creates data elements for specific medications that were either discharge medications or administered at CHOP within their specified windows **prior** to the ED visit.

### Outputs
* History Date
* History Indicator

### Notes
Pipeline matches based on:
  - Medication Name ***OR*** Medication Generic Name matching the supplied pattern
  - Accepts an interval for the window

Only facts pertaining to the first instance of the event are carried to the final table.

### DAG
<pre>lookup_ed_events_medication_history
  ↳ stg_ed_events_medication_history
    ↳ ed_events
      ↳ stg_ed_encounter_metric_medication_history
        ↳ <b><i>ed_encounter_metric_medication_history</b></i></pre>


## ed_encounter_metric_medical_history
### Description
Pulls indicators allergies/infections active/known as of the ED visit (even if deleted/cleared after).

### Outputs
* Indicators

### Notes
Uses hard-coded values to manually create indicators. Candidate for future expansion into a proper pipeline.

### DAG
*None*


## ed_encounter_metric_pathway
### Description
Indicates if an ED Pathway tracer/tracker Order was ordered. Lookup file is not a seed, but rather utilizes a matching pattern of `'ed%pathway%'`and a hardcoded list for additional orders that do not match the pattern.

### Outputs
* Indicator

### Notes
Only includes Child Orders.

### DAG
<pre>lookup_ed_events_pathway_order
  ↳ stg_ed_events_pathway_order
    ↳ ed_events
      ↳ stg_ed_encounter_metric_pathway
        ↳ <b><i>ed_encounter_metric_pathway</b></i></pre>


## ed_encounter_metric_procedure_order
### Description
Returns high-level information about procedure orders.

### Outputs 
* The Procedure Name is returned in a column named after the event name (from lookup file)
* A timestamp - either date placed, date resulted, or specimen collected date
  * **May  be null** unless timestamp is placed date. The result and specimen timestamps in `procedure_order_clinical` may be null, while the placed date is always non-null. Therefore, this metric will always produce a non-null timestamp when a matching event is found and the selected timestamp is 'placed'. However, when the selected timestamp is result or specimen, the timestamp recorded will be the first/last _not null_ value, if such a value exists. If there are procedure orders matching the selection criteria but all of the desired timestamps are null, a result row will be produced with a null timestamp value.
* Total number of matching procedure orders (regardless of whether or not the timestamp value is null)

### Notes
Select event timestamp by specifying either `placed`, `result`, or `specimen` in the `event_timestamp_selection` column of the seed file.

Matches based on:
* Procedure ID ***OR*** Procedure Name matching the supplied pattern on lookup file
* First vs. Last instances of the event based on lookup file (e.g. `first`)
* Selections are limited to provided settings between ED, EDECU, and IP. Please provide a `/` delimited list of allowable settings in lookup file (e.g. `ED/EDECU/IP`)
* Optionally excludes orders based on order status. Please provide a `/` delimited list of values from `procedure_order_clinical.order_status` (e.g. `canceled/invalid`)

**The event name may _not_ end with `_count`.** This is to ensure that there are no name collisions when the pipeline adds `_count` to the event name in order to record the total count of matching procedure orders.

### DAG
<pre>lookup_ed_events_procedure_order_clinical
  ↳ stg_ed_events_procedure_order
    ↳ ed_events
      ↳ stg_ed_encounter_metric_procedure_order
        ↳ <b><i>ed_encounter_metric_procedure_order</b></i></pre>


## ed_encounter_metric_procedure_details
### Description
Returns details related to procedure orders that have results.

### Outputs
* **Order**
  * Description: Provides a `1` if a match was found
  * Default Length: 1
* **Order Date**
  * Description: Minimum matched Placed Date
* **Specimen**
  * Description: List of *distinct* specimen
  * Default Length: 50
* **Specimen Date**
  * Description: Minimum matched Collection Date
* **Result**
  * Description: Concatenation of *all* matched results
  * Default Length: 50
* **Result Date**
  * Description: Minimum matched Result Date
* **Organism**
  * Description: List of *distinct* organisms
  * Default Length: 150
* **Organism Date**
  * Description: Minimum matched Result Date


### Notes
#### Matches based on:
* Any combination of the following that are provided:
  * Procedure ID
  * Procedure Name (Pattern)
  * Result Component ID
  * Result Component Name (Pattern)
  * Result Value (Pattern)
  * Specimen (Pattern)
* First vs. Last vs. All instances of the event based on lookup file (e.g. `first`)
* Selections are limited to provided settings between ED, EDECU, and IP. Please provide a `/` deliminted list of allowable settings in lookup file (e.g. `ED/EDECU/IP`)

#### Lengths
If any of the attributes being added for your procedure *routinely* exceed the default maxes listed above with *substantive* data, please consider adjusting the pipeline to account for them. If you are not comfortable making this change, or are unable to trace back where to make it, please reach out to the ED data team member on Teams/Slack. If you are not sure who to reach out to, please open an issue on GitHub and email [pediatricsdata@chop.edu](mailto:pediatricsdata@chop.edu).

### DAG
<pre>lookup_ed_events_procedure_details
  ↳ stg_ed_events_procedure_details_raw
    ↳ stg_ed_events_procedure_details_sequenced
      ↳ stg_ed_events_procedure_details
        ↳ stg_ed_encounter_metric_procedure_details
          ↳ <b><i>ed_encounter_metric_procedure_details</b></i></pre>


## ed_encounter_metric_smart_data_element
### Description
Returns high-level details about Smart Data Elements.

### Outputs 
* Value(s)
  * Text, Numeric, or Timestamp, depending on selection (see below)
* Entered Date

### Notes
SDEs associated with deleted notes are ignored.

Selection and Output Criteria:
* Concept ID
  * + Element Value (if provided)
  * + IP Note Type Category (if provided)
* Care Settings
  * Selections are limited to provided settings between ED, EDECU, and IP. Please provide a `/` deliminted list of allowable settings in lookup file (e.g. `ED/EDECU/IP`)
* Selection Type
  * The pipeline accepts descriptive (`first`, `last`, `all`) as well as positional (`-n` to `n`) selection criteria:
    * Positive numbers select occurances based on chronological order (i.e. `1` is the first, `2` is the second, etc.)
    * Negative numbers select occurances based on ***reverse*** chronological order (i.e. `-1` is the last, `-2` is the second to last, etc.)
    * `all` and `0` both select ***all*** matched non-null values
      * Note: When selecting `all`, results will be a group concatenated list in chronological order
* Output Type
  * Text, Numeric, or Timestamp
    * Text returns based on Data Type:
      * `boolean`: A value of `Yes` **or** `No`
      * `element id`: The returned concept's abbreviation (if populated) -> name
      * `date`: The returned Epic DTE converted to the format `YYYY-MM-DD`
      * `database` (`SER`): The returned provider's current name on their SER record
      * All Others: The unaltered text value in Epic
    * Numeric returns based on Data Type:
      * `boolean`: A value of `1` **or** `0`
      * `date`: The returned Epic DTE converted to a **number** in the format `YYYYMMDD`
      * All Others: The unaltered numeric value in Epic
    Timestamp: The returned Epic DTE converted to a timestamp (only for `date` data type)

### DAG
<pre>lookup_ed_events_smart_data_element_all
stg_ed_events_smart_data_element_notes
  ↳ stg_ed_encounter_metric_smart_data_element
        ↳ <b><i>ed_encounter_metric_smart_data_element</b></i></pre>


## ed_encounter_metric_flowsheet
### Description
Returns high-level facts related to flowsheets utilized during the encounter.

### Outputs
* Value(s)
  * Text or Numeric, depending on selection (see below)
* Max Recorded Date

### Notes
Disregards null values on flowsheets when selecting the requested value(s).

Selection and Output Criteria:
* Flowsheet ID
* Care Settings
  * Selections are limited to provided settings between ED, EDECU, and IP. Please provide a `/` deliminted list of allowable settings in lookup file (e.g. `ED/EDECU/IP`)
* Selection Type
  * The pipeline accepts descriptive (`first`, `last`, `all`) as well as positional (`-n` to `n`) selection criteria:
    * Positive numbers select occurances based on chronological order (i.e. `1` is the first, `2` is the second, etc.)
    * Negative numbers select occurances based on ***reverse*** chronological order (i.e. `-1` is the last, `-2` is the second to last, etc.)
    * `all` and `0` both select ***all*** matched non-null values
      * Note: When selecting `all`, results will be a group concatenated list in chronological order
* Output Type
  * Text vs. Numeric
    * Note: Numeric selects numeric data from Epic. If the data is not available/stored as Numeric, please utilize text and conver yourself.

### DAG
<pre>lookup_ed_events_flowsheets
  ↳ stg_ed_encounter_metric_flowsheet
    ↳ <b><i>ed_encounter_metric_flowsheet</b></i></pre>


## ed_encounter_metric_descriptive
### Description
These tables are classic data marts that make columns based on SQL that an analyst would need to create/update.

### Outputs
*Various*

### Notes
Uses various sources (`cdwuat..fact_edqi`, custom, etc.).

### DAG
*None*


## ed_encounter_metric_flow_timestamp
### Description
These tables are classic data marts that make columns based on SQL that an analyst would need to create/update.

### Outputs
* Timestamps

### Notes
Mostly pulls directly from `cdwuat..fact_edqi`

### DAG
*None*


# Additional Models and Functionality

This section describes the more advanced/non-standard models and functionality within the ED QI Pipeline.

## ed_encounter_metric_additional_transform
### Description
This portion of the pipeline houses any project-specific data elements and transforms otherwise too complex/non-performant to create within the ED QI pipeline/associated BI tool, or if they must leverage the facts generated by upstream portions of the ED QI Pipeline. Examples include:

  - A limited set of attributes that are sourced from a one-off data source, which therefore does not warrant a new pipeline being created upon it
  - Instances where complex transformations are required (e.g. using loops within dbt)

### Outputs
Each attribute (sans duplicative `visit_key`) contained within the staging tables found within [/etl/ED/QI/metric_additional_transforms/stage](/etl/ED/QI/metric_additional_transforms/stage).

### Notes
In order to maintain alignment with the general design philosophy of the ED QI pipeline, facts/metrics created within this section of the pipeline ***should not*** be limited to an individual cohort wherever possible, just on the off chance that they can/must be reused in future projects.

Once you've created your staging table within [/etl/ED/QI/metric_additional_transforms/stage](/etl/ED/QI/metric_additional_transforms/stage), you must add your staging table to the list of tables contained within `ed_encounter_metric_additional_transform` at the top of the file (titled `level_two_tables`).

### DAG
<pre>etl\ED\metric_additional_transforms\stage
  ↳ <b><i>ed_encounter_metric_additional_transform</b></i></pre>


## ed_qi_concatenated_value_compare
### Description
This Macro facilitates numeric-like comparisons on the facts generated by the ED QI Pipeline. For additional details, please reference the information contained within the [ed_qi_concatenated_value_compare](/macros/ED/ed_qi_concatenated_value_compare.sql) macro itself.