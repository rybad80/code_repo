{% docs __overview__ %}

# Chop Data Blocks

This repository contain research, documentation, SQL files that create the CHOP Data Blocks tables in the CHOP Data Warehouse.

The CHOP Data Blocks, or "CHOP Blocks", are a curated data layer (i.e. tables) that analysts and visualization tool users can use to answer most (~80%) of questions quickly and reliably.

## What's New in Chop-Data-Blocks

* Check out the [release notes](https://github.research.chop.edu/analytics/chop-data-blocks/blob/master/news.md)
* See all the blog posts our team has written [here](https://wiki.chop.edu/label/SSDC/chop-blocks)

## Learn the basics about the project

* [Motivation and background of the blocks](https://wiki.chop.edu/display/DA/CHOP+Data+Blocks?preview=/219847845/219847896/CHOP%20Data%20Blocks%20Background.pptx)
* [Analyst-led contribution model of the blocks](https://wiki.chop.edu/display/DA/CHOP+Data+Blocks?preview=/219847845/219847897/CHOP-Data-Blocks-Analyst-led-contribution-wide.pptx)

## Blocks Core Team

* [David Maier](mailto:maierd@chop.edu)
* [Christian Minich](mailto:minichc@chop.edu)
* [Mary Rosman](mailto:rosmanm@chop.edu)
* [Mayank Sardana](mailto:sardanam@chop.edu)

## Related Repositories

* [Netezza Adapter for dbt](https://github.research.chop.edu/analytics/dbt-netezza)
* [dbt utils for CHOP](https://github.research.chop.edu/analytics/dbt-chop-utils)
* [CDW-Dependency-API](https://github.research.chop.edu/EAR/readyornot/)

## Data Platform SQL Style Guide

### What is a code style?

Code style is anything that is a stylistic choice in the code that has no affect on code behavior.

A great example of this is tabs versus spaces.

### Why should we use a code style?

Consistency: it makes the code base easier to read and follow.

Ideally, it should look like there was only one person developing the code.

### What SQL conventions should we follow?

* DO NOT USE UPPERCASE. lowercase is much friendlier
* When doing string comparisons use lower()
* Do **not** use tabs. Use 4 spaces instead
* Keyword clauses should be on their own line (`select`, `from`, `where`, `group by`, `order by`, etc...)
* be explict it your join type, call it `inner join` and call its `left join`
* never use a `right join`
* `on` conditions for table joins
	* Placed on the same line as the table (unless there are multiple conditions)
	* Aligned to the `on` conditions on the previous line (when applicable)
	* Place the current table first when linking to other tables
	* No table aliasing, always write out the full table name
* When aliasing the `cdw_dictionary` table name it after the key you are joining on and drop the `_key` part. For example when joining to `visit.dict_pat_class_key` call the dictionary like this `cdw_dictionary as dict_pat_class`
* When aliasing table or renaming columns use `as`
* Use CTEs instead of subqueries

```sql
-- NO
from t1
inner join table2 t2
 t1.columnA = t2.columnA
inner join table3 t3 on t1.columnB = t3.columnA
inner join tableWithLongerName t4 on t1.columnB = t4.columnA

-- YES
from
  table1
  inner join table2       on table2.columnA = table1.columnA
  inner join table3       on table3.columnA = table1.columnB
  inner join tablewithlongername on tablewithlongername.columnA = table1.columnB
```

* `and` and `or` conditionals should be at the beginning of a line, not at the end of a previous line

```sql
-- NO
where 
  column1 > 45 and
  column2 < 4.5

--YES  
where
  column1 > 45
  and column2 < 4.5

```

#### Column Names

* Use underscores (`this_is_column_one`), not camelcase (`ThisIsColumnOne`)
* Be explict with the table name before the column name `procedure_order.proc_ord_create_dt`
* Avoid using abbreviations in the column name and write out the full word instead `procedure_order.proc_ord_create_dt as order_create_date`
* Leave IDs and keys as-is, so `prov_nm` becomes `provider_name` but `prov_id` and `prov_key` stay abbreviated
* When including both a name and ID for a data element, have the columns next to each other and order the columns so name comes before ID
* Each column should be on its own line
* Commas are placed at the end of a line, not at the beginning

```sql
-- NO

select 

  c1 columnOne
  , c2 columnTwo
  
-- YES
select

  c1 as column_one,
  c2 as column_two

```

### Core Blocks Specific Standards

#### Core Columns

These are columns that every block must contain. They are "pre-formatted" and can be found in the blocks stage tables `stg_patient` and `stg_encounter`

In order:

1. Block Specific Primary Key
2. `stg_patient.patient_name`
3. `stg_patient.mrn`
4. `stg_patient.dob`
5. `stg_encounter.csn`
6. `stg_encounter.encounter_date`
7. Block Specific Columns
8. Block Specific Key (i.e. those used to join out such as `dept_key` or `prov_key`)
9. `stg_patient.pat_key`
10. `stg_encounter.hsp_acct_key`
11. `stg_encounter.visit_key`

### Stack Specific Standards

#### Table Standards

* Stage = `stg_`
* Stack Prefix = short prefix for entire stack
* Table naming should follow the standard of prefix (`harm_ip`) + table description (`clabsi`)
* i.e. `harm_ip_clabsi` 

#### Column Standards**

* When applicable, include Blocks core columns within the stack (`patient_name`. `mrn`, `dob`, `csn`, `encounter_date`) 
* Stacks *_should not_* use columns in a stage table that falls outside of the stack (i.e. those that are contained in a blocks stage)

### Putting it all together

```sql
-- Example query
select
  table1.primary_key,
  stg_patient.patient_name,
  stg_patient.mrn,
  stg_patient.dob,
  stg_encounter.csn,
  stg_encounter.encounter_date,
  table1.c1 as column_one,
  table1.c2 as column_two,
  table4.c3 as column_three,
  stg_patient.pat_key,
  stg_encounter.hsp_acct_key,
  stg_encounter.visit_key
from
  table1
  inner join table2       on table2.primary_key = table1.primary_key
  inner join tablelongername  on tablelongername.number_column = table2.other_column
  inner join table4 
    on table4.key1 = table2.key1
    and table4.key2 = table1.key2
where
  table1.number_column < 45
  and lower(table2.string_column) = 'hello'

```

{% enddocs %}
