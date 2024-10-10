# Dagster & DBT

In this demo, we’ll focus on integrating a dbt project with Dagster from end to end. We’ll use data from [NYC OpenData](https://opendata.cityofnewyork.us).

By the end of the course, you will:

* Create dbt models and load them into Dagster as assets
* Run dbt and store the transformed data in a DuckDB database
* Apply partitions to incremental dbt models

<details>
<summary>
<b>Introduction</b>
</summary>

## Introduction

<button data-command="run:git reset --hard 2eca8baa0731fdb406ed867e101aedf67d9bf9e5">Checkpoint: Introduction</button>

In the world of ETL/ELT, dbt - that’s right, all lowercase - is the ‘T’ in the process of Extracting, Loading, and **Transforming** data. Using familiar languages like SQL and Python, dbt is open-source software that allows users to write and run data transformations against the data loaded into their data warehouses.

dbt isn’t popular only for its easy, straightforward adoption, but also because it embraces software engineering best practices. Data analysts can use skills they already have - like SQL expertise - and simultaneously take advantage of:

* **Keeping things DRY (Don’t Repeat Yourself).** dbt models, which are business definitions represented in SQL `SELECT` statements, can be referenced in other models. Focusing on modularity allows you to reduce bugs, standardize analytics logic, and get a running start on new analyses.
* **Automatically managing dependencies and generating documentation.** Dependencies between models are not only easy to declare, they’re automatically managed by dbt. Additionally, dbt also generates a DAG (directed acyclic graph), which shows how models in a dbt project relate to each other.
* **Preventing negative impact on end-users.** Support for multiple environments ensures that development can occur without impacting users in production.

Dagster’s approach to building data platforms maps directly to these same best practices, making dbt and Dagster a natural, powerful pairing.

At a glance, it might seem like Dagster and dbt do the same thing. Both technologies, after all, work with data assets and are instrumental in modern data platforms.

However, dbt Core can only transform data that is already in a data warehouse - it can’t extract from a source, load it into its final destination, or automate either of these operations. And while you could use dbt Cloud’s native features to schedule running your models, other portions of your data pipelines - such as Fivetran-ingested tables or data from Amazon S3 - won’t be included.

To have everything running together, you need an orchestrator. This is where Dagster comes in:

> Dagster’s core design principles go really well together with dbt. The similarities between the way that Dagster thinks about data pipelines and the way that dbt thinks about data pipelines means that Dagster can orchestrate dbt much more faithfully than other general-purpose orchestrators like Airflow.  
>
> At the same time, Dagster is able to compensate for dbt’s biggest limitations. dbt is rarely used in a vacuum: the data transformed using dbt needs to come from somewhere and go somewhere. When a data platform needs more than just dbt, Dagster is a better fit than dbt-specific orchestrators, like the job scheduling system inside dbt Cloud. ([source](https://dagster.io/blog/orchestrating-dbt-with-dagster))  

At a glance, using dbt alongside Dagster gives analytics and data engineers the best of both their worlds:

* **Analytics engineers** can author analytics code in a familiar language while adhering to software engineering best practices
* **Data engineers** can easily incorporate dbt into their organization’s wider data platform, ensuring observability and reliability
  
There’s more, however. Other orchestrators will provide you with one of two less-than-appealing options: running dbt as a single task that lacks visibility, or running each dbt model as an individual task and pushing the execution into the orchestrator, which goes against how dbt is intended to be run.

Using dbt with Dagster is unique, as Dagster separates data assets from the execution that produces them and gives you the ability to monitor and debug each dbt model individually.
</details>

<details>
<summary>
<b>Lesson 1</b>
</summary>

## The dbt project

<button data-command="run:git reset --hard 2eca8baa0731fdb406ed867e101aedf67d9bf9e5">Checkpoint: Lesson 1</button>

We have a dbt project in the `analytics` directory. Throughout the duration of this module, you’ll add new dbt models and see them reflected in Dagster.

To get started we will use the models already provided in `analytics/models/sources/` & `analytics/models/staging/`.

To begin let's install the dbt package dependencies & run the project:

<button data-command="run:cd analytics && dbt deps && dbt build; read">Run `dbt deps && dbt build`</button>

Ignore any errors for now, we will resolve those shortly.

</details>

<details>
<summary>
<b>Lesson 2</b>
</summary>

## Connecting dbt & Dagster

<button data-command="run:git reset --hard 2eca8baa0731fdb406ed867e101aedf67d9bf9e5">Checkpoint: Lesson 2</button>

### Constructing the dbt project

Independent of Dagster, running most dbt commands creates a set of files in a new directory called `target`. The most important file is the `manifest.json`. More commonly referred to as “the manifest file,” this file is a complete representation of your dbt project in a predictable format.

When Dagster builds your code location, it reads the manifest file to discover the dbt models and turn them into Dagster assets.

### Representing the dbt project in Dagster

As you’ll frequently point your Dagster code to the `target/manifest.json` file and your dbt project in this course, it’ll be helpful to keep a reusable representation of the dbt project. This can be easily done using the `DbtProject` class.

In the `dagster_university` directory, in the `project.py` file, let's add the following code:

<button data-command="open:dagster_university/project.py">Open `dagster_university/project.py`</button>

<insert-text file="./dagster_university/project.py" line="3" col="0">
```python
# This code creates a representation of the dbt project called dbt_project.
dbt_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "analytics").resolve(),
)

dbt_project.prepare_if_dev()
```
</insert-text>

The `prepare_if_dev()` method automatically prepares your dbt project at run time during development

### Creating a Dagster resource to run dbt

Our next step is to define a Dagster resource as the entry point used to run dbt commands and configure its execution.

The `DbtCliResource` is the main resource that you’ll be working with.

> *💡 **Resource refresher**: Resources are Dagster’s recommended way of connecting to other services and tools, such as dbt, your data warehouse, or a BI tool.*

We create the resource in `dagster_university/resources/__init__.py`, which is where other resources are defined.

<button data-command="open:dagster_university/resources/__init__.py">Open `dagster_university/resources/__init__.py`</button>

<insert-text file="./dagster_university/resources/__init__.py" line="5" col="0">
```
from ..project import dbt_project        # Imports the dbt_project representation we just defined

dbt_resource = DbtCliResource(           # Instantiate a new DbtCliResource
    project_dir=dbt_project,             # Tell the resource that the dbt project to execute is the dbt_project
)
```
</insert-text>

### Loading dbt models into Dagster as assets

#### Turning dbt models into assets with @dbt_assets
  
The star of the show here is the `@dbt_assets` decorator. This is a specialized asset decorator that wraps around a dbt project to tell Dagster what dbt models exist. In the body of the `@dbt_assets` definition, you write exactly how you want Dagster to run your dbt models.
 
#### Loading the models as assets
Let's insert the following code into `dagster_university/assets/dbt.py`

<button data-command="open:dagster_university/assets/dbt.py">Open `dagster_university/assets/dbt.py`</button>

<insert-text file="./dagster_university/assets/dbt.py" line="6" col="0">
```python
from ..project import dbt_project


@dbt_assets(
    manifest=dbt_project.manifest_path,
)
def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
    with Locker():
        yield from dbt.cli(["build"], context=context).stream()
```
</insert-text>

The `@dbt_assets` decorator creates a new asset function and provides it with a reference to the project's manifest file.

Notice we provided two arguments to the `dbt_analytics` function. The first argument is the `context`, which indicates which dbt models to run and any related configurations. The second refers to the dbt resource you’ll be using to run dbt.

Let’s review what’s happening in this function in a bit more detail:

* We use the `dbt` argument (which is a `DbtCliResource`) to execute a dbt command through its `.cli` method.
* The `.stream()` method fetches the events and results of this dbt execution.
* This is one of multiple ways to get the Dagster events, such as what models materialized or tests passed. We recommend starting with this and exploring other methods in the future as your use cases grow (such as fetching the run artifacts after a run). In this case, the above line will execute `dbt run`.
*  The results of the `stream` are a Python generator of what Dagster events happened. We used [`yield from`](https://pythonalgos.com/generator-functions-yield-and-yield-from-in-python/) (not just `yield`!) to have Dagster track asset materializations.

### Updating the Definitions object

The last step in setting up your dbt project in Dagster is adding the definitions you made (ex. your `dbt_resource` and `dbt_analytics` asset) to your code location’s Definitions object.

<button data-command="open:dagster_university/__init__.py">Open `dagster_university/__init__.py`</button>

<insert-text file="./dagster_university/__init__.py" line="3" col="0">
```python
from .assets import dbt # Import the dbt assets
```
</insert-text>

<insert-text file="./dagster_university/__init__.py" line="6" col="0">
```python
from .resources import dbt_resource # Import the dbt resource
```
</insert-text>

Now, register the `dbt_resource` under the resource key `dbt`

<insert-text file="./dagster_university/__init__.py" line="30" col="0">
```python
        "dbt": dbt_resource,
```
</insert-text>

### Viewing dbt models in the Dagster UI

You’re ready to see your dbt models represented as assets!

Let's launch the dagster UI by running the following command and clicking "Open in New Tab" when the popup appears on the bottom right of your screen

<button data-command="run:dagster dev -h 0.0.0.0">Run `dagster dev`</button>

1. Navigate to the Asset graph by clicking "Assets" on the top navigation bar
2. On the top right click "View global asset lineage"
2. Use the left asset graph window to expand the `dbt_models` group
3. You should see your two dbt models, `stg_trips` and `stg_zones` converted as assets within your Dagster project!

If you’re familiar with the Dagster metadata system, you’ll notice that the descriptions you defined for the dbt models in `staging.yml` are carried over as those for your dbt models.

And, of course, the orange dbt logo attached to the assets indicates that they are dbt models.

Click the `stg_trips` node on the asset graph and look at the right sidebar. You’ll get some metadata out-of-the-box, such as the dbt code used for the model, how long the model takes to materialize over time, and the schema of the model.
</details>

<details>
<summary>
<b>Lesson 3</b>
</summary>

## Adding dependencies and automations to dbt models

<button data-command="run:git reset --hard 6a0e9b1bf31c97c86d1b5eed5eae028e44f23a29">Checkpoint: Lesson 3</button>

### Connecting dbt models to Dagster assets

You may have noticed that the sources for your dbt projects are not just tables that exist in DuckDB, but also assets that Dagster created. However, the staging models (`stg_trips`, `stg_zones`) that use those sources aren’t linked to the Dagster assets (`taxi_trips`, `taxi_zones`) that produced them.

Let’s fix that by telling Dagster that the dbt sources are the tables that the `taxi_trips` and `taxi_zones` asset definitions produce. To match up these assets, we'll override the dbt assets' keys. By having the asset keys line up, Dagster will know that these assets are the same and should merge them.

This is accomplished by changing the dbt source’s asset keys to be the same as the matching assets that Dagster makes. In this case, the dbt source’s default asset key is `raw_taxis/trips`, and the table that we’re making with Dagster has an asset key of `taxi_trips`.

To adjust how Dagster names the asset keys for your project’s dbt models, we’ll need to override the `dagster-dbt` integration’s default logic for how to interpret the dbt project. This mapping is contained in the `DagsterDbtTranslator` class.

#### Customizing how Dagster understands dbt projects

The `DagsterDbtTranslator` class is the default mapping for how Dagster interprets and maps your dbt project. As Dagster loops through each of your dbt models, it will execute each of the translator’s functions and use the return value to configure your new Dagster asset.

However, you can override its methods by making a new class that inherits from and provides your logic for a dbt model. Refer to the `dagster-dbt` package’s [API Reference](https://docs.dagster.io/_apidocs/libraries/dagster-dbt#dagster_dbt.DagsterDbtTranslator) for more info on the different functions you can override in the `DagsterDbtTranslator` class.

For now, we’ll customize how asset keys are defined by overriding the translator’s `get_asset_key` method.

<button data-command="open:dagster_university/assets/dbt.py">Open `dagster_university/assets/dbt.py`</button>

<insert-text file="dagster_university/assets/dbt.py" line="8" col="0">
```python
class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]
        if resource_type == "source":
            return AssetKey(f"taxi_{name}")
        else:
            return super().get_asset_key(dbt_resource_props)
```
</insert-text>

This code:
* Creates a new class called `CustomizedDagsterDbtTranslator` that inherits from the `DagsterDbtTranslator`.
* Overrides the `DagsterDbtTranslator` class method `get_asset_key`.
    * It is an instance method, so we'll have its first argument be `self`, to follow Pythonic conventions.
    * The second argument, `dbt_resource_props` refers to a dictionary/JSON object for the dbt model’s properties, which is based on the manifest file from earlier.
* There are two properties that we’ll want from `dbt_resource_props`
    1. `resource_type` (ex., model, source, seed, snapshot)
    2. `name`, such as `trips` or `stg_trips`.
* The asset keys of our existing Dagster assets used by our dbt project are named `taxi_trips` and `taxi_zones`. If you were to print out the `name`, you’d see that the dbt sources are named `trips` and `zones`. Therefore, to match our asset keys up, we can prefix our keys with the string `taxi_`.
* We leverage the existing implementations of the parent class by using the `super` method. We’ll default to the original logic for deciding the model asset keys.

Finally, we update the definition that uses `@dbt_assets` to be configured with an instance of the `CustomizedDagsterDbtTranslator`.

<insert-text file="./dagster_university/assets/dbt.py" line="19" col="0">
```python
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
```
</insert-text>

### Creating assets that depend on dbt models

At this point, you’ve loaded your dbt models as Dagster assets and linked the dependencies between the dbt assets and their source Dagster assets. However, a dbt model is typically not the last asset in a pipeline. For example, you might want to:
* Generate a chart,
* Update a dashboard, or
* Send data to Salesforce

In this section, you’ll learn how to do this by defining a new Dagster asset that depends on a dbt model. We’ll make some metrics in a dbt model and then use Python to generate a chart with that data.

If you’re familiar with New York City, you might know that there are three major airports - JFK, LGA, and EWR - in different parts of the metropolitan area. Let's say you’re curious if a traveler's final destination impacts the airport they fly into. For example, how many people staying in Queens flew into LGA?

#### Creating the dbt model

To answer these questions, let’s define a new dbt model that builds a series of metrics from the staging models you wrote earlier.

<button data-command="run:cd analytics/models/ && mkdir marts && touch marts/location_metrics.sql">Create `analytics/models/marts/location_metrics.sql`</button>

<button data-command="open:analytics/models/marts/location_metrics.sql">Open `analytics/models/marts/location_metrics.sql`</button>

<insert-text file="./analytics/models/marts/location_metrics.sql" line="0" col="0">
```sql
with
    trips as (
        select *
        from {{ ref('stg_trips') }}
    ),
    zones as (
        select *
        from {{ ref('stg_zones') }}
    ),
    trips_by_zone as (
        select
            pickup_zones.zone_name as zone,
            dropoff_zones.borough as destination_borough,
            pickup_zones.is_airport as from_airport,
            count(*) as trips,
            sum(trips.trip_distance) as total_distance,
            sum(trips.duration) as total_duration,
            sum(trips.total_amount) as fare,
            sum(case when duration > 30 then 1 else 0 end) as trips_over_30_min
        from trips
        left join zones as pickup_zones on trips.pickup_zone_id = pickup_zones.zone_id
        left join zones as dropoff_zones on trips.dropoff_zone_id = dropoff_zones.zone_id
        group by all
    )
select *
from trips_by_zone
```
</insert-text>

In the Dagster UI, reload the code location & observe and materialize the new `location_metrics` dbt asset.

#### Creating the Dagster asset

Next, we’ll create an asset that uses some of the columns in the `location_metrics` model to chart the number of taxi trips that happen per major NYC airport and the borough they come from.

##### Adding a new constant

Let's start by adding a new string constant to reference when building the new asset. This will make it easier for us to reference the correct location of the chart in the asset.

<button data-command="open:dagster_university/assets/constants.py">Open `dagster_university/assets/constants.py`</button>

<insert-text file="./dagster_university/assets/constants.py" line="38" col="0">
```python
AIRPORT_TRIPS_FILE_PATH = get_path_for_env(os.path.join("data", "outputs", "airport_trips.png"))
```
</insert-text>

This creates a path to where we want to save the chart.

##### Creating the airport_trips asset

Now we’re ready to create the asset!

<button data-command="open:dagster_university/assets/metrics.py">Open `dagster_university/assets/metrics.py`</button>

<insert-text file="./dagster_university/assets/metrics.py" line="141" col="0">
```python
def airport_trips(database: DuckDBResource) -> MaterializeResult:
    """
        A chart of where trips from the airport go
    """

    query = """
        select
            zone,
            destination_borough,
            trips
        from location_metrics
        where from_airport
    """
    with Locker(), database.get_connection() as conn:
        airport_trips = conn.execute(query).fetch_df()

    fig = px.bar(
        airport_trips,
        x="zone",
        y="trips",
        color="destination_borough",
        barmode="relative",
        labels={
            "zone": "Zone",
            "trips": "Number of Trips",
            "destination_borough": "Destination Borough"
        },
    )

    pio.write_image(fig, constants.AIRPORT_TRIPS_FILE_PATH)

    with open(constants.AIRPORT_TRIPS_FILE_PATH, 'rb') as file:
        image_data = file.read()

    # Convert the image data to base64
    base64_data = base64.b64encode(image_data).decode('utf-8')
    md_content = f"![Image](data:image/jpeg;base64,{base64_data})"

    return MaterializeResult(
        metadata={
            "preview": MetadataValue.md(md_content)
        }
    )
```
</insert-text>

Finally, we add the asset decorator to the airport_trips function and specify the location_metrics model as a dependency:

<insert-text file="./dagster_university/assets/metrics.py" line="141" col="0">
```python
@asset(
    deps=["location_metrics"],
)
```
</insert-text>

> **Note**: Because Dagster doesn’t discriminate and treats all dbt models as assets, you’ll add this dependency just like you would with any other asset.

Reload your code location to see the new `airport_trips` asset within the `metrics` group. Notice how the asset graph links the dependency between the `location_metrics` dbt asset and the new `airport_trips` chart asset:

### Automating dbt models in Dagster

Did you realize that your dbt models have already been scheduled to run on a regular basis because of an existing schedule within this Dagster project?

Check it out in the Dagster UI by clicking "Overview" in the top navigation bar, then the Jobs tab. Click `trip_update_job` to check out the job’s details. It looks like the dbt models are already attached to this job!

Pretty cool, right? Let’s check out the code that made this happen.
In the following file, look at the definition for `trip_update_job`

<button data-command="open:dagster_university/jobs/__init__.py">Open `dagster_university/jobs/__init__.py`</button>

The dbt models were included in this job because of the `AssetSelection.all()` call. This reinforces the idea that once you load your dbt project into your Dagster project, Dagster will recognize and treat all of your dbt models as assets.

#### Excluding specific dbt models

Treating dbt models as assets is great, but one of the core tenets of Dagster’s dbt integration is respecting how dbt is used, along with meeting dbt users where they are. That’s why there are a few utility methods that should feel familiar to dbt users. Let’s use one of these methods to remove some of our dbt models from this job explicitly.

Pretend that you’re working with an analytics engineer, iterating on the `stg_trips` model and planning to add new models that depend on it soon. Therefore, you’d like to exclude `stg_trips` and any new hypothetical dbt models downstream of it until the pipeline stabilizes. The analytics engineer you’re working with is really strong with dbt, but not too familiar with Dagster.

This is where you’d lean on a function like [`build_dbt_asset_selection`](https://docs.dagster.io/_apidocs/libraries/dagster-dbt#dagster_dbt.build_dbt_asset_selection). This utility method will help your analytics engineer contribute without needing to know Dagster’s asset selection syntax. It takes two arguments:

* A list of `@dbt_assets` definitions to select models from
* A string of the selector using [dbt’s selection syntax](https://docs.getdbt.com/reference/node-selection/syntax) of the models you want to select

The function will return an `AssetSelection` of the dbt models that match your dbt selector. Let’s put this into practice:

<insert-text file="./dagster_university/jobs/__init__.py" line="3" col="0">
```python
from ..assets.dbt import dbt_analytics

dbt_trips_exclusion = build_dbt_asset_selection([dbt_analytics], "stg_trips").downstream()
```
</insert-text>

Update `trip_update_job` such that `cbt_trips_exclusion` gets removed from `selection`.

Reload the code location and confirm that the dbt models are not in the `trip_update_job` anymore!

> 💡 Want an even more convenient utility to do this work for you? Consider using the similar [`build_schedule_from_dbt_selection`](https://docs.dagster.io/_apidocs/libraries/dagster-dbt#dagster_dbt.build_schedule_from_dbt_selection) function to quickly create a job and schedule for a given dbt selection.
</details>

<details>
<summary>
<b>Lesson 4</b>
</summary>

## Partitioning dbt models

<button data-command="run:git reset --hard ef534f01ef5986204c0c460ff15465d26d15f084">Checkpoint: Lesson 4</button>

### Creating an incremental model

As mentioned, partitions don’t **replace** incremental models, but you’ll soon see how you can expand their functionality by partitioning them. In fact, we’ll first write an incremental dbt model and then show you how to use Dagster to partition it.

This model will be a series of stats about all New York taxi trips. It would be expensive to compute this every day because of the granularity of the metrics and the fact that some of the measures are computationally expensive to calculate. Therefore, this model will be incremental.

<button data-command="run:touch analytics/models/marts/daily_metrics.sql">Create `analytics/models/marts/daily_metrics.sql`</button>

<button data-command="open:analytics/models/marts/daily_metrics.sql">Open `analytics/models/marts/daily_metrics.sql`</button>

<insert-text file="./analytics/models/marts/daily_metrics.sql" line="0" col="0">
```sql
{{
    config(
        materialized='incremental',
        unique_key='date_of_business'
    )
}}

with
    trips as (
        select *
        from {{ ref('stg_trips') }}
    ),
    daily_summary as (
        select
            date_trunc('day', pickup_datetime) as date_of_business,
            count(*) as trip_count,
            sum(duration) as total_duration,
            sum(duration) / count(*) as average_duration,
            sum(total_amount) as total_amount,
            sum(total_amount) / count(*) as average_amount,
            sum(case when duration > 30 then 1 else 0 end) / count(*) as pct_over_30_min
        from trips
        group by all
    )
select *
from daily_summary
{% if is_incremental() %}
    where date_of_business > (select max(date_of_business) from {{ this }})
{% endif %}
```
</insert-text>

### Creating a partitioned dbt asset

We’ve built the foundation on the dbt side, and now we can make the appropriate changes on the Dagster side. We’ll refactor our existing Dagster code to tell dbt that the incremental models are partitioned and what data to fill in.

We want to configure some of these models (the incremental ones) with partitions. In this section, we’ll show you a use case that has multiple `@dbt_assets` definitions.

To partition an incremental dbt model, you’ll need first to partition your `@dbt_assets` definition. Then, when it runs, we’ll figure out what partition is running and tell dbt what the partition’s range is. Finally, we’ll modify our dbt model only to insert the records found in that range.

#### Defining an incremental selector

We now need a way to indicate that we’re selecting or excluding incremental models, so we’ll make a new constant called `INCREMENTAL_SELECTOR`

So, we have a few changes to make to our dbt setup to get things working.

<button data-command="open:dagster_university/assets/dbt.py">Open `./dagster_university/assets/dbt.py`</button>

<insert-text file="./dagster_university/assets/dbt.py" line="7" col="0">
```python
from ..partitions import monthly_partition

# This string follows dbt’s selection syntax to select all incremental models.
# In your own projects, you can customize this to select only the specific incremental models that you want to partition.
INCREMENTAL_SELECTOR = "config.materialized:incremental"
```
</insert-text>

### Creating a new @dbt_assets function

Previously, we used the `@dbt_assets` decorator to say *“this function produces assets based on this dbt project”*. Now, we also want to say *“this function produces partitioned assets based on a selected set of models from this dbt project.”* We’ll write an additional `@dbt_assets`-decorated function to express this.

Let's define another `@dbt_assets` function below the original one. It will use the same manifest that we’ve been using. Also, add arguments to specify which models to select (`select`) and what partition (`partitions_def`) to use:

<insert-text file="./dagster_university/assets/dbt.py" line="30" col="0">
```python
@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    select=INCREMENTAL_SELECTOR,     # select only models with INCREMENTAL_SELECTOR
    partitions_def=monthly_partition   # partition those models using daily_partition
)
```
</insert-text>

### Partitioning the incremental_dbt_models function

Now that the `@dbt_assets` definition has been created, let's write the function.

We’ll start by using the `context` argument, which contains metadata about the Dagster run.  The `context` includes **the partition this execution is trying to materialize**!

<insert-text file="./dagster_university/assets/dbt.py" line="36" col="0">
```python
def incremental_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
```
</insert-text>

In our case, since it’s a time-based partition, we can get the **time window** of the partitions we’re materializing, such as `2023-03-04T00:00:00+00:00` to `2023-03-05T00:00:00+00:00.`

<insert-text file="./dagster_university/assets/dbt.py" line="37" col="0">
```python
    time_window = context.partition_time_window
    dbt_vars = {
        "min_date": time_window.start.strftime('%Y-%m-%d'),
        "max_date": time_window.end.strftime('%Y-%m-%d')
    }
```
</insert-text>

Now that we know *what* partitions we’re executing, the next step is to tell dbt the partition currently being materialized. To do that, we’ll take advantage of dbt’s `vars` argument to pass this information at runtime. To communicate this time window, we’ll pass in a `min_date` and `max_date` variable.

<insert-text file="./dagster_university/assets/dbt.py" line="45" col="0">
```python
    with Locker():
        yield from dbt.cli(
            ["build", "--vars", json.dumps(dbt_vars)], context=context
        ).stream()
```
</insert-text>

### Updating the dbt_analytics function

Now that you have a dedicated `@dbt_assets` definition for the incremental models, you’ll need to **exclude** these models from your original dbt execution.

<insert-text file="./dagster_university/assets/dbt.py" line="25" col="0">
```python
    exclude=INCREMENTAL_SELECTOR,
```
</insert-text>

### Updating the daily_metrics model

Finally, we’ll modify the `daily_metrics.sql` file to reflect that dbt knows what partition range is being materialized. Since the partition range is passed in as variables at runtime, the dbt model can access them using the `var` dbt macro.

<button data-command="open:analytics/models/marts/daily_metrics.sql">Open `analytics/models/marts/daily_metrics.sql`</button>

<insert-text file="./analytics/models/marts/daily_metrics.sql" line="27" col="0">
```sql
    where date_of_business between '{{ var('min_date') }}' and '{{ var('max_date') }}'
```
</insert-text>

Here, we’ve changed the logic to say that we only want to select rows between the `min_date` and the `max_date`.

### Running the pipeline

That’s it! Now you can check out the new `daily_metrics` asset in Dagster.

1. In the Dagster UI, reload the code location. Once loaded, you should see the new partitioned daily_metrics asset
2. Click the `daily_metrics` asset and then the "Materialize selected" button. You’ll be prompted to select some partitions first.
3. Once the run starts, navigate to the run’s details page to check out the event logs. The executed dbt command should look something like this:
```
dbt build --vars {"min_date": "2023-03-04T00:00:00+00:00", "max_date": "2023-03-05T00:00:00+00:00"} --select config.materialized:incremental
```
</details>

<details>
<summary>
<b> Lesson 5</b>
</summary>

## Building BI Dashboards

<button data-command="run:git reset --hard 70ac8863a16430cd0d58783481e944bbe14b0a9f">Checkpoint: Lesson 5</button>

Now that you have a running pipeline, let's do something with the output.

<button data-command="run:cd /superset; FLASK_APP=superset SUPERSET_CONFIG_PATH=superset_config.py superset run -h 0.0.0.0 -p 8082">Run `superset`</button>

Credentials are `admin`:`admin`

To connect to DuckDB
* Click the **`+`** icon in top right
* Select **`Data`** -> **`Connect database`** 
* Type is **`Other`**
* SQLAlchemy URI: **`duckdb:////projects/code/data/staging/data.duckdb`**

To Create A Dataset
* Click the **`+`** icon in top right
* Select **`Data`** -> **`Create dataset`**
* DATABASE: `DuckDB`
* SCHEMA:  `data.main`
* TABLE: `daily_metrics`

From here you are able to create visualizations of the data.

</details>
