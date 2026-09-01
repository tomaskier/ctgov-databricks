from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, lit, row_number, sha2, struct, to_json
from pyspark.sql.window import Window

TRIAL_COLUMNS = [
    "nct_id",
    "brief_title",
    "official_title",
    "overall_status",
    "study_type",
    "phases",
    "brief_summary",
    "conditions",
    "keywords",
    "start_date",
    "primary_completion_date",
    "completion_date",
    "enrollment_count",
    "enrollment_type",
    "minimum_age",
    "maximum_age",
    "sex",
    "healthy_volunteers",
    "eligibility_criteria",
    "last_update_post_date",
    "study_first_post_date",
]


def deduped_studies(bronze_batch: DataFrame) -> DataFrame:
    """One row per nct_id: the payload from the highest page number it appeared on - Spark
    equivalent of the original's studies_by_id dict, overwritten as pages are read in ascending
    order (page_0000, page_0001, ...), so the last page a trial appears on wins."""
    exploded = bronze_batch.select(explode("studies").alias("study"), col("page"))
    nct_id = col("study.protocolSection.identificationModule.nctId")
    w = Window.partitionBy(nct_id).orderBy(col("page").desc())
    return (
        exploded.withColumn("_rn", row_number().over(w))
        .filter((col("_rn") == 1) & nct_id.isNotNull())
        .select("study")
    )


def parse_trials(deduped: DataFrame, condition: str, run_id: str) -> DataFrame:
    ident = "study.protocolSection.identificationModule"
    status = "study.protocolSection.statusModule"
    design = "study.protocolSection.designModule"
    desc = "study.protocolSection.descriptionModule"
    cond = "study.protocolSection.conditionsModule"
    elig = "study.protocolSection.eligibilityModule"

    trials = deduped.select(
        col(f"{ident}.nctId").alias("nct_id"),
        col(f"{ident}.briefTitle").alias("brief_title"),
        col(f"{ident}.officialTitle").alias("official_title"),
        col(f"{status}.overallStatus").alias("overall_status"),
        col(f"{design}.studyType").alias("study_type"),
        col(f"{design}.phases").alias("phases"),
        col(f"{desc}.briefSummary").alias("brief_summary"),
        col(f"{cond}.conditions").alias("conditions"),
        col(f"{cond}.keywords").alias("keywords"),
        col(f"{status}.startDateStruct.date").alias("start_date"),
        col(f"{status}.primaryCompletionDateStruct.date").alias("primary_completion_date"),
        col(f"{status}.completionDateStruct.date").alias("completion_date"),
        col(f"{design}.enrollmentInfo.count").cast("int").alias("enrollment_count"),
        col(f"{design}.enrollmentInfo.type").alias("enrollment_type"),
        col(f"{elig}.minimumAge").alias("minimum_age"),
        col(f"{elig}.maximumAge").alias("maximum_age"),
        col(f"{elig}.sex").alias("sex"),
        col(f"{elig}.healthyVolunteers").alias("healthy_volunteers"),
        col(f"{elig}.eligibilityCriteria").alias("eligibility_criteria"),
        col(f"{status}.lastUpdatePostDateStruct.date").alias("last_update_post_date"),
        col(f"{status}.studyFirstPostDateStruct.date").alias("study_first_post_date"),
    )

    # Hash over the parsed columns only (not source_condition/source_run_id, which change on
    # every rerun regardless of whether the trial itself changed - including them here would
    # break change detection by making every batch look "changed").
    row_hash = sha2(to_json(struct(*TRIAL_COLUMNS)), 256)
    return (
        trials.withColumn("row_hash", row_hash)
        .withColumn("source_condition", lit(condition))
        .withColumn("source_run_id", lit(run_id))
    )


def parse_sponsors(deduped: DataFrame) -> DataFrame:
    nct_id = col("study.protocolSection.identificationModule.nctId").alias("nct_id")
    sponsor_module = col("study.protocolSection.sponsorCollaboratorsModule")

    lead = deduped.select(
        nct_id,
        sponsor_module["leadSponsor"]["name"].alias("sponsor_name"),
        sponsor_module["leadSponsor"]["class"].alias("sponsor_class"),
        lit(True).alias("is_lead"),
    ).filter(col("sponsor_name").isNotNull())

    collaborators = deduped.select(
        nct_id, explode(sponsor_module["collaborators"]).alias("c")
    ).select(
        "nct_id",
        col("c.name").alias("sponsor_name"),
        col("c.class").alias("sponsor_class"),
        lit(False).alias("is_lead"),
    )
    return lead.unionByName(collaborators)


def parse_locations(deduped: DataFrame) -> DataFrame:
    nct_id = col("study.protocolSection.identificationModule.nctId").alias("nct_id")
    locations = deduped.select(
        nct_id, explode(col("study.protocolSection.contactsLocationsModule.locations")).alias("loc")
    )
    return locations.select(
        "nct_id",
        col("loc.facility").alias("facility"),
        col("loc.city").alias("city"),
        col("loc.state").alias("state"),
        col("loc.country").alias("country"),
        col("loc.status").alias("status"),
        col("loc.geoPoint.lat").alias("latitude"),
        col("loc.geoPoint.lon").alias("longitude"),
    )


def parse_interventions(deduped: DataFrame) -> DataFrame:
    nct_id = col("study.protocolSection.identificationModule.nctId").alias("nct_id")
    interventions = deduped.select(
        nct_id, explode(col("study.protocolSection.armsInterventionsModule.interventions")).alias("iv")
    )
    return interventions.select(
        "nct_id",
        col("iv.type").alias("intervention_type"),
        col("iv.name").alias("name"),
        col("iv.description").alias("description"),
    )


def parse_outcomes(deduped: DataFrame) -> DataFrame:
    nct_id = col("study.protocolSection.identificationModule.nctId").alias("nct_id")
    outcomes_module = col("study.protocolSection.outcomesModule")

    def _one(field: str, outcome_type: str) -> DataFrame:
        return deduped.select(
            nct_id, explode(outcomes_module[field]).alias("o")
        ).select(
            "nct_id",
            lit(outcome_type).alias("outcome_type"),
            col("o.measure").alias("measure"),
            col("o.description").alias("description"),
            col("o.timeFrame").alias("time_frame"),
        )

    return _one("primaryOutcomes", "PRIMARY").unionByName(_one("secondaryOutcomes", "SECONDARY"))
