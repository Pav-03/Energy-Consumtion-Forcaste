import datetime
from typing import Optional
import fire
import pandas as pd

from feature_piepline.ETL import extract, transform, load, validation
from feature_piepline import utils

loggers = utils.get_logger(__name__)

def run(
        export_end_reference_datetime: Optional[datetime.datetime] = None,
        days_delay: int = 15,
        days_export: int = 30,
        url: str = "https://drive.google.com/uc?export=download&id=1y48YeDymLurOTUO-GeFOUXVNc9MCApG5",
        feature_group_version: int = 1,
) -> dict:
    
    loggers.info(f"Extracting the data form API")
    data, metadata = extract.from_file(
        export_end_reference_datetime, days_delay, days_export, url
    )

    if metadata["num_unique_samples_per_time_series"] < days_export * 24:
        raise RuntimeError(
            f"Could not extract the expected number of samples from API : {metadata["num_unique_samples_per_time_series"]} < {days_export * 24}. \
                check out the API at: https://www.energidataservice.dk/tso-electricity/ConsumptionDE35Hour "
        )
    
    loggers.info("Successfully extracted data from API")

    loggers.info("Transforming the data")

    data = transform(data)

    loggers.info("successfully transformed the data ")

    loggers.info("Building validation expectation suite.")
    validation_expectation_suite = validation.build_expectation_suite()
    loggers.info("Successfully built validation expectation suite.")

    loggers.info(f"Validating data and loading it to the feature store.")
    load.to_feature_store(
        data,
        validation_expectation_suite=validation_expectation_suite,
        feature_group_version=feature_group_version,
    )
    metadata["feature_group_version"] = feature_group_version
    loggers.info("Successfully validated data and loaded it to the feature store.")

    loggers.info(f"Wrapping up the pipeline.")
    utils.save_json(metadata, file_name="feature_pipeline_metadata.json")
    loggers.info("Done!")

    return metadata

def transform(data: pd.DataFrame):

    # Wrapper containing all the transformation form ETL pipeline 

    data = transform.rename_columns(data)
    data = transform.cast_columns(data)
    data = transform.encode_area_column(data)

    return data

if __name__ == "__main__":
    fire.Fire(run)