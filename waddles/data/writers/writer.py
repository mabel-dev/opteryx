import os
import orjson
import datetime
from pydantic import BaseModel
from typing import Any, Optional, Union, List
from mabel.data.writers.internals.blob_writer import BlobWriter
from mabel.data.internals.schema_validator import Schema
from mabel.data.internals.zone_map_writer import ZoneMapWriter
from mabel.utils import paths, dates
from mabel.errors import ValidationError, InvalidDataSetError, MissingDependencyError
from mabel.logging import get_logger


class Writer:
    def _get_writer_date(self, date):
        # default to today if not given a date
        batch_date = datetime.datetime.now()
        if isinstance(date, datetime.date):
            batch_date = date  # type:ignore
        if isinstance(date, str):
            batch_date = dates.parse_iso(date)
        return batch_date

    def __init__(
        self,
        *,
        schema: Optional[Union[Schema, list]] = None,
        set_of_expectations: Optional[list] = None,
        format: str = "zstd",
        date: Any = None,
        clustered_index: str = None,
        partitioning=("year_{yyyy}", "month_{mm}", "day_{dd}"),
        **kwargs,
    ):
        """
        Simple Writer provides a basic writer capability.


        """

        dataset = kwargs.get("dataset", "")

        if "BACKOUT" in dataset:
            InvalidDataSetError(
                "BACKOUT is a reserved word and cannot be used in Dataset names"
            )
        if dataset.endswith("/"):
            InvalidDataSetError("Dataset names cannot end with /")

        self.schema = None
        if isinstance(schema, list):
            schema = Schema(schema)
        if isinstance(schema, Schema):
            self.schema = schema

        self.expectations = None
        if set_of_expectations:
            try:
                import data_expectations as de  # type: ignore
            except:
                raise MissingDependencyError(
                    "`data_expectations` is missing, please install or include in requirements.txt"
                )
            self.expectations = de.Expectations(set_of_expectations=set_of_expectations)

        self.finalized = False
        self.batch_date = self._get_writer_date(date)
        self.clustered_index = clustered_index

        self.dataset_template = dataset
        self.partitioning = partitioning
        if partitioning:
            self.dataset_template += "/" + "/".join(partitioning)
            self.partitioning = None

        self.dataset = paths.build_path(self.dataset_template, self.batch_date)

        # add the values to kwargs

        kwargs["format"] = format
        kwargs["dataset"] = self.dataset

        arg_dict = kwargs.copy()
        arg_dict["dataset"] = f"{self.dataset}"
        arg_dict[
            "inner_writer"
        ] = f"{arg_dict.get('inner_writer', type(None)).__name__}"  # type:ignore
        get_logger().debug(orjson.dumps(arg_dict))

        # default index
        # kwargs["index_on"] = kwargs.get("index_on", [])
        # kwargs["clustered_index"] = clustered_index

        # create the writer
        self.blob_writer = BlobWriter(**kwargs)
        self.records = 0
        self.zone_map_writer = ZoneMapWriter(self.schema)

    def append(self, record: Union[dict, BaseModel]):
        """
        Append a new record to the Writer

        Parameters:
            record: dictionary or pydantic.BaseModel
                The record to append to the Writer

        Returns:
            integer
                The number of records in the current blob
        """
        if isinstance(record, BaseModel):
            record = record.dict()

        if self.schema and not self.schema.validate(
            subject=record, raise_exception=False
        ):
            raise ValidationError(
                f"Schema Validation Failed ({self.schema.last_error})"
            )

        if self.expectations:
            import data_expectations as de  # type: ignore

            de.evaluate_record(self.expectations, record)

        self.blob_writer.append(record)
        self.zone_map_writer.add(record, self.dataset)
        self.records += 1

    def __del__(self):
        if hasattr(self, "finalized") and not self.finalized and self.records > 0:
            get_logger().error(
                f"{type(self).__name__} has not been finalized - {self.records} may have been lost, use `.finalize()` to finalize writers."
            )

    def finalize(self, **kwargs):
        self.finalized = True
        try:
            zone_map_path = os.path.split(self.dataset)[0] + "/frame.metadata"
            self.blob_writer.inner_writer.commit(
                byte_data=self.zone_map_writer.profile(),
                blob_name=zone_map_path,
            )
            return self.blob_writer.commit()
        except Exception as e:
            get_logger().error(
                f"{type(self).__name__} failed to close pool: {type(e).__name__} - {e}"
            )
            raise
        return None
