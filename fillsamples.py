#!/usr/bin/env python
# Copyright [2016-2025] EMBL-European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Extract ENA sample, analysis and studies information and store it in the provided SQLite database."""

import argparse
import logging
from pathlib import Path
import sqlite3
from typing import Any

from ensembl.utils.argparse import ArgumentParser
from ensembl.utils.logging import init_logging_with_args
import ijson
from lxml import etree


__BATCH_SIZE = 50000
_INSERT_SAMPLE_QUERY = (
    "INSERT OR IGNORE INTO SAMPLE (ID, TAX, NAME, TITLE, DESC, CENTER, BROKER, STUDY, AE) VALUES "
    "(:sample_id, :taxon_id, :taxon_name, :title, :description, :center, :broker, :study, :array_express)"
)
_INSERT_ATTRIBUTES_QUERY = (
    "INSERT OR IGNORE INTO ATTRIBUTES (ID, FIELD, VALUE, UNITS) VALUES (:sample_id, :field, :value, :units)"
)
_INSERT_DATA_QUERY = (
    "INSERT OR IGNORE INTO DATA (ID, DATA_ID, TYPE, URL, STUDY, TITLE, MD5) VALUES "
    "(:sample_id, :data_id, :data_type, :url, :study, :title, :md5)"
)
_INSERT_STUDY_QUERY = (
    "INSERT OR IGNORE INTO STUDY (PROJECT, STUDY, NAME, TITLE, DESC, ISOLATE, CULTIVAR, BREED, GEO_ACC) "
    "VALUES (:project, :study, :name, :title, :description, :isolate, :cultivar, :breed, :geo_accession)"
)


def _extract_sample_data(sample: etree.Element, sample_count: int) -> dict[str, Any] | None:
    """Extract BioSample ID, title, description and taxonomy information from ENA sample.

    Args:
        sample: ENA sample element.
        sample_count: Sample number to add to log report if identifiers are not present.

    Returns:
        Dictionary with the extracted values or `None` if any but description is missing.

    """
    # Get BioSample ID
    identifiers = sample.find("IDENTIFIERS")
    if identifiers is not None:
        all_external_tags = identifiers.findall("EXTERNAL_ID")
        external_tag = [tag.text for tag in all_external_tags if tag.get("namespace", "") == "BioSample"]
        if len(external_tag) > 0:
            biosample_id = external_tag[0]
            # Get title
            title = sample.findtext("TITLE")
            if title is not None:
                # Get taxon ID and name
                sample_name_element = sample.find("SAMPLE_NAME")
                if sample_name_element is not None:
                    taxon_id = sample_name_element.findtext("TAXON_ID")
                    if taxon_id is not None:
                        taxon_name = sample_name_element.findtext(
                            "SCIENTIFIC_NAME", default=sample_name_element.findtext("COMMON_NAME")
                        )
                        if taxon_name is not None:
                            return {
                                "sample_id": biosample_id,
                                "taxon_id": taxon_id,
                                "taxon_name": taxon_name,
                                "title": title,
                                "description": sample.findtext("DESCRIPTION", default=""),
                                "center": sample.get("center_name", ""),
                                "broker": sample.get("broker_name", ""),
                                "study": "",
                                "array_express": "",
                            }
                        logging.error(f"Sample {biosample_id} has no taxon name, skipping")
                    else:
                        logging.error(f"Sample {biosample_id} has no taxon ID, skipping")
                else:
                    logging.error(f"Sample {biosample_id} has no sample name, skipping")
            else:
                logging.error(f"Sample {biosample_id} has no title, skipping")
        else:
            logging.error(f"Sample #{sample_count} has no BioSample ID, skipping")
    else:
        logging.error(f"Sample #{sample_count} has no identifiers, skipping")
    return None


def _extract_sample_attributes(sample: etree.Element, sample_id: int) -> list[dict[str, Any]]:
    """Extract ENA sample attributes: "field", "value" and "units".

    Args:
        sample: ENA sample element.
        sample_id: BioSample ID.

    Returns:
        List of dictionaries with the extracted values for each attribute found.

    """
    all_attributes = sample.find("SAMPLE_ATTRIBUTES")
    attributes_values = []
    for attribute in all_attributes:
        field = attribute.findtext("TAG")
        value = attribute.findtext("VALUE")
        units = attribute.findtext("UNITS", default="")
        if field is None or value is None:
            logging.error(f"Sample {sample_id} has an attribute with no field or value, skipping")
            continue
        attributes_values.append(
            {
                "sample_id": sample_id,
                "field": field.lower(),
                "value": value,
                "units": units,
            }
        )
    return attributes_values


def _expand_range(id_range: str) -> list[str]:
    """Expand a range in the format "SRR123456-SRR123460" to its individual IDs.

    Args:
        id_range: Range of IDs in the format "ID1-ID2".

    Returns:
        List of individual IDs.

    """
    range_tuple = id_range.split("-")
    if len(range_tuple) == 1:
        return [id_range]
    prefix = range_tuple[0][:3]
    return [f"{prefix}{x:06d}" for x in range(int(range_tuple[0][3:]), int(range_tuple[1][3:]) + 1)]


def _infer_run_link(run_id: str) -> str:
    """Infer the SRA URL link to the FastQ file corresponding to the given ENA run ID.

    Args:
        run_id: ENA run ID

    Returns:
        SRA URL for the FastQ file corresponding to the given ENA run ID.

    """
    full_url = f"ftp://ftp.sra.ebi.ac.uk/vol1/fastq/{run_id[:6]}/"
    if len(run_id) > 9:
        full_url += f"00{run_id[-1]}/"
    full_url += run_id
    return full_url


def _extract_sample_links(sample: etree.Element, sample_values: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract ENA sample links: ENA study and Array Express, as well as ENA run information.

    ENA study and Array Express information is added to `sample_values`.

    Args:
        sample: ENA sample element.
        sample_values: ENA sample attributes.

    Returns:
        List of dictionaries with the extracted values for each ENA run found.

    """
    all_links = sample.find("SAMPLE_LINKS")
    data_values: list[dict[str, Any]] = []
    for link in all_links:
        db_tag = link[0][0].text
        if db_tag == "ENA-RUN":
            for dash in link[0][1].text.split(","):
                data_values.extend(
                    {
                        "sample_id": sample_values["sample_id"],
                        "data_id": run_id,
                        "data_type": "ENA RUN",
                        "url": _infer_run_link(run_id),
                        "study": "",
                        "title": "",
                        "md5": "",
                    }
                    for run_id in _expand_range(dash)
                )
        elif db_tag == "ENA-STUDY":
            sample_values["study"] = link[0][1].text
        elif db_tag == "ARRAYEXPRESS":
            sample_values["array_express"] = link[0][1].text
    return data_values


def _commit_sample_data(
    connection: sqlite3.Connection,
    sample_table_data: list[dict[str, Any]],
    attributes_table_data: list[dict[str, Any]],
    data_table_data: list[dict[str, Any]],
) -> None:
    """Commit all sample data collected in one transaction.

    Args:
        connection: Open connection to SQLite database.
        sample_table_data: Data to commit to SAMPLE table.
        attributes_table_data: Data to commit to ATTRIBUTES table.
        data_table_data: Data to commit to DATA table.

    """
    connection.executemany(_INSERT_SAMPLE_QUERY, sample_table_data)
    connection.executemany(_INSERT_ATTRIBUTES_QUERY, attributes_table_data)
    connection.executemany(_INSERT_DATA_QUERY, data_table_data)
    connection.commit()
    logging.info(f"Stored {len(sample_table_data)} samples in the database")
    logging.info(f"Stored {len(attributes_table_data)} attributes in the database")
    logging.info(f"Stored {len(data_table_data)} data entries in the database")


def populate_db_from_samples(db_file: Path, samples_file: Path) -> None:
    """Populate the database with ENA samples.

    Args:
        db_file: SQLite database file (schema must be created first).
        samples_file: ENA samples XML file.

    """
    # Prepare data to import
    connection = sqlite3.connect(db_file)
    sample_count = 0
    sample_table_data: list[dict[str, Any]] = []
    attributes_table_data: list[dict[str, Any]] = []
    data_table_data: list[dict[str, Any]] = []
    for _, sample in etree.iterparse(samples_file, events=("end",), tag="SAMPLE"):
        sample_count += 1
        if sample_count % __BATCH_SIZE == 1:
            logging.info(f"Processing study #{sample_count}")
        sample_values = _extract_sample_data(sample, sample_count)
        if not sample_values:
            continue
        sample_table_data.append(sample_values)
        attributes_values = _extract_sample_attributes(sample, sample_values["sample_id"])
        attributes_table_data.extend(attributes_values)
        data_values = _extract_sample_links(sample, sample_values)
        data_table_data.extend(data_values)
        # Remove the subtree related with this sample to speed up traversing the XML tree
        sample.clear()
        while sample.getprevious() is not None:
            del sample.getparent()[0]
        # Commit values into the database in batches so improve performance
        if sample_count % __BATCH_SIZE == 0:
            _commit_sample_data(connection, sample_table_data, attributes_table_data, data_table_data)
            sample_table_data.clear()
            attributes_table_data.clear()
            data_table_data.clear()
    _commit_sample_data(connection, sample_table_data, attributes_table_data, data_table_data)
    connection.close()
    logging.info(f"Finished processing {sample_count} samples")


def _commit_analysis_data(connection: sqlite3.Connection, data_table_data: list[dict[str, Any]]) -> None:
    """Commit all analysis data collected in one transaction.

    Args:
        connection: Open connection to SQLite database.
        data_table_data: Data to commit to DATA table.

    """
    connection.executemany(_INSERT_DATA_QUERY, data_table_data)
    connection.commit()
    logging.info(f"Stored {len(data_table_data)} data entries in the database")


def populate_db_from_analysis(db_file: Path, analysis_file: Path) -> None:
    """Populate the database with ENA analysis data.

    Args:
        db_file: SQLite database file (schema must be created first).
        analysis_file: Analysis JSON file.

    """
    connection = sqlite3.connect(db_file)
    data_table_data: list[dict[str, Any]] = []
    index = 0
    with analysis_file.open("rb") as analysis_data:
        for analysis in ijson.items(analysis_data, "item"):
            index += 1
            if index % __BATCH_SIZE == 1:
                logging.info(f"Processing analysis #{index}")
            analysis_accession = analysis["analysis_accession"]
            ftp_url = analysis["submitted_ftp"]
            if not ftp_url:
                logging.debug(f"Analysis #{analysis_accession} is missing the file URL, skipping")
                continue
            urls = ftp_url.split(";")
            url_index = 0
            if len(urls) == 2:
                url_index = 1 if urls[0].endswith(".md5") else 0
            file_url = urls[url_index]
            if len(urls) > 2:
                file_url = str(Path(file_url).parent)
            data_table_data.append(
                {
                    "sample_id": analysis["sample_accession"],
                    "data_id": analysis_accession,
                    "data_type": analysis["analysis_type"],
                    "url": file_url,
                    "study": analysis["study_accession"],
                    "title": analysis["analysis_title"],
                    "md5": analysis["submitted_md5"].split(";")[url_index],
                }
            )
            # Commit values into the database in batches so improve performance
            if index % __BATCH_SIZE == 0:
                _commit_analysis_data(connection, data_table_data)
                data_table_data.clear()
    _commit_analysis_data(connection, data_table_data)
    connection.close()
    logging.info(f"Finished processing {index} analysis")


def _commit_studies_data(connection: sqlite3.Connection, study_table_data: list[dict[str, Any]]) -> None:
    """Commit all studies data collected in one transaction.

    Args:
        connection: Open connection to SQLite database.
        study_table_data: Data to commit to STUDY table.

    """
    connection.executemany(_INSERT_STUDY_QUERY, study_table_data)
    connection.commit()
    logging.info(f"Stored {len(study_table_data)} studies in the database")


def populate_db_from_studies(db_file: Path, studies_file: Path) -> None:
    """Populate the database with ENA studies data.

    Args:
        db_file: SQLite database file (schema must be created first).
        studies_file: ENA studies JSON file.

    """
    connection = sqlite3.connect(db_file)
    study_table_data: list[dict[str, Any]] = []
    index = 0
    with studies_file.open("rb") as studies:
        for study in ijson.items(studies, "item"):
            index += 1
            if index % __BATCH_SIZE == 1:
                logging.info(f"Processing study #{index}")
            study_table_data.append(
                {
                    "project": study["study_accession"],
                    "study": study["secondary_study_accession"],
                    "name": study.get("study_name", study.get("study_alias", "")),
                    "title": study.get("study_title", ""),
                    "description": study.get("study_description", ""),
                    "isolate": study.get("isolate", ""),
                    "cultivar": study.get("cultivar", ""),
                    "breed": study.get("breed", ""),
                    "geo_accession": study.get("geo_accession", ""),
                }
            )
            # Commit values into the database in batches so improve performance
            if index % __BATCH_SIZE == 0:
                _commit_studies_data(connection, study_table_data)
                study_table_data.clear()
    _commit_studies_data(connection, study_table_data)
    connection.close()
    logging.info(f"Finished processing {index} studies")


def parse_args(arg_list: list[str] | None) -> argparse.Namespace:
    """Parse the arguments from a list or from the command line.

    Args:
        arg_list: List of arguments to parse. If `None`, grab them from the command line.

    Returns:
        Parsed arguments.

    """
    parser = ArgumentParser(description=__doc__)
    parser.add_argument_dst_path(
        "--db", required=True, help="SQLite database file (schema must be created first)"
    )
    parser.add_argument_src_path("--samples", required=True, help="ENA samples XML file")
    parser.add_argument_src_path("--analysis", required=True, help="analysis JSON file")
    parser.add_argument_dst_path("--studies", required=True, help="ENA studies JSON file")
    parser.add_log_arguments(add_log_file=True)
    return parser.parse_args(arg_list)


def main(arg_list: list[str] | None = None) -> None:
    """Run module's entry-point.

    Args:
        arg_list: Arguments to parse passing list to `parse_args()`.

    """
    args = parse_args(arg_list)
    if not args.db.exists():
        raise FileNotFoundError(f"Database file {args.db} does not exist. Please create the schema first.")
    init_logging_with_args(args)
    populate_db_from_samples(args.db, args.samples)
    populate_db_from_analysis(args.db, args.analysis)
    populate_db_from_studies(args.db, args.studies)


if __name__ == "__main__":
    main()
