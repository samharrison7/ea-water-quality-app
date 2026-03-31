"""
Utility file for generating metadata from the full EA water quality dataset.
This file is generally used when new data are downloaded, saving the metadata
to a JSON file so that the full dataset doesn't need to be queried every time
the app is run.
"""
import json


def generate_metadata(datadir):
    """
    Generate metadata from the full EA water quality dataset.

    Parameters
    ----------
    datadir : str
        The directory where the full EA water quality dataset is stored,
        containing a collection of parquet files, partitioned by determinand.

    Returns
    -------
    dict
        A dictionary containing the metadata for the EA water quality dataset.
    """