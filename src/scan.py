from dataclasses import dataclass
from services.db import run_db_statement
from services.azure_storage import get_blob_as_bytes
from azure.storage.blob import BlobServiceClient

import os


@dataclass
class ScanMetadata:
    """
    Object representing metadata (such as size, destination in Azure Storage or coordinates of bounding boxes) of a
    single scan.
    """
    id: str
    annotation_bboxes: str

def get_scans_metadata_with_double_match(annotation: str) -> list[ScanMetadata]:
    """
    Query the database for all scans that have a double label match (2 different annotators agree on some label).

    Hint: You can use the `run_db_statement` function from the `services.db` module to execute the SQL query.
    """

    sql_query = """
    SELECT id, mmg_scan_id, annotation_bboxes
    FROM mmgscans_mmgscanannotation
    WHERE mmg_scan_id IN (
        SELECT mmg_scan_id
        FROM mmgscans_mmgscanannotation
        WHERE {annotation} = True
        GROUP BY mmg_scan_id
        HAVING COUNT(*) >= 2
    );
    """.format(annotation=annotation)

    results = run_db_statement(sql_query)
    unique_ids = set()
    scans_metadata = []

    for row in results:
        mmg_scan_id = row[1]
        if mmg_scan_id not in unique_ids:
            unique_ids.add(mmg_scan_id)
            scans_metadata.append(ScanMetadata(id=mmg_scan_id, annotation_bboxes=row[2]))

    return scans_metadata

def download_scan_png(scan_metadata: ScanMetadata) -> None:
    """
    Download the scan (in PNG format) from Azure Storage and save it to the local filesystem.
    Destination directory where the files will be saved is defined by the `PNG_DIR` environment variable.

    Hint: Use the `get_blob_as_bytes` function from the `services.azure_storage` module.
    """

    scan_id = scan_metadata.id

    try:
        sql_query = f"SELECT png_image FROM mmgscans_mmgscan WHERE id = '{scan_id}'"
        result = run_db_statement(sql_query)
        if not result:
            print(f"Scan with ID {scan_id} not found in the database.")
            return
        print(f"Scan with ID {scan_id} found!")
        file_path = result[0][0]
    except Exception as e:
        print(f"Error occurred while retrieving the filename for scan with ID {scan_id}: {e}")
        return
    
    png_dir = os.getenv("PNG_DIR")
    if not png_dir:
        raise ValueError("`PNG_DIR` environment variable is not set.")
    
    try:
        # TODO: catching errors
        blob_data = get_blob_as_bytes(container_name=os.getenv('DATABASE'), blob_name=file_path)
        file_dir, file_name = os.path.split(file_path)

        destination_path = os.path.join(png_dir, str(scan_id) + ".png")

        with open(destination_path, 'wb') as file:
            file.write(blob_data)
        print(f"Scan downloaded to: {destination_path}")
    except NameError as e:
        print(e)
        return

def draw_bounding_boxes(annotation: str, scan_metadata: ScanMetadata) -> None:
    """
    Load PNG file from the local filesystem, draw bounding boxes (defined in `scan_metadat` metadata) on it and save it
    back to the local filesystem.
    PNG file is loaded from the directory defined by the `PNG_DIR` environment variable.
    Destination directory where the files will be saved is defined by the `BBOXES_DIR` environment variable.
    """
    raise NotImplementedError()


def main(annotation: str) -> None:
    scans_metadata = get_scans_metadata_with_double_match(annotation)

    for scan_metadata in scans_metadata:
        download_scan_png(scan_metadata)
        #draw_bounding_boxes(annotation, scan_metadata)

if __name__ == "__main__":
    # has_malign_mass
    # has_benign_mass
    # has_benign_microcalsifications
    # has_malign_microcalcifications
    annotation_to_filter_by = "has_malign_mass"
    main(annotation_to_filter_by)
