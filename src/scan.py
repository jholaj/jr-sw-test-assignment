from dataclasses import dataclass
from src.services.db import run_db_statement
from src.services.azure_storage import get_blob_as_bytes
from azure.storage.blob import BlobServiceClient

import os
from PIL import Image, ImageDraw, ImageFont

"""
USE:
This app is designed to mark scans, that:
    - have a double label match per study
    - have identical comments in annotation_bboxes
"""

diagnoses_dict = {
  "has_malign_mass":"Mal. Mass",
  "has_benign_mass":"Ben. Mass",
  "has_benign_microcalsifications":"Ben. Calc",
  "has_malign_microcalcifications":"Mal. Calc"
}

@dataclass
class ScanMetadata:
    """
    Object representing metadata (such as size, destination in Azure Storage or coordinates of bounding boxes) of a
    single scan.
    """
    id: str
    annotation_bboxes: list

def get_scans_metadata_with_double_match(annotation: str) -> list[ScanMetadata]:
    """
    Query the database for all scans that have a double label match (2 different annotators agree on some label).

    Hint: You can use the `run_db_statement` function from the `services.db` module to execute the SQL query.
    """

    if not annotation:
        raise ValueError("Annotation is not provided.")

    try: 
        sql_query = """
        SELECT id, mmg_scan_id, annotation_bboxes,{annotation}
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

    except Exception as e:
        raise ValueError(f"Error occurred while querying the database: {e}")

    unique_ids = set()
    scans_metadata = []

    annotations = []  # init list for annotations outside the loop

    for row in results:
        mmg_scan_id = row[1]
        if mmg_scan_id not in unique_ids:
            unique_ids.add(mmg_scan_id)
            annotations = []  # clear the list for each new mmg_scan_id
            scans_metadata.append(ScanMetadata(id=mmg_scan_id, annotation_bboxes=annotations))
        annotations.append(row[2])
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
        blob_data = get_blob_as_bytes(container_name=os.getenv('DATABASE'), blob_name=file_path)
        file_dir, file_name = os.path.split(file_path)

        destination_path = os.path.join(png_dir, str(scan_id) + ".png")

        with open(destination_path, 'wb') as file:
            file.write(blob_data)
        print(f"Scan downloaded to: {destination_path}")

    except FileNotFoundError as e:
        print(f"File not found: {file_path}")
        return

    except Exception as e:
        print(f"Error downloading scan {scan_id} from Azure Storage: {e}")
        return

def draw_bounding_boxes(annotation: str, scan_metadata: ScanMetadata) -> None:
    """
    Load PNG file from the local filesystem, draw bounding boxes (defined in `scan_metadata` metadata) on it and save it
    back to the local filesystem.
    PNG file is loaded from the directory defined by the `PNG_DIR` environment variable.
    Destination directory where the files will be saved is defined by the `BBOXES_DIR` environment variable.
    """

    """
    Using PIL for its power and ease to control
    """
    # if annotators found diagnosis in the same place
    # threshold could be tweaked
    def _is_rectangle_close(rect1, rect2, threshold=50):
        x1, y1, w1, h1 = rect1
        x2, y2, w2, h2 = rect2
        return abs(x1 - x2) < threshold and abs(y1 - y2) < threshold

    png_dir = os.getenv('PNG_DIR')

    boxes_dir = os.getenv('BBOXES_DIR')
    if not boxes_dir:
        raise ValueError("`BBOXES_DIR` environment variable is not set.")

    try:
        image_path = (os.path.join(png_dir, str(scan_metadata.id) + ".png"))
        image = Image.open(image_path)
        draw = ImageDraw.Draw(image)
        font = ImageFont.truetype("arial.ttf", 20)
    except FileNotFoundError:
        print(f"Image file not found: {image_path}")
        return

    processed_rectangles = []

    # fetching border values // drawing borders
    for annotation_list in scan_metadata.annotation_bboxes:
        for box_info in annotation_list:
            # get comment if available else use annotation
            comment = box_info.get('comment', annotation)
            
            # check if the comment contains diagnosis in dict
            if comment and diagnoses_dict[annotation] in comment:
                mark = box_info['mark']
                x, y, width, height = mark['x'], mark['y'], mark['width'], mark['height']
                rect = (x, y, x + width, y + height)
                
                # if any rect overlaps => overlapping = True
                overlapping = any(_is_rectangle_close(rect, other_rect) for other_rect in processed_rectangles)
                if not overlapping:
                    draw.rectangle(rect, outline=255, width=2)
                    draw.text((x, y - 25), comment, fill=255, stroke_width=2, stroke_fill='black', font=font)
                    processed_rectangles.append(rect)
                else:
                    print(f"Overlapping rectangle in {scan_metadata.id} found!")
    
    output_folder = os.path.join(boxes_dir, annotation)
    
    if not os.path.exists(output_folder):
        try:
            os.makedirs(output_folder)
        except OSError as e:
            print(f"Failed to create directory: {output_folder}")
            return
    try:
        output_file_path = os.path.join(output_folder, str(scan_metadata.id) + "_with_boxes.png")
        image.save(output_file_path)
    except Exception as e:
        print(f"Error occurred while saving the image: {e}")

def main(annotation: str) -> None:
    scans_metadata = get_scans_metadata_with_double_match(annotation)
    for scan_metadata in scans_metadata:
        download_scan_png(scan_metadata)
        draw_bounding_boxes(annotation, scan_metadata)

if __name__ == "__main__":
    # has_malign_mass
    # has_benign_mass
    # has_benign_microcalsifications
    # has_malign_microcalcifications
    annotation_to_filter_by = "has_benign_mass"
    main(annotation_to_filter_by)
