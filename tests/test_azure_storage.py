import unittest
from unittest.mock import patch
from src.scan import download_scan_png, ScanMetadata, get_scans_metadata_with_double_match, diagnoses_dict
from src.services.db import run_db_statement

import os

class TestDownloadScanPNG(unittest.TestCase):
    annotation_arr = list(diagnoses_dict.keys())
    @patch('src.services.db.run_db_statement')
    @patch('src.services.azure_storage.get_blob_as_bytes')
    def test_succesful_download(self, mock_get_blob_as_bytes, mock_run_db_statement):
        downloaded_files = []
        expected_downloads = 0
        for annotation in self.annotation_arr:
            scan_metadata_list = get_scans_metadata_with_double_match(annotation)
            expected_downloads = expected_downloads + sum([len(scan_metadata_list)])
            print(annotation)
            print("_________________")
            for scan_metadata in scan_metadata_list:
                mmg_scan_id = scan_metadata.id
                download_scan_png(scan_metadata)
                destination_path = os.path.join(os.getenv("PNG_DIR"), f"{mmg_scan_id}.png")
                file_exists = os.path.exists(destination_path)
                # was file downloaded to location that i want?
                self.assertTrue((file_exists), "Scan file not downloaded to expected location.")
                if file_exists:
                    downloaded_files.append(mmg_scan_id)
                    print(f"Scan file {mmg_scan_id} downloaded to expected location: {destination_path}")
                    print("----------------------------------------------")
        
        # scan_metadata_list IDs equals downloaded files
        if len(downloaded_files) == expected_downloads:
            print("Number of downloaded files matches the number of IDs in scan_metadata_list.")
        else:
            print("Number of downloaded files does not match the number of IDs in scan_metadata_list.") 
        self.assertEqual(len(downloaded_files), expected_downloads)
    
if __name__ == "__main__":
    unittest.main()