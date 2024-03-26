import unittest
from unittest.mock import patch
from src.scan import get_scans_metadata_with_double_match, diagnoses_dict, ScanMetadata
from src.services.db import run_db_statement

class TestDatabaseQueries(unittest.TestCase):
    annotation_arr = list(diagnoses_dict.keys())
    @patch('src.services.db.run_db_statement')
    def test_valid_annotation(self, mock_run_db_statement):
        for annotation in self.annotation_arr:
            actual_results = get_scans_metadata_with_double_match(annotation)
            self.assertGreater(len(actual_results), 1)
            print(f"Annotation: {annotation} is valid!")

    def test_invalid_annotation(self):
        with self.assertRaises(ValueError):
            get_scans_metadata_with_double_match("")

    def test_query(self):
        for annotation in self.annotation_arr:
            print(f"----- Searching in {annotation} record -----")
            try: 
                scan_metadata_list = get_scans_metadata_with_double_match(annotation)

                for scan_metadata in scan_metadata_list:
                    mmg_scan_id = scan_metadata.id
                    print(f"Scan with mmg_scan_id {mmg_scan_id}:")

                    # Count of 'True' in currently searched annotation for mmg_scan_id in db
                    try: 
                        sql_query = f"""
                            SELECT COUNT(*) AS true_count
                            FROM mmgscans_mmgscanannotation
                            WHERE mmg_scan_id = '{mmg_scan_id}' AND {annotation} = True
                            """
                        result = run_db_statement(sql_query)
                        true_count = result[0][0] if result else 0
                        print(f"Found {true_count} 'True' occurrences for ID: {mmg_scan_id} for annotation '{annotation}' in the database.")

                        self.assertGreaterEqual(true_count, 2, f"Scan with mmg_scan_id {mmg_scan_id} does not have at least 2 occurrences of 'True' for annotation '{annotation}'.")

                    except Exception as e:
                        print(f"Error occurred while querying the database: {e}")

            except Exception as e:
                print(f"Error occurred while querying the database: {e}")

if __name__ == '__main__':
    suite = unittest.TestSuite()

    suite.addTest(TestDatabaseQueries('test_valid_annotation'))
    suite.addTest(TestDatabaseQueries('test_invalid_annotation'))
    suite.addTest(TestDatabaseQueries('test_query'))

    unittest.TextTestRunner().run(suite)