import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from dagster_dag_factory.resources.s3 import S3Resource
from dagster_dag_factory.models.s3_info import S3Info

class TestS3Listing(unittest.TestCase):
    def setUp(self):
        self.resource = S3Resource(
            access_key="test",
            secret_key="test",
            endpoint_url="http://localhost:9000",
            region_name="us-east-1"
        )
        
    @patch('dagster_dag_factory.resources.s3.S3Resource.get_client')
    def test_list_files_basic(self, mock_get_client):
        # Mock S3 Paginator
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        
        # Mock page content
        mock_paginator.paginate.return_value = [
            {
                'Contents': [
                    {'Key': 'raw/data1.csv', 'Size': 100, 'LastModified': datetime(2023, 1, 1, tzinfo=timezone.utc)},
                    {'Key': 'raw/data2.csv', 'Size': 200, 'LastModified': datetime(2023, 1, 2, tzinfo=timezone.utc)},
                    {'Key': 'other/file.txt', 'Size': 50, 'LastModified': datetime(2023, 1, 3, tzinfo=timezone.utc)}
                ]
            }
        ]
        
        # 1. Test basic listing with prefix
        files = self.resource.list_files(bucket_name="test-bucket", prefix="raw/")
        self.assertEqual(len(files), 3) # Note: list_objects_v2 returns all requested by prefix
        self.assertEqual(files[0].key, 'raw/data1.csv')
        self.assertEqual(files[0].object_name, 'data1.csv')
        self.assertEqual(files[0].path, 'raw')
        
    @patch('dagster_dag_factory.resources.s3.S3Resource.get_client')
    def test_list_files_predicate(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        
        mock_paginator.paginate.return_value = [
            {
                'Contents': [
                    {'Key': 'raw/data1.csv', 'Size': 100, 'LastModified': datetime(2023, 1, 1, tzinfo=timezone.utc)},
                    {'Key': 'raw/data2.csv', 'Size': 200, 'LastModified': datetime(2023, 1, 2, tzinfo=timezone.utc)}
                ]
            }
        ]
        
        # Test string predicate (eval)
        files = self.resource.list_files(bucket_name="test-bucket", predicate="info.size > 150")
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0].key, 'raw/data2.csv')
        
        # Test callable predicate
        files = self.resource.list_files(bucket_name="test-bucket", predicate=lambda x: x.key.endswith('1.csv'))
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0].key, 'raw/data1.csv')

    @patch('dagster_dag_factory.resources.s3.S3Resource.get_client')
    def test_list_files_on_each(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        
        mock_paginator.paginate.return_value = [
            {
                'Contents': [
                    {'Key': 'file1.csv', 'Size': 10, 'LastModified': datetime.now(timezone.utc)},
                    {'Key': 'file2.csv', 'Size': 10, 'LastModified': datetime.now(timezone.utc)},
                    {'Key': 'file3.csv', 'Size': 10, 'LastModified': datetime.now(timezone.utc)}
                ]
            }
        ]
        
        # Test callback that stops early
        processed = []
        def my_callback(info, index):
            processed.append(info.key)
            if index == 2:
                return False # Stop
            return True
            
        files = self.resource.list_files(bucket_name="test-bucket", on_each=my_callback)
        self.assertEqual(len(processed), 2)
        self.assertEqual(len(files), 2)
        self.assertEqual(files[0].key, 'file1.csv')
        self.assertEqual(files[1].key, 'file2.csv')

if __name__ == "__main__":
    unittest.main()
