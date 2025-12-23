import unittest
from unittest.mock import MagicMock, ANY, patch
import io
from dagster_dag_factory.resources.s3 import S3Resource
from dagster_dag_factory.configs.compression import CompressConfig

class TestS3Resource(unittest.TestCase):
    def setUp(self):
        self.s3_resource = S3Resource(bucket_name="test-bucket")

    def test_upload_stream_chunks_multi_file_header(self):
        # Scenario: Header Retention Test
        header = b"h\n"
        line1 = b"line1\n"
        line2 = b"line2\n"
        
        # Prepare stream mock
        stream = MagicMock()
        chunk1 = header + line1 + line2[:1]
        chunk2 = line2[1:]
        stream.read.side_effect = [chunk1, chunk2, b""]
        
        # Mock get_client using patch context manager to avoid frozen error
        with patch.object(S3Resource, 'get_client') as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            
            # Call method
            results = self.s3_resource.upload_stream_chunks(
                stream=stream, 
                key="test.csv", 
                multi_file=True, 
                chunk_size_mb=1
            )
            
            self.assertEqual(len(results), 2)
            
            # Verify call args
            put_calls = mock_client.put_object.call_args_list
            self.assertEqual(len(put_calls), 2)
            
            # Part 1: Should be Header + Line 1
            args1 = put_calls[0][1]
            self.assertEqual(args1['Key'], "test_part_1.csv")
            self.assertEqual(args1['Body'], header + line1)
            
            # Part 2: Should be Header + Line 2
            args2 = put_calls[1][1]
            self.assertEqual(args2['Key'], "test_part_2.csv")
            self.assertEqual(args2['Body'], header + line2)

    def test_single_file_upload(self):
        stream = io.BytesIO(b"content")
        
        with patch.object(S3Resource, 'get_client') as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client

            results = self.s3_resource.upload_stream_chunks(stream, "test.txt", multi_file=False)
            
            self.assertEqual(len(results), 1)
            mock_client.put_object.assert_called_with(
                Bucket="test-bucket",
                Key="test.txt",
                Body=b"content"
            )

if __name__ == '__main__':
    unittest.main()
