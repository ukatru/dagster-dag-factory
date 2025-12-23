import unittest
from unittest.mock import MagicMock, ANY, patch
import io
from dagster_dag_factory.resources.s3 import S3Resource
from dagster_dag_factory.configs.compression import CompressConfig

class TestS3Resource(unittest.TestCase):
    def setUp(self):
        self.s3_resource = S3Resource(bucket_name="test-bucket")

    def test_smart_buffer_header_split(self):
        # Scenario: Header Retention + Push Model
        header = b"h\n"
        line1 = b"line1\n"
        line2 = b"line2\n"
        
        # Test Data: "h\nline1\nline2\n"
        # We push it in small chunks to simulate network stream
        
        # Mock get_client using patch context manager to avoid frozen error
        with patch.object(S3Resource, 'get_client') as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            
            # Create Buffer with tiny chunk size (size of header+line1)
            # chunk_size_mb needs to be tiny. 
            # 1 MB is huge for this test.
            # We must trick it or rely on write calls being aggregated.
            # SafeSplitBuffer uses chunk_size_mb * 1024 * 1024
            
            # Let's subclass to override chunk_size just for test?
            # Or just update the instance attribute manually?
            # It's not frozen.
            
            smart_buf = self.s3_resource.create_smart_buffer(
                key="test.csv",
                multi_file=True,
                chunk_size_mb=1 # We will override below
            )
            smart_buf.chunk_size = len(header) + len(line1) # Force split after line1
            
            # Write 1: Header + Line 1 (Exact chunk size)
            smart_buf.write(header + line1)
            
            # Should trigger split?
            # Logic: while len(buffer) >= chunk_size
            # buffer = header+line1. len == chunk_size.
            # rfind('\n') will be at end.
            # So chunk 1 = header+line1.
            
            # Write 2: Line 2
            smart_buf.write(line2)
            
            # Close/Flush
            smart_buf.close()
            
            # Verify call args
            put_calls = mock_client.put_object.call_args_list
            self.assertEqual(len(put_calls), 2)
            
            # Part 1: Should be Header + Line 1
            # Note: put_object args are (Bucket=..., Key=..., Body=...)
            # kwargs check
            args1 = put_calls[0][1]
            self.assertEqual(args1['Key'], "test_part_1.csv")
            self.assertEqual(args1['Body'], header + line1)
            
            # Part 2: Should be Header + Line 2 (Header prepended!)
            args2 = put_calls[1][1]
            self.assertEqual(args2['Key'], "test_part_2.csv")
            self.assertEqual(args2['Body'], header + line2)

    def test_single_file_upload(self):
        with patch.object(S3Resource, 'get_client') as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client

            smart_buf = self.s3_resource.create_smart_buffer("test.txt", multi_file=False)
            smart_buf.write(b"content")
            smart_buf.close()
            
            mock_client.put_object.assert_called_with(
                Bucket="test-bucket",
                Key="test.txt",
                Body=b"content"
            )

if __name__ == '__main__':
    unittest.main()
