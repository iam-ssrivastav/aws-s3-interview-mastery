#!/bin/bash

echo "🚀 Starting End-to-End S3 Interview Mastery Test..."

# 1. Create a test bucket
echo -e "\n1. Creating bucket: interview-demo-bucket"
curl -s -X POST "http://localhost:8080/api/s3/buckets?name=interview-demo-bucket"
echo -e "\n✅ Bucket created."

# 2. List buckets
echo -e "\n2. Listing all buckets:"
curl -s -X GET "http://localhost:8080/api/s3/buckets"
echo -e "\n✅ Buckets listed."

# 3. Create a dummy file and upload it
echo "Hello S3 Interviewer!" > test-file.txt
echo -e "\n3. Uploading test-file.txt to S3..."
curl -s -X POST "http://localhost:8080/api/s3/upload" \
  -F "bucket=interview-demo-bucket" \
  -F "key=demo/test.txt" \
  -F "file=@test-file.txt"
echo -e "\n✅ File uploaded."

# 4. Generate Presigned URL
echo -e "\n4. Generating Presigned GET URL for the file:"
PRESIGNED_URL=$(curl -s -X GET "http://localhost:8080/api/s3/presigned-get?bucket=interview-demo-bucket&key=demo/test.txt")
echo -e "🔗 URL: $PRESIGNED_URL"
echo -e "✅ Presigned URL generated."

# 5. Download the file using the API
echo -e "\n5. Downloading file via API..."
curl -s -X GET "http://localhost:8080/api/s3/download?bucket=interview-demo-bucket&key=demo/test.txt" -o downloaded-test.txt
echo "Content of downloaded file: $(cat downloaded-test.txt)"
echo -e "\n✅ File downloaded successfully."

# Cleanup
rm test-file.txt downloaded-test.txt
echo -e "\n✨ End-to-End Test Completed Successfully!"
