package com.shivam.s3.mastery.service;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class S3Service {

    private static final Logger log = LoggerFactory.getLogger(S3Service.class);

    private final S3Client s3Client;
    private final S3Presigner s3Presigner;

    public S3Service(S3Client s3Client, S3Presigner s3Presigner) {
        this.s3Client = s3Client;
        this.s3Presigner = s3Presigner;
    }

    // --- BUCKET OPERATIONS ---

    public void createBucket(String bucketName) {
        CreateBucketRequest request = CreateBucketRequest.builder()
                .bucket(bucketName)
                .build();
        s3Client.createBucket(request);
        log.info("Bucket created: {}", bucketName);
    }

    public List<String> listBuckets() {
        return s3Client.listBuckets().buckets().stream()
                .map(Bucket::name)
                .collect(Collectors.toList());
    }

    public void deleteBucket(String bucketName) {
        s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build());
        log.info("Bucket deleted: {}", bucketName);
    }

    // --- BUCKET CONFIGURATION (INTERVIEW FAVORITES) ---

    public void enableVersioning(String bucketName) {
        PutBucketVersioningRequest request = PutBucketVersioningRequest.builder()
                .bucket(bucketName)
                .versioningConfiguration(v -> v.status(BucketVersioningStatus.ENABLED))
                .build();
        s3Client.putBucketVersioning(request);
    }

    public void setLifecyclePolicy(String bucketName) {
        // Example: Move to GLACIER after 30 days
        LifecycleRule rule = LifecycleRule.builder()
                .id("GlacierRule")
                .status(ExpirationStatus.ENABLED)
                .filter(f -> f.prefix("archive/"))
                .transitions(t -> t.days(30).storageClass(TransitionStorageClass.GLACIER))
                .build();

        s3Client.putBucketLifecycleConfiguration(p -> p.bucket(bucketName)
                .lifecycleConfiguration(l -> l.rules(rule)));
    }

    public void setBucketPolicy(String bucketName, String policy) {
        s3Client.putBucketPolicy(p -> p.bucket(bucketName).policy(policy));
    }

    // --- OBJECT OPERATIONS ---

    public void uploadObject(String bucket, String key, MultipartFile file, Map<String, String> metadata)
            throws IOException {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .metadata(metadata)
                .contentType(file.getContentType())
                .build();

        s3Client.putObject(request, RequestBody.fromInputStream(file.getInputStream(), file.getSize()));
    }

    public byte[] downloadObject(String bucket, String key) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        ResponseBytes<GetObjectResponse> response = s3Client.getObjectAsBytes(request);
        return response.asByteArray();
    }

    public void deleteObject(String bucket, String key) {
        s3Client.deleteObject(d -> d.bucket(bucket).key(key));
    }

    public void copyObject(String sourceBucket, String sourceKey, String destBucket, String destKey) {
        CopyObjectRequest request = CopyObjectRequest.builder()
                .sourceBucket(sourceBucket)
                .sourceKey(sourceKey)
                .destinationBucket(destBucket)
                .destinationKey(destKey)
                .build();
        s3Client.copyObject(request);
    }

    // --- ADVANCED OPERATIONS ---

    public String generatePresignedGetUrl(String bucket, String key, int durationMinutes) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(Duration.ofMinutes(durationMinutes))
                .getObjectRequest(getObjectRequest)
                .build();

        return s3Presigner.presignGetObject(presignRequest).url().toString();
    }

    public String generatePresignedPutUrl(String bucket, String key, int durationMinutes) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
                .signatureDuration(Duration.ofMinutes(durationMinutes))
                .putObjectRequest(putObjectRequest)
                .build();

        return s3Presigner.presignPutObject(presignRequest).url().toString();
    }

    public void multipartUpload(String bucket, String key, MultipartFile file) throws IOException {
        // Logic for Manual Multipart Upload (Initiate -> Upload Parts -> Complete)
        CreateMultipartUploadResponse createResponse = s3Client.createMultipartUpload(b -> b.bucket(bucket).key(key));
        String uploadId = createResponse.uploadId();

        try {
            List<CompletedPart> completedParts = new ArrayList<>();
            byte[] buffer = file.getBytes();
            int partSize = 5 * 1024 * 1024; // 5MB minimum

            int partNumberBase = 1;
            for (int i = 0; i < buffer.length; i += partSize) {
                final int currentPartNumber = partNumberBase;
                int length = Math.min(partSize, buffer.length - i);
                byte[] partData = new byte[length];
                System.arraycopy(buffer, i, partData, 0, length);

                UploadPartResponse partResponse = s3Client.uploadPart(
                        u -> u.bucket(bucket).key(key).uploadId(uploadId).partNumber(currentPartNumber),
                        RequestBody.fromBytes(partData));

                completedParts
                        .add(CompletedPart.builder().partNumber(currentPartNumber).eTag(partResponse.eTag()).build());
                partNumberBase++;
            }

            s3Client.completeMultipartUpload(c -> c.bucket(bucket).key(key).uploadId(uploadId)
                    .multipartUpload(m -> m.parts(completedParts)));
        } catch (Exception e) {
            s3Client.abortMultipartUpload(a -> a.bucket(bucket).key(key).uploadId(uploadId));
            throw e;
        }
    }

    public List<String> listObjects(String bucket, String prefix) {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .build();
        return s3Client.listObjectsV2(request).contents().stream()
                .map(S3Object::key)
                .collect(Collectors.toList());
    }
}
