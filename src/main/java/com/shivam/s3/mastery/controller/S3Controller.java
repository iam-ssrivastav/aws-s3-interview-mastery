package com.shivam.s3.mastery.controller;

import com.shivam.s3.mastery.service.S3Service;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/s3")
public class S3Controller {

    private final S3Service s3Service;

    public S3Controller(S3Service s3Service) {
        this.s3Service = s3Service;
    }

    @PostMapping("/buckets")
    public ResponseEntity<String> createBucket(@RequestParam String name) {
        s3Service.createBucket(name);
        return ResponseEntity.ok("Bucket created");
    }

    @GetMapping("/buckets")
    public List<String> listBuckets() {
        return s3Service.listBuckets();
    }

    @DeleteMapping("/buckets/{name}")
    public ResponseEntity<Void> deleteBucket(@PathVariable String name) {
        s3Service.deleteBucket(name);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/upload")
    public ResponseEntity<String> upload(
            @RequestParam String bucket,
            @RequestParam String key,
            @RequestParam MultipartFile file) throws IOException {
        s3Service.uploadObject(bucket, key, file, Map.of("uploaded-by", "shivam"));
        return ResponseEntity.ok("Uploaded successfully");
    }

    @GetMapping("/download")
    public ResponseEntity<byte[]> download(@RequestParam String bucket, @RequestParam String key) {
        byte[] data = s3Service.downloadObject(bucket, key);
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .body(data);
    }

    @GetMapping("/presigned-get")
    public String getPresignedUrl(@RequestParam String bucket, @RequestParam String key) {
        return s3Service.generatePresignedGetUrl(bucket, key, 10);
    }

    @PostMapping("/multipart-upload")
    public ResponseEntity<String> multipartUpload(
            @RequestParam String bucket,
            @RequestParam String key,
            @RequestParam MultipartFile file) throws IOException {
        s3Service.multipartUpload(bucket, key, file);
        return ResponseEntity.ok("Multipart upload complete");
    }
}
