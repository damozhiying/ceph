package com.shengye.ceph.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.UUID;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

/**
 * Ceph 文件相关操作接口封装类
 * @author pero.yan
 *
 */
public class CephFileService implements ICephFileService {

	private String cephEnv;
	
	private static CephStorageService cephStorageService;

	public CephFileService(String cephEnv,String accessKey,String secretKey,String host) {
		this.cephEnv=cephEnv;
		cephStorageService = new CephStorageService(accessKey, secretKey, host);
	}
	private String getBucketName(String bucketName,String cephProjectName){
		return  cephProjectName + "" + cephEnv + "" + bucketName;
	}
	
	@Override
	public Bucket createBucket(String bucketName,String cephProjectName) throws AmazonS3Exception {
		bucketName = getBucketName(bucketName,cephProjectName);
		return cephStorageService.createBucket(bucketName);
	}

	@Override
	public List<Bucket> getBuckets() throws AmazonS3Exception {
		return cephStorageService.getBuckets();
	}

	@Override
	public boolean isBucketExists(String bucketName,String cephProjectName) throws AmazonS3Exception {
		bucketName = getBucketName(bucketName,cephProjectName);
		return cephStorageService.isBucketExists(bucketName);
	}

	@Override
	public boolean deleteBucket(String bucketName,String cephProjectName) throws AmazonS3Exception {
		bucketName = getBucketName(bucketName,cephProjectName);
		return cephStorageService.deleteBucket(bucketName);
	}

	@Override
	public boolean deleteBucketAndFiles(String bucketName,String cephProjectName) throws AmazonS3Exception {
		bucketName = getBucketName(bucketName,cephProjectName);
		return cephStorageService.deleteBucketAndFiles(bucketName);
	}

	@Override
	public List<String> getFiles(String bucketName,String cephProjectName) throws AmazonS3Exception {
		bucketName = getBucketName(bucketName,cephProjectName);
		return cephStorageService.getFiles(bucketName);
	}

	@Override
	public boolean isFileExists(String bucketName, String cephKey,String cephProjectName) throws AmazonS3Exception {
		bucketName = getBucketName(bucketName,cephProjectName);
		return cephStorageService.isFileExists(bucketName, cephKey);
	}

	@Override
	public void deleteFile(String bucketName, String cephKey,String cephProjectName) throws AmazonS3Exception {
		bucketName = getBucketName(bucketName,cephProjectName);
		cephStorageService.deleteFile(bucketName, cephKey);
	}

	@Override
	public S3ObjectInputStream getObject(String bucketName, String cephKey,String cephProjectName) throws AmazonS3Exception {
		bucketName = getBucketName(bucketName,cephProjectName);
		return cephStorageService.getObject(bucketName, cephKey);
	}

	@Override
	public InputStream getInputStream(String bucketName, String cephKey,String cephProjectName) throws AmazonS3Exception {
		bucketName = getBucketName(bucketName,cephProjectName);
		return cephStorageService.getInputStream(bucketName, cephKey);
	}

	@Override
	public CephResponse uploadInputStream(String bucketName, String cephKey, InputStream is,String cephProjectName) throws AmazonS3Exception {
		CephResponse response = new CephResponse();
		response.setCephBucket(bucketName);
		bucketName = getBucketName(bucketName,cephProjectName);
		if (!cephStorageService.isBucketExists(bucketName)) {
			cephStorageService.createBucket(bucketName);
		}
		PutObjectResult result = cephStorageService.saveInputStream(bucketName, cephKey, is);

		response.setCephKey(cephKey);
		response.setVersionId(result.getVersionId());
		return response;
	}

	@Override
	public CephResponse uploadInputStream(String bucketName, InputStream is,String cephProjectName) throws AmazonS3Exception, IOException {

		CephResponse response = new CephResponse();
		response.setCephBucket(bucketName);
		bucketName = getBucketName(bucketName,cephProjectName);

		if (!cephStorageService.isBucketExists(bucketName)) {
			cephStorageService.createBucket(bucketName);
		}
		String cephKey = UUID.randomUUID().toString().replaceAll("-", "");
		PutObjectResult result = cephStorageService.saveInputStream(bucketName, cephKey, is);

		response.setCephKey(cephKey);
		response.setVersionId(result.getVersionId());
		return response;
	}
	
}
