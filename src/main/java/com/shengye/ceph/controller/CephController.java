package com.shengye.ceph.controller;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.shengye.ceph.service.CephResponse;
import com.shengye.ceph.service.ICephFileService;

@RestController
public class CephController {

	@Autowired
	ICephFileService cephFileService;

	private static Logger log = LoggerFactory.getLogger(CephController.class);

	@RequestMapping(value = "/createBucket", method = RequestMethod.POST)
	public Bucket createBucket(String bucketName, String cephProjectName) {
		Bucket bucket = null;
		try {
			bucket = cephFileService.createBucket(bucketName, cephProjectName);
		} catch (AmazonS3Exception e) {
			log.error(e.getMessage());
			throw e;
		}
		return bucket;
	}

	@RequestMapping(value = "/getBuckets", method = RequestMethod.POST)
	public List<Bucket> getBuckets() {
		List<Bucket> list = null;
		try {
			list = cephFileService.getBuckets();
		} catch (AmazonS3Exception e) {
			log.error(e.getMessage());
			throw e;
		}
		return list;
	}

	@RequestMapping(value = "/isBucketExists", method = RequestMethod.POST)
	public boolean isBucketExists(String bucketName, String cephProjectName) {
		boolean flag = false;
		try {
			flag = cephFileService.isBucketExists(bucketName, cephProjectName);
		} catch (AmazonS3Exception e) {
			log.error(e.getMessage());
			throw e;
		}
		return flag;
	}

	@RequestMapping(value = "/deleteBucket", method = RequestMethod.POST)
	public boolean deleteBucket(String bucketName, String cephProjectName) {
		boolean flag = false;
		try {
			flag = cephFileService.deleteBucket(bucketName, cephProjectName);
		} catch (AmazonS3Exception e) {
			log.error(e.getMessage());
			throw e;
		}
		return flag;
	}

	@RequestMapping(value = "/deleteBucketAndFiles", method = RequestMethod.POST)
	public boolean deleteBucketAndFiles(String bucketName, String cephProjectName) {
		boolean flag = false;
		try {
			flag = cephFileService.deleteBucketAndFiles(bucketName, cephProjectName);
		} catch (AmazonS3Exception e) {
			log.error(e.getMessage());
			throw e;
		}
		return flag;
	}

	@RequestMapping(value = "/isFileExists", method = RequestMethod.POST)
	public boolean isFileExists(String bucketName, String cephKey, String cephProjectName) {
		boolean flag = false;
		try {
			flag = cephFileService.isFileExists(bucketName, cephKey, cephProjectName);
		} catch (AmazonS3Exception e) {
			log.error(e.getMessage());
			throw e;
		}
		return flag;
	}

	@RequestMapping(value = "/getFiles", method = RequestMethod.POST)
	public List<String> getFiles(String bucketName, String cephProjectName) {
		List<String> list = null;
		try {
			list = cephFileService.getFiles(bucketName, cephProjectName);
		} catch (AmazonS3Exception e) {
			log.error(e.getMessage());
			throw e;
		}
		return list;
	}

	@RequestMapping(value = "/deleteFile", method = RequestMethod.POST)
	public boolean deleteFile(String bucketName, String cephKey, String cephProjectName) {
		boolean flag = true;
		try {
			cephFileService.deleteFile(bucketName, cephKey, cephProjectName);
		} catch (AmazonS3Exception e) {
			log.error(e.getMessage());
			flag = false;
		}
		return flag;
	}

	@RequestMapping(value = "/getObject", method = RequestMethod.POST)
	public byte[] getObject(String bucketName, String cephKey, String cephProjectName) throws IOException {
		S3ObjectInputStream s3 = null;
		try {
			s3 = cephFileService.getObject(bucketName, cephKey, cephProjectName);
		} catch (AmazonS3Exception e) {
			log.error(e.getMessage());
			throw e;
		}
		ByteArrayOutputStream output = new ByteArrayOutputStream();
	    byte[] buffer = new byte[4096];
	    int n = 0;
	    try {
			while (-1 != (n = s3.read(buffer))) {
			    output.write(buffer, 0, n);
			}
		} catch (IOException e) {
			log.error(e.getMessage());
			throw e;
		}
	    return output.toByteArray();
	}

	@RequestMapping(value = "/getInputStream", method = RequestMethod.POST)
	public InputStream getInputStream(String bucketName, String cephKey, String cephProjectName) {
		InputStream is = null;
		try {
			is = cephFileService.getInputStream(bucketName, cephKey, cephProjectName);
		} catch (AmazonS3Exception e) {
			log.error(e.getMessage());
			throw e;
		}
		return is;
	}
	
	@PostMapping(value = "/uploadFileWithKey", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
	public CephResponse uploadFileWithKey(@RequestPart(value = "file") MultipartFile file,String bucketName, String cephKey,String cephProjectName) throws IOException {
		CephResponse ceph = null;
		try {
			ceph = cephFileService.uploadInputStream(bucketName, cephKey, file.getInputStream(), cephProjectName);
		} catch (AmazonS3Exception e) {
			log.error(e.getMessage());
			throw e;
		} catch (IOException e) {
			log.error(e.getMessage());
			throw e;
		}
		return ceph;
	}

	@PostMapping(value = "/uploadFile", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
	public CephResponse uploadFile(@RequestPart(value = "file") MultipartFile file,String bucketName,String cephProjectName) throws IOException {
		CephResponse ceph = null;
		try {
			ceph = cephFileService.uploadInputStream(bucketName, file.getInputStream(), cephProjectName);
		} catch (AmazonS3Exception e) {
			log.error(e.getMessage());
			throw e;
		} catch (IOException e) {
			log.error(e.getMessage());
			throw e;
		}
		return ceph;
	}
	
}
