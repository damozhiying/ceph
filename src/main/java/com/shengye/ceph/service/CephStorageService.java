package com.shengye.ceph.service;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * 原子服务，不建议在业务代码中直接使用，建议使用CephFileService
 * @author pero.yan
 *
 */
public class CephStorageService {

	protected static AmazonS3 amazonS3 = null;

	private static Logger logger = LoggerFactory.getLogger(CephStorageService.class);
	
	public CephStorageService(String accessKey, String secretKey, String host) {
		logger.info("================开始初始化ceph配置..." + accessKey + "   " + host + "   " + secretKey);
		AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
		ClientConfiguration clientConfig = new ClientConfiguration();
		clientConfig.setProtocol(Protocol.HTTP);
		amazonS3 = new AmazonS3Client(credentials, clientConfig);
		amazonS3.setEndpoint(host);// This creates a connection so that you can
		// interact with the server.
		logger.info("================ceph配置初始化成功！");
	}

	/**
	 * 传参bucketname，结合运行环境cephEnv，项目名称cephProjectName生成新的bucketname2，最终创建bucketname2，
	 * bucketname cephEnv cephProjectName 只能是小写字母
	 * @param bucketName
	 * @return Bucket
	 * @throws AmazonS3Exception
	 */
	public Bucket createBucket(String bucketName) throws AmazonS3Exception {

		logger.info("================createBucket开始创建桶：" + bucketName);
		Bucket bucket = amazonS3.createBucket(bucketName);
		logger.info("================createBucket创建桶成功 ： " + bucket.getName());
		return bucket;

	}

	/**
	 * 获取所有的 bucket
	 * @return
	 * @throws AmazonS3Exception
	 */
	public List<Bucket> getBuckets() throws AmazonS3Exception {
		logger.info("================getBuckets获取所有bucket start... " );
		List<Bucket> buckets = amazonS3.listBuckets();
		logger.info("================getBuckets获取所有bucket end... " );
		return buckets;

	}

	/**
	 * 获取某个bucket下所有的文件名称
	 * @param bucketName
	 * @return
	 * @throws AmazonS3Exception
	 */
	public List<String> getFiles(String bucketName) throws AmazonS3Exception {

		logger.info("================getFiles查找文件start " + bucketName);

		List<String> list = new ArrayList<String>();

		ObjectListing objects = amazonS3.listObjects(bucketName);

		int i = 0;

		do {

			for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {

				list.add(i, objectSummary.getKey());
				i++;

			}

			objects = amazonS3.listNextBatchOfObjects(objects);

		} while (objects.isTruncated());

		logger.info("================getFiles查找文件 end " + bucketName);

		return list;

	}

	/**
	 * 获取文件数据
	 * @param bucketName
	 * @param cephKey
	 * @return
	 * @throws AmazonS3Exception
	 */
	public S3ObjectInputStream getObject(String bucketName, String cephKey) throws AmazonS3Exception {
		logger.info("================getObject获取文件 start... " + bucketName+"---" + cephKey);
		S3Object s3Object = amazonS3.getObject(new GetObjectRequest(bucketName, cephKey));

		S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent();
		logger.info("================getObject获取文件 end... " + bucketName+"---" + cephKey);
		return s3ObjectInputStream;

	}

	/**
	 * 获取文件数据
	 * @param bucketName
	 * @param cephKey
	 * @return
	 * @throws AmazonS3Exception
	 */
	public InputStream getInputStream(String bucketName, String cephKey) throws AmazonS3Exception {
		logger.info("================getInputStream获取文件 start... " + bucketName+"---" + cephKey);
		S3Object s3Object = amazonS3.getObject(new GetObjectRequest(bucketName, cephKey));

		InputStream is = s3Object.getObjectContent();
		logger.info("================getInputStream获取文件 end... " + bucketName+"---" + cephKey);
		return is;

	}

	/**
	 * 删除一个文件
	 * @param bucketName
	 * @param cephKey
	 * @throws AmazonS3Exception
	 */
	public void deleteFile(String bucketName, String cephKey) throws AmazonS3Exception {
		logger.info("================deleteFile删除文件 start... " + bucketName+"---" + cephKey);
		amazonS3.deleteObject(bucketName, cephKey);
		logger.info("================deleteFile删除文件 end... " + bucketName+"---" + cephKey);
	}

	/**
	 * 删除一个bucket
	 * @param bucketName
	 * @return
	 * @throws AmazonS3Exception
	 */
	public boolean deleteBucket(String bucketName) throws AmazonS3Exception {
		if (getFiles(bucketName).isEmpty()) {
			logger.info("================deleteBucket删除文件 start... " + bucketName);

			amazonS3.deleteBucket(bucketName);

			logger.info("================deleteBucket删除文件 end... " + bucketName);
			return true;

		} else {

			throw new AmazonServiceException("bucket: " + bucketName + " 下有文件，不能删除");

		}

	}

	/**
	 * 删除某个bucket和他里面的所有文件
	 * @param bucketName
	 * @return
	 * @throws AmazonS3Exception
	 */
	public boolean deleteBucketAndFiles(String bucketName) throws AmazonS3Exception {
		logger.info("================deleteBucketAndFiles删除文件 start... " + bucketName);
		if (getFiles(bucketName).isEmpty()) {

			amazonS3.deleteBucket(bucketName);
			logger.info("================deleteBucketAndFiles删除文件 end... " + bucketName);
			return true;

		} else {

			List<String> list = getFiles(bucketName);

			for (String s : list) {

				amazonS3.deleteObject(bucketName, s);

			}

			amazonS3.deleteBucket(bucketName);
			logger.info("================deleteBucketAndFiles删除文件 end... " + bucketName);
			return true;

		}

	}

	/**
	 * bucket是否存在
	 * @param bucketName
	 * @return
	 * @throws AmazonS3Exception
	 */
	public boolean isBucketExists(String bucketName) throws AmazonS3Exception {

		List<Bucket> list = getBuckets();

		for (Bucket b : list) {

			if (b.getName().equals(bucketName)) {

				return true;

			}

		}

		return false;

	}

	/**
	 * 文件是否存在
	 * @param bucketName
	 * @param cephKey
	 * @return
	 * @throws AmazonS3Exception
	 */
	public boolean isFileExists(String bucketName, String cephKey) throws AmazonS3Exception {

		List<String> files = getFiles(bucketName);

		logger.info("================in " + bucketName + " file size: " + files.size());

		for (int i = 0; i < files.size(); i++) {

			String file = files.get(i);

			// logger.info("当前目录下的文件名：" + file);

			if (cephKey.equals(file)) {

				return true;

			}
		}

		return false;

	}

	/**
	 * 保存一个文件数据
	 * @param bucketName
	 * @param cephKey
	 * @param inputStream
	 * @return
	 * @throws AmazonS3Exception
	 */
	public PutObjectResult saveInputStream(String bucketName, String cephKey, InputStream inputStream)
			throws AmazonS3Exception {

		logger.info("================写入数据start... " + cephKey);

		PutObjectResult result = amazonS3.putObject(bucketName, cephKey, inputStream, new ObjectMetadata());

		logger.info("================写入数据end... " + cephKey);

		return result;

	}

	/**
	 * * 保存一个文件数据
	 * @param bucketName
	 * @param cephKey
	 * @param file
	 * @return
	 * @throws AmazonS3Exception
	 */
	public PutObjectResult saveFile(String bucketName, String cephKey, File file) throws AmazonS3Exception {

		logger.info("================写入数据start... " + cephKey);

		PutObjectResult result = amazonS3.putObject(bucketName, cephKey, file);

		logger.info("================写入数据end..." + cephKey);

		return result;

	}

}