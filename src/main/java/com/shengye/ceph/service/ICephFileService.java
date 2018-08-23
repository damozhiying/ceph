package com.shengye.ceph.service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public interface ICephFileService {

	/**
	 * 传参bucketname，结合运行环境cephEnv，项目名称cephProjectName生成新的bucketname2，最终创建bucketname2，
	 * bucketname cephEnv cephProjectName 只能是小写字母
	 * 
	 *<p>
     * To conform with DNS requirements, the following constraints apply:
     *  <ul>
     *      <li>Bucket names should not contain underscores</li>
     *      <li>Bucket names should be between 3 and 63 characters long</li>
     *      <li>Bucket names should not end with a dash</li>
     *      <li>Bucket names cannot contain adjacent periods</li>
     *      <li>Bucket names cannot contain dashes next to periods (e.g.,
     *      "my-.bucket.com" and "my.-bucket" are invalid)</li>
     *      <li>Bucket names cannot contain uppercase characters</li>
     *  </ul>
     * </p>
	 *
	 * @param bucketName
	 * @return Bucket
	 * @throws AmazonS3Exception
	 */
	public Bucket createBucket(String bucketName,String cephProjectName) throws AmazonS3Exception;

	/**
	 * 获取所有的 bucket
	 * 
	 * @return  A list of all of the Amazon S3 buckets owned by the authenticated sender of the request
	 * @throws AmazonS3Exception
	 */
	public List<Bucket> getBuckets() throws AmazonS3Exception;

	/**
	 * bucket是否存在
	 * 
	 * @param bucketName
	 * @return
	 * @throws AmazonS3Exception
	 */
	public boolean isBucketExists(String bucketName,String cephProjectName) throws AmazonS3Exception;

	/**
	 * 删除一个bucket
	 * 
	 * @param bucketName
	 * @return
	 * @throws AmazonS3Exception
	 */
	public boolean deleteBucket(String bucketName,String cephProjectName) throws AmazonS3Exception;

	/**
	 * 删除某个bucket和他里面的所有文件
	 * 
	 * @param bucketName
	 * @return
	 * @throws AmazonS3Exception
	 */
	public boolean deleteBucketAndFiles(String bucketName,String cephProjectName) throws AmazonS3Exception;

	/**
	 * 文件是否存在
	 * 
	 * @param bucketName
	 * @param cephKey
	 * @return
	 * @throws AmazonS3Exception
	 */
	public boolean isFileExists(String bucketName, String cephKey,String cephProjectName) throws AmazonS3Exception;

	/**
	 * 获取某个bucket下所有的文件名称
	 * 
	 * @param bucketName
	 * @return
	 * @throws AmazonS3Exception
	 */
	public List<String> getFiles(String bucketName,String cephProjectName) throws AmazonS3Exception;

	/**
	 * 删除一个文件
	 * 
	 * @param bucketName
	 * @param cephKey
	 * @throws AmazonS3Exception
	 */
	public void deleteFile(String bucketName, String cephKey,String cephProjectName) throws AmazonS3Exception;

	/**
	 * 获取文件数据
	 * 
	 * @param bucketName
	 * @param cephKey
	 * @return
	 * @throws AmazonS3Exception
	 */
	public S3ObjectInputStream getObject(String bucketName, String cephKey,String cephProjectName) throws AmazonS3Exception;

	/**
	 * 获取文件数据
	 * 
	 * @param bucketName
	 * @param cephKey
	 * @return
	 * @throws AmazonS3Exception
	 */
	public InputStream getInputStream(String bucketName, String cephKey,String cephProjectName) throws AmazonS3Exception;

	/**
	 * 上传文件，cephKey 生成规则自定义，在CephResponse中返回，入库时用CephResponse中bucketName 和
	 * cephKey
	 * 
	 * @param bucketName
	 * @param cephKey
	 * @param is
	 * @return CephResponse
	 * @throws AmazonS3Exception
	 */
	public CephResponse uploadInputStream(String bucketName, String cephKey, InputStream is,String cephProjectName) throws AmazonS3Exception;

	/**
	 * cephKey 自动生成，在CephResponse中返回，入库时用CephResponse中bucketName 和 cephKey
	 * 
	 * @param bucketName
	 * @param is
	 * @return CephResponse
	 * @throws AmazonS3Exception
	 */
	public CephResponse uploadInputStream(String bucketName, InputStream is,String cephProjectName) throws AmazonS3Exception, IOException;

}
