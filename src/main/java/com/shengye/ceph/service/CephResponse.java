package com.shengye.ceph.service;

public class CephResponse {
	
	private String cephBucket; //桶名
	private String cephKey; //存放文件的key
	private String versionId;//版本ID
	private String fileMd5;//文件的MD5值
	
	public String getCephBucket() {
		return cephBucket;
	}
	public void setCephBucket(String cephBucket) {
		this.cephBucket = cephBucket;
	}
	public String getCephKey() {
		return cephKey;
	}
	public void setCephKey(String cephKey) {
		this.cephKey = cephKey;
	}
	public String getVersionId() {
		return versionId;
	}
	public void setVersionId(String versionId) {
		this.versionId = versionId;
	}
	public String getFileMd5() {
		return fileMd5;
	}
	public void setFileMd5(String fileMd5) {
		this.fileMd5 = fileMd5;
	}
	
}
