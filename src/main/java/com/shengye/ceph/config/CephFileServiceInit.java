package com.shengye.ceph.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.shengye.ceph.service.CephFileService;

@Configuration
public class CephFileServiceInit {
	
	@Value("${spring.profiles.active}")
	private String cephEnv;
		
	@Value("${ceph.accessKey}")
	private String accessKey;
	
	@Value("${ceph.secretKey}")
	private String secretKey;
	
	@Value("${ceph.gateway}")
	private String host;
	
	@Bean(name = "cephFileService")
    public CephFileService getCephFileService() {
        return  new CephFileService(cephEnv,accessKey,secretKey,host);
    }

}
