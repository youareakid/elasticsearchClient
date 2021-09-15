package cn.com.taiji.elasticsearchclient.service;

import java.io.IOException;
import java.util.Map;

public interface ImportDataFromMysqlService {

    String addOrUpdateDocument(String dbName, String tbName) throws IOException;

    boolean existIndex(String index) throws IOException;

    boolean createIndex(String index) throws IOException;

    boolean existDocument(String index, String id) throws IOException;

    int addDocument(String index, String id, Map<String, Object> map) throws IOException;

    int updateDocument(String index, String id, Map<String, Object> map) throws IOException;


}
