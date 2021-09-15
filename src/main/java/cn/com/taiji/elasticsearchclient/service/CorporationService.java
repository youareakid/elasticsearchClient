package cn.com.taiji.elasticsearchclient.service;

import cn.com.taiji.elasticsearchclient.dto.CorporationDTO;

import java.io.IOException;
import java.util.ArrayList;

public interface CorporationService {

    CorporationDTO listCorporationByName(String name, Integer pageNum, Integer pageSize) throws IOException;

    CorporationDTO listCorporationByLabelId(ArrayList<String> labelIdList, Integer pageNum, Integer pageSize) throws IOException;

    CorporationDTO listCorporationByDataMasking(String name, Integer level, Integer pageNum, Integer pageSize) throws IOException;

}
