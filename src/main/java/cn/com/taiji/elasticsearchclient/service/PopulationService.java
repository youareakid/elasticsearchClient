package cn.com.taiji.elasticsearchclient.service;

import cn.com.taiji.elasticsearchclient.dto.PopulationDTO;

import java.io.IOException;
import java.util.ArrayList;

public interface PopulationService {

    PopulationDTO populationStatsByLabelId(ArrayList<String> labelIdList, ArrayList<String> genderLabelIdList, ArrayList<String> birthdateLabelIdList) throws IOException;


}
