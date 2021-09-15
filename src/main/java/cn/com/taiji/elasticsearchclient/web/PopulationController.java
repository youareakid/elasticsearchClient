package cn.com.taiji.elasticsearchclient.web;

import cn.com.taiji.elasticsearchclient.dto.PopulationDTO;
import cn.com.taiji.elasticsearchclient.dto.ResultDTO;
import cn.com.taiji.elasticsearchclient.service.PopulationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.ArrayList;

@Controller
@RequestMapping("/population")
public class PopulationController {

    @Autowired
    PopulationService populationService;

    private String maleLabelId = "9f8918d1-d2cf-4e89-9458-1acd8140ab6e";
    private String femaleLabelId = "749fe8f9-bd5b-4b7f-8dbe-041ccd74462f";
    private String birthdate1950s = "20a8cee4-4ca6-4144-b6b6-1f41957f3bcf";
    private String birthdate1960s = "ef40fa6a-935b-475d-8e3a-317bfaa23444";
    private String birthdate1970s = "9b29963b-c426-473a-b815-569739cfa0e3";
    private String birthdate1980s = "7fd9155d-02fc-456f-9054-d637a0c26be7";
    private String birthdate1990s = "27907383-1a01-4e51-b6a1-60451a946026";
    private String birthdate2000s = "277f3966-0715-4fec-8eb4-0e61210da75c";

    @GetMapping(value = "/search-by-labelId")
    @ResponseBody
    public ResultDTO<PopulationDTO> searchByLabelId(@RequestParam ArrayList<String> labelIdList) throws IOException {

        ArrayList<String> genderLabelIdList = new ArrayList<>();
        genderLabelIdList.add(maleLabelId);
        genderLabelIdList.add(femaleLabelId);

        ArrayList<String> birthdateLabelIdList = new ArrayList<>();
        birthdateLabelIdList.add(birthdate1950s);
        birthdateLabelIdList.add(birthdate1960s);
        birthdateLabelIdList.add(birthdate1970s);
        birthdateLabelIdList.add(birthdate1980s);
        birthdateLabelIdList.add(birthdate1990s);
        birthdateLabelIdList.add(birthdate2000s);

        PopulationDTO populationDTO = populationService.populationStatsByLabelId(labelIdList, genderLabelIdList, birthdateLabelIdList);
        return new ResultDTO(populationDTO);

    }

}
