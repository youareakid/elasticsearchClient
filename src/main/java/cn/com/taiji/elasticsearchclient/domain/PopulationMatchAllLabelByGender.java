package cn.com.taiji.elasticsearchclient.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Slf4j
public class PopulationMatchAllLabelByGender implements Serializable {

    private String genderLabelId;

    private Long matchAllLabelByGender;

}
