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
// @Document(indexName = "rkzhk.label")
public class TestCase implements Serializable {

    private String name;

    private String tyshxydm;

    private String fddbrxm;

    private String zcdz;
    
    private Double zczj;

}
