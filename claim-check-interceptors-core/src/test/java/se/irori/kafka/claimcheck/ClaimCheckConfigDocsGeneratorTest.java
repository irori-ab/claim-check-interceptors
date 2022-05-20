package se.irori.kafka.claimcheck;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.Test;

public class ClaimCheckConfigDocsGeneratorTest {

  @Test
  public void generateDocs() {
    ConfigDef configDef = BaseClaimCheckConfig.buildConfigDef(new ConfigDef());
    // RST / markdown hack
    String rst = configDef.toEnrichedRst().replace("``", "`");

    System.out.println("-----");
    System.out.println(rst);
    System.out.println("-----");

    // RST output seems to work fine as markdown
  }

}