package org.apache.rya.export.client.conf;

import static org.junit.Assert.assertEquals;

import java.io.File;

import javax.xml.bind.JAXBException;

import org.apache.rya.export.DBType;
import org.apache.rya.export.MergePolicy;
import org.apache.rya.export.MergeToolConfiguration;
import org.apache.rya.export.api.conf.MergeConfigurationException;
import org.junit.Test;

public class MergeConfigurationCLITest {
    @Test
    public void testCreate1ConfigurationFromFile() throws MergeConfigurationException, JAXBException {

        final MergeToolConfiguration conf = MergeConfigurationCLI.createConfigurationFromFile(new File("conf/config.xml"));
        assertEquals("10.10.10.100", conf.getParentHostname());
        assertEquals("accumuloUsername", conf.getParentUsername());
        assertEquals("accumuloPassword", conf.getParentPassword());
        assertEquals("accumuloInstance", conf.getParentRyaInstanceName());
        assertEquals("rya_demo_export_", conf.getParentTablePrefix());
        assertEquals("http://10.10.10.100:8080", conf.getParentTomcatUrl());
        assertEquals(DBType.ACCUMULO, conf.getParentDBType());
        assertEquals(1111, conf.getParentPort());
        assertEquals("10.10.10.101", conf.getChildHostname());
        assertEquals("rya_demo_child", conf.getChildRyaInstanceName());
        assertEquals("rya_demo_export_", conf.getChildTablePrefix());
        assertEquals("http://10.10.10.101:8080", conf.getChildTomcatUrl());
        assertEquals(DBType.MONGO, conf.getChildDBType());
        assertEquals(27017, conf.getChildPort());
        assertEquals(MergePolicy.TIMESTAMP, conf.getMergePolicy());
        assertEquals(Boolean.FALSE, conf.isUseNtpServer());
        assertEquals(null, conf.getNtpServerHost());
    }
}