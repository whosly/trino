/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.airlift.log.Logger;
import io.trino.hadoop.ConfigurationInstantiator;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.hive.gcs.GoogleGcsConfigurationInitializer;
import io.trino.plugin.hive.gcs.HiveGcsConfig;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Base64;

import static io.trino.plugin.hive.containers.HiveHadoop.HIVE3_IMAGE;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.FileFormat.ORC;

public class TestIcebergGcsConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private static final Logger LOG = Logger.get(TestIcebergGcsConnectorSmokeTest.class);

    private static final FileAttribute<?> READ_ONLY_PERMISSIONS = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));
    private final String schema;
    private final String gcpStorageBucket;
    private final Path gcpCredentialsFile;
    private final HiveHadoop hiveHadoop;
    private final FileSystem fileSystem;

    @Parameters({"testing.gcp-storage-bucket", "testing.gcp-credentials-key"})
    public TestIcebergGcsConnectorSmokeTest(String gcpStorageBucket, String gcpCredentialKey)
    {
        super(ORC);
        this.schema = "test_iceberg_gcs_connector_smoke_test_" + randomTableSuffix();
        this.gcpStorageBucket = requireNonNull(gcpStorageBucket, "gcpStorageBucket is null");

        requireNonNull(gcpCredentialKey, "gcpCredentialKey is null");
        InputStream jsonKey = new ByteArrayInputStream(Base64.getDecoder().decode(gcpCredentialKey));
        try {
            this.gcpCredentialsFile = Files.createTempFile("gcp-credentials", ".json", READ_ONLY_PERMISSIONS);
            gcpCredentialsFile.toFile().deleteOnExit();
            Files.write(gcpCredentialsFile, jsonKey.readAllBytes());

            String gcpSpecificCoreSiteXmlContent = Resources.toString(Resources.getResource("hdp3.1-core-site.xml.gcs-template"), UTF_8)
                    .replace("%GCP_CREDENTIALS_FILE_PATH%", "/etc/hadoop/conf/gcp-credentials.json");

            Path hadoopCoreSiteXmlTempFile = Files.createTempFile("core-site", ".xml", READ_ONLY_PERMISSIONS);
            hadoopCoreSiteXmlTempFile.toFile().deleteOnExit();
            Files.writeString(hadoopCoreSiteXmlTempFile, gcpSpecificCoreSiteXmlContent);

            this.hiveHadoop = closeAfterClass(HiveHadoop.builder()
                    .withImage(HIVE3_IMAGE)
                    .withFilesToMount(ImmutableMap.of(
                            "/etc/hadoop/conf/core-site.xml", hadoopCoreSiteXmlTempFile.normalize().toAbsolutePath().toString(),
                            "/etc/hadoop/conf/gcp-credentials.json", gcpCredentialsFile.toAbsolutePath().toString()))
                    .build());
            this.hiveHadoop.start();

            HiveGcsConfig gcsConfig = new HiveGcsConfig().setJsonKeyFilePath(gcpCredentialsFile.toAbsolutePath().toString());
            Configuration configuration = ConfigurationInstantiator.newEmptyConfiguration();
            new GoogleGcsConfigurationInitializer(gcsConfig).initializeConfiguration(configuration);

            this.fileSystem = FileSystem.newInstance(new URI(schemaUrl()), configuration);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass(alwaysRun = true)
    public void removeTestData()
    {
        if (fileSystem != null) {
            try {
                fileSystem.delete(new org.apache.hadoop.fs.Path(schemaUrl()), true);
            }
            catch (IOException e) {
                // The GCS bucket should be configured to expire objects automatically. Clean up issues do not need to fail the test.
                LOG.warn(e, "Failed to clean up GCS test directory: %s", schemaUrl());
            }
        }
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        if (connectorBehavior == TestingConnectorBehavior.SUPPORTS_RENAME_SCHEMA) {
            // GCS tests use the Hive Metastore catalog which does not support renaming schemas
            return false;
        }

        return super.hasBehavior(connectorBehavior);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.catalog.type", "hive_metastore")
                        .put("hive.gcs.json-key-file-path", gcpCredentialsFile.toAbsolutePath().toString())
                        .put("hive.metastore.uri", "thrift://" + hiveHadoop.getHiveMetastoreEndpoint())
                        .put("iceberg.file-format", format.name())
                        .buildOrThrow())
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withClonedTpchTables(REQUIRED_TPCH_TABLES)
                                .withSchemaName(schema)
                                .withSchemaProperties(ImmutableMap.of("location", "'" + schemaUrl() + "'"))
                                .build())
                .build();
    }

    @Override
    protected String createSchemaSql(String schema)
    {
        return format("CREATE SCHEMA %1$s WITH (location = '%2$s%1$s')", schema, schemaUrl());
    }

    private String schemaUrl()
    {
        return format("gs://%s/%s/", gcpStorageBucket, schema);
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertQueryFails(
                format("ALTER SCHEMA %s RENAME TO %s", schemaName, schemaName + randomTableSuffix()),
                "Hive metastore does not support renaming schemas");
    }
}
