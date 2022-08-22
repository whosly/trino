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
package io.trino.plugin.deltalake;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.glue.DefaultGlueColumnStatisticsProviderFactory;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.plugin.deltalake.DeltaLakeConnectorFactory.CONNECTOR_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeRenameToWithGlueMetastore
        extends AbstractTestQueryFramework
{
    protected static final String SCHEMA = "test_tables_with_custom_location" + randomTableSuffix();
    protected static final String CATALOG_NAME = "delta_with_custom_location";
    protected File metastoreDir;
    protected HiveMetastore metastore;
    protected HdfsEnvironment hdfsEnvironment;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session deltaLakeSession = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
                .setSchema(SCHEMA)
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(deltaLakeSession).build();

        this.metastoreDir = new File(queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_data").toString());
        this.metastoreDir.deleteOnExit();

        queryRunner.installPlugin(new DeltaLakePlugin());
        queryRunner.createCatalog(
                CATALOG_NAME,
                CONNECTOR_NAME,
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "glue")
                        .put("hive.metastore.glue.region", "us-east-2")
                        .put("hive.metastore.glue.default-warehouse-dir", metastoreDir.getPath())
                        .buildOrThrow());

        HdfsConfig hdfsConfig = new HdfsConfig();
        hdfsEnvironment = new HdfsEnvironment(
                new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of()),
                hdfsConfig,
                new NoHdfsAuthentication());
        GlueHiveMetastoreConfig glueConfig = new GlueHiveMetastoreConfig()
                .setGlueRegion("us-east-2");
        metastore = new GlueHiveMetastore(
                hdfsEnvironment,
                glueConfig,
                DefaultAWSCredentialsProviderChain.getInstance(),
                directExecutor(),
                new DefaultGlueColumnStatisticsProviderFactory(directExecutor(), directExecutor()),
                Optional.empty(),
                table -> true);

        queryRunner.execute("CREATE SCHEMA " + SCHEMA + " WITH (location = '" + metastoreDir.getPath() + "')");
        return queryRunner;
    }

    @Test
    public void testRenameOfExternalTable()
    {
        String oldTable = "table_external_to_be_renamed_" + randomTableSuffix();
        String newTable = "table_external_renamed_" + randomTableSuffix();
        String location = metastoreDir.getAbsolutePath() + "/tableLocation/";
        try {
            assertQuerySucceeds(format("CREATE TABLE %s WITH (location = '%s') AS SELECT 1 as val ", oldTable, location));
            String oldLocation = (String) computeScalar("SELECT \"$path\" FROM " + oldTable);
            assertThat(query("SELECT val FROM " + oldTable))
                    .matches("VALUES (1)");

            assertUpdate("ALTER TABLE " + oldTable + " RENAME TO " + newTable);
            assertThat(query("SHOW TABLES LIKE '" + oldTable + "'"))
                    .returnsEmptyResult();
            assertThat(query("SELECT val FROM " + newTable))
                    .matches("VALUES (1)");
            assertThat((String) computeScalar("SELECT \"$path\" FROM " + newTable))
                    .isEqualTo(oldLocation);
        }
        finally {
            assertUpdate("DROP TABLE " + newTable);
        }
    }
}
