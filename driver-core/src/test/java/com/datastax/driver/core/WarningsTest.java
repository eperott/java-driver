/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.base.Strings;
import java.util.List;
import org.apache.log4j.Layout;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

@CCMConfig(config = {"batch_size_warn_threshold_in_kb:5"})
@CassandraVersion("2.2.0")
public class WarningsTest extends CCMTestsSupport {

  private MemoryAppender logAppender;

  @Override
  public void onTestContextInitialized() {
    execute("CREATE TABLE foo(k int primary key, v text)");
  }

  @Override
  public void beforeTestClass(Object instance) throws Exception {
    // create a MemoryAppender and add it
    logAppender = new MemoryAppender();
    logAppender.enableFor(LoggerFactory.getLogger(RequestHandler.class));
    super.beforeTestClass(instance);
  }

  @Override
  public void afterTestClass() throws Exception {
    super.afterTestClass();
    // remove the log appender
    logAppender.disableFor(LoggerFactory.getLogger(RequestHandler.class));
    logAppender = null;
  }

  @Test(groups = "short")
  public void should_expose_warnings_on_execution_info() {
    // the default batch size warn threshold is 5 * 1024 bytes, but after CASSANDRA-10876 there must
    // be
    // multiple mutations in a batch to trigger this warning so the batch includes 2 different
    // inserts.
    ResultSet rs =
        session()
            .execute(
                String.format(
                    "BEGIN UNLOGGED BATCH\n"
                        + "INSERT INTO foo (k, v) VALUES (1, '%s')\n"
                        + "INSERT INTO foo (k, v) VALUES (2, '%s')\n"
                        + "APPLY BATCH",
                    Strings.repeat("1", 2 * 1024), Strings.repeat("1", 3 * 1024)));

    List<String> warnings = rs.getExecutionInfo().getWarnings();
    assertThat(warnings).hasSize(1);
    // also assert that by default, the warning is logged and truncated to MAX_QUERY_LOG_LENGTH
    String log = logAppender.getNext();
    assertThat(log).isNotNull();
    assertThat(log).isNotEmpty();
    assertThat(log)
        .isEqualTo(
            "Query '"
                + "BEGIN UNLOGGED BATCH\nINSERT INTO foo (k, v) VALUES"
                + "' generated server side warning(s): "
                + "Batch for [ks_1.foo] is of size 5152, exceeding specified threshold of 5120 by 32."
                + Layout.LINE_SEP);
  }

  @Test(groups = "short")
  public void should_execute_query_and_log_server_side_warnings() {
    // Assert that logging of server-side query warnings is NOT disabled
    assertThat(Boolean.getBoolean(RequestHandler.DISABLE_QUERY_WARNING_LOGS)).isFalse();

    // Given a query that will produce server side warnings that will be embedded in the
    // ExecutionInfo
    final String query = "SELECT count(*) FROM foo";
    SimpleStatement statement = new SimpleStatement(query);
    // When the query is executed
    ResultSet rs = session().execute(statement);
    // Then the result has 1 Row
    Row row = rs.one();
    assertThat(row).isNotNull();
    // And there is a server side warning captured in the ResultSet's ExecutionInfo
    ExecutionInfo ei = rs.getExecutionInfo();
    List<String> warnings = ei.getWarnings();
    assertThat(warnings).isNotEmpty();
    assertThat(warnings.size()).isEqualTo(1);
    assertThat(warnings.get(0)).isEqualTo("Aggregation query used without partition key");
    // And the driver logged the server side warning
    String log = logAppender.getNext();
    assertThat(log).isNotNull();
    assertThat(log).isNotEmpty();
    assertThat(log)
        .isEqualTo(
            "Query '"
                + query
                + "' generated server side warning(s): Aggregation query used without partition key"
                + Layout.LINE_SEP);
  }

  @Test(groups = "short")
  public void should_execute_query_and_not_log_server_side_warnings() {
    // Get the system property value for disabling logging server side warnings
    final String disabledLogFlag =
        System.getProperty(RequestHandler.DISABLE_QUERY_WARNING_LOGS, "false");
    // assert that logs are NOT disabled
    assertThat(disabledLogFlag).isEqualTo("false");
    // Disable the logs
    System.setProperty(RequestHandler.DISABLE_QUERY_WARNING_LOGS, "true");

    // Given a query that will produce server side warnings that will be embedded in the
    // ExecutionInfo
    SimpleStatement statement = new SimpleStatement("SELECT count(*) FROM foo");
    // When the query is executed
    ResultSet rs = session().execute(statement);
    // Then the result has 1 Row
    Row row = rs.one();
    assertThat(row).isNotNull();
    // And there is a server side warning captured in the ResultSet's ExecutionInfo
    ExecutionInfo ei = rs.getExecutionInfo();
    List<String> warnings = ei.getWarnings();
    assertThat(warnings).isNotEmpty();
    assertThat(warnings.size()).isEqualTo(1);
    assertThat(warnings.get(0)).isEqualTo("Aggregation query used without partition key");
    // And the driver di NOT log the server side warning
    String log = logAppender.getNext();
    assertThat(log).isNullOrEmpty();

    // reset the logging flag
    System.setProperty(RequestHandler.DISABLE_QUERY_WARNING_LOGS, disabledLogFlag);
  }
}
