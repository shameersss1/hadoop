/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.Constants.S3N_FOLDER_SUFFIX;

/**
 * This test verifies that the EMRFS or legacy S3N filesystem compatibility with
 * S3A works as expected.
 */
public class ITestEMRFSCompatibility extends AbstractS3ATestBase {

  private Path basePath;
  private static final String FILE_NAME = "file1.txt";

  @Override
  public void setup() throws Exception {
    super.setup();
    S3AFileSystem fs = getFileSystem();
    basePath = methodPath();

    // define the paths and create them.
    describe("Creating test directories and files");

    // write an empty S3N folder marker object
    Path folderMarker = new Path(basePath, S3N_FOLDER_SUFFIX);
    touch(fs, folderMarker);

    // write an empty object
    Path file = new Path(basePath, FILE_NAME);
    touch(fs, file);
  }

  @Test
  public void testSkipS3NFolderMarker() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    FileStatus[] fileStatus = fs.listStatus(basePath);
    Assertions.assertThat(fileStatus.length)
        .describedAs("The expected number of files did not match")
        .isEqualTo(1);
    Assertions.assertThat(fileStatus[0].getPath().getName())
        .describedAs("The expected name does not match")
        .isEqualTo(FILE_NAME);
  }
}
