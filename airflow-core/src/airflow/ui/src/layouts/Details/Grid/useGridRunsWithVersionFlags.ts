/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { useMemo } from "react";

import type { GridRunsResponse } from "openapi/requests";
import { VersionIndicatorOptions } from "src/constants/showVersionIndicatorOptions";

export type GridRunWithVersionFlags = {
  isBundleVersionChange: boolean;
  isDagVersionChange: boolean;
} & GridRunsResponse;

type UseGridRunsWithVersionFlagsParams = {
  gridRuns: Array<GridRunsResponse> | undefined;
  showVersionIndicatorMode?: VersionIndicatorOptions;
};

// Hook to calculate version change flags for grid runs.
export const useGridRunsWithVersionFlags = ({
  gridRuns,
  showVersionIndicatorMode,
}: UseGridRunsWithVersionFlagsParams): Array<GridRunWithVersionFlags> | undefined => {
  const isVersionIndicatorEnabled = showVersionIndicatorMode !== VersionIndicatorOptions.NONE;

  return useMemo(() => {
    if (!gridRuns) {
      return undefined;
    }

    if (!isVersionIndicatorEnabled) {
      return gridRuns.map((run) => ({ ...run, isBundleVersionChange: false, isDagVersionChange: false }));
    }

    return gridRuns.map((run, index) => {
      const nextRun = gridRuns[index + 1];

      const isBundleVersionChange = Boolean(
        nextRun &&
        run.bundle_version !== null &&
        nextRun.bundle_version !== null &&
        run.bundle_version !== nextRun.bundle_version,
      );

      const isDagVersionChange = Boolean(
        nextRun &&
        run.dag_version_number !== null &&
        nextRun.dag_version_number !== null &&
        run.dag_version_number !== nextRun.dag_version_number,
      );

      return { ...run, isBundleVersionChange, isDagVersionChange };
    });
  }, [gridRuns, isVersionIndicatorEnabled]);
};
