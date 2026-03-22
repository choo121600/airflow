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
import { testConfig } from "playwright.config";
import { expect, test as baseTest } from "tests/e2e/fixtures";
import {
  apiDeleteDagRun,
  apiRespondToHITL,
  apiTriggerDagRun,
  setupHITLFlowViaAPI,
  waitForDagReady,
  waitForTaskInstanceState,
} from "tests/e2e/utils/test-helpers";

const hitlDagId = testConfig.testDag.hitlId;

/**
 * Worker-scoped fixture that tracks HITL DAG run IDs and guarantees cleanup.
 * Replaces the module-level `createdRunIds` array which was not shared across workers.
 */
/* eslint-disable react-hooks/rules-of-hooks -- Playwright's `use` is not a React Hook. */
const test = baseTest.extend<{ hitlRunTracker: { track: (runId: string) => void } }>({
  hitlRunTracker: async ({ authenticatedRequest }, use) => {
    const trackedRunIds: Array<string> = [];

    await use({ track: (runId: string) => trackedRunIds.push(runId) });

    for (const runId of trackedRunIds) {
      await apiDeleteDagRun(authenticatedRequest, hitlDagId, runId).catch(() => undefined);
    }
  },
});

const beforeAllRunIds: Array<string> = [];

test.describe("Verify Required Action page", () => {
  test.describe.configure({ mode: "serial" });
  test.slow();

  test.beforeAll(async ({ authenticatedRequest }) => {
    test.setTimeout(600_000);

    beforeAllRunIds.push(await setupHITLFlowViaAPI(authenticatedRequest, hitlDagId, true));
    beforeAllRunIds.push(await setupHITLFlowViaAPI(authenticatedRequest, hitlDagId, false));
  });

  test.afterAll(async ({ authenticatedRequest }) => {
    for (const runId of beforeAllRunIds) {
      await apiDeleteDagRun(authenticatedRequest, hitlDagId, runId).catch(() => undefined);
    }
  });

  test("Verify the actions list/table is displayed (or empty state if none)", async ({
    page,
    requiredActionsPage,
  }) => {
    await requiredActionsPage.navigateToRequiredActionsPage();

    await expect(requiredActionsPage.actionsTable.or(requiredActionsPage.emptyStateMessage)).toBeVisible();

    if (await requiredActionsPage.actionsTable.isVisible()) {
      await expect(page.locator("th").filter({ hasText: "Dag ID" })).toBeVisible();
      await expect(page.locator("th").filter({ hasText: "Task ID" })).toBeVisible();
      await expect(page.locator("th").filter({ hasText: "Dag Run ID" })).toBeVisible();
      await expect(page.locator("th").filter({ hasText: "Response created at" })).toBeVisible();
      await expect(page.locator("th").filter({ hasText: "Response received at" })).toBeVisible();
    } else {
      await expect(requiredActionsPage.emptyStateMessage).toBeVisible();
    }
  });

  test("Verify HITL approval UI interaction flow", async ({ authenticatedRequest, hitlRunTracker, page }) => {
    test.setTimeout(300_000);

    await waitForDagReady(authenticatedRequest, hitlDagId);
    await authenticatedRequest.patch(`/api/v2/dags/${hitlDagId}`, { data: { is_paused: false } });

    const { dagRunId } = await apiTriggerDagRun(authenticatedRequest, hitlDagId);

    hitlRunTracker.track(dagRunId);

    await waitForTaskInstanceState(authenticatedRequest, {
      dagId: hitlDagId,
      expectedState: "success",
      runId: dagRunId,
      taskId: "wait_for_default_option",
    });

    await waitForTaskInstanceState(authenticatedRequest, {
      dagId: hitlDagId,
      expectedState: "deferred",
      runId: dagRunId,
      taskId: "wait_for_input",
    });
    await apiRespondToHITL(authenticatedRequest, {
      chosenOptions: ["OK"],
      dagId: hitlDagId,
      paramsInput: { information: "test" },
      runId: dagRunId,
      taskId: "wait_for_input",
    });

    await waitForTaskInstanceState(authenticatedRequest, {
      dagId: hitlDagId,
      expectedState: "deferred",
      runId: dagRunId,
      taskId: "wait_for_option",
    });
    await apiRespondToHITL(authenticatedRequest, {
      chosenOptions: ["option 1"],
      dagId: hitlDagId,
      runId: dagRunId,
      taskId: "wait_for_option",
    });

    await waitForTaskInstanceState(authenticatedRequest, {
      dagId: hitlDagId,
      expectedState: "deferred",
      runId: dagRunId,
      taskId: "wait_for_multiple_options",
    });
    await apiRespondToHITL(authenticatedRequest, {
      chosenOptions: ["option 4", "option 5"],
      dagId: hitlDagId,
      runId: dagRunId,
      taskId: "wait_for_multiple_options",
    });

    // Wait for the approval task to become deferred — this is what we test via UI.
    await waitForTaskInstanceState(authenticatedRequest, {
      dagId: hitlDagId,
      expectedState: "deferred",
      runId: dagRunId,
      taskId: "valid_input_and_options",
    });

    await page.goto(`/dags/${hitlDagId}/runs/${dagRunId}/tasks/valid_input_and_options/required_actions`);

    const approveButton = page.getByTestId("hitl-option-Approve");

    await expect(approveButton).toBeVisible({ timeout: 30_000 });
    await expect(approveButton).toBeEnabled({ timeout: 10_000 });
    await approveButton.click();

    await expect
      .poll(
        async () => {
          try {
            const response = await authenticatedRequest.get(
              `/api/v2/dags/${hitlDagId}/dagRuns/${dagRunId}/taskInstances/valid_input_and_options/-1/hitlDetails`,
              { timeout: 10_000 },
            );

            if (!response.ok()) {
              return false;
            }

            const data = (await response.json()) as { response_received: boolean };

            return data.response_received;
          } catch {
            return false;
          }
        },
        { intervals: [2000, 5000], message: "HITL response was not recorded", timeout: 60_000 },
      )
      .toBe(true);

    await waitForTaskInstanceState(authenticatedRequest, {
      dagId: hitlDagId,
      expectedState: "success",
      runId: dagRunId,
      taskId: "valid_input_and_options",
      timeout: 60_000,
    });
  });
});
