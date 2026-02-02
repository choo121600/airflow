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
import { expect, type Locator, type Page } from "@playwright/test";
import { BasePage } from "tests/e2e/pages/BasePage";

import type { DAGRunResponse } from "openapi/requests/types.gen";

/**
 * Dags Page Object
 */
export class DagsPage extends BasePage {
  // Page URLs
  public static get dagsListUrl(): string {
    return "/dags";
  }

  public readonly cardViewButton: Locator;
  public readonly confirmButton: Locator;
  public readonly failedFilter: Locator;
  public readonly needsReviewFilter: Locator;
  public readonly operatorFilter: Locator;
  public readonly paginationNextButton: Locator;
  public readonly paginationPrevButton: Locator;
  public readonly queuedFilter: Locator;
  public readonly retriesFilter: Locator;
  public readonly runningFilter: Locator;
  public readonly searchBox: Locator;
  public readonly searchInput: Locator;
  public readonly sortSelect: Locator;
  public readonly successFilter: Locator;
  public readonly tableViewButton: Locator;
  public readonly triggerButton: Locator;
  public readonly triggerRuleFilter: Locator;

  public get taskRows(): Locator {
    return this.page.getByTestId("table-list").locator("tbody > tr");
  }

  public constructor(page: Page) {
    super(page);
    this.triggerButton = page.getByRole("button", { name: "Trigger Dag" });
    this.confirmButton = page
      .locator('[role="dialog"], [role="alertdialog"]')
      .getByRole("button", { name: "Trigger" });
    this.paginationNextButton = page.getByTestId("next");
    this.paginationPrevButton = page.getByTestId("prev");
    this.searchBox = page.getByRole("textbox", { name: /search/i });
    this.searchInput = page.getByPlaceholder("Search Dags");
    this.operatorFilter = page.getByRole("combobox").filter({ hasText: /operator/i });
    this.triggerRuleFilter = page.getByRole("combobox").filter({ hasText: /trigger/i });
    this.retriesFilter = page.getByRole("combobox").filter({ hasText: /retr/i });
    // View toggle buttons
    this.cardViewButton = page.getByRole("button", { name: "Show card view" });
    this.tableViewButton = page.getByRole("button", { name: "Show table view" });
    // Sort select (card view only)
    this.sortSelect = page.getByTestId("sort-by-select");
    // Status filter buttons
    this.successFilter = page.getByRole("button", { name: "Success" });
    this.failedFilter = page.getByRole("button", { name: "Failed" });
    this.runningFilter = page.getByRole("button", { name: "Running" });
    this.queuedFilter = page.getByRole("button", { name: "Queued" });
    this.needsReviewFilter = page.getByRole("button", { name: "Needs Review" });
  }

  // URL builders for dynamic paths
  public static getDagDetailUrl(dagName: string): string {
    return `/dags/${dagName}`;
  }

  public static getDagRunDetailsUrl(dagName: string, dagRunId: string): string {
    return `/dags/${dagName}/runs/${dagRunId}/details`;
  }

  /**
   * Clear the search input and wait for list to reset
   */
  public async clearSearch(): Promise<void> {
    const responsePromise = this.page
      .waitForResponse((resp) => resp.url().includes("/dags") && resp.status() === 200, {
        timeout: 5000,
      })
      .catch(() => undefined); // OK if cached

    await this.searchInput.clear();
    await this.searchInput.blur();

    await responsePromise;
    await this.waitForDagList();
  }

  /**
   * Click next page button and wait for list to change
   */
  public async clickNextPage(): Promise<void> {
    const initialDagNames = await this.getDagNames();

    await this.paginationNextButton.click();

    await expect
      .poll(() => this.getDagNames(), {
        intervals: [500, 1000, 2000],
        message: "List did not update after clicking next page",
        timeout: 20_000,
      })
      .not.toEqual(initialDagNames);

    await this.waitForDagList();
  }

  /**
   * Click previous page button and wait for list to change
   */
  public async clickPrevPage(): Promise<void> {
    const initialDagNames = await this.getDagNames();

    await this.paginationPrevButton.click();

    await expect
      .poll(() => this.getDagNames(), {
        intervals: [500, 1000, 2000],
        message: "List did not update after clicking prev page",
        timeout: 20_000,
      })
      .not.toEqual(initialDagNames);

    await this.waitForDagList();
  }

  /**
   * Click sort select (only works in card view)
   */
  public async clickSortSelect(): Promise<void> {
    await this.sortSelect.click();
  }

  public async filterByOperator(operator: string): Promise<void> {
    await this.selectDropdownOption(this.operatorFilter, operator);
  }

  public async filterByRetries(retries: string): Promise<void> {
    await this.selectDropdownOption(this.retriesFilter, retries);
  }

  /**
   * Filter Dags by status
   */
  public async filterByStatus(
    status: "failed" | "needs_review" | "queued" | "running" | "success",
  ): Promise<void> {
    const filterMap: Record<typeof status, Locator> = {
      failed: this.failedFilter,
      needs_review: this.needsReviewFilter,
      queued: this.queuedFilter,
      running: this.runningFilter,
      success: this.successFilter,
    };

    const filterButton = filterMap[status];

    await expect(filterButton).toBeVisible({ timeout: 5000 });
    await filterButton.click();
    await this.page.waitForURL(new RegExp(`last_dag_run_state=${status}`), {
      timeout: 10_000,
    });

    await this.waitForSkeletonToDisappear();
  }

  public async filterByTriggerRule(rule: string): Promise<void> {
    await this.selectDropdownOption(this.triggerRuleFilter, rule);
  }

  /**
   * Get all Dag links from the list
   */
  public async getDagLinks(): Promise<Array<string>> {
    await this.waitForDagList();

    // Check which view is active
    const cardList = this.page.getByTestId("card-list");
    const isCardView = await cardList.isVisible();

    const linkLocator = isCardView
      ? this.page.getByTestId("dag-id")
      : this.page.getByTestId("table-list").locator("tbody tr td:nth-child(2) a");

    const hrefs = await linkLocator.evaluateAll((elements) =>
      elements
        .map((el) => el.getAttribute("href"))
        .filter((href): href is string => href !== null && href !== ""),
    );

    return hrefs;
  }

  /**
   * Get all Dag names from the current page
   */
  public async getDagNames(): Promise<Array<string>> {
    await this.waitForDagList();

    // Check which view is active
    const cardList = this.page.getByTestId("card-list");
    const isCardView = await cardList.isVisible();

    const dagLinks = isCardView
      ? this.page.getByTestId("dag-id")
      : this.page.getByTestId("table-list").locator("tbody tr td:nth-child(2) a");

    const texts = await dagLinks.allTextContents();

    return texts.map((text) => text.trim()).filter((text) => text !== "");
  }

  /**
   * Get count of Dags on current page
   */
  public async getDagsCount(): Promise<number> {
    await this.waitForDagList();

    // Check which view is active
    const cardList = this.page.getByTestId("card-list");
    const isCardView = await cardList.isVisible();

    if (isCardView) {
      // Card view: count dag-id elements
      const dagCards = this.page.getByTestId("dag-id");

      return dagCards.count();
    } else {
      // Table view: count table body rows
      const tableRows = this.page.getByTestId("table-list").locator("tbody tr");

      return tableRows.count();
    }
  }

  public async getFilterOptions(filter: Locator): Promise<Array<string>> {
    await filter.click();

    const listbox = this.page.locator('div[role="listbox"][data-state="open"]').first();

    await expect(listbox).toBeVisible({ timeout: 5000 });

    const options = listbox.locator('div[role="option"]');

    await expect(options.first()).toBeVisible({ timeout: 5000 });

    const dataValues = await options.evaluateAll((elements) =>
      elements
        .map((el) => el.dataset.value)
        .filter((value): value is string => value !== undefined && value.trim().length > 0),
    );

    await this.page.keyboard.press("Escape");

    return dataValues;
  }

  /**
   * Navigate to Dags list page
   */
  public async navigate(): Promise<void> {
    const responsePromise = this.page
      .waitForResponse((resp) => resp.url().includes("/api/v2/dags") && resp.status() === 200, {
        timeout: 30_000,
      })
      .catch(() => undefined);

    await this.navigateTo(DagsPage.dagsListUrl);

    await responsePromise;
    await this.waitForDagList();
  }

  /**
   * Navigate to Dag detail page
   */
  public async navigateToDagDetail(dagName: string): Promise<void> {
    await this.navigateTo(DagsPage.getDagDetailUrl(dagName));
  }

  public async navigateToDagTasks(dagId: string): Promise<void> {
    const responsePromise = this.page
      .waitForResponse((resp) => resp.url().includes(`/dags/${dagId}/tasks`) && resp.status() === 200, {
        timeout: 20_000,
      })
      .catch(() => undefined);

    await this.page.goto(`/dags/${dagId}/tasks`);

    await responsePromise;

    const operatorHeader = this.page.getByRole("columnheader", { name: "Operator" });

    await expect(operatorHeader).toBeVisible({ timeout: 15_000 });
    await expect(this.taskRows.first()).toBeVisible({ timeout: 15_000 });
  }

  /**
   * Search for a Dag by name
   */
  public async searchDag(searchTerm: string): Promise<void> {
    const currentNames = await this.getDagNames();

    const responsePromise = this.page
      .waitForResponse((resp) => resp.url().includes("/dags") && resp.status() === 200, {
        timeout: 10_000,
      })
      .catch(() => undefined);

    await this.searchInput.fill(searchTerm);

    await responsePromise;

    await expect
      .poll(
        async () => {
          const noDagFound = this.page.getByText(/no dag/i);
          const isNoDagVisible = await noDagFound.isVisible().catch(() => false);

          if (isNoDagVisible) {
            return true;
          }

          const newNames = await this.getDagNames();

          return newNames.join(",") !== currentNames.join(",");
        },
        {
          intervals: [500, 1000, 2000],
          message: "List did not update after search",
          timeout: 20_000,
        },
      )
      .toBe(true);

    await this.waitForDagList();
  }

  /**
   * Switch to card view
   */
  public async switchToCardView(): Promise<void> {
    await expect(this.cardViewButton).toBeVisible({ timeout: 15_000 });
    await expect(this.cardViewButton).toBeEnabled({ timeout: 10_000 });
    await this.cardViewButton.click();

    const cardList = this.page.getByTestId("card-list");

    await expect(cardList).toBeVisible({ timeout: 10_000 });
  }

  /**
   * Switch to table view
   */
  public async switchToTableView(): Promise<void> {
    await expect(this.tableViewButton).toBeVisible({ timeout: 15_000 });
    await expect(this.tableViewButton).toBeEnabled({ timeout: 10_000 });
    await this.tableViewButton.click();

    const table = this.page.getByRole("table");

    await expect(table).toBeVisible({ timeout: 10_000 });
  }

  /**
   * Trigger a Dag run
   */
  public async triggerDag(dagName: string): Promise<string | null> {
    await this.navigateToDagDetail(dagName);
    await expect(this.triggerButton).toBeVisible({ timeout: 15_000 });
    await this.triggerButton.click();
    const dagRunId = await this.handleTriggerDialog();

    return dagRunId;
  }

  /**
   * Verify card view is visible (throws if not visible)
   */
  public async verifyCardViewVisible(): Promise<void> {
    const cardList = this.page.getByTestId("card-list");

    await expect(cardList).toBeVisible({ timeout: 10_000 });
  }

  /**
   * Navigate to details tab and verify Dag details are displayed correctly
   */
  public async verifyDagDetails(dagName: string): Promise<void> {
    await this.page.goto(`/dags/${dagName}/details`, { waitUntil: "domcontentloaded" });

    await this.waitForSkeletonToDisappear();

    // Use getByRole to precisely target the heading element
    // This avoids "strict mode violation" from matching breadcrumbs, file paths, etc.
    await expect(this.page.getByRole("heading", { name: dagName })).toBeVisible({
      timeout: 15_000,
    });
  }

  /**
   * Verify if a specific Dag exists in the list
   */
  public async verifyDagExists(dagId: string): Promise<boolean> {
    await this.waitForDagList();

    // Check which view is active
    const cardList = this.page.getByTestId("card-list");
    const isCardView = await cardList.isVisible();

    const dagLink = isCardView
      ? this.page.getByTestId("dag-id").filter({ hasText: dagId })
      : this.page.getByTestId("table-list").locator("tbody tr td a").filter({ hasText: dagId });

    return dagLink.isVisible({ timeout: 10_000 }).catch(() => false);
  }

  public async verifyDagRunStatus(dagName: string, dagRunId: string | null): Promise<void> {
    if (dagRunId === null || dagRunId === "") {
      return;
    }

    const apiUrl = `/api/v2/dags/${dagName}/dagRuns/${dagRunId}`;

    // Optimized polling intervals: check more frequently at first, then back off
    await expect
      .poll(
        async () => {
          try {
            const response = await this.page.request.get(apiUrl, {
              timeout: 15_000,
            });

            if (!response.ok()) {
              return "unknown";
            }

            const data = (await response.json()) as DAGRunResponse;

            return data.state;
          } catch {
            return "unknown";
          }
        },
        {
          intervals: [2000, 3000, 5000, 10_000, 15_000],
          message: `Waiting for Dag run ${dagRunId} to complete`,
          timeout: 5 * 60 * 1000,
        },
      )
      .toMatch(/^(success|failed)$/);

    const finalResponse = await this.page.request.get(apiUrl);
    const finalData = (await finalResponse.json()) as DAGRunResponse;

    if (finalData.state === "failed") {
      throw new Error(`Dag run failed: ${dagRunId}`);
    }
  }

  /**
   * Verify that the Dags list is visible
   */
  public async verifyDagsListVisible(): Promise<void> {
    await this.waitForDagList();
  }

  /**
   * Verify table view is visible (throws if not visible)
   */
  public async verifyTableViewVisible(): Promise<void> {
    const table = this.page.getByRole("table");

    await expect(table).toBeVisible({ timeout: 10_000 });
  }

  private async handleTriggerDialog(): Promise<string | null> {
    const responsePromise = this.page
      .waitForResponse(
        (response) => {
          const url = response.url();
          const method = response.request().method();

          return method === "POST" && url.includes("dagRuns") && !url.includes("hitlDetails");
        },
        { timeout: 15_000 },
      )
      .catch(() => undefined);

    await expect(this.confirmButton).toBeVisible({ timeout: 10_000 });
    await expect(this.confirmButton).toBeEnabled({ timeout: 5000 });
    await this.confirmButton.click();

    const apiResponse = await responsePromise;

    if (apiResponse) {
      try {
        const responseBody = await apiResponse.text();
        const responseJson = JSON.parse(responseBody) as DAGRunResponse;

        if (responseJson.dag_run_id) {
          return responseJson.dag_run_id;
        }
      } catch {
        // Response parsing failed
      }
    }

    // eslint-disable-next-line unicorn/no-null
    return null;
  }

  private async selectDropdownOption(filter: Locator, value: string): Promise<void> {
    await filter.click();

    const option = this.page.locator(`div[role="option"][data-value="${value}"]`);

    await option.click({ timeout: 5000 });
  }

  /**
   * Wait for Dag list to be rendered
   */
  private async waitForDagList(): Promise<void> {
    // Define multiple possible UI states: Card View, Table View, or Empty State.
    // Use regex (/no dag/i) to handle case and singular/plural variations
    // (e.g. "No Dag found", "No Dags found", "NO DAG FOUND").
    const cardList = this.page.getByTestId("card-list");
    const tableList = this.page.getByTestId("table-list");
    const noDagFound = this.page.getByText(/no dag/i);
    const fallbackTable = this.page.getByRole("table");

    await expect(cardList.or(tableList).or(noDagFound).or(fallbackTable)).toBeVisible({
      timeout: 20_000,
    });

    // If empty state is shown, consider the list as successfully rendered
    if (await noDagFound.isVisible().catch(() => false)) {
      return;
    }

    await this.waitForSkeletonToDisappear(15_000);

    if (await noDagFound.isVisible().catch(() => false)) {
      return;
    }

    const isCardView = await cardList.isVisible().catch(() => false);

    if (isCardView) {
      const dagCards = this.page.getByTestId("dag-id");

      await expect(dagCards.first().or(noDagFound)).toBeVisible({
        timeout: 15_000,
      });
    } else {
      const rowsInTableList = tableList.locator("tbody tr");
      const anyTableRows = fallbackTable.locator("tbody tr");

      await expect(rowsInTableList.first().or(anyTableRows.first()).or(noDagFound)).toBeVisible({
        timeout: 15_000,
      });
    }
  }

  /**
   * Wait for skeleton loaders to disappear
   */
  private async waitForSkeletonToDisappear(timeout: number = 10_000): Promise<void> {
    const skeleton = this.page.getByTestId("skeleton");

    await expect(skeleton).toHaveCount(0, { timeout });
  }
}
