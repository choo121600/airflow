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

export class DagCalendarTab extends BasePage {
  public readonly dailyToggle: Locator;
  public readonly failedToggle: Locator;
  public readonly hourlyToggle: Locator;
  public readonly totalToggle: Locator;

  public get activeCells(): Locator {
    return this.page.locator('[data-testid="calendar-cell"][data-has-data="true"]');
  }

  public get calendarCells(): Locator {
    return this.page.getByTestId("calendar-cell");
  }

  public get tooltip(): Locator {
    return this.page.getByTestId("calendar-tooltip");
  }

  public constructor(page: Page) {
    super(page);

    this.failedToggle = page.getByRole("button", { name: /failed/i });
    this.hourlyToggle = page.getByRole("button", { name: /hourly/i });
    this.dailyToggle = page.getByRole("button", { name: /daily/i });
    this.totalToggle = page.getByRole("button", { name: /total/i });
  }

  public async getActiveCellColors(): Promise<Array<string>> {
    const count = await this.activeCells.count();
    const colors: Array<string> = [];

    for (let i = 0; i < count; i++) {
      const cell = this.activeCells.nth(i);
      const bg = await cell.evaluate((el) => window.getComputedStyle(el).backgroundColor);

      colors.push(bg);
    }

    return colors;
  }

  public async getActiveCellCount(): Promise<number> {
    return this.activeCells.count();
  }

  public async getManualRunStates(): Promise<Array<string>> {
    const count = await this.activeCells.count();
    const states: Array<string> = [];

    for (let i = 0; i < count; i++) {
      const cell = this.activeCells.nth(i);

      // Firefox sometimes fails to trigger tooltips on hover.
      // Retry the hover + tooltip visibility check to handle this.
      let text = "";

      await expect(async () => {
        await cell.hover({ force: true });
        await expect(this.tooltip).toBeVisible({ timeout: 3000 });
        text = ((await this.tooltip.textContent()) ?? "").toLowerCase();
      }).toPass({ intervals: [1000], timeout: 20_000 });

      if (text.includes("success")) states.push("success");
      if (text.includes("failed")) states.push("failed");
      if (text.includes("running")) states.push("running");
    }

    return states;
  }

  public async navigateToCalendar(dagId: string) {
    await expect(async () => {
      await this.page.goto(`/dags/${dagId}/calendar`);
      await expect(this.page.getByTestId("dag-calendar-root")).toBeVisible({ timeout: 10_000 });
    }).toPass({ intervals: [2000], timeout: 60_000 });
    await this.waitForCalendarReady();
  }

  public async switchToFailedView() {
    await this.failedToggle.click();
  }

  public async switchToHourly() {
    await this.hourlyToggle.click();
    await expect(this.page.getByTestId("calendar-hourly-view")).toBeVisible({ timeout: 30_000 });
    await this.waitForCalendarReady();
  }

  public async switchToTotalView() {
    await this.totalToggle.click();
  }

  public async waitForCalendarReady(): Promise<void> {
    await expect(this.page.getByTestId("dag-calendar-root")).toBeVisible({ timeout: 60_000 });
    await expect(this.page.getByTestId("calendar-current-period")).toBeVisible({ timeout: 60_000 });
    await expect(this.page.getByTestId("calendar-loading-overlay")).toBeHidden({ timeout: 60_000 });
    await expect(this.page.getByTestId("calendar-grid")).toBeVisible({ timeout: 60_000 });
    await expect(this.page.getByTestId("calendar-cell").first()).toBeVisible({ timeout: 30_000 });
  }
}
