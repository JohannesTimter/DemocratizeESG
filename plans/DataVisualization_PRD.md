# Data Visualization Page PRD

## Problem Statement

Users currently lack an intuitive way to visually analyze the ESG data within the platform. While the DataTable provides raw numbers, users cannot easily spot trends over time, discover correlations, or compare the performance of different companies and industries visually.

## Solution

A dedicated "Data Visualization" page that acts as the heart of the website. It will allow users to dynamically create, configure, and visualize multiple interactive charts. Users will be able to select specific numerical indicators, select a range of years, and compare multiple companies against each other, as well as against their overarching industry averages.

## User Stories

1. As a user, I want to access a dedicated Data Visualization page so that I can visually analyze ESG data.
2. As a user, I want to click a prominent "+" button so that I can add a new, empty chart widget to my dashboard.
3. As a user, I want to select multiple companies from a dropdown so that I can compare their ESG performance on the same chart.
4. As a user, I want to select a specific industry so that I can see the industry average plotted alongside my selected companies.
5. As a user, I want to select a range of years (e.g., 2018 To 2023) so that I can analyze data trends over a specific time period.
6. As a user, I want to select a single numerical indicator so that the chart clearly displays an unconfused metric on the Y-axis.
7. As a user, I want the indicator dropdown to exclusively show numerical indicators so that I don't accidentally try to chart text-based data.
8. As a user, I want to toggle between a Line Chart and a Vertical Bar Chart so that I can view the data in the most appropriate format for my analysis.
9. As a user, I want the chart lines/bars to be automatically assigned distinguishable colors so that I can easily tell companies and industry averages apart without manual configuration.
10. As a user, I want to see placeholder text in the chart area before I finish configuring my data so that I understand action is required.
11. As a user, I want to be able to create multiple chart widgets on the same page so that I can build a comprehensive dashboard of different metrics.

## Implementation Decisions

- A new `DataVisualization.tsx` page will be created to manage the state of all chart widgets.
- A `ChartWidget.tsx` component will be built, split into a left-hand configuration panel and a right-hand charting area.
- The configuration panel will include inputs for Companies (multi-select), Industry (single-select), Indicator (single-select, filtered), Year Range, and Chart Type.
- Recharts will be utilized as the core charting library. A `ChartRenderingEngine` component will wrap Recharts to abstract away its implementation details and automatically handle distinct color assignments for lines/bars.
- A custom React hook (e.g., `useChartData`) will be created to act as a Data Fetching Manager, coordinating queries to the backend and merging company data with industry averages into a unified dataset.
- The existing `/api/indicator_ids` endpoint payload will be utilized by the frontend to filter the indicator dropdown based on the `is_numeric` field ("Yes").
- A new backend REST endpoint `/api/industry_average/<industry_name>` will be created to fetch aggregated industry data.
- A new database aggregation function `select_industry_average` will be written to execute an `AVG(value)` SQL query grouped by year for the specified industry and indicator.
- Support will be strictly limited to numerical data on the Y-axis and Years on the X-axis.

## Testing Decisions

- Zero automated unit tests or Test-Driven Development (TDD) will be implemented for this feature at this time, as per the explicit request to focus entirely on building out the feature quickly.

## Out of Scope

- Visualizing text-based indicators or boolean metrics.
- Comparing multiple indicators on the same chart (no multi-Y-axis support).
- Custom color selection by the user.
- Exporting the charts as images or PDFs.
- Saving chart configurations across user sessions (persistent dashboarding).

## Further Notes

- The Data Visualization page is intended to be the central analytical tool of the platform. Its UX/UI must be highly polished, interactive, and responsive to user configuration changes.
