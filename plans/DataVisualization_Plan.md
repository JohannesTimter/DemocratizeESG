# Plan: Data Visualization

> Source PRD: `./plans/DataVisualization_PRD.md`

## Architectural decisions

Durable decisions that apply across all phases:

- **Routes**: Frontend `/data-visualization`, Backend new endpoint `/api/industry_average/<industry_name>`.
- **Schema**: `big_dataset_consolidated_unit_converted3` and `democratizeesg_ekpis` tables from the existing `democratizeesg` database.
- **Key models / state**: `ChartWidget` configuration state, list of available companies, industries, and indicators (filtered by numeric type).

---

## Phase 1: Basic Chart Rendering

**User stories**: 1, 2, 6, 8, 10

### What to build

The first tracer bullet is setting up the new page structure, the chart widget component, and a basic end-to-end flow to render a single chart. This slice will allow the user to navigate to the Data Visualization page, click a '+' button to add an empty widget, configure it with a single company and a single numeric indicator for a fixed/static year range, and view the resulting line or bar chart using Recharts. Before full configuration, a clear placeholder is shown.

### Acceptance criteria

- [ ] New `DataVisualization` page is accessible via the main navigation menu.
- [ ] A prominent "+" button exists on the page to add a new `ChartWidget` to the dashboard.
- [ ] The `ChartWidget` initially displays informational placeholder text before data is selected.
- [ ] The configuration panel allows selecting exactly one company and one numeric indicator.
- [ ] User can toggle between Line Chart and Vertical Bar Chart representations.
- [ ] Selecting both a company and an indicator fetches data and dynamically renders the selected chart type (Line/Bar) with Recharts.

---

## Phase 2: Multiple Companies & Dynamic Filtering

**User stories**: 3, 5, 7, 9

### What to build

Enhancing the widget configuration to support multi-selection and dynamic filters. The indicator dropdown will now specifically enforce the `is_numeric` frontend filtering (based on API payload). The company dropdown becomes a multi-select, and a year range slider/dropdown is added. The charting engine will automatically generate easily distinguishable colors for each company added to the chart.

### Acceptance criteria

- [ ] The company dropdown allows the selection of multiple companies simultaneously.
- [ ] The indicator dropdown is filtered globally to exclusively show numerical indicators.
- [ ] A dynamic "Year Range" selector allows users to define the bounds of the X-axis (e.g., 2018 to 2023).
- [ ] Data is correctly visualized comparing multiple company lines/bars on the same axis.
- [ ] The charting component automatically assigns distinct colors for each selected company line/bar to guarantee visual separability.

---

## Phase 3: Industry Averages Integration

**User stories**: 4

### What to build

Introduce industry-level aggregation. This slice requires a new database query (`select_industry_average`) via a new backend API endpoint `/api/industry_average/<industry_name>`. The frontend widget configuration is expanded with a single-select for industry. The frontend charting manager will fetch the aggregated average data and plot it alongside the selected company data on the chart.

### Acceptance criteria

- [ ] A new REST endpoint `/api/industry_average/<industry_name>` calculates `AVG(value)` for the given industry logically grouped by year.
- [ ] The configuration panel adds a single-select dropdown for "Industry".
- [ ] When an industry is selected, the industry average data is fetched and effectively plotted as its own distinct line/bar alongside existing company lines/bars.

---

## Phase 4: Multi-Widget Dashboarding

**User stories**: 11

### What to build

Finalize the dashboarding aspect by lifting state appropriately, managing multiple instances of the `ChartWidget`, and allowing them to be added or potentially removed independently. Up to this point, the page might only fully support a single widget or statically scoped widgets.

### Acceptance criteria

- [ ] Clicking the "+" button can add multiple independent chart widgets to the page layout.
- [ ] Configuration state is cleanly encapsulated so that changing a setting in Widget A does not affect Widget B.
- [ ] The page layout responsively scales to accommodate multiple widgets without breaking the UI.
