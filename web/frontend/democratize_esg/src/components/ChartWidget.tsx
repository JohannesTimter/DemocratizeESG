import { useState, useEffect } from 'react';
import {
    LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts';
import styles from './ChartWidget.module.css';
import MultiSelect, { type SelectOption } from './MultiSelect';

const API_BASE = 'http://127.0.0.1:5000/api';
const AVAILABLE_YEARS = ['2020', '2021', '2022', '2023', '2024'];

interface ChartWidgetProps {
    companies: SelectOption[];
    indicators: SelectOption[];
    industries: SelectOption[];
    onRemove: () => void;
}

// Colors for company series
const COLORS = [
    '#2563eb', // blue
    '#dc2626', // red
    '#16a34a', // green
    '#d97706', // orange
    '#9333ea', // purple
    '#0891b2', // cyan
    '#be123c', // rose
    '#4338ca', // indigo
];

// Distinct color palette for industry average series
const INDUSTRY_COLORS = [
    '#0d9488', // teal
    '#7c3aed', // violet
    '#db2777', // pink
    '#ea580c', // orange-red
    '#65a30d', // lime
    '#0284c7', // sky
];

const INDUSTRY_AVG_PREFIX = '⌀ ';

type RelativeTo = '' | 'revenue' | 'profit' | 'employees';

const RELATIVE_TO_OPTIONS: { value: RelativeTo; label: string }[] = [
    { value: 'revenue',   label: 'Revenue' },
    { value: 'profit',    label: 'Profit' },
    { value: 'employees', label: 'Employee Count' },
];

const RELATIVE_TO_SUFFIX: Record<string, string> = {
    revenue:   'per billion euros of Revenue',
    profit:    'per billion euros of Profit',
    employees: 'per employee',
};

export default function ChartWidget({ companies, indicators, industries, onRemove }: ChartWidgetProps) {
    const [selectedCompanies, setSelectedCompanies] = useState<string[]>([]);
    const [selectedIndicator, setSelectedIndicator] = useState<string>('');
    const [selectedYears, setSelectedYears] = useState<string[]>(['2020', '2021', '2022', '2023', '2024']);
    const [selectedIndustries, setSelectedIndustries] = useState<string[]>([]);
    const [chartType, setChartType] = useState<'line' | 'bar'>('line');
    const [relativeTo, setRelativeTo] = useState<RelativeTo>('');

    const [chartData, setChartData] = useState<any[]>([]);
    const [loading, setLoading] = useState<boolean>(false);
    const [yAxisUnit, setYAxisUnit] = useState<string>('');

    useEffect(() => {
        const hasCompanies = selectedCompanies.length > 0;
        const hasIndustries = selectedIndustries.length > 0;
        const hasIndicator = !!selectedIndicator;
        const hasYears = selectedYears.length > 0;

        // Need at least indicator + years + (company OR industry) to fetch anything
        if (!hasIndicator || !hasYears || (!hasCompanies && !hasIndustries)) {
            setChartData([]);
            return;
        }

        setLoading(true);

        // Build year-keyed skeleton for sorted X-axis
        const sortedYears = [...selectedYears].sort();
        const dataByYear: Record<string, any> = {};
        sortedYears.forEach(y => { dataByYear[y] = { year: y }; });

        // Denominator maps built during fetches, resolved after Promise.all
        // companyDenominator: companyName -> { year -> value }
        const companyDenominator: Record<string, Record<string, number>> = {};
        // industryDenominator: industryKey -> { year -> value }
        const industryDenominator: Record<string, Record<string, number>> = {};

        const fetches: Promise<void>[] = [];

        // --- Company indicator fetch ---
        if (hasCompanies) {
            const params = new URLSearchParams();
            params.append('company', selectedCompanies.join(','));
            params.append('indicator_ids', selectedIndicator);
            params.append('years', selectedYears.join(','));

            const companyFetch = fetch(`${API_BASE}/data?${params.toString()}`)
                .then(res => res.json())
                .then((rawData: any[][]) => {
                    let unitDetected = '';
                    rawData.forEach(row => {
                        const year = String(row[3]);
                        const companyName = row[2];
                        const valStr = row[6];
                        if (!unitDetected && row[7]) {
                            unitDetected = String(row[7]);
                        }
                        if (dataByYear[year]) {
                            const numVal = parseFloat(valStr.toString().replace(/,/g, ''));
                            if (!isNaN(numVal)) {
                                // Store under a temporary raw key; resolved after all fetches
                                dataByYear[year][`__raw__${companyName}`] = numVal;
                            }
                        }
                    });
                    if (unitDetected) setYAxisUnit(unitDetected);
                });
            fetches.push(companyFetch);

            // --- Company denominator fetch (only when relativeTo is set) ---
            if (relativeTo) {
                const denomParams = new URLSearchParams();
                denomParams.append('company', selectedCompanies.join(','));
                denomParams.append('indicator_ids', relativeTo);
                denomParams.append('years', selectedYears.join(','));

                const denomFetch = fetch(`${API_BASE}/data?${denomParams.toString()}`)
                    .then(res => res.json())
                    .then((rawData: any[][]) => {
                        rawData.forEach(row => {
                            const year = String(row[3]);
                            const companyName = row[2];
                            const valStr = row[6];
                            const numVal = parseFloat(valStr.toString().replace(/,/g, ''));
                            if (!isNaN(numVal)) {
                                if (!companyDenominator[companyName]) {
                                    companyDenominator[companyName] = {};
                                }
                                companyDenominator[companyName][year] = numVal;
                            }
                        });
                    });
                fetches.push(denomFetch);
            }
        }

        // --- Industry average fetch (one per selected industry) ---
        selectedIndustries.forEach(industry => {
            const params = new URLSearchParams();
            params.append('indicator_id', selectedIndicator);
            params.append('years', selectedYears.join(','));

            const industryKey = `${INDUSTRY_AVG_PREFIX}${industry}`;

            const industryFetch = fetch(`${API_BASE}/industry_average/${encodeURIComponent(industry)}?${params.toString()}`)
                .then(res => res.json())
                .then((rawData: [string, number][]) => {
                    rawData.forEach(([year, avg]) => {
                        if (dataByYear[year]) {
                            dataByYear[year][`__raw__${industryKey}`] = avg;
                        }
                    });
                });
            fetches.push(industryFetch);

            // --- Industry denominator fetch (only when relativeTo is set) ---
            if (relativeTo) {
                const denomParams = new URLSearchParams();
                denomParams.append('indicator_id', relativeTo);
                denomParams.append('years', selectedYears.join(','));

                const industryDenomFetch = fetch(`${API_BASE}/industry_average/${encodeURIComponent(industry)}?${denomParams.toString()}`)
                    .then(res => res.json())
                    .then((rawData: [string, number][]) => {
                        if (!industryDenominator[industryKey]) {
                            industryDenominator[industryKey] = {};
                        }
                        rawData.forEach(([year, avg]) => {
                            industryDenominator[industryKey][year] = avg;
                        });
                    });
                fetches.push(industryDenomFetch);
            }
        });

        Promise.all(fetches)
            .then(() => {
                // Resolve raw values → apply division (or pass through) after all fetches complete
                sortedYears.forEach(year => {
                    // Company series
                    selectedCompanies.forEach(companyName => {
                        const rawVal = dataByYear[year]?.[`__raw__${companyName}`];
                        if (rawVal !== undefined) {
                            if (relativeTo) {
                                const denom = companyDenominator[companyName]?.[year];
                                if (denom !== undefined && denom !== 0) {
                                    dataByYear[year][companyName] = rawVal / denom;
                                }
                                // denom missing or zero → omit data point (don't set key)
                            } else {
                                dataByYear[year][companyName] = rawVal;
                            }
                            delete dataByYear[year][`__raw__${companyName}`];
                        }
                    });

                    // Industry-average series
                    selectedIndustries.forEach(industry => {
                        const industryKey = `${INDUSTRY_AVG_PREFIX}${industry}`;
                        const rawVal = dataByYear[year]?.[`__raw__${industryKey}`];
                        if (rawVal !== undefined) {
                            if (relativeTo) {
                                const denom = industryDenominator[industryKey]?.[year];
                                if (denom !== undefined && denom !== 0) {
                                    dataByYear[year][industryKey] = rawVal / denom;
                                }
                                // denom missing or zero → omit data point
                            } else {
                                dataByYear[year][industryKey] = rawVal;
                            }
                            delete dataByYear[year][`__raw__${industryKey}`];
                        }
                    });
                });

                setChartData(Object.values(dataByYear));
            })
            .catch(err => console.error('Could not fetch chart data', err))
            .finally(() => setLoading(false));
    }, [selectedCompanies, selectedIndicator, selectedYears, selectedIndustries, relativeTo]);

    const companySeriesInData = selectedCompanies.filter(c => chartData.some(d => d[c] !== undefined));
    const industrySeriesInData = selectedIndustries
        .map(ind => `${INDUSTRY_AVG_PREFIX}${ind}`)
        .filter(key => chartData.some(d => d[key] !== undefined));

    const isConfigured = (selectedCompanies.length > 0 || selectedIndustries.length > 0) && !!selectedIndicator && selectedYears.length > 0;

    const yAxisLabel = relativeTo
        ? `${yAxisUnit} ${RELATIVE_TO_SUFFIX[relativeTo]}`
        : yAxisUnit;

    return (
        <div className={styles.widgetContainer}>
            {/* Configuration Panel */}
            <div className={styles.configPanel}>
                <h3 className={styles.configTitle}>Configuration</h3>

                <div className={styles.formGroup}>
                    <MultiSelect
                        label="Companies"
                        options={companies}
                        selected={selectedCompanies}
                        onChange={setSelectedCompanies}
                        placeholder="Select companies..."
                    />
                </div>

                <div className={styles.formGroup}>
                    <MultiSelect
                        label="Industry Average"
                        options={industries}
                        selected={selectedIndustries}
                        onChange={setSelectedIndustries}
                        placeholder="Select industries..."
                    />
                </div>

                <div className={styles.formGroup}>
                    <label>Indicator</label>
                    <select
                        className={styles.selectInput}
                        value={selectedIndicator}
                        onChange={(e) => setSelectedIndicator(e.target.value)}
                    >
                        <option value="">-- Select Indicator --</option>
                        {indicators.map(i => <option key={i.value} value={i.value}>{i.label}</option>)}
                    </select>
                </div>

                <div className={styles.formGroup}>
                    <MultiSelect
                        label="Years"
                        options={AVAILABLE_YEARS.map(y => ({ value: y, label: y }))}
                        selected={selectedYears}
                        onChange={setSelectedYears}
                        placeholder="Select years..."
                    />
                </div>

                <div className={styles.formGroup}>
                    <label>Relative To</label>
                    <select
                        className={styles.selectInput}
                        value={relativeTo}
                        onChange={(e) => setRelativeTo(e.target.value as RelativeTo)}
                    >
                        <option value="">-- None --</option>
                        {RELATIVE_TO_OPTIONS.map(o => (
                            <option key={o.value} value={o.value}>{o.label}</option>
                        ))}
                    </select>
                </div>

                <div className={styles.formGroup}>
                    <label>Chart Type</label>
                    <div className={styles.toggleGroup}>
                        <button
                            className={`${styles.toggleButton} ${chartType === 'line' ? styles.activeToggle : ''}`}
                            onClick={() => setChartType('line')}
                        >Line</button>
                        <button
                            className={`${styles.toggleButton} ${chartType === 'bar' ? styles.activeToggle : ''}`}
                            onClick={() => setChartType('bar')}
                        >Bar</button>
                    </div>
                </div>
            </div>

            {/* Charting Area */}
            <div className={styles.chartArea}>
                {/* Remove button — top-right of chart area */}
                <button className={styles.removeButton} onClick={onRemove} title="Remove widget">✕</button>

                {!isConfigured ? (
                    <div className={styles.placeholder}>
                        <p>Select Companies or an Industry Average, an Indicator, and Years to view the chart.</p>
                    </div>
                ) : loading ? (
                    <div className={styles.placeholder}>
                        <p>Loading data...</p>
                    </div>
                ) : chartData.length === 0 ? (
                    <div className={styles.placeholder}>
                        <p>No numeric data found for this selection.</p>
                    </div>
                ) : (
                    <div style={{ width: '100%', height: '100%', minHeight: '350px' }}>
                        <ResponsiveContainer>
                            {chartType === 'line' ? (
                                <LineChart data={chartData} margin={{ top: 20, right: 40, left: 60, bottom: 40 }}>
                                    <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                                    <XAxis dataKey="year" label={{ value: 'Years', position: 'insideBottom', offset: -10 }} />
                                    <YAxis label={{ value: yAxisLabel, angle: -90, position: 'insideLeft', offset: -10, style: { textAnchor: 'middle' } }} />
                                    <Tooltip contentStyle={{ borderRadius: '8px', border: '1px solid #e5e7eb' }} />
                                    <Legend />
                                    {companySeriesInData.map((company, idx) => (
                                        <Line
                                            key={company}
                                            type="monotone"
                                            dataKey={company}
                                            stroke={COLORS[idx % COLORS.length]}
                                            strokeWidth={3}
                                            activeDot={{ r: 8 }}
                                        />
                                    ))}
                                    {industrySeriesInData.map((key, idx) => (
                                        <Line
                                            key={key}
                                            type="monotone"
                                            dataKey={key}
                                            stroke={INDUSTRY_COLORS[idx % INDUSTRY_COLORS.length]}
                                            strokeWidth={3}
                                            strokeDasharray="6 3"
                                            activeDot={{ r: 8 }}
                                        />
                                    ))}
                                </LineChart>
                            ) : (
                                <BarChart data={chartData} margin={{ top: 20, right: 40, left: 60, bottom: 40 }}>
                                    <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                                    <XAxis dataKey="year" label={{ value: 'Years', position: 'insideBottom', offset: -10 }} />
                                    <YAxis label={{ value: yAxisLabel, angle: -90, position: 'insideLeft', offset: -10, style: { textAnchor: 'middle' } }} />
                                    <Tooltip contentStyle={{ borderRadius: '8px', border: '1px solid #e5e7eb' }} />
                                    <Legend />
                                    {companySeriesInData.map((company, idx) => (
                                        <Bar
                                            key={company}
                                            dataKey={company}
                                            fill={COLORS[idx % COLORS.length]}
                                            radius={[4, 4, 0, 0]}
                                        />
                                    ))}
                                    {industrySeriesInData.map((key, idx) => (
                                        <Bar
                                            key={key}
                                            dataKey={key}
                                            fill={INDUSTRY_COLORS[idx % INDUSTRY_COLORS.length]}
                                            radius={[4, 4, 0, 0]}
                                            opacity={0.75}
                                        />
                                    ))}
                                </BarChart>
                            )}
                        </ResponsiveContainer>
                    </div>
                )}
            </div>
        </div>
    );
}
