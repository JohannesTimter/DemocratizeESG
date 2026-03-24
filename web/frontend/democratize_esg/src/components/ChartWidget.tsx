import { useState, useEffect } from 'react';
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts';
import styles from './ChartWidget.module.css';
import MultiSelect, { type SelectOption } from './MultiSelect';

const API_BASE = 'http://127.0.0.1:5000/api';
// Dynamic year range for Phase 2
const AVAILABLE_YEARS = ['2018', '2019', '2020', '2021', '2022', '2023', '2024'];

interface ChartWidgetProps {
    companies: SelectOption[];
    indicators: SelectOption[];
}

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

export default function ChartWidget({ companies, indicators }: ChartWidgetProps) {
    const [selectedCompanies, setSelectedCompanies] = useState<string[]>([]);
    const [selectedIndicator, setSelectedIndicator] = useState<string>('');
    const [selectedYears, setSelectedYears] = useState<string[]>(['2020', '2021', '2022', '2023']);
    const [chartType, setChartType] = useState<'line' | 'bar'>('line');
    
    const [chartData, setChartData] = useState<any[]>([]);
    const [loading, setLoading] = useState<boolean>(false);

    useEffect(() => {
        if (selectedCompanies.length === 0 || !selectedIndicator || selectedYears.length === 0) {
            setChartData([]);
            return;
        }

        setLoading(true);
        const params = new URLSearchParams();
        params.append('company', selectedCompanies.join(','));
        params.append('indicator_ids', selectedIndicator);
        params.append('years', selectedYears.join(','));

        fetch(`${API_BASE}/data?${params.toString()}`)
            .then(res => res.json())
            .then((rawData: any[][]) => {
                const dataByYear: Record<string, any> = {};
                
                // Ensure years are sorted on X-Axis
                const sortedYears = [...selectedYears].sort();
                sortedYears.forEach(y => {
                    dataByYear[y] = { year: y };
                });

                rawData.forEach(row => {
                    const year = String(row[3]);
                    const companyName = row[2];
                    const valStr = row[6];
                    
                    if (dataByYear[year]) {
                        const cleanStr = valStr.toString().replace(/,/g, '');
                        const numVal = parseFloat(cleanStr);
                        if (!isNaN(numVal)) {
                           dataByYear[year][companyName] = numVal;
                        }
                    }
                });

                setChartData(Object.values(dataByYear));
            })
            .catch(err => console.error("Could not fetch chart data", err))
            .finally(() => setLoading(false));
    }, [selectedCompanies, selectedIndicator, selectedYears]);

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
                {selectedCompanies.length === 0 || !selectedIndicator || selectedYears.length === 0 ? (
                    <div className={styles.placeholder}>
                        <p>Please select Companies, an Indicator, and Years to view the chart.</p>
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
                                <LineChart data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
                                    <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                                    <XAxis dataKey="year" />
                                    <YAxis />
                                    <Tooltip contentStyle={{ borderRadius: '8px', border: '1px solid #e5e7eb' }} />
                                    <Legend />
                                    {selectedCompanies.filter(c => chartData.some(d => d[c] !== undefined)).map((company, idx) => (
                                        <Line 
                                            key={company}
                                            type="monotone" 
                                            dataKey={company} 
                                            stroke={COLORS[idx % COLORS.length]} 
                                            strokeWidth={3}
                                            activeDot={{ r: 8 }} 
                                        />
                                    ))}
                                </LineChart>
                            ) : (
                                <BarChart data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
                                    <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                                    <XAxis dataKey="year" />
                                    <YAxis />
                                    <Tooltip contentStyle={{ borderRadius: '8px', border: '1px solid #e5e7eb' }} />
                                    <Legend />
                                    {selectedCompanies.filter(c => chartData.some(d => d[c] !== undefined)).map((company, idx) => (
                                        <Bar 
                                            key={company}
                                            dataKey={company} 
                                            fill={COLORS[idx % COLORS.length]} 
                                            radius={[4, 4, 0, 0]} 
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
