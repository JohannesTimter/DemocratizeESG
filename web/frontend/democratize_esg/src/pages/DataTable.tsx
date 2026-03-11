import { useState, useEffect, useRef, useCallback } from 'react';
import styles from './DataTable.module.css';
import MultiSelect from '../components/MultiSelect';
import { ArrowUpDown, ArrowUp, ArrowDown, X } from 'lucide-react';

const API_BASE = 'http://127.0.0.1:5000/api';

const YEARS = ['2020', '2021', '2022', '2023', '2024'];

interface DataRow {
    id: number;
    industry: string;
    companyName: string;
    year: string;
    indicator: string;
    isNotDisclosed: string;
    value: string;
    unit: string;
    pageNumber: string;
    sourceTitle: string;
    textSection: string;
    inputTokenCount: number;
    outputTokenCount: number;
    thoughtSummary: string;
}

const MOCK_DATA: DataRow[] = [
    {
        id: 1,
        industry: 'Tech',
        companyName: 'Google',
        year: '2022',
        indicator: 'CO2 Emissions',
        isNotDisclosed: 'No',
        value: '100.5',
        unit: 'metric tons',
        pageNumber: '12',
        sourceTitle: '2022 Sustainability Report',
        textSection: 'Direct emissions from data centers...',
        inputTokenCount: 120,
        outputTokenCount: 45,
        thoughtSummary: 'Extracted from greenhouse gas inventory table.'
    }
];

const COLUMNS = [
    { key: 'industry', label: 'Industry' },
    { key: 'companyName', label: 'Company name' },
    { key: 'year', label: 'Year' },
    { key: 'indicator', label: 'Indicator' },
    { key: 'isDisclosed', label: 'Is Disclosed' },
    { key: 'value', label: 'Value' },
    { key: 'unit', label: 'Unit' },
    { key: 'details', label: 'Details' },
];

export default function DataTable() {
    // Options fetched from API
    const [industries, setIndustries] = useState<string[]>([]);
    const [companies, setCompanies] = useState<string[]>([]);
    const [indicators, setIndicators] = useState<string[]>([]);

    // Selected values
    const [selectedIndustries, setSelectedIndustries] = useState<string[]>([]);
    const [selectedCompanies, setSelectedCompanies] = useState<string[]>([]);
    const [selectedIndicators, setSelectedIndicators] = useState<string[]>([]);
    const [selectedYears, setSelectedYears] = useState<string[]>(YEARS);
    const [selectUndisclosed, setSelectUndisclosed] = useState(false);

    const [selectedRowsForDetails, setSelectedRowForDetails] = useState<DataRow | null>(null);

    // Table state
    const [data, setData] = useState<DataRow[]>(MOCK_DATA);
    const [sortConfig, setSortConfig] = useState<{ key: keyof DataRow | null, direction: 'asc' | 'desc' | null }>({ key: null, direction: null });
    const [columnWidths, setColumnWidths] = useState<Record<string, number>>({
        industry: 160,
        companyName: 160,
        year: 100,
        indicator: 200,
        isNotDisclosed: 140,
        value: 120,
        unit: 140,
        details: 160
    });

    // Loading / error states for filters
    const [loadingIndustries, setLoadingIndustries] = useState(true);
    const [loadingCompanies, setLoadingCompanies] = useState(true);
    const [loadingIndicators, setLoadingIndicators] = useState(true);

    const [errorIndustries, setErrorIndustries] = useState<string | null>(null);
    const [errorCompanies, setErrorCompanies] = useState<string | null>(null);
    const [errorIndicators, setErrorIndicators] = useState<string | null>(null);

    // Loading / error states for data fetch
    const [isLoadingData, setIsLoadingData] = useState(false);
    const [dataError, setDataError] = useState<string | null>(null);

    // Resizing refs
    const isResizing = useRef<string | null>(null);
    const startX = useRef<number>(0);
    const startWidth = useRef<number>(0);

    const handleSort = (key: string) => {
        const columnKey = key as keyof DataRow;
        let direction: 'asc' | 'desc' | null = 'asc';
        if (sortConfig.key === columnKey && sortConfig.direction === 'asc') {
            direction = 'desc';
        } else if (sortConfig.key === columnKey && sortConfig.direction === 'desc') {
            direction = null;
        }
        setSortConfig({ key: direction ? columnKey : null, direction });
    };

    const handleGoClick = async () => {
        setIsLoadingData(true);
        setDataError(null);

        try {
            const params = new URLSearchParams();
            if (selectedCompanies.length > 0) params.append('company', selectedCompanies.join(','));
            if (selectedYears.length > 0) params.append('years', selectedYears.join(','));
            if (selectedIndicators.length > 0) params.append('indicator_ids', selectedIndicators.join(','));
            if (selectUndisclosed) params.append('selectUndisclosed', 'True');

            const response = await fetch(`${API_BASE}/data?${params.toString()}`);
            if (!response.ok) throw new Error('Failed to fetch data');

            const rawData: any[][] = await response.json();

            // Map raw arrays to DataRow objects
            const mappedData: DataRow[] = rawData.map((row) => ({
                id: row[0],
                industry: row[1],
                companyName: row[2],
                year: String(row[3]),
                indicator: row[4],
                isNotDisclosed: row[5] === 0 ? 'Yes' : 'No',
                value: row[6],
                unit: row[7],
                pageNumber: String(row[8]),
                sourceTitle: row[9],
                textSection: row[10],
                inputTokenCount: row[11],
                outputTokenCount: row[12],
                thoughtSummary: row[13]
            }));

            setData(mappedData);
        } catch (err) {
            setDataError(err instanceof Error ? err.message : 'An error occurred');
            console.error('Data fetch error:', err);
        } finally {
            setIsLoadingData(false);
        }
    };

    const handleMouseMove = useCallback((e: MouseEvent) => {
        if (!isResizing.current) return;
        const delta = e.clientX - startX.current;
        const newWidth = Math.max(80, startWidth.current + delta);
        setColumnWidths(prev => ({ ...prev, [isResizing.current as string]: newWidth }));
    }, []);

    const handleMouseUp = useCallback(() => {
        isResizing.current = null;
        document.body.style.cursor = 'default';
        document.body.style.userSelect = 'auto';
        window.removeEventListener('mousemove', handleMouseMove);
        window.removeEventListener('mouseup', handleMouseUp);
    }, [handleMouseMove]);

    const startResize = (key: string, e: React.MouseEvent) => {
        e.stopPropagation();
        isResizing.current = key;
        startX.current = e.clientX;
        startWidth.current = columnWidths[key];
        document.body.style.cursor = 'col-resize';
        document.body.style.userSelect = 'none';
        window.addEventListener('mousemove', handleMouseMove);
        window.addEventListener('mouseup', handleMouseUp);
    };

    useEffect(() => {
        // Fetch Industries
        fetch(`${API_BASE}/industries`)
            .then((res) => res.json())
            .then((data: string[]) => setIndustries(data))
            .catch(() => setErrorIndustries('Failed to load'))
            .finally(() => setLoadingIndustries(false));

        // Fetch Companies
        fetch(`${API_BASE}/companies`)
            .then((res) => res.json())
            .then((data: string[]) => setCompanies(data))
            .catch(() => setErrorCompanies('Failed to load'))
            .finally(() => setLoadingCompanies(false));

        // Fetch Indicator IDs
        fetch(`${API_BASE}/indicator_ids`)
            .then((res) => res.json())
            .then((data: string[]) => setIndicators(data))
            .catch(() => setErrorIndicators('Failed to load'))
            .finally(() => setLoadingIndicators(false));

        return () => {
            window.removeEventListener('mousemove', handleMouseMove);
            window.removeEventListener('mouseup', handleMouseUp);
        };
    }, [handleMouseMove, handleMouseUp]);

    const sortedData = [...data].sort((a, b) => {
        if (!sortConfig.key || !sortConfig.direction) return 0;
        const aVal = a[sortConfig.key];
        const bVal = b[sortConfig.key];
        if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
        if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;
        return 0;
    });

    return (
        <div className={styles.container}>
            {/* Page Header */}
            <div className={styles.pageHeader}>
                <h1 className={styles.pageTitle}>Data Table</h1>
                <p className={styles.pageSubtitle}>
                    Filter and explore extracted environmental KPIs across companies and industries.
                </p>
            </div>

            {/* Filter Dropdowns */}
            <div className={styles.filterSection}>
                <div className={styles.filterBar}>
                    <MultiSelect
                        label="Industry"
                        options={industries}
                        selected={selectedIndustries}
                        onChange={setSelectedIndustries}
                        placeholder="Select industries…"
                        loading={loadingIndustries}
                        error={errorIndustries}
                    />
                    <MultiSelect
                        label="Company"
                        options={companies}
                        selected={selectedCompanies}
                        onChange={setSelectedCompanies}
                        placeholder="Select companies…"
                        loading={loadingCompanies}
                        error={errorCompanies}
                    />
                </div>
                <div className={styles.filterBar}>
                    <MultiSelect
                        label="Indicator ID"
                        options={indicators}
                        selected={selectedIndicators}
                        onChange={setSelectedIndicators}
                        placeholder="Select indicators…"
                        loading={loadingIndicators}
                        error={errorIndicators}
                    />
                    <MultiSelect
                        label="Years"
                        options={YEARS}
                        selected={selectedYears}
                        onChange={setSelectedYears}
                        placeholder="Select years…"
                    />
                </div>

                {/* Additional Options */}
                <div className={styles.additionalOptions}>
                    <label className={styles.checkboxLabel}>
                        <input
                            type="checkbox"
                            checked={selectUndisclosed}
                            onChange={(e) => setSelectUndisclosed(e.target.checked)}
                            className={styles.checkbox}
                        />
                        <span>Also show undisclosed indicators</span>
                    </label>
                </div>
            </div>

            {/* Search Button */}
            <div className={styles.searchAction}>
                <button
                    className={styles.goButton}
                    onClick={handleGoClick}
                    disabled={isLoadingData}
                >
                    {isLoadingData ? 'Loading...' : 'Go!'}
                </button>
            </div>

            {/* Error Message */}
            {dataError && (
                <div className={styles.errorMessage}>
                    <p>{dataError}</p>
                </div>
            )}

            {/* Table */}
            <div className={styles.resultsSection}>
                <div className={styles.tableWrapper}>
                    <table className={styles.dataTable} style={{ tableLayout: 'fixed', width: 'fit-content' }}>
                        <thead>
                            <tr>
                                {COLUMNS.map((col) => (
                                    <th
                                        key={col.key}
                                        style={{ width: columnWidths[col.key] }}
                                        onClick={() => handleSort(col.key)}
                                        className={styles.sortableHeader}
                                    >
                                        <div className={styles.headerContent}>
                                            <span className={styles.headerLabel}>{col.label}</span>
                                            <span className={styles.sortIndicator}>
                                                {sortConfig.key === col.key ? (
                                                    sortConfig.direction === 'asc' ? <ArrowUp size={14} /> : <ArrowDown size={14} />
                                                ) : (
                                                    <ArrowUpDown size={14} className={styles.idleSortIcon} />
                                                )}
                                            </span>
                                        </div>
                                        <div
                                            className={styles.resizeHandle}
                                            onMouseDown={(e) => startResize(col.key, e)}
                                            onClick={(e) => e.stopPropagation()}
                                        />
                                    </th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {isLoadingData && data.length === 0 ? (
                                <tr>
                                    <td colSpan={COLUMNS.length} className={styles.loadingCell}>
                                        Fetching data...
                                    </td>
                                </tr>
                            ) : sortedData.length === 0 ? (
                                <tr>
                                    <td colSpan={COLUMNS.length} className={styles.emptyTableMessage}>
                                        No data found. Select filters and click "Go!"
                                    </td>
                                </tr>
                            ) : (
                                sortedData.map((row) => (
                                    <tr key={row.id}>
                                        <td>{row.industry}</td>
                                        <td>{row.companyName}</td>
                                        <td>{row.year}</td>
                                        <td>{row.indicator}</td>
                                        <td>{row.isNotDisclosed}</td>
                                        <td>{row.value}</td>
                                        <td>{row.unit}</td>
                                        <td>
                                            <button
                                                className={styles.showDetailsButton}
                                                onClick={() => setSelectedRowForDetails(row)}
                                            >
                                                Show Details
                                            </button>
                                        </td>
                                    </tr>
                                ))
                            )}
                        </tbody>
                    </table>
                </div>
            </div>

            {/* Details Popup Overlay */}
            {selectedRowsForDetails && (
                <div className={styles.modalOverlay} onClick={() => setSelectedRowForDetails(null)}>
                    <div className={styles.modalContent} onClick={(e) => e.stopPropagation()}>
                        <div className={styles.modalHeader}>
                            <h2 className={styles.modalTitle}>Row Details</h2>
                            <button className={styles.closeButton} onClick={() => setSelectedRowForDetails(null)}>
                                <X size={24} />
                            </button>
                        </div>
                        <div className={styles.modalBody}>
                            <div className={styles.detailGrid}>
                                <div className={styles.detailItem}>
                                    <label>Indicator</label>
                                    <span>{selectedRowsForDetails.indicator} ({selectedRowsForDetails.year})</span>
                                </div>
                                <div className={styles.detailItem}>
                                    <label>Value & Unit</label>
                                    <span>{selectedRowsForDetails.value} {selectedRowsForDetails.unit}</span>
                                </div>
                                <div className={styles.detailItem}>
                                    <label>Source & Page</label>
                                    <span>{selectedRowsForDetails.sourceTitle} (Page {selectedRowsForDetails.pageNumber})</span>
                                </div>
                                <div className={styles.detailItem}>
                                    <label>Tokens</label>
                                    <span>In: {selectedRowsForDetails.inputTokenCount} / Out: {selectedRowsForDetails.outputTokenCount}</span>
                                </div>
                                <div className={styles.detailItemFull}>
                                    <label>Text Section</label>
                                    <div className={styles.scrollableText}>{selectedRowsForDetails.textSection}</div>
                                </div>
                                <div className={styles.detailItemFull}>
                                    <label>Thought Summary</label>
                                    <div className={styles.thoughtBox}>{selectedRowsForDetails.thoughtSummary}</div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}