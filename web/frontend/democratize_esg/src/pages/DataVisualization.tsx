import { useState, useEffect } from 'react';
import ChartWidget from '../components/ChartWidget.tsx';
import styles from './DataVisualization.module.css';

const API_BASE = 'http://127.0.0.1:5000/api';

export default function DataVisualization() {
    const [widgets, setWidgets] = useState<number[]>([1]); // Array of widget IDs
    const [companies, setCompanies] = useState<{ value: string, label: string }[]>([]);
    const [indicators, setIndicators] = useState<{ value: string, label: string }[]>([]);

    useEffect(() => {
        fetch(`${API_BASE}/companies`)
            .then(res => res.json())
            .then((data: any[]) => setCompanies(data.map(item => ({ value: String(item), label: String(item) }))));

        fetch(`${API_BASE}/indicator_ids`)
            .then(res => res.json())
            .then((data: any[]) => {
                // Return mapping of indicator id to name, filtering for numeric only (item[6] === 'Yes')
                const numericIndicators = data.filter(item => item[6] === 'Yes');
                setIndicators(numericIndicators.map(item => ({ value: String(item[1]), label: String(item[2]) })));
            });
    }, []);

    const addWidget = () => {
        setWidgets([...widgets, Date.now()]);
    };

    return (
        <div className={styles.container}>
            <div className={styles.pageHeader}>
                <div className={styles.titleSection}>
                    <h1 className={styles.pageTitle}>Data Visualization</h1>
                    <p className={styles.pageSubtitle}>
                        Visually analyze ESG data trends and compare companies over time.
                    </p>
                </div>
                <button className={styles.addButton} onClick={addWidget}>
                    + Add Chart Widget
                </button>
            </div>

            <div className={styles.widgetsContainer}>
                {widgets.map(id => (
                    <ChartWidget key={id} companies={companies} indicators={indicators} />
                ))}
            </div>
        </div>
    );
}