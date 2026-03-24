import { useState, useEffect } from 'react';
import ChartWidget from '../components/ChartWidget.tsx';
import styles from './DataVisualization.module.css';

const API_BASE = 'http://127.0.0.1:5000/api';

export default function DataVisualization() {
    const [widgets, setWidgets] = useState<number[]>([]); // Array of widget IDs
    const [companies, setCompanies] = useState<{ value: string, label: string }[]>([]);
    const [indicators, setIndicators] = useState<{ value: string, label: string }[]>([]);
    const [industries, setIndustries] = useState<{ value: string, label: string }[]>([]);

    useEffect(() => {
        fetch(`${API_BASE}/companies`)
            .then(res => res.json())
            .then((data: any[]) => setCompanies(data.map(item => ({ value: String(item[0]), label: String(item[0]), sublabel: String(item[1]) }))));

        fetch(`${API_BASE}/indicator_ids`)
            .then(res => res.json())
            .then((data: any[]) => {
                const numericIndicators = data.filter(item => item[6] === 'Yes');
                setIndicators(numericIndicators.map(item => ({ value: String(item[1]), label: String(item[2]) })));
            });

        fetch(`${API_BASE}/industries`)
            .then(res => res.json())
            .then((data: any[]) => setIndustries(data.map(item => ({ value: String(item), label: String(item) }))));
    }, []);

    const addWidget = () => {
        setWidgets(prev => [...prev, Date.now()]);
    };

    const removeWidget = (id: number) => {
        setWidgets(prev => prev.filter(w => w !== id));
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
            </div>

            <div className={styles.widgetsContainer}>
                {widgets.map(id => (
                    <ChartWidget
                        key={id}
                        companies={companies}
                        indicators={indicators}
                        industries={industries}
                        onRemove={() => removeWidget(id)}
                    />
                ))}
            </div>

            <div className={styles.addButtonWrapper}>
                <button className={styles.addButton} onClick={addWidget} title="Add Chart Widget">
                    <svg xmlns="http://www.w3.org/2000/svg" width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="#d1d5db" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                        <line x1="12" y1="5" x2="12" y2="19" />
                        <line x1="5" y1="12" x2="19" y2="12" />
                    </svg>
                </button>
            </div>
        </div>
    );
}