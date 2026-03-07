import { useEffect, useRef } from 'react';
import { Link } from 'react-router-dom';
import gsap from 'gsap';
import ScrollTrigger from 'gsap/ScrollTrigger';
import styles from './LandingPage.module.css';

// SVG Animations
import RotatingMotif from '../components/animations/RotatingMotif';
import LaserScanner from '../components/animations/LaserScanner';
import PulsingWaveform from '../components/animations/PulsingWaveform';

// Images
import ca100Image from '../assets/ca100+.svg';
import eKPIsImage from '../assets/eKPIs.png';
import pipelineImage from '../assets/DivideAndConquer_Architecture.png';
import readingLeafAnimation from '../assets/readingLeafAnimation.svg';

gsap.registerPlugin(ScrollTrigger);

export default function LandingPage() {
    const archiveRef = useRef<HTMLElement>(null);
    const cardsRef = useRef<(HTMLDivElement | null)[]>([]);

    useEffect(() => {
        if (!archiveRef.current) return;

        const cards = cardsRef.current.filter(Boolean) as HTMLDivElement[];
        if (cards.length === 0) return;

        const ctx = gsap.context(() => {
            const tl = gsap.timeline({
                scrollTrigger: {
                    trigger: archiveRef.current,
                    start: "center center",
                    end: `+=${120 * cards.length}%`,
                    pin: true,
                    scrub: 1,
                }
            });

            // Initialize all cards after the first one to be below the screen
            cards.forEach((card, index) => {
                if (index > 0) {
                    gsap.set(card, { yPercent: 150 });
                }
            });

            // Build the staggered animation
            cards.forEach((card, i) => {
                if (i === 0) return;

                tl.to(cards[i - 1], {
                    scale: 0.9,
                    y: -30,
                    opacity: 0.3,
                    filter: "blur(20px)",
                    duration: 1,
                    ease: "none"
                }, `card_${i}`);

                tl.to(card, {
                    yPercent: 0,
                    boxShadow: "0 -20px 40px rgba(0,0,0,0.15)",
                    duration: 1,
                    ease: "none"
                }, `card_${i}`);
            });
        }, archiveRef);

        return () => ctx.revert();
    }, []);

    return (
        <div className={styles.container}>
            {/* HERO SECTION */}
            <section className={styles.hero}>
                <div className={styles.heroContent}>
                    <h1 className={styles.title}>
                        Understanding the True <br />
                        <span className={styles.titleHighlight}>Environmental Impact</span>
                    </h1>
                    <p className={styles.subtitle}>
                        DemocratizeESG extracts and analyzes environmental key performance indicators
                        from annual ESG reports using an advanced LLM-based information extraction pipeline.
                    </p>
                    <div className={styles.heroActions}>
                        <Link to="/data-table" className={styles.primaryButton}>
                            Explore the Data
                        </Link>
                        <Link to="/report-pipeline" className={styles.secondaryButton}>
                            Extract New Reports
                        </Link>
                    </div>
                </div>
                <div className={styles.heroVisual}>
                    <img src={readingLeafAnimation} alt="Reading Leaf Animation" className={styles.heroImage} />
                </div>
            </section>

            {/* STATS SECTION */}
            <section className={styles.statsContainer}>
                <div className={styles.statItem}>
                    <span className={styles.statValue}>170+</span>
                    <span className={styles.statLabel}>Companies Analyzed</span>
                </div>
                <div className={styles.statItem}>
                    <span className={styles.statValue}>11</span>
                    <span className={styles.statLabel}>Industries Covered</span>
                </div>
                <div className={styles.statItem}>
                    <span className={styles.statValue}>2020-24</span>
                    <span className={styles.statLabel}>Reporting Years</span>
                </div>
                <div className={styles.statItem}>
                    <span className={styles.statValue}>92%</span>
                    <span className={styles.statLabel}>LLM Extraction F1-Score</span>
                </div>
            </section>

            {/* STICKY ARCHIVE SECTION */}
            <section className={styles.archiveSection} ref={archiveRef}>
                <div className={styles.cardContainer}>
                    {/* Card 1 */}
                    <div
                        className={styles.archiveCard}
                        ref={el => { cardsRef.current[0] = el; }}
                        style={{ zIndex: 10 }}
                    >
                        <div className={styles.archiveCardContent}>
                            <span className={styles.stepNumber}>01</span>
                            <h3 className={styles.archiveCardTitle}>The Corpus</h3>
                            <p className={styles.archiveCardText}>
                                We have manually collected annual reports and ESG reports from 170 of the world's top CO2 emitting companies, according to ClimateAction100+. The document corpus covers the years 2020 - 2024 and contains over 1350 documents and is publicly available on Google Drive.
                            </p>
                            <a href="https://drive.google.com/drive/folders/1ysF7PHBu29_0LGV-c22iwBoPx8X_NS9N?usp=sharing" target="_blank" rel="noopener noreferrer" className={styles.archiveCardLink}>
                                Access Corpus on Google Drive &rarr;
                            </a>
                        </div>
                        <div className={styles.archiveCardVisual}>
                            <div className="absolute inset-0 w-full h-full flex items-center justify-center opacity-15 mix-blend-overlay">
                                <RotatingMotif />
                            </div>
                            <img src={ca100Image} alt="Climate Action 100+" className={styles.fgImg} />
                        </div>
                    </div>

                    {/* Card 2 */}
                    <div
                        className={styles.archiveCard}
                        ref={el => { cardsRef.current[1] = el; }}
                        style={{ zIndex: 20 }}
                    >
                        <div className={styles.archiveCardContent}>
                            <span className={styles.stepNumber}>02</span>
                            <h3 className={styles.archiveCardTitle}>Environmental KPIs</h3>
                            <p className={styles.archiveCardText}>
                                We have created a custom set of 50 industry-agnostic environmental metrics, covering topics such as emissions, waste generation and energy usage. The metric set is publicly available on Google Sheets.
                            </p>
                            <a href="https://docs.google.com/spreadsheets/d/1Ezxv7jIAin8RrFseVCTXWd1V5L2W7QsHtAlhYRuzpvM/edit?usp=sharing" target="_blank" rel="noopener noreferrer" className={styles.archiveCardLink}>
                                Access Metric Set on Google Sheets &rarr;
                            </a>
                        </div>
                        <div className={styles.archiveCardVisual}>
                            <div className="absolute inset-0 w-full h-full flex items-center justify-center opacity-15 mix-blend-overlay">
                                <LaserScanner />
                            </div>
                            <img src={eKPIsImage} alt="Environmental KPIs" className={styles.fgImg} />
                        </div>
                    </div>

                    {/* Card 3 */}
                    <div
                        className={styles.archiveCard}
                        ref={el => { cardsRef.current[2] = el; }}
                        style={{ zIndex: 30 }}
                    >
                        <div className={styles.archiveCardContent}>
                            <span className={styles.stepNumber}>03</span>
                            <h3 className={styles.archiveCardTitle}>Extraction Pipeline</h3>
                            <p className={styles.archiveCardText}>
                                We built an LLM-based information extraction pipeline that extracts the defined metrics from PDF reports. Using Gemini 2.5 Pro (and Flash), it achieves an F1-score of 92%.
                            </p>
                        </div>
                        <div className={styles.archiveCardVisual}>
                            <div className="absolute inset-0 w-full h-full flex items-center justify-center opacity-15 mix-blend-overlay">
                                <PulsingWaveform />
                            </div>
                            <img src={pipelineImage} alt="Information Extraction Pipeline Architecture" className={styles.fgImg} />
                        </div>
                    </div>
                </div>
            </section>

            {/* FEATURES SECTION */}
            <section className={styles.featuresSection}>
                <div className={styles.featuresHeader}>
                    <h2 className={styles.featuresTitle}>Platform Capabilities</h2>
                    <p className={styles.featuresSubtitle}>What you can do with DemocratizeESG</p>
                </div>

                <div className={styles.featuresGrid}>
                    <div className={styles.featureCard}>
                        <div className={styles.featureIcon}>
                            {/* Table Icon */}
                            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
                                <path strokeLinecap="round" strokeLinejoin="round" d="M3.375 19.5h17.25m-17.25 0a1.125 1.125 0 01-1.125-1.125M3.375 19.5h7.5c.621 0 1.125-.504 1.125-1.125m-9.75 0V5.625m0 12.75v-1.5c0-.621.504-1.125 1.125-1.125m18.375 2.625V5.625m0 12.75c0 .621-.504 1.125-1.125 1.125m1.125-1.125v-1.5c0-.621-.504-1.125-1.125-1.125m0 3.75h-7.5A1.125 1.125 0 0112 18.375m9.75-12.75c0-.621-.504-1.125-1.125-1.125H3.375c-.621 0-1.125.504-1.125 1.125m19.5 0v1.5c0 .621-.504 1.125-1.125 1.125M2.25 5.625v1.5c0 .621.504 1.125 1.125 1.125m0 0h17.25m-17.25 0h7.5c.621 0 1.125.504 1.125 1.125M3.375 8.25c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125m17.25-3.75h-7.5c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125m-17.25 0h7.5c.621 0 1.125.504 1.125 1.125v1.5c0 .621-.504 1.125-1.125 1.125M12 10.875v-1.5m0 1.5c0 .621-.504 1.125-1.125 1.125M12 10.875c0 .621.504 1.125 1.125 1.125m-2.25 0c-.621 0-1.125.504-1.125 1.125M13.125 12h7.5m-7.5 0c-.621 0-1.125.504-1.125 1.125M20.625 12c.621 0 1.125.504 1.125 1.125v1.5c0 .621-.504 1.125-1.125 1.125m-17.25 0h7.5M12 14.625v-1.5m0 1.5c0 .621-.504 1.125-1.125 1.125M12 14.625c0 .621.504 1.125 1.125 1.125m-2.25 0c-.621 0-1.125.504-1.125 1.125m0 1.5v-1.5m0 0c0-.621.504-1.125 1.125-1.125m0 0h7.5" />
                            </svg>
                        </div>
                        <h3 className={styles.featureTitle}>Filterable Master Table</h3>
                        <p className={styles.featureDescription}>
                            Access the complete dataset of extracted key performance indicators.
                            Filter by company, industry, or specific environmental metrics across multiple years.
                        </p>
                    </div>

                    <div className={styles.featureCard}>
                        <div className={styles.featureIcon}>
                            {/* Chart Icon */}
                            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
                                <path strokeLinecap="round" strokeLinejoin="round" d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 013 19.875v-6.75zM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V8.625zM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V4.125z" />
                            </svg>
                        </div>
                        <h3 className={styles.featureTitle}>Data Visualization</h3>
                        <p className={styles.featureDescription}>
                            Analyze trends visually. Compare companies against each other or against
                            industry averages to understand true environmental performance.
                        </p>
                    </div>

                    <div className={styles.featureCard}>
                        <div className={styles.featureIcon}>
                            {/* Document/Pipeline Icon */}
                            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
                                <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m3.75 9v6m3-3H9m1.5-12H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
                            </svg>
                        </div>
                        <h3 className={styles.featureTitle}>Extraction Pipeline</h3>
                        <p className={styles.featureDescription}>
                            Submit new annual ESG reports for analysis. Our LLM-based pipeline will
                            extract relevant KPIs and integrate them directly into the database.
                        </p>
                    </div>
                </div>
            </section>

            {/* CTA SECTION */}
            <section className={styles.ctaSection}>
                <h2 className={styles.ctaTitle}>Ready to explore the data?</h2>
                <p className={styles.ctaText}>
                    Dive into our database of environmental KPIs or contribute by submitting
                    a new report for extraction.
                </p>
                <Link to="/data-table" className={styles.ctaButton}>
                    Open Data Table
                </Link>
            </section>
        </div>
    );
}