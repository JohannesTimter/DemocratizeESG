import { Link, Outlet, useLocation } from 'react-router-dom';
import styles from './RootLayout.module.css';

export default function RootLayout() {
    const location = useLocation();

    return (
        <div className={styles.layout}>
            {/* Fixed Solid Navbar */}
            <header className={`${styles.header} ${styles.headerSolid}`}>
                {/* Logo */}
                <div className={styles.logo}>
                    <Link to="/" className={`${styles.logoLink} ${styles.logoSolid}`}>
                        DemocratizeESG
                    </Link>
                </div>
                
                {/* Nav Links */}
                <nav className={styles.navContainer}>
                    <ul className={styles.navList}>
                        <li>
                            <Link to="/" className={`${styles.navLink} ${location.pathname === '/' ? styles.navLinkActive : styles.linkSolid}`}>
                                Home
                            </Link>
                        </li>
                        <li>
                            <Link to="/data-table" className={`${styles.navLink} ${location.pathname === '/data-table' ? styles.navLinkActive : styles.linkSolid}`}>
                                Data Table
                            </Link>
                        </li>
                        <li>
                            <Link to="/data-visualization" className={`${styles.navLink} ${location.pathname === '/data-visualization' ? styles.navLinkActive : styles.linkSolid}`}>
                                Visualization
                            </Link>
                        </li>
                        <li>
                            <Link to="/report-pipeline" className={`${styles.navLink} ${location.pathname === '/report-pipeline' ? styles.navLinkActive : styles.linkSolid}`}>
                                Report Pipeline
                            </Link>
                        </li>
                    </ul>
                </nav>

                {/* CTA Button */}
                <div>
                    <Link to="/report-pipeline" className={styles.ctaButton}>
                        Get Started
                    </Link>
                </div>
            </header>

            {/* This is where your page content will be injected */}
            <main className={styles.mainContent}>
                <Outlet />
            </main>
        </div>
    );
}