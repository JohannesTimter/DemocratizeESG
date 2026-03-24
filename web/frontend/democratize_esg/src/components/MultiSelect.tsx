import { useState, useRef, useEffect, useCallback } from 'react';
import { createPortal } from 'react-dom';

export interface SelectOption {
    value: string;
    label: string;
    sublabel?: string; // optional right-aligned secondary label (e.g. industry)
}

interface MultiSelectProps {
    label: string;
    options: SelectOption[];
    selected: string[]; // array of 'value's
    onChange: (selected: string[]) => void;
    placeholder?: string;
    loading?: boolean;
    error?: string | null;
}

export default function MultiSelect({
    label,
    options,
    selected,
    onChange,
    placeholder = 'Select...',
    loading = false,
    error = null,
}: MultiSelectProps) {
    const [isOpen, setIsOpen] = useState(false);
    const [search, setSearch] = useState('');
    const [dropdownRect, setDropdownRect] = useState<{ top: number; left: number; width: number } | null>(null);

    const containerRef = useRef<HTMLDivElement>(null);
    const panelRef = useRef<HTMLDivElement>(null);

    // Recalculate position whenever open state changes
    const updatePosition = useCallback(() => {
        if (containerRef.current) {
            const rect = containerRef.current.getBoundingClientRect();
            setDropdownRect({
                top: rect.bottom + window.scrollY + 6,
                left: rect.left + window.scrollX,
                width: rect.width,
            });
        }
    }, []);

    const handleToggle = () => {
        if (!isOpen) updatePosition();
        setIsOpen(prev => !prev);
    };

    // Close on click outside (both trigger and portal panel)
    useEffect(() => {
        function handleClickOutside(e: MouseEvent) {
            const target = e.target as Node;
            const insideContainer = containerRef.current?.contains(target) ?? false;
            const insidePanel = panelRef.current?.contains(target) ?? false;
            if (!insideContainer && !insidePanel) {
                setIsOpen(false);
            }
        }
        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    // Close on scroll/resize so position doesn't become stale,
    // but ignore scroll events that originate inside the panel itself
    useEffect(() => {
        if (!isOpen) return;
        const handleScroll = (e: Event) => {
            if (panelRef.current?.contains(e.target as Node)) return;
            setIsOpen(false);
        };
        const close = () => setIsOpen(false);
        window.addEventListener('scroll', handleScroll, true);
        window.addEventListener('resize', close);
        return () => {
            window.removeEventListener('scroll', handleScroll, true);
            window.removeEventListener('resize', close);
        };
    }, [isOpen]);

    const filtered = options.filter((opt) =>
        opt.label.toLowerCase().includes(search.toLowerCase())
    );

    const toggleOption = (val: string) => {
        if (selected.includes(val)) {
            onChange(selected.filter((s) => s !== val));
        } else {
            onChange([...selected, val]);
        }
    };

    const selectAll = () => onChange(options.map((o) => o.value));
    const clearAll = () => onChange([]);

    const dropdownPanel = isOpen && dropdownRect ? createPortal(
        <div
            ref={panelRef}
            style={{
                position: 'absolute',
                top: dropdownRect.top,
                left: dropdownRect.left,
                minWidth: Math.max(dropdownRect.width, 350),
                maxWidth: 700,
                zIndex: 9999,
            }}
            className="bg-white border border-[var(--border-color)] rounded-lg shadow-xl
                        max-h-[300px] flex flex-col overflow-hidden"
        >
            {/* Search */}
            <div className="p-2 border-b border-[var(--border-color)]">
                <input
                    type="text"
                    value={search}
                    onChange={(e) => setSearch(e.target.value)}
                    placeholder="Search…"
                    className="w-full px-3 py-2 text-[0.95rem] rounded-md border border-[var(--border-color)] bg-[var(--background-color)]
                               focus:outline-none focus:ring-2 focus:ring-[var(--primary-color)]/30 focus:border-[var(--primary-color)]
                               placeholder:text-[var(--text-muted)]"
                />
            </div>

            {/* Select All / Clear All */}
            <div className="flex items-center justify-between px-3 py-2.5 border-b border-[var(--border-color)] bg-[var(--background-color)]">
                <button
                    type="button"
                    onClick={selectAll}
                    className="text-sm font-bold text-[var(--primary-color)] hover:text-[var(--primary-dark)] transition-colors cursor-pointer"
                >
                    Select All
                </button>
                <button
                    type="button"
                    onClick={clearAll}
                    className="text-sm font-bold text-[var(--text-muted)] hover:text-red-500 transition-colors cursor-pointer"
                >
                    Clear All
                </button>
            </div>

            {/* Options */}
            <ul className="overflow-y-auto flex-1 py-1">
                {filtered.length === 0 ? (
                    <li className="px-3 py-2.5 text-[0.95rem] text-[var(--text-muted)] italic text-center">
                        No results found
                    </li>
                ) : (
                    filtered.map((opt) => (
                        <li key={opt.value}>
                            <label
                                className="flex items-center gap-3 px-3 py-2.5 text-[0.95rem] cursor-pointer
                                           hover:bg-[var(--primary-color)]/5 transition-colors"
                            >
                                <input
                                    type="checkbox"
                                    checked={selected.includes(opt.value)}
                                    onChange={() => toggleOption(opt.value)}
                                    className="w-[18px] h-[18px] rounded border-[var(--border-color)] text-[var(--primary-color)]
                                               focus:ring-[var(--primary-color)]/30 accent-[var(--primary-color)] cursor-pointer"
                                />
                                <span className="flex-1 flex items-center justify-between min-w-0 gap-2">
                                    <span className="text-[var(--text-dominant)] truncate">{opt.label}</span>
                                    {opt.sublabel && (
                                        <span className="text-[0.8rem] text-[var(--text-muted)] shrink-0 italic">{opt.sublabel}</span>
                                    )}
                                </span>
                            </label>
                        </li>
                    ))
                )}
            </ul>
        </div>,
        document.body
    ) : null;

    return (
        <div ref={containerRef} className="relative w-full min-w-[220px] flex-1">
            {/* Label */}
            <label className="block text-base font-semibold text-[var(--secondary-color)] mb-1.5 tracking-wide">
                {label}
            </label>

            {/* Trigger */}
            <button
                type="button"
                onClick={handleToggle}
                className="w-full min-h-[50px] px-3 py-2.5 rounded-lg border border-[var(--border-color)] bg-white
                           text-left text-[0.95rem] cursor-pointer flex items-center gap-2
                           shadow-sm transition-all duration-200
                           hover:border-[var(--primary-light)] focus:outline-none focus:ring-2 focus:ring-[var(--primary-color)]/30 focus:border-[var(--primary-color)]"
            >
                {/* Content area — grows and wraps */}
                <div className="flex-1 flex flex-wrap items-center gap-2 min-w-0">
                    {loading ? (
                        <span className="text-[var(--text-muted)] text-[0.95rem] italic">Loading…</span>
                    ) : error ? (
                        <span className="text-red-500 text-[0.95rem] italic">{error}</span>
                    ) : selected.length === 0 ? (
                        <span className="text-[var(--text-muted)] pl-1">{placeholder}</span>
                    ) : (
                        <>
                            {selected.slice(0, 3).map((itemVal) => {
                                const opt = options.find((o) => o.value === itemVal);
                                return (
                                    <span
                                        key={itemVal}
                                        className="inline-flex items-center gap-1.5 !p-1 !m-1 rounded-full text-sm font-semibold
                                               bg-[var(--primary-color)]/10 text-[var(--primary-color)] border border-[var(--primary-color)]/20"
                                    >
                                        {opt ? opt.label : itemVal}
                                        <span
                                            role="button"
                                            onClick={(e) => {
                                                e.stopPropagation();
                                                toggleOption(itemVal);
                                            }}
                                            className="ml-0.5 text-base leading-none hover:text-red-500 cursor-pointer font-bold transition-colors"
                                        >
                                            ×
                                        </span>
                                    </span>
                                )
                            })}
                            {selected.length > 3 && (
                                <span className="text-sm text-[var(--text-muted)] font-medium">
                                    +{selected.length - 3} more
                                </span>
                            )}
                        </>
                    )}
                </div>

                {/* Chevron — always pinned to the right */}
                <svg
                    className={`shrink-0 w-5 h-5 text-[var(--text-muted)] transition-transform duration-200 ${isOpen ? 'rotate-180' : ''}`}
                    xmlns="http://www.w3.org/2000/svg"
                    fill="none"
                    viewBox="0 0 24 24"
                    strokeWidth="2"
                    stroke="currentColor"
                >
                    <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 8.25l-7.5 7.5-7.5-7.5" />
                </svg>
            </button>

            {/* Portal dropdown — rendered directly on document.body to escape any overflow clipping */}
            {dropdownPanel}
        </div>
    );
}
