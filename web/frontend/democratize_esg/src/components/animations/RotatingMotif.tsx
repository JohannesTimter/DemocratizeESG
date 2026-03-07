export default function RotatingMotif() {
    return (
        <svg viewBox="0 0 200 200" className="w-full h-full opacity-60">
            <style>
                {`
                    @keyframes rotate {
                        from { transform: rotate(0deg); }
                        to { transform: rotate(360deg); }
                    }
                    @keyframes rotate-reverse {
                        from { transform: rotate(360deg); }
                        to { transform: rotate(0deg); }
                    }
                    .motif-outer {
                        animation: rotate 20s linear infinite;
                        transform-origin: center;
                    }
                    .motif-inner {
                        animation: rotate-reverse 15s linear infinite;
                        transform-origin: center;
                    }
                    .motif-core {
                        animation: rotate 10s linear infinite;
                        transform-origin: center;
                    }
                `}
            </style>
            <g>
                <circle cx="100" cy="100" r="80" fill="none" stroke="currentColor" strokeWidth="2" strokeDasharray="10 15" className="motif-outer" />
                <circle cx="100" cy="100" r="55" fill="none" stroke="currentColor" strokeWidth="3" strokeDasharray="20 10 5 10" className="motif-inner" />
                <circle cx="100" cy="100" r="30" fill="none" stroke="currentColor" strokeWidth="4" strokeDasharray="15 15" className="motif-core" />
                <circle cx="100" cy="100" r="10" fill="currentColor" />
            </g>
        </svg>
    );
}
