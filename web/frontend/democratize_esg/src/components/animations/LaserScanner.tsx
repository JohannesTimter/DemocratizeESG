export default function LaserScanner() {
    // Generate a grid of dots
    const dots = [];
    for (let x = 10; x <= 190; x += 15) {
        for (let y = 10; y <= 190; y += 15) {
            dots.push(<circle key={`${x}-${y}`} cx={x} cy={y} r="1.5" fill="currentColor" opacity="0.3" />);
        }
    }

    return (
        <svg viewBox="0 0 200 200" className="w-full h-full text-blue-500">
            <style>
                {`
                    @keyframes scan {
                        0% { transform: translateY(-10px); }
                        50% { transform: translateY(210px); }
                        100% { transform: translateY(-10px); }
                    }
                    .laser-line {
                        animation: scan 4s ease-in-out infinite;
                    }
                    .grid-dots {
                        color: rgba(255, 255, 255, 0.4);
                    }
                `}
            </style>
            <g className="grid-dots">
                {dots}
            </g>
            <g className="laser-line">
                <line x1="0" y1="0" x2="200" y2="0" stroke="currentColor" strokeWidth="2" filter="drop-shadow(0 0 4px currentColor)" />
                <rect x="0" y="-20" width="200" height="20" fill="url(#laser-gradient)" opacity="0.3" />
            </g>
            <defs>
                <linearGradient id="laser-gradient" x1="0" y1="1" x2="0" y2="0">
                    <stop offset="0%" stopColor="currentColor" stopOpacity="1" />
                    <stop offset="100%" stopColor="currentColor" stopOpacity="0" />
                </linearGradient>
            </defs>
        </svg>
    );
}
