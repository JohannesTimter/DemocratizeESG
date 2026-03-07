export default function PulsingWaveform() {
    return (
        <svg viewBox="0 0 200 100" className="w-full h-full text-emerald-400">
            <style>
                {`
                    @keyframes dash {
                        to {
                            stroke-dashoffset: 0;
                        }
                    }
                    .waveform {
                        stroke-dasharray: 400;
                        stroke-dashoffset: 400;
                        animation: dash 3s linear infinite;
                    }
                    .waveform-glow {
                        stroke-dasharray: 400;
                        stroke-dashoffset: 400;
                        animation: dash 3s linear infinite;
                        filter: drop-shadow(0 0 8px currentColor);
                        opacity: 0.6;
                    }
                `}
            </style>
            
            {/* Background grid line */}
            <line x1="0" y1="50" x2="200" y2="50" stroke="currentColor" strokeWidth="0.5" opacity="0.2" />
            
            {/* EKG Path */}
            <path 
                className="waveform-glow"
                d="M 0 50 L 40 50 L 50 30 L 60 70 L 70 20 L 80 80 L 90 20 L 100 70 L 110 30 L 120 50 L 200 50" 
                fill="none" 
                stroke="currentColor" 
                strokeWidth="4" 
                strokeLinecap="round"
                strokeLinejoin="round"
            />
            {/* Exact same path for the crisp core line */}
             <path 
                className="waveform"
                d="M 0 50 L 40 50 L 50 30 L 60 70 L 70 20 L 80 80 L 90 20 L 100 70 L 110 30 L 120 50 L 200 50" 
                fill="none" 
                stroke="#ffffff" 
                strokeWidth="2" 
                strokeLinecap="round"
                strokeLinejoin="round"
            />
        </svg>
    );
}
