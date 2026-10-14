import React from "react";

// The Sadomi wordmark ("sado" = voice, "mi" = the Uzbek question particle).
// Set as live text until a drawn logo exists; textLength pins its width so the
// layout does not depend on which system font the machine has.
// A brand name, never translated.
const WORDMARK = "sadomi";

const HandyTextLogo = ({
  width,
  height,
  className,
}: {
  width?: number;
  height?: number;
  className?: string;
}) => {
  return (
    <svg
      width={width}
      height={height}
      className={className}
      viewBox="0 0 803 328"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      role="img"
      aria-label="Sadomi"
    >
      <text
        x="20"
        y="236"
        className="fill-text"
        fontFamily="system-ui, 'Segoe UI', sans-serif"
        fontSize="220"
        fontWeight="800"
        letterSpacing="-6"
        textLength="763"
        lengthAdjust="spacingAndGlyphs"
      >
        {WORDMARK}
      </text>
    </svg>
  );
};

export default HandyTextLogo;
