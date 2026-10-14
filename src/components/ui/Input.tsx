import React from "react";

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  variant?: "default" | "compact";
}

export const Input: React.FC<InputProps> = ({
  className = "",
  variant = "default",
  disabled,
  ...props
}) => {
  const baseClasses =
    "px-2 py-1 text-sm font-semibold bg-surface border-2 border-text rounded-lg text-start transition-shadow duration-150";

  const interactiveClasses = disabled
    ? "opacity-60 cursor-not-allowed border-text/40"
    : "focus:outline-none focus:shadow-[3px_3px_0_0_var(--color-logo-primary)]";

  const variantClasses = {
    default: "px-3 py-2",
    compact: "px-2 py-1",
  } as const;

  return (
    <input
      className={`${baseClasses} ${variantClasses[variant]} ${interactiveClasses} ${className}`}
      disabled={disabled}
      {...props}
    />
  );
};
