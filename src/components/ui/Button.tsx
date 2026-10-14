import React from "react";

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?:
    | "primary"
    | "primary-soft"
    | "secondary"
    | "warning"
    | "danger"
    | "danger-ghost"
    | "ghost";
  size?: "sm" | "md" | "lg";
}

export const Button: React.FC<ButtonProps> = ({
  children,
  className = "",
  variant = "primary",
  size = "md",
  ...props
}) => {
  const baseClasses =
    "font-semibold rounded-lg border-2 focus:outline-none transition-[transform,box-shadow,background-color] duration-100 disabled:opacity-50 disabled:cursor-not-allowed cursor-pointer";

  const variantClasses = {
    primary:
      "text-ink bg-background-ui border-text shadow-[2px_2px_0_0_var(--color-text)] hover:-translate-x-px hover:-translate-y-px hover:shadow-[3px_3px_0_0_var(--color-text)] active:translate-x-0.5 active:translate-y-0.5 active:shadow-none focus-visible:ring-2 focus-visible:ring-logo-primary",
    "primary-soft":
      "text-text bg-logo-primary/20 border-transparent hover:bg-logo-primary/30 focus:ring-1 focus:ring-logo-primary",
    secondary:
      "bg-surface border-text shadow-[2px_2px_0_0_var(--color-text)] hover:bg-logo-primary hover:text-ink active:translate-x-0.5 active:translate-y-0.5 active:shadow-none focus:outline-none",
    // Secondary's neutral resting look, but hover/focus use the semantic
    // --color-warning token (theme.css) instead of the pink accent — for
    // buttons sitting on warning surfaces like SecureInputWarning
    warning:
      "text-text bg-mid-gray/10 border-mid-gray/20 hover:bg-warning/15 hover:border-warning focus:ring-1 focus:ring-warning",
    danger:
      "text-white bg-red-600 border-mid-gray/20 hover:bg-red-700 hover:border-red-700 focus:ring-1 focus:ring-red-500",
    "danger-ghost":
      "text-red-400 border-transparent hover:text-red-300 hover:bg-red-500/10 focus:bg-red-500/20",
    ghost:
      "text-current border-transparent hover:bg-mid-gray/10 hover:border-logo-primary focus:bg-mid-gray/20",
  };

  const sizeClasses = {
    sm: "px-2 py-1 text-xs",
    md: "px-4 py-[5px] text-sm",
    lg: "px-4 py-2 text-base",
  };

  return (
    <button
      className={`${baseClasses} ${variantClasses[variant]} ${sizeClasses[size]} ${className}`}
      {...props}
    >
      {children}
    </button>
  );
};
