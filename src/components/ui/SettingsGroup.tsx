import React from "react";

interface SettingsGroupProps {
  title?: string;
  description?: string;
  children: React.ReactNode;
}

export const SettingsGroup: React.FC<SettingsGroupProps> = ({
  title,
  description,
  children,
}) => {
  return (
    <div className="space-y-2">
      {title && (
        <div className="px-4">
          <h2 className="text-xs font-bold text-text/70 uppercase tracking-wider">
            {title}
          </h2>
          {description && (
            <p className="text-xs text-mid-gray mt-1">{description}</p>
          )}
        </div>
      )}
      <div className="bg-surface border-2 border-text rounded-xl shadow-[4px_4px_0_0_var(--color-text)] overflow-visible">
        <div className="divide-y divide-text/15">{children}</div>
      </div>
    </div>
  );
};
