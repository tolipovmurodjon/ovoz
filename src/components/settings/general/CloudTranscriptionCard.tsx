import React, { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { commands } from "@/bindings";
import { useSettings } from "../../../hooks/useSettings";
import { SettingsGroup } from "../../ui/SettingsGroup";
import { SettingContainer } from "../../ui/SettingContainer";
import { ToggleSwitch } from "../../ui/ToggleSwitch";
import { Input } from "../../ui/Input";

const normalizeLanguageCode = (text: string) => text.trim().toLowerCase();

const parseLanguageList = (text: string) =>
  text
    .split(",")
    .map(normalizeLanguageCode)
    .filter((code) => code.length > 0);

export const CloudTranscriptionCard: React.FC = () => {
  const { t } = useTranslation();
  const { getSetting, updateSetting, isUpdating } = useSettings();
  // null until the backend answers; the status line stays hidden meanwhile.
  const [keyPresent, setKeyPresent] = useState<boolean | null>(null);

  const enabled = getSetting("cloud_stt_enabled") ?? true;
  const language = getSetting("cloud_stt_language") ?? "uz";
  const secondaryLanguagesText = (
    getSetting("cloud_stt_secondary_languages") ?? ["en"]
  ).join(", ");

  // The inputs keep a draft and save on blur, so typing "en," does not get
  // rewritten under the cursor by the parsed list.
  const [languageDraft, setLanguageDraft] = useState(language);
  const [secondaryDraft, setSecondaryDraft] = useState(secondaryLanguagesText);

  useEffect(() => setLanguageDraft(language), [language]);
  useEffect(
    () => setSecondaryDraft(secondaryLanguagesText),
    [secondaryLanguagesText],
  );

  // The key comes from an environment variable, so it cannot change while the
  // app runs: one check on mount is enough.
  useEffect(() => {
    commands
      .isCloudSttKeyPresent()
      .then(setKeyPresent)
      .catch((error) =>
        console.error("Failed to check the ElevenLabs key:", error),
      );
  }, []);

  const saveLanguage = () => {
    const normalized = normalizeLanguageCode(languageDraft);
    setLanguageDraft(normalized);
    if (normalized !== language) {
      updateSetting("cloud_stt_language", normalized);
    }
  };

  const saveSecondaryLanguages = () => {
    const parsed = parseLanguageList(secondaryDraft);
    const parsedText = parsed.join(", ");
    setSecondaryDraft(parsedText);
    if (parsedText !== secondaryLanguagesText) {
      updateSetting("cloud_stt_secondary_languages", parsed);
    }
  };

  const blurOnEnter = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      e.currentTarget.blur();
    }
  };

  return (
    <SettingsGroup title={t("settings.cloudStt.title")}>
      <ToggleSwitch
        checked={enabled}
        onChange={(value) => updateSetting("cloud_stt_enabled", value)}
        isUpdating={isUpdating("cloud_stt_enabled")}
        label={t("settings.cloudStt.enabled.label")}
        description={t("settings.cloudStt.enabled.description")}
        descriptionMode="tooltip"
        grouped={true}
      />
      {keyPresent !== null && (
        <div className="px-4 p-2 text-sm text-mid-gray">
          {keyPresent
            ? t("settings.cloudStt.keyStatus.found")
            : t("settings.cloudStt.keyStatus.missing")}
        </div>
      )}
      <SettingContainer
        title={t("settings.cloudStt.language.title")}
        description={t("settings.cloudStt.language.description")}
        descriptionMode="tooltip"
        grouped={true}
        disabled={!enabled}
      >
        <Input
          type="text"
          className="max-w-40"
          value={languageDraft}
          onChange={(e) => setLanguageDraft(e.target.value)}
          onBlur={saveLanguage}
          onKeyDown={blurOnEnter}
          placeholder={t("settings.cloudStt.language.placeholder")}
          variant="compact"
          disabled={!enabled || isUpdating("cloud_stt_language")}
        />
      </SettingContainer>
      <SettingContainer
        title={t("settings.cloudStt.secondaryLanguages.title")}
        description={t("settings.cloudStt.secondaryLanguages.description")}
        descriptionMode="tooltip"
        grouped={true}
        disabled={!enabled}
      >
        <Input
          type="text"
          className="max-w-40"
          value={secondaryDraft}
          onChange={(e) => setSecondaryDraft(e.target.value)}
          onBlur={saveSecondaryLanguages}
          onKeyDown={blurOnEnter}
          placeholder={t("settings.cloudStt.secondaryLanguages.placeholder")}
          variant="compact"
          disabled={!enabled || isUpdating("cloud_stt_secondary_languages")}
        />
      </SettingContainer>
      <div
        className={`px-4 p-2 text-xs text-mid-gray ${enabled ? "" : "opacity-50"}`}
      >
        {t("settings.cloudStt.fallbackNote")}
      </div>
    </SettingsGroup>
  );
};
