import { commands } from "@/bindings";

// The languages offered as one-tap choices. "" is auto-detect, which is poor
// for Uzbek, so it comes last.
export const MAIN_LANGUAGE_CHOICES = ["en", "uz", "ru", ""] as const;

/**
 * The other languages after switching the main one: the old main language
 * joins them (you still mix it in), the new one leaves them, order is kept.
 */
export const secondaryAfterSwitch = (
  oldMain: string,
  newMain: string,
  secondary: string[],
): string[] =>
  [...new Set([...secondary, oldMain])].filter(
    (code) => code !== "" && code !== newMain,
  );

/** Saves a new main language and the matching list of other languages. */
export const switchMainLanguage = async (
  oldMain: string,
  newMain: string,
  secondary: string[],
): Promise<void> => {
  await commands.changeCloudSttLanguageSetting(newMain);
  await commands.changeCloudSttSecondaryLanguagesSetting(
    secondaryAfterSwitch(oldMain, newMain, secondary),
  );
};
