// Generates the test clips with ElevenLabs text-to-speech.
// Usage: node generate.mjs --list   (show candidate models and voices, spend nothing)
//        node generate.mjs [model_id voice_id_1 voice_id_2]   (generate; stops at the first API error)
// The key is read from ELEVENLABS_API_KEY and is never printed.
import { writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const key = process.env.ELEVENLABS_API_KEY;
if (!key) {
  console.error("ELEVENLABS_API_KEY is not set");
  process.exit(1);
}
const api = "https://api.elevenlabs.io";

const sentences = {
  uz: [
    "Bugun ertalab soat oltida uyg'ondim va bomdod namozini o'qidim.",
    "Humopedia bolalarga matematikani o'yin orqali o'rgatadi.",
    "Ertaga universitetda uchta darsim bor, keyin kutubxonaga boraman.",
    "Iltimos, bu faylni saqlab qo'y va menga xabar yubor.",
    "Samarqanddagi Registon maydoni juda chiroyli ekan.",
    "Men hozir Toshkentda yashayman, lekin oilam Farg'onada.",
    "Bu hafta ichida loyihani tugatishimiz kerak.",
  ],
  en: [
    "Open the settings file and change the port to seven seven nine zero.",
    "I finished seventeen lectures today and built every one of them myself.",
    "Please commit this on a feature branch, push it, and open a pull request.",
    "The player controller should use the new input system, not the old one.",
    "Remind me to buy headphones tomorrow after the second class.",
    "This function returns null when the list is empty, so add a check.",
  ],
  mixed: [
    "Bro, bugun Unity'da Cinemachine camerani setup qildim, juda zo'r ishladi.",
    "Dashboardni deploy qilib, keyin pull request och.",
    "ElevenLabs API key ni credential store ga saqla.",
    "Blender'da low poly modelni export qilib Unity'ga import qilaman.",
    "Humopedia web versiyasini beta ga push qildim, endi test qilish kerak.",
    "PAGEL serverda restart bo'ldi, logni tekshirib ko'r.",
    "Shu bug ni fix qil, keyin test yoz va commit qil.",
  ],
};

async function getJson(path) {
  const res = await fetch(api + path, { headers: { "xi-api-key": key } });
  if (!res.ok) throw new Error(`GET ${path} -> ${res.status} ${await res.text()}`);
  return res.json();
}

function wavFromPcm16(pcm, sampleRate) {
  const header = Buffer.alloc(44);
  header.write("RIFF", 0);
  header.writeUInt32LE(36 + pcm.length, 4);
  header.write("WAVEfmt ", 8);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20); // PCM
  header.writeUInt16LE(1, 22); // mono
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(sampleRate * 2, 28);
  header.writeUInt16LE(2, 32);
  header.writeUInt16LE(16, 34);
  header.write("data", 36);
  header.writeUInt32LE(pcm.length, 40);
  return Buffer.concat([header, pcm]);
}

if (process.argv[2] === "--list") {
  const models = await getJson("/v1/models");
  console.log("TTS models that list Uzbek:");
  for (const m of models.filter((m) => m.can_do_text_to_speech)) {
    const uz = (m.languages ?? []).some((l) => l.language_id === "uz");
    console.log(`  ${uz ? "UZ " : "-- "} ${m.model_id}  (${m.name}, ${(m.languages ?? []).length} languages)`);
  }
  const voices = await getJson("/v2/voices?page_size=100");
  console.log("Voices:");
  for (const v of voices.voices) {
    console.log(`  ${v.voice_id}  ${v.name}  ${v.category}  ${JSON.stringify(v.labels ?? {})}`);
  }
  process.exit(0);
}

const [modelId, voiceA, voiceB] = process.argv.slice(2);
if (!modelId || !voiceA || !voiceB) {
  console.error("usage: node generate.mjs <model_id> <voice_id_1> <voice_id_2>");
  process.exit(1);
}
const voices = [voiceA, voiceB];
const manifest = [];
let n = 0;
for (const [lang, list] of Object.entries(sentences)) {
  for (const [i, text] of list.entries()) {
    const voice = voices[n % 2];
    n += 1;
    const id = `${lang}-${String(i + 1).padStart(2, "0")}`;
    const res = await fetch(`${api}/v1/text-to-speech/${voice}?output_format=pcm_16000`, {
      method: "POST",
      headers: { "xi-api-key": key, "content-type": "application/json" },
      body: JSON.stringify({ text, model_id: modelId }),
    });
    if (!res.ok) {
      // Stop at the first failure so a refused quota is never hammered.
      console.error(`TTS failed for ${id}: ${res.status} ${await res.text()}`);
      process.exit(2);
    }
    const pcm = Buffer.from(await res.arrayBuffer());
    const file = `${id}.wav`;
    writeFileSync(join(here, file), wavFromPcm16(pcm, 16000));
    manifest.push({ id, lang, reference: text, file, voice, model: modelId, seconds: +(pcm.length / 32000).toFixed(2) });
    console.log(`${id} ok ${manifest.at(-1).seconds}s`);
  }
}
writeFileSync(join(here, "manifest.json"), JSON.stringify(manifest, null, 2) + "\n");
console.log(`wrote ${manifest.length} clips and manifest.json`);
