# Whisper → TFLite conversion

`convert.py` turns the four Whisper checkpoints voxAI uses into builtin-only TFLite graphs
("generate in graph": log-mel in, token ids out, language/task baked in) plus the companion
files the app's `speech/WhisperModel.kt` expects.

| id | checkpoint | language | model.tflite |
|---|---|---|---|
| `whisper_tiny_en` | `openai/whisper-tiny.en` | en | ~40.6 MB |
| `whisper_base_en` | `openai/whisper-base.en` | en | ~76.9 MB |
| `whisper_nb_tiny` | `NbAiLab/nb-whisper-tiny` | no (forced) | ~40.6 MB |
| `whisper_nb_base` | `NbAiLab/nb-whisper-base` | no (forced) | ~76.9 MB |

## Run

```bash
pip install -r requirements.txt          # TF 2.21 + tf_keras, transformers 4.46, scipy
python convert.py                        # all four → models_out/<id>/
python convert.py whisper_nb_tiny        # one model
python convert.py --validate-only        # re-run the sample transcriptions, rewrite manifest/validation
```

Takes ~1–2 min per model on a CPU. Output per model:

- `model.tflite` — dynamic-range int8 weights, input `input_features (1,80,3000) float32`, output `sequences (1,225) int32`.
- `vocab.bin` — byte-level BPE table: `uint32 count`, then per id `uint16 len` + raw bytes.
- `mel_filters.bin` — 80 × 201 float32 LE Slaney mel filter bank (`WhisperFeatureExtractor.mel_filters`).
- `manifest.json` — ids (`eot_id`, `sot_id`, `timestamp_begin_id`), front-end constants, forced decoder ids. Deterministic, md5-checked by the app.
- `validation.json` — sample transcriptions from the TFLite model next to the HF reference.

The script prints the md5 of every file: paste them into `speech/WhisperModels.kt` after a
re-conversion and upload the folders to the model host
(`https://raw.githubusercontent.com/pcmoan70/Aibirder_Models/main/whisper/<id>/`). For an
offline phone push them directly:

```bash
adb push models_out/whisper_tiny_en /storage/emulated/0/VoxAI/models/
```

## Why this format

- LiteRT has no select-TF-ops (Flex) artifact on Google Maven, so the converter is run with
  `TFLITE_BUILTINS` only; conversion fails loudly if the graph needs anything else.
- Norwegian is handled by forcing `<|no|><|transcribe|><|notimestamps|>` into the graph
  (`forced_decoder_ids`), so the app never has to feed decoder prompts.
- The first conversion also writes the Kotlin front-end fixtures
  (`flutter/android/app/src/test/resources/whisper/`): a 2 s chirp and the first 300 mel
  frames HF computes for it. `WhisperMelTest` compares the Kotlin log-mel to them.

## Samples

`samples/*.wav` (16 kHz mono) are transcribed by every run: `jfk.wav` (whisper.cpp's sample),
`king_no.wav` (first 30 s of the NB-Whisper demo speech) and `speech_phone.wav` (a clip
recorded by the app). Expected results are in each model's `validation.json`.
