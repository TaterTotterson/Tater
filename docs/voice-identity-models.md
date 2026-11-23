# Voice identity models

Speaker ID uses the trained w2v-BERT 2.0 speaker-verification model, and Emotion ID uses Emotion2vec+ Base. Both run on the Tater server, or on its paired Spud Hub when that route is selected. Neither backend imports SpeechBrain.

## Speaker identification

- Repository: `zl389/w2v-bert-2.0_SV`
- Revision: `4e7b92e88f69b6a0aa5a6fdd3dfdb458ed615967`
- Checkpoint: `model_lmft_0.14.pth` (about 2.35 GB)
- Output: 256-dimensional, unit-normalized speaker embeddings
- Matching: cosine similarity against the average of a speaker's enrollment samples

This is the complete trained speaker model: its w2v-BERT encoder, layer adapters, multi-layer feature aggregation, attentive statistics pooling, and embedding head. The bare `facebook/w2v-bert-2.0` encoder is not a substitute. Tater uses stock Transformers with the published unpruned inference architecture and requires every inference parameter to load. It does not execute training YAML, download model Python, or accept partial checkpoint matches. Weight downloads are revision-pinned and SHA-256 checked.

The initial matching threshold is 0.60, with a 0.05 runner-up margin. These are starting settings, not a claimed calibration for every household. Tune with recordings from the actual satellites and include un-enrolled voices when checking false matches.

The speaker inference architecture follows [Li, Cheng and Li's implementation](https://github.com/ZXHY-82/w2v-BERT-2.0_SV), *Enhancing Speaker Verification with w2v-BERT 2.0 and Knowledge Distillation Guided Structured Pruning*. Its upstream license is CC BY-NC-SA 4.0; the full notice is retained in `tater_voice/third_party/w2v_bert2_sv_LICENSE.txt`. Checkpoint and upstream license terms still apply when distributing or using these models.

## Vocal emotion

- Repository: `emotion2vec/emotion2vec_plus_base`
- Revision: `b318240bfe67db81a8c572ecb37ce9c3759b81c9`
- Runtime: pinned FunASR 1.3.1, using installed inference code
- Downloaded checkpoint: about 1.12 GB
- Input: 16 kHz mono waveform

The seven supported emotion categories are angry, disgusted, fearful, happy, neutral, sad, and surprised. The model also predicts other and unknown. Those results never become tone hints. A known category only adds a hint if it clears the configured confidence threshold (initially 0.60), and neutral hints respect the existing preference. Hints are weak cues about vocal delivery; they do not instruct Tater to announce a classification or assert how the user feels.

Emotion2vec+ Base is already fine-tuned for classification, rather than the unadapted emotion2vec representation model. Downloads include its exact config and label vocabulary. Model-specific terms are published with the [Emotion2vec+ release](https://huggingface.co/emotion2vec/emotion2vec_plus_base).

## Updating an existing installation

1. Run the normal Tater setup/update to install the updated requirements, then restart Tater.
2. Existing enable, prompt, route, and hardware preferences remain. The old SpeechBrain hardware setting is renamed to Identity Model Acceleration. Old model-specific thresholds reset to the new starting defaults.
3. Enroll speakers again and reconnect their voice profiles to People as needed. The new backend starts with `agent_lab/speaker_id/profiles-w2v-bert2.json`; it never interprets old ECAPA embeddings as w2v-BERT embeddings. Old conversation identities and last-result records are not reused as new-model results.
4. Enabled models download on warmup or first use. A successful replacement load retires only its known old managed SpeechBrain weight directory. Unrelated models and People records are not removed. The unused old enrollment file is not read or converted.
5. Update both ends of a SpudLink identity route. Speaker embedding metadata includes provider, model, revision, checkpoint and dimensions. A mismatched Hub produces an update-required error or uses an explicitly configured local fallback. It never silently mixes embedding spaces.

Initial downloads total about 3.5 GB plus small frontend files. Model caches stay under the configured Tater agent root. CUDA, ROCm, Apple MPS and CPU preferences use the existing hardware selector, with CPU fallback when an accelerated model load fails.
