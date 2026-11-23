# Tater v3.0.9

## What's Changed

- **New Speaker ID:** Trained w2v-BERT 2.0 SV replaces SpeechBrain ECAPA for voice enrollment and matching.
- **Seven emotion categories:** Emotion2vec+ Base replaces the old SpeechBrain emotion model. Other, unknown, and low-confidence results do not add emotion hints.
- **Automatic model setup:** Enabled models download on warmup or first use on Mac and Docker. Old managed SpeechBrain weights are removed after their replacements load successfully; Mac/local setup also removes the retired SpeechBrain package.
- **Popup effects:** Tilt is now the default. Existing saved effect choices are preserved.

**After updating:** Enroll speakers again and reconnect their new profiles to People. Old voice embeddings are not converted. If you use SpudLink identity routing, update both the Hub and Spudlet. The Home Assistant bridge is unchanged.

[Voice identity setup and model details](https://github.com/TaterTotterson/Tater/blob/v3.0.9/docs/voice-identity-models.md)

**Full Changelog**: [v3.0.8...v3.0.9](https://github.com/TaterTotterson/Tater/compare/v3.0.8...v3.0.9)
