import { createApp, reactive } from "vue";
import MusicCoreApp from "./music/MusicCoreApp.vue";
import type { CoreTabPayload, MusicCoreController, MusicCoreMountOptions } from "./music/types";
import "./music/music-core.css";

export function mountMusicCore(
  container: HTMLElement,
  options: MusicCoreMountOptions,
): MusicCoreController {
  const state = reactive<{ payload: CoreTabPayload }>({ payload: options.initialPayload });
  const app = createApp(MusicCoreApp, { state, options });
  app.mount(container);

  return {
    update(payload: CoreTabPayload) {
      state.payload = payload;
    },
    unmount() {
      app.unmount();
    },
  };
}

export type { CoreTabPayload, MusicCoreController, MusicCoreMountOptions };
