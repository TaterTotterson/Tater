import type { CoreTabPayload } from "./types";

async function responseJson<T>(response: Response): Promise<T> {
  if (response.ok) {
    return (await response.json()) as T;
  }
  let detail = response.statusText || "Request failed";
  try {
    const body = (await response.json()) as { detail?: unknown };
    if (typeof body.detail === "string") {
      detail = body.detail;
    } else if (body.detail && typeof body.detail === "object") {
      const value = body.detail as { message?: string; detail?: string };
      detail = value.message || value.detail || detail;
    }
  } catch {
    // Keep the HTTP status text when the response is not JSON.
  }
  throw new Error(detail);
}

export async function fetchMusicState(endpoint: string): Promise<CoreTabPayload> {
  return responseJson<CoreTabPayload>(
    await fetch(endpoint, {
      headers: { Accept: "application/json" },
      credentials: "same-origin",
    }),
  );
}

export async function runMusicAction(
  endpoint: string,
  action: string,
  payload: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  return responseJson<Record<string, unknown>>(
    await fetch(endpoint, {
      method: "POST",
      credentials: "same-origin",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ action, payload }),
    }),
  );
}
