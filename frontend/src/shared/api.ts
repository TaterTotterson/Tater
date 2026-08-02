export async function responseJson<T>(response: Response): Promise<T> {
  if (response.ok) {
    return (await response.json()) as T;
  }
  let detail = response.statusText || "Request failed";
  try {
    const body = (await response.json()) as { detail?: unknown; message?: unknown };
    if (typeof body.detail === "string") {
      detail = body.detail;
    } else if (typeof body.message === "string") {
      detail = body.message;
    } else if (body.detail && typeof body.detail === "object") {
      const nested = body.detail as { message?: string; detail?: string };
      detail = nested.message || nested.detail || detail;
    }
  } catch {
    // Keep the HTTP status text when the response is not JSON.
  }
  throw new Error(detail);
}

export async function getJson<T>(endpoint: string): Promise<T> {
  return responseJson<T>(
    await fetch(endpoint, {
      credentials: "same-origin",
      headers: { Accept: "application/json" },
    }),
  );
}

export async function postJson<T>(endpoint: string, body: unknown = {}): Promise<T> {
  return responseJson<T>(
    await fetch(endpoint, {
      method: "POST",
      credentials: "same-origin",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    }),
  );
}
