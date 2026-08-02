function escapeHtml(value: unknown): string {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function safeHref(value: unknown): string {
  const href = String(value ?? "").trim();
  const lowered = href.toLowerCase();
  if (
    lowered.startsWith("https://") ||
    lowered.startsWith("http://") ||
    lowered.startsWith("mailto:") ||
    lowered.startsWith("tel:") ||
    href.startsWith("/") ||
    href.startsWith("#")
  ) {
    return href;
  }
  return "";
}

function inlineMarkdown(value: unknown): string {
  const placeholders: string[] = [];
  let source = String(value ?? "");
  source = source.replace(/`([^`\n]+)`/g, (_match, code) => {
    const token = `@@TC_${placeholders.length}@@`;
    placeholders.push(`<code>${escapeHtml(code)}</code>`);
    return token;
  });
  source = source.replace(/\[([^\]\n]+)\]\(([^)\s]+)\)/g, (_match, label, href) => {
    const token = `@@TC_${placeholders.length}@@`;
    const safe = safeHref(href);
    placeholders.push(
      safe
        ? `<a href="${escapeHtml(safe)}" target="_blank" rel="noopener noreferrer">${escapeHtml(label)}</a>`
        : `${escapeHtml(label)} (${escapeHtml(href)})`,
    );
    return token;
  });
  source = escapeHtml(source)
    .replace(/\*\*([^*\n][^*\n]*?)\*\*/g, "<strong>$1</strong>")
    .replace(/\*([^*\n][^*\n]*?)\*/g, "<em>$1</em>");
  placeholders.forEach((html, index) => {
    source = source.replaceAll(`@@TC_${index}@@`, html);
  });
  return source;
}

export function renderMarkdown(value: unknown): string {
  const lines = String(value ?? "").replace(/\r\n?/g, "\n").split("\n");
  const chunks: string[] = [];
  let paragraph: string[] = [];
  let listType = "";
  let listItems: string[] = [];
  let inCode = false;
  let codeLanguage = "";
  let codeLines: string[] = [];

  const flushParagraph = () => {
    if (!paragraph.length) return;
    chunks.push(`<p>${paragraph.map((line) => inlineMarkdown(line.trim())).join("<br />")}</p>`);
    paragraph = [];
  };
  const flushList = () => {
    if (listType && listItems.length) {
      chunks.push(`<${listType}>${listItems.map((item) => `<li>${inlineMarkdown(item)}</li>`).join("")}</${listType}>`);
    }
    listType = "";
    listItems = [];
  };
  const flushCode = () => {
    if (!inCode) return;
    const language = codeLanguage.replace(/[^A-Za-z0-9_+\-]/g, "");
    chunks.push(`<pre><code${language ? ` class="language-${language}"` : ""}>${escapeHtml(codeLines.join("\n"))}</code></pre>`);
    inCode = false;
    codeLanguage = "";
    codeLines = [];
  };

  lines.forEach((rawLine) => {
    const fence = rawLine.match(/^```(?:\s*([A-Za-z0-9_+\-]+))?\s*$/);
    if (fence) {
      flushParagraph();
      flushList();
      if (inCode) flushCode();
      else {
        inCode = true;
        codeLanguage = String(fence[1] || "");
      }
      return;
    }
    if (inCode) {
      codeLines.push(rawLine);
      return;
    }
    const line = rawLine.trim();
    if (!line) {
      flushParagraph();
      flushList();
      return;
    }
    const heading = line.match(/^(#{1,6})\s+(.*)$/);
    if (heading) {
      flushParagraph();
      flushList();
      const level = Math.min(6, heading[1].length);
      chunks.push(`<h${level}>${inlineMarkdown(heading[2])}</h${level}>`);
      return;
    }
    const ordered = line.match(/^\d+\.\s+(.*)$/);
    const bullet = line.match(/^[-*+]\s+(.*)$/);
    if (ordered || bullet) {
      flushParagraph();
      const nextType = ordered ? "ol" : "ul";
      if (listType && listType !== nextType) flushList();
      listType = nextType;
      listItems.push(String((ordered || bullet)?.[1] || ""));
      return;
    }
    if (line.startsWith("> ")) {
      flushParagraph();
      flushList();
      chunks.push(`<blockquote>${inlineMarkdown(line.slice(2))}</blockquote>`);
      return;
    }
    if (listType) flushList();
    paragraph.push(line);
  });
  flushParagraph();
  flushList();
  flushCode();
  return chunks.join("") || `<p>${inlineMarkdown(value)}</p>`;
}

export { escapeHtml, safeHref };
