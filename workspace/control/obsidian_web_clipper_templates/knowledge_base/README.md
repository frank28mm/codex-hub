# Knowledge Base | Obsidian Web Clipper Template Pack

These templates provide the web capture entry point for the `Knowledge Base` project.

## Fixed Target Path

All templates save clips into the same Obsidian Vault folder:

- Vault: `memory`
- Folder: `02_episodic/clips/Knowledge Base/inbox/`

This means:

- If you choose any template from this pack, the save path lands in that folder automatically.
- If you keep using your old default template, this pack does not control the destination.

## Template List

- `knowledge-base-general-article-clipper.json`
  - general articles
- `knowledge-base-official-docs-clipper.json`
  - official docs / guides / references
- `knowledge-base-research-web-clipper.json`
  - research pages / arXiv / OpenReview / Papers With Code
- `knowledge-base-highlight-clipper.json`
  - highlights
- `knowledge-base-youtube-video-clipper.json`
  - YouTube videos / thumbnails / transcript capture

## Import

1. Open Obsidian Web Clipper in Chrome.
2. Open the template manager.
3. Choose Import template.
4. Import the `.json` files from this directory.
5. Pick the matching template when clipping a page.

## Images And Ad Filtering

- Article templates keep body images and try to include the main image near the top.
- Common ad / sponsor / related-content containers are filtered out.
- If the page extracts more cleanly with Defuddle or Reader mode, prefer that body content.

## YouTube Template

- `Knowledge Base - YouTube Video` saves:
  - title
  - source URL
  - thumbnail
  - description
  - transcript
- To capture the transcript too, open:
  - `...more -> Show transcript`
- The note still lands in the same Knowledge Base inbox path.

## Exact Save Guarantee

The fixed destination only holds when:

- you explicitly choose one of these templates, or
- you set one of them as the default clipper template

If you just open the extension without choosing one of these templates, the save path is not guaranteed by this pack.

## Relation To Knowledge Intake Automation

These templates only collect source material into the inbox.

The follow-up automation runs through:

- `python3 ops/knowledge_intake.py run-once`

plus the daily launch agent. It will:

- scan the clip inbox
- update `SOURCE_REGISTRY.md`
- generate companion summaries
- produce candidate inputs for topic pages
