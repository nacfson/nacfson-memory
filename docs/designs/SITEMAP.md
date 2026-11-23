# Design Specifications Sitemap

Index of EGM RFCs (`/egm-brainstorm`) and design specifications (`/egm-draft`) for `nacfson-memory`. See the [Repository Blueprint](../../README.md) for top-level repository boundaries.

| Link | Date | Author | Components | Description |
| :--- | :--- | :----- | :--------- | :---------- |
| [mem1 (nacfson-memory v1)](./mem1-v1-design.md) | 2026-11-23 | nacfson | PostgreSQL 17, Go (`mem1`), River, `pgvector`, MCP, HTTP Observability API | Single-tenant Go + PostgreSQL 17 cross-agent memory backend with non-blocking inbox, serialized background `Curator`, read-time `DEPENDS_ON` drift tracking, unified `'general'` (`'constitution_and_durable'` default) + coupled project workspaces, and unified HTTP/MCP API + `mem1 mcp-stdio` |
