# Changelog

## 0.2.1 · 2026-11-28

- Fix plugin load failure: a queue entry for a deleted session no longer aborts activation. Retain errors are contained per session, retried while the session catalog is still loading, and stale entries are marked failed once the catalog is ready.
- Page scripts are trusted by default again (still toggleable) and HTML opens in browse mode with the bottom panel collapsed.
- Bottom panel collapses as one unit; completed tasks are no longer listed.

## 0.2.0 · 2026-11-27

First public submission release.

- Static preview by default; explicit trust opt-in for page scripts.
- Nonce-only script policy and active URI removal in static mode.
- Restrict packaged local assets to document-local common static resource types.
- Show every unfinished/failed task and the latest 10 completed/cancelled tasks.
- Add Chinese user guide, MIT license, repository and host compatibility metadata.
- Retain serial queue, independent hover/selection highlights, region-level iframe selection, collapsible opinion panel, and trusted local password-form support.

## 0.1.4

Independent hover preview and fixed selection; iframe region targeting.

## 0.1.3

Allow trusted local password-form validation in browse mode without removing password checks.

## 0.1.2

Direct collapse/expand control in the lower opinion panel.

## 0.1.1

Selection lock and collapsible browsing layout.

## 0.1.0

Local development prototype: HTML preview, annotations and serial Agent editing tasks.
