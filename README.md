# AI Support DevOps Demo

DevOps/MLOps demo project: backend, retrieval pipeline and orchestrator for AI-powered support draft generation.

This repository is a sanitized demo version. It does not contain real customer data, production URLs, tokens, logs, browser sessions, database dumps or exported conversations.

## What this project demonstrates

- Backend API for AI-powered draft generation.
- Token-based communication between services.
- Retrieval pipeline for preparing a knowledge base from historical support dialogs.
- Pipeline orchestrator with staged execution, logs and resume logic.
- Safe repository practices: `.env.example`, `.gitignore`, no secrets or runtime artifacts.

## High-level architecture

Client / Browser Extension
        |
        | POST /chat
        v
Backend API
  - validates token
  - anonymizes sensitive data
  - adds retrieval context
  - forwards request to relay
        |
        v
Relay / AI Provider API
        |
        v
Draft answer

## Knowledge pipeline

TXT snapshot
    -> normalized messages
    -> QA pairs
    -> clusters
    -> solution cards
    -> rating
    -> retrieval pack

## Repository structure

backend/          backend API demo
kb_pipeline/      local knowledge base processing pipeline
kb_orchestrator/  pipeline runner and orchestrator
docs/             architecture and security notes
scripts/          helper scripts
.env.example      environment variables template
.gitignore        excludes secrets and runtime artifacts

## Security notes

Before publishing, the following files and values are excluded:

- `.env`, API keys, tokens, passwords;
- real domains, production URLs, IP addresses and login data;
- logs, browser sessions, cookies and credentials;
- database files and runtime state;
- exported conversations and personal data;
- `jsonl`, `parquet`, `csv`, `xlsx`, `duckdb` artifacts.

## Suggested improvements

- Add Dockerfile and docker-compose.
- Add unit tests for anonymization and retrieval scoring.
- Add GitHub Actions for linting and secret scanning.
- Add OpenAPI documentation.
- Replace demo tokens with a production-safe auth flow.
